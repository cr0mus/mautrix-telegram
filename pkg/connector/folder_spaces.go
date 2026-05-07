// mautrix-telegram - A Matrix-Telegram puppeting bridge.
// Copyright (C) 2025 Sumner Evans
// Copyright (C) 2026 Tulir Asokan
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package connector

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"go.mau.fi/mautrix-telegram/pkg/gotd/tg"
)

const folderSpaceDeferredSync = 20 * time.Second
const folderTitleMaxRunes = 200

func (tc *TelegramClient) applyFolderPeersUpdate(ctx context.Context, u *tg.UpdateFolderPeers) error {
	if !tc.main.Config.FolderSpaces {
		return nil
	}
	for _, fp := range u.FolderPeers {
		portalKey := tc.makePortalKeyFromPeer(fp.Peer, 0)
		portal, err := tc.main.Bridge.GetPortalByKey(ctx, portalKey)
		if err != nil {
			return fmt.Errorf("folder peer update for %s: %w", portalKey, err)
		}
		pm := portal.Metadata.(*PortalMetadata)
		if pm.TelegramFolderID == fp.FolderID {
			continue
		}
		pm.TelegramFolderID = fp.FolderID
		if err = portal.Save(ctx); err != nil {
			return fmt.Errorf("save portal folder id for %s: %w", portalKey, err)
		}
	}
	return nil
}

func (tc *TelegramClient) scheduleDeferredFolderSpaceSync() {
	if !tc.main.Config.FolderSpaces || !tc.main.Bridge.Config.PersonalFilteringSpaces {
		return
	}
	tc.folderSpaceResyncMu.Lock()
	defer tc.folderSpaceResyncMu.Unlock()
	if tc.folderSpaceResyncTimer != nil {
		tc.folderSpaceResyncTimer.Stop()
	}
	tc.folderSpaceResyncTimer = time.AfterFunc(folderSpaceDeferredSync, func() {
		tc.folderSpaceResyncMu.Lock()
		tc.folderSpaceResyncTimer = nil
		tc.folderSpaceResyncMu.Unlock()
		ctx := tc.userLogin.Log.WithContext(tc.main.Bridge.BackgroundCtx)
		if err := tc.reconcileTelegramFolderSpaces(ctx); err != nil {
			zerolog.Ctx(ctx).Err(err).Msg("Telegram folder space reconcile failed")
		}
	})
}

func folderSpaceMapKey(filterID int) string {
	return strconv.Itoa(filterID)
}

func sanitizeFolderTitle(title string) string {
	title = strings.TrimSpace(title)
	if title == "" {
		title = "Folder"
	}
	if utf8.RuneCountInString(title) <= folderTitleMaxRunes {
		return title
	}
	runes := []rune(title)
	return string(runes[:folderTitleMaxRunes])
}

func (tc *TelegramClient) setPortalSpaceChild(ctx context.Context, spaceID id.RoomID, portalMXID id.RoomID, remove bool) error {
	via := []string{tc.main.Bridge.Matrix.ServerName()}
	if remove {
		via = nil
	}
	_, err := tc.main.Bridge.Bot.SendState(ctx, spaceID, event.StateSpaceChild, portalMXID.String(), &event.Content{
		Parsed: &event.SpaceChildEventContent{Via: via},
	}, time.Now())
	return err
}

func (tc *TelegramClient) linkChildSpaceUnderRoot(ctx context.Context, rootSpace, childSpace id.RoomID) error {
	via := []string{tc.main.Bridge.Matrix.ServerName()}
	ts := time.Now()
	_, err := tc.main.Bridge.Bot.SendState(ctx, rootSpace, event.StateSpaceChild, childSpace.String(), &event.Content{
		Parsed: &event.SpaceChildEventContent{Via: via},
	}, ts)
	if err != nil {
		return fmt.Errorf("set space child on root: %w", err)
	}
	_, err = tc.main.Bridge.Bot.SendState(ctx, childSpace, event.StateSpaceParent, rootSpace.String(), &event.Content{
		Parsed: &event.SpaceParentEventContent{
			Via:       via,
			Canonical: true,
		},
	}, ts)
	if err != nil {
		return fmt.Errorf("set space parent on folder space: %w", err)
	}
	return nil
}

func (tc *TelegramClient) ensureTGFolderSpaces(ctx context.Context, rootSpace id.RoomID) ([]int, error) {
	resp, err := tc.client.API().MessagesGetDialogFilters(ctx)
	if err != nil {
		return nil, fmt.Errorf("get dialog filters: %w", err)
	}

	if tc.metadata.TGFolderSpaces == nil {
		tc.metadata.TGFolderSpaces = make(map[string]id.RoomID)
	}

	validIDs := make([]int, 0, len(resp.Filters))
	for _, raw := range resp.Filters {
		df, ok := raw.(*tg.DialogFilter)
		if !ok {
			continue
		}
		validIDs = append(validIDs, df.ID)
		key := folderSpaceMapKey(df.ID)

		wantName := sanitizeFolderTitle(df.Title.Text)
		if existing, ok := tc.metadata.TGFolderSpaces[key]; ok && existing != "" {
			continue
		}

		childID, err := tc.createTGFolderSpaceRoom(ctx, rootSpace, df.ID, wantName)
		if err != nil {
			return nil, fmt.Errorf("create folder space for filter %d: %w", df.ID, err)
		}
		tc.metadata.TGFolderSpaces[key] = childID
		if err = tc.userLogin.Save(ctx); err != nil {
			return nil, fmt.Errorf("save user login after folder space create: %w", err)
		}
	}
	return validIDs, nil
}

func (tc *TelegramClient) createTGFolderSpaceRoom(ctx context.Context, rootSpace id.RoomID, filterID int, title string) (id.RoomID, error) {
	ul := tc.userLogin
	netName := tc.main.GetName()
	autoJoin := tc.main.Bridge.Matrix.GetCapabilities().AutoJoinInvites
	doublePuppet := ul.User.DoublePuppet(ctx)

	req := &mautrix.ReqCreateRoom{
		Visibility: "private",
		Name:       title,
		Topic:      fmt.Sprintf("Telegram folder #%d (%s)", filterID, ul.RemoteName),
		InitialState: []*event.Event{{
			Type: event.StateRoomAvatar,
			Content: event.Content{
				Parsed: &event.RoomAvatarEventContent{
					URL: netName.NetworkIcon,
				},
			},
		}},
		CreationContent: map[string]any{
			"type": event.RoomTypeSpace,
		},
		PowerLevelOverride: &event.PowerLevelsEventContent{
			Users: map[id.UserID]int{
				ul.Bridge.Bot.GetMXID(): 9001,
				ul.UserMXID:             50,
			},
		},
		Invite: []id.UserID{ul.UserMXID},
		BeeperLocalRoomID: ul.Bridge.Matrix.GenerateDeterministicRoomID(networkid.PortalKey{
			ID:       networkid.PortalID(fmt.Sprintf("__tg_folder_space__/%d", filterID)),
			Receiver: ul.ID,
		}),
	}
	if autoJoin {
		req.BeeperInitialMembers = []id.UserID{ul.UserMXID}
		req.BeeperAutoJoinInvites = true
	}

	roomID, err := ul.Bridge.Bot.CreateRoom(ctx, req)
	if err != nil {
		return "", err
	}
	if !autoJoin && doublePuppet != nil {
		err = doublePuppet.EnsureJoined(ctx, roomID)
		if err != nil {
			zerolog.Ctx(ctx).Err(err).
				Int("folder_id", filterID).
				Stringer("folder_space", roomID).
				Msg("Failed to join folder space with double puppet")
		}
	}
	if err := tc.linkChildSpaceUnderRoot(ctx, rootSpace, roomID); err != nil {
		return "", err
	}
	return roomID, nil
}

func (tc *TelegramClient) reconcileTelegramFolderSpaces(ctx context.Context) error {
	if !tc.main.Config.FolderSpaces || !tc.main.Bridge.Config.PersonalFilteringSpaces {
		return nil
	}

	rootSpace, err := tc.userLogin.GetSpaceRoom(ctx)
	if err != nil {
		return fmt.Errorf("personal filtering space: %w", err)
	}
	if rootSpace == "" {
		return nil
	}

	validIDs, err := tc.ensureTGFolderSpaces(ctx, rootSpace)
	if err != nil {
		return err
	}
	validSet := make(map[int]struct{}, len(validIDs))
	for _, idv := range validIDs {
		validSet[idv] = struct{}{}
	}

	upRows, err := tc.main.Bridge.DB.UserPortal.GetAllForLogin(ctx, tc.userLogin.UserLogin)
	if err != nil {
		return fmt.Errorf("user portals: %w", err)
	}

	for _, up := range upRows {
		portal, err := tc.main.Bridge.GetExistingPortalByKey(ctx, up.Portal)
		if err != nil {
			return err
		} else if portal == nil || portal.MXID == "" || portal.Parent != nil {
			continue
		}
		tc.reconcileOnePortalFolders(ctx, rootSpace, validSet, portal)
	}
	return nil
}

func (tc *TelegramClient) reconcileOnePortalFolders(ctx context.Context, rootSpace id.RoomID, validFolders map[int]struct{}, portal *bridgev2.Portal) {
	pm := portal.Metadata.(*PortalMetadata)
	fid := pm.TelegramFolderID

	if fid != 0 {
		if _, ok := validFolders[fid]; !ok {
			fid = 0
		}
	}

	allFolderRooms := tc.collectFolderRooms()

	if fid == 0 {
		for _, roomID := range allFolderRooms {
			_ = tc.setPortalSpaceChild(ctx, roomID, portal.MXID, true)
		}
		if err := tc.setPortalSpaceChild(ctx, rootSpace, portal.MXID, false); err != nil {
			zerolog.Ctx(ctx).Err(err).
				Object("portal_key", portal.PortalKey).
				Msg("Failed to attach portal to root Telegram space")
		}
		return
	}

	folderMXID := tc.metadata.TGFolderSpaces[folderSpaceMapKey(fid)]
	if folderMXID == "" {
		for _, roomID := range allFolderRooms {
			_ = tc.setPortalSpaceChild(ctx, roomID, portal.MXID, true)
		}
		if err := tc.setPortalSpaceChild(ctx, rootSpace, portal.MXID, false); err != nil {
			zerolog.Ctx(ctx).Err(err).
				Object("portal_key", portal.PortalKey).
				Int("telegram_folder_id", fid).
				Msg("Failed to attach portal to root (missing folder Matrix space)")
		}
		return
	}

	if err := tc.setPortalSpaceChild(ctx, rootSpace, portal.MXID, true); err != nil {
		zerolog.Ctx(ctx).Err(err).
			Object("portal_key", portal.PortalKey).
			Msg("Failed to remove portal from root Telegram space")
	}
	for _, otherFolder := range tc.metadata.TGFolderSpaces {
		if otherFolder == "" || otherFolder == folderMXID {
			continue
		}
		_ = tc.setPortalSpaceChild(ctx, otherFolder, portal.MXID, true)
	}
	if err := tc.setPortalSpaceChild(ctx, folderMXID, portal.MXID, false); err != nil {
		zerolog.Ctx(ctx).Err(err).
			Object("portal_key", portal.PortalKey).
			Int("telegram_folder_id", fid).
			Stringer("folder_space", folderMXID).
			Msg("Failed to attach portal to folder Matrix space")
	}
}

func (tc *TelegramClient) collectFolderRooms() []id.RoomID {
	out := make([]id.RoomID, 0, len(tc.metadata.TGFolderSpaces))
	for _, mx := range tc.metadata.TGFolderSpaces {
		if mx != "" {
			out = append(out, mx)
		}
	}
	return out
}
