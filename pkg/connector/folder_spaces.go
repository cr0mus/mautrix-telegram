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
	"slices"
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

	"go.mau.fi/mautrix-telegram/pkg/connector/ids"
	"go.mau.fi/mautrix-telegram/pkg/gotd/tg"
)

const folderSpaceDeferredSync = 20 * time.Second
const folderTitleMaxRunes = 200
const folderDialogsPageLimit = 100

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

// mergeTelegramCustomFolderOrder returns custom DialogFilter IDs: first in hint order (for IDs
// present in fromAPI), then any remaining fromAPI IDs in API list order.
func mergeTelegramCustomFolderOrder(fromAPI []int, hint []int) []int {
	seen := make(map[int]struct{}, len(fromAPI))
	for _, idv := range fromAPI {
		seen[idv] = struct{}{}
	}
	out := make([]int, 0, len(fromAPI))
	for _, idv := range hint {
		if _, ok := seen[idv]; ok {
			out = append(out, idv)
			delete(seen, idv)
		}
	}
	for _, idv := range fromAPI {
		if _, ok := seen[idv]; ok {
			out = append(out, idv)
		}
	}
	return out
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

func (tc *TelegramClient) linkChildSpaceUnderRoot(ctx context.Context, rootSpace, childSpace id.RoomID, order string) error {
	via := []string{tc.main.Bridge.Matrix.ServerName()}
	ts := time.Now()
	childEv := &event.SpaceChildEventContent{Via: via}
	if order != "" {
		childEv.Order = order
	}
	_, err := tc.main.Bridge.Bot.SendState(ctx, rootSpace, event.StateSpaceChild, childSpace.String(), &event.Content{
		Parsed: childEv,
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

	log := zerolog.Ctx(ctx)
	log.Debug().
		Int("filter_count", len(resp.Filters)).
		Msg("Got Telegram dialog filters for folder spaces")

	if tc.metadata.TGFolderSpaces == nil {
		tc.metadata.TGFolderSpaces = make(map[string]id.RoomID)
	}

	fromAPI := make([]int, 0, len(resp.Filters))
	for _, raw := range resp.Filters {
		df, ok := raw.(*tg.DialogFilter)
		if !ok {
			continue
		}
		fromAPI = append(fromAPI, df.ID)
	}
	ordered := mergeTelegramCustomFolderOrder(fromAPI, tc.metadata.TGFolderOrder)
	if len(tc.metadata.TGFolderOrder) == 0 && len(ordered) > 0 {
		tc.metadata.TGFolderOrder = slices.Clone(ordered)
		if err := tc.userLogin.Save(ctx); err != nil {
			return nil, fmt.Errorf("save initial Telegram folder order: %w", err)
		}
	}

	validIDs := make([]int, 0, len(fromAPI))
	for _, raw := range resp.Filters {
		df, ok := raw.(*tg.DialogFilter)
		if !ok {
			continue
		}
		validIDs = append(validIDs, df.ID)
		key := folderSpaceMapKey(df.ID)

		wantName := sanitizeFolderTitle(df.Title.Text)
		if existing, ok := tc.metadata.TGFolderSpaces[key]; ok && existing != "" {
			log.Debug().
				Int("folder_id", df.ID).
				Stringer("space_room_id", existing).
				Msg("Reusing existing folder space room")
			continue
		}

		childID, err := tc.createTGFolderSpaceRoom(ctx, rootSpace, df.ID, wantName)
		if err != nil {
			return nil, fmt.Errorf("create folder space for filter %d: %w", df.ID, err)
		}
		log.Info().
			Int("folder_id", df.ID).
			Str("folder_title", wantName).
			Stringer("folder_space_room_id", childID).
			Msg("Created Telegram folder Matrix space")
		tc.metadata.TGFolderSpaces[key] = childID
		if err = tc.userLogin.Save(ctx); err != nil {
			return nil, fmt.Errorf("save user login after folder space create: %w", err)
		}
	}
	if err := tc.syncTGFolderSpaceOrderUnderRoot(ctx, rootSpace, ordered); err != nil {
		return nil, err
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
	if err := tc.linkChildSpaceUnderRoot(ctx, rootSpace, roomID, ""); err != nil {
		return "", err
	}
	return roomID, nil
}

func (tc *TelegramClient) syncTGFolderSpaceOrderUnderRoot(ctx context.Context, rootSpace id.RoomID, orderedFolderIDs []int) error {
	if len(orderedFolderIDs) == 0 {
		return nil
	}
	via := []string{tc.main.Bridge.Matrix.ServerName()}
	ts := time.Now()
	for idx, fid := range orderedFolderIDs {
		child := tc.metadata.TGFolderSpaces[folderSpaceMapKey(fid)]
		if child == "" {
			continue
		}
		orderKey := fmt.Sprintf("tg-folder-%08d", idx)
		_, err := tc.main.Bridge.Bot.SendState(ctx, rootSpace, event.StateSpaceChild, child.String(), &event.Content{
			Parsed: &event.SpaceChildEventContent{
				Via:   via,
				Order: orderKey,
			},
		}, ts)
		if err != nil {
			return fmt.Errorf("set space child order for folder %d: %w", fid, err)
		}
	}
	return nil
}

func (tc *TelegramClient) reconcileTelegramFolderSpaces(ctx context.Context) error {
	if !tc.main.Config.FolderSpaces || !tc.main.Bridge.Config.PersonalFilteringSpaces {
		return nil
	}

	log := zerolog.Ctx(ctx)

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
	if err := tc.refreshPortalFolderIDsFromTelegram(ctx, validIDs); err != nil {
		zerolog.Ctx(ctx).Err(err).Msg("Failed to refresh portal folder IDs from Telegram, continuing with stored metadata")
	}
	validSet := make(map[int]struct{}, len(validIDs))
	for _, idv := range validIDs {
		validSet[idv] = struct{}{}
	}

	upRows, err := tc.main.Bridge.DB.UserPortal.GetAllForLogin(ctx, tc.userLogin.UserLogin)
	if err != nil {
		return fmt.Errorf("user portals: %w", err)
	}

	log.Info().
		Stringer("root_space", rootSpace).
		Int("folder_count", len(validIDs)).
		Int("user_portal_count", len(upRows)).
		Msg("Reconciling Telegram folder spaces for login")

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
			zerolog.Ctx(ctx).Debug().
				Object("portal_key", portal.PortalKey).
				Int("telegram_folder_id", fid).
				Msg("Portal has folder ID that is no longer valid, falling back to root space")
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
		} else {
			zerolog.Ctx(ctx).Debug().
				Object("portal_key", portal.PortalKey).
				Stringer("root_space", rootSpace).
				Msg("Attached portal to root Telegram space (no folder)")
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
		} else {
			zerolog.Ctx(ctx).Debug().
				Object("portal_key", portal.PortalKey).
				Int("telegram_folder_id", fid).
				Stringer("root_space", rootSpace).
				Msg("Attached portal to root Telegram space (folder Matrix space missing)")
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
	} else {
		zerolog.Ctx(ctx).Debug().
			Object("portal_key", portal.PortalKey).
			Int("telegram_folder_id", fid).
			Stringer("folder_space", folderMXID).
			Msg("Attached portal to Telegram folder Matrix space")
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

func (tc *TelegramClient) refreshPortalFolderIDsFromTelegram(ctx context.Context, folderIDs []int) error {
	log := zerolog.Ctx(ctx)
	if len(folderIDs) == 0 {
		return nil
	}
	assignments := make(map[networkid.PortalKey]int)

	// First, try explicit assignments from dialog filters (include_peers).
	// This works even when messages.getDialogs(folder_id=...) is unsupported for custom folders.
	if filtersResp, err := tc.client.API().MessagesGetDialogFilters(ctx); err != nil {
		log.Warn().Err(err).Msg("Failed to get dialog filters for include_peers assignments")
	} else {
		for _, raw := range filtersResp.Filters {
			df, ok := raw.(*tg.DialogFilter)
			if !ok {
				continue
			}
			for _, p := range df.IncludePeers {
				if key, ok := tc.inputPeerToPortalKey(p); ok {
					assignments[key] = df.ID
				}
			}
		}
	}
	for _, folderID := range folderIDs {
		dialogs, err := tc.fetchDialogsForFolder(ctx, folderID)
		if err != nil {
			// Telegram may reject some custom folder IDs here with FOLDER_ID_INVALID.
			// Keep explicit include_peers assignments and continue.
			if strings.Contains(err.Error(), "FOLDER_ID_INVALID") {
				log.Debug().
					Int("folder_id", folderID).
					Err(err).
					Msg("Skipping folder_id for getDialogs, using include_peers assignments")
				continue
			}
			return fmt.Errorf("fetch dialogs for folder %d: %w", folderID, err)
		}
		for _, d := range dialogs {
			dialog, ok := d.(*tg.Dialog)
			if !ok {
				continue
			}
			portalKey := tc.makePortalKeyFromPeer(dialog.GetPeer(), 0)
			assignments[portalKey] = folderID
		}
		log.Debug().
			Int("folder_id", folderID).
			Int("dialog_count", len(dialogs)).
			Msg("Fetched dialogs for Telegram folder")
	}

	upRows, err := tc.main.Bridge.DB.UserPortal.GetAllForLogin(ctx, tc.userLogin.UserLogin)
	if err != nil {
		return fmt.Errorf("list user portals for folder assignment refresh: %w", err)
	}
	updated := 0
	for _, up := range upRows {
		portal, err := tc.main.Bridge.GetExistingPortalByKey(ctx, up.Portal)
		if err != nil || portal == nil {
			continue
		}
		wantFolder := assignments[portal.PortalKey]
		pm := portal.Metadata.(*PortalMetadata)
		if pm.TelegramFolderID == wantFolder {
			continue
		}
		pm.TelegramFolderID = wantFolder
		if err = portal.Save(ctx); err != nil {
			return fmt.Errorf("save portal folder assignment for %s: %w", portal.PortalKey, err)
		}
		updated++
	}
	log.Info().
		Int("assigned_portals", len(assignments)).
		Int("updated_portals", updated).
		Msg("Refreshed portal folder assignments from Telegram")
	return nil
}

func (tc *TelegramClient) inputPeerToPortalKey(peer tg.InputPeerClass) (networkid.PortalKey, bool) {
	switch p := peer.(type) {
	case *tg.InputPeerUser:
		return tc.makePortalKeyFromID(ids.PeerTypeUser, p.UserID, 0), true
	case *tg.InputPeerChat:
		return tc.makePortalKeyFromID(ids.PeerTypeChat, p.ChatID, 0), true
	case *tg.InputPeerChannel:
		return tc.makePortalKeyFromID(ids.PeerTypeChannel, p.ChannelID, 0), true
	default:
		return networkid.PortalKey{}, false
	}
}

func (tc *TelegramClient) fetchDialogsForFolder(ctx context.Context, folderID int) ([]tg.DialogClass, error) {
	req := tg.MessagesGetDialogsRequest{
		Limit:      folderDialogsPageLimit,
		OffsetPeer: &tg.InputPeerEmpty{},
	}
	req.SetFolderID(folderID)
	dialogsClass, err := tc.client.API().MessagesGetDialogs(ctx, &req)
	if err != nil {
		return nil, err
	}
	modified, ok := dialogsClass.AsModified()
	if !ok {
		return nil, fmt.Errorf("unexpected dialogs response type for folder %d: %T", folderID, dialogsClass)
	}
	return modified.GetDialogs(), nil
}
