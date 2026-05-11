package connector

import (
	"context"

	"github.com/rs/zerolog"
)

// Ensure the user remains in their personal filtering space even if they left it manually.
// Without this, the space can "exist" in the DB and be used for children, but not show up in the client's UI.
func (tc *TelegramClient) ensurePersonalSpaceMembership(ctx context.Context) {
	if !tc.main.Bridge.Config.PersonalFilteringSpaces {
		return
	}
	spaceRoom, err := tc.userLogin.GetSpaceRoom(ctx)
	if err != nil || spaceRoom == "" {
		return
	}
	// Best-effort: ensure the user is joined (via bot invite+join if needed).
	if err := tc.main.Bridge.Bot.EnsureJoined(ctx, spaceRoom); err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).Stringer("space_room", spaceRoom).Msg("Failed to ensure bridge bot is joined to personal space")
	}
	if err := tc.main.Bridge.Bot.EnsureInvited(ctx, spaceRoom, tc.userLogin.UserMXID); err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).Stringer("space_room", spaceRoom).Msg("Failed to ensure user is invited to personal space")
	}
}
