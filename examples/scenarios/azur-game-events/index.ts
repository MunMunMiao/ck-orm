// Azur Games mobile telemetry — see ./README.md.
import { ck, fn } from "../../ck-orm";
import { gameEvents } from "../../schema/scenarios";

export { gameEvents };

/** ARPU (average revenue per user) per A/B variant — from purchase events. */
export const buildGameArpuExample = () => ({
  select: (db: ReturnType<typeof import("../../ck-orm").clickhouseClient>) =>
    db
      .select({
        variant: gameEvents.ab_variant,
        users: fn.uniqExact(gameEvents.player_id).as("users"),
        totalRevenue: fn.sum(gameEvents.revenue_usd).as("total_revenue"),
      })
      .from(gameEvents)
      .where(ck.eq(gameEvents.event_type, "purchase"))
      .groupBy(gameEvents.ab_variant),
});

/** Wins vs. attempts per level for one game (level-difficulty heatmap). */
export const buildGameLevelPassRateExample = (gameId: string) => ({
  select: (db: ReturnType<typeof import("../../ck-orm").clickhouseClient>) =>
    db
      .select({
        level: gameEvents.level_id,
        wins: fn.countIf(ck.eq(gameEvents.is_win, true)).as("wins"),
        attempts: fn.count().as("attempts"),
      })
      .from(gameEvents)
      .where(ck.and(ck.eq(gameEvents.game_id, gameId), ck.eq(gameEvents.event_type, "level_complete")))
      .groupBy(gameEvents.level_id),
});
