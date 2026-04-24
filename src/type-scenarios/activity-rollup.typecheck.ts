import { ck, clickhouseClient, fn, type Selection } from "../index";
import { activityLedger, typeScenarioSchema } from "./fixtures";
import type { Equal, Expect, InferBuilderResult } from "./helpers";

const db = clickhouseClient({
  databaseUrl: "http://localhost:8123/activity_rollup_typecheck",
  schema: typeScenarioSchema,
});

const targetBatches = db.$with("target_batches").as(
  db.select({
    target: fn
      .arrayJoin<readonly [string, "alpha" | "beta", number, number]>(
        fn.arrayZip(["system_a"], ["beta" as const], [9001], [10001]),
      )
      .as("target"),
  }),
);

const targetKeys = db.$with("target_keys").as(
  db
    .select({
      system_id: fn.tupleElement<string>(targetBatches.target, 1),
      source_kind: fn.tupleElement<"alpha" | "beta">(targetBatches.target, 2),
      batch_id: fn.tupleElement<number>(targetBatches.target, 3),
      actor_id: fn.tupleElement<number>(targetBatches.target, 4),
    })
    .from(targetBatches),
);

const scopedDedupEvents = db.$with("scoped_dedup_events").as(
  db
    .select({
      system_id: activityLedger.system_id,
      source_kind: activityLedger.source_kind,
      event_id: activityLedger.event_id,
      batch_id: activityLedger.batch_id,
      entity_id: activityLedger.entity_id,
      actor_id: activityLedger.actor_id,
      event_time: activityLedger.event_time,
      event_phase: activityLedger.event_phase,
      action_kind: activityLedger.action_kind,
      observed_value: activityLedger.observed_value,
      delta_value: activityLedger.delta_value,
      _peerdb_is_deleted: activityLedger._peerdb_is_deleted,
      _peerdb_version: activityLedger._peerdb_version,
    })
    .from(activityLedger)
    .where(
      ck.inArray(
        fn.tuple(
          activityLedger.system_id,
          activityLedger.source_kind,
          activityLedger.batch_id,
          activityLedger.actor_id,
        ),
        targetKeys,
      ),
    )
    .orderBy(
      ck.desc(activityLedger._peerdb_version),
      ck.desc(activityLedger.event_time),
      ck.desc(activityLedger.event_id),
    )
    .limitBy([activityLedger.system_id, activityLedger.source_kind, activityLedger.event_id], 1),
);

const endDeltaSummary = db.$with("end_delta_summary").as(
  db
    .select({
      system_id: scopedDedupEvents.system_id,
      source_kind: scopedDedupEvents.source_kind,
      actor_id: scopedDedupEvents.actor_id,
      batch_id: scopedDedupEvents.batch_id,
      total_delta: fn.sum(scopedDedupEvents.delta_value).as("total_delta"),
    })
    .from(scopedDedupEvents)
    .where(
      ck.eq(scopedDedupEvents._peerdb_is_deleted, 0),
      ck.inArray(scopedDedupEvents.event_phase, [1, 2, 3]),
      ck.inArray(scopedDedupEvents.action_kind, [0, 1]),
    )
    .groupBy(
      scopedDedupEvents.system_id,
      scopedDedupEvents.source_kind,
      scopedDedupEvents.actor_id,
      scopedDedupEvents.batch_id,
    ),
);

const latestEndEvents = db.$with("latest_end_events").as(
  db
    .select({
      system_id: scopedDedupEvents.system_id,
      source_kind: scopedDedupEvents.source_kind,
      actor_id: scopedDedupEvents.actor_id,
      batch_id: scopedDedupEvents.batch_id,
      entity_id: scopedDedupEvents.entity_id,
      end_value: scopedDedupEvents.observed_value,
      end_event_time: scopedDedupEvents.event_time,
      event_id: scopedDedupEvents.event_id,
    })
    .from(scopedDedupEvents)
    .where(
      ck.eq(scopedDedupEvents._peerdb_is_deleted, 0),
      ck.inArray(scopedDedupEvents.event_phase, [1, 2, 3]),
      ck.inArray(scopedDedupEvents.action_kind, [0, 1]),
    )
    .orderBy(ck.desc(scopedDedupEvents.event_time), ck.desc(scopedDedupEvents.event_id))
    .limitBy(
      [
        scopedDedupEvents.system_id,
        scopedDedupEvents.source_kind,
        scopedDedupEvents.actor_id,
        scopedDedupEvents.batch_id,
      ],
      1,
    ),
);

const startEntityKeys = db
  .select({
    system_id: latestEndEvents.system_id,
    source_kind: latestEndEvents.source_kind,
    actor_id: latestEndEvents.actor_id,
    entity_id: latestEndEvents.entity_id,
  })
  .from(latestEndEvents)
  .where(ck.gt(latestEndEvents.entity_id, 0))
  .as("start_entity_keys");

const startEvents = db.$with("start_events").as(
  db
    .select({
      system_id: scopedDedupEvents.system_id,
      source_kind: scopedDedupEvents.source_kind,
      actor_id: scopedDedupEvents.actor_id,
      entity_id: scopedDedupEvents.entity_id,
      start_value: scopedDedupEvents.observed_value,
      start_event_time: scopedDedupEvents.event_time,
      event_id: scopedDedupEvents.event_id,
    })
    .from(scopedDedupEvents)
    .where(
      ck.eq(scopedDedupEvents._peerdb_is_deleted, 0),
      ck.eq(scopedDedupEvents.event_phase, 0),
      ck.inArray(scopedDedupEvents.action_kind, [0, 1]),
      ck.inArray(
        fn.tuple(
          scopedDedupEvents.system_id,
          scopedDedupEvents.source_kind,
          scopedDedupEvents.actor_id,
          scopedDedupEvents.entity_id,
        ),
        startEntityKeys,
      ),
    )
    .orderBy(ck.asc(scopedDedupEvents.event_time), ck.asc(scopedDedupEvents.event_id))
    .limitBy(
      [
        scopedDedupEvents.system_id,
        scopedDedupEvents.source_kind,
        scopedDedupEvents.actor_id,
        scopedDedupEvents.entity_id,
      ],
      1,
    ),
);

const activityLedgerRollup = db
  .with(targetBatches, targetKeys, scopedDedupEvents, endDeltaSummary, latestEndEvents, startEvents)
  .select({
    systemId: latestEndEvents.system_id.as("systemId"),
    sourceKind: latestEndEvents.source_kind.as("sourceKind"),
    actorId: latestEndEvents.actor_id.as("actorId"),
    batchId: latestEndEvents.batch_id.as("batchId"),
    startValue: fn.coalesce<string | null>(startEvents.start_value, null).as("startValue"),
    startTime: fn.toString(startEvents.start_event_time).as("startTime"),
    endValue: latestEndEvents.end_value.as("endValue"),
    endTime: fn.toString(latestEndEvents.end_event_time).as("endTime"),
    deltaValue: fn.coalesce<number | string>(endDeltaSummary.total_delta, 0).as("deltaValue"),
  })
  .from(latestEndEvents)
  .leftJoin(
    startEvents,
    ck.and(
      ck.eq(latestEndEvents.system_id, startEvents.system_id),
      ck.eq(latestEndEvents.source_kind, startEvents.source_kind),
      ck.eq(latestEndEvents.actor_id, startEvents.actor_id),
      ck.eq(latestEndEvents.entity_id, startEvents.entity_id),
    ),
  )
  .leftJoin(
    endDeltaSummary,
    ck.and(
      ck.eq(latestEndEvents.system_id, endDeltaSummary.system_id),
      ck.eq(latestEndEvents.source_kind, endDeltaSummary.source_kind),
      ck.eq(latestEndEvents.actor_id, endDeltaSummary.actor_id),
      ck.eq(latestEndEvents.batch_id, endDeltaSummary.batch_id),
    ),
  );

type _ActivityLedgerRollupType = Expect<
  Equal<
    InferBuilderResult<typeof activityLedgerRollup>,
    {
      systemId: string;
      sourceKind: "alpha" | "beta";
      actorId: number;
      batchId: number;
      startValue: string | null;
      startTime: string;
      endValue: string;
      endTime: string;
      deltaValue: number | string;
    }
  >
>;

type _CteAndSubqueryReferenceTypes = Expect<
  Equal<
    [
      typeof targetKeys.actor_id,
      typeof scopedDedupEvents.system_id,
      typeof latestEndEvents.actor_id,
      typeof latestEndEvents.batch_id,
      typeof startEntityKeys.entity_id,
    ],
    [
      Selection<number, "target_keys">,
      Selection<string, "scoped_dedup_events">,
      Selection<number, "latest_end_events">,
      Selection<number, "latest_end_events">,
      Selection<number, "start_entity_keys">,
    ]
  >
>;
