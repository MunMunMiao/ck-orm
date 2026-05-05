import { ck, clickhouseClient, fn } from "../index";
import { workflowEntity, workflowEvent } from "./fixtures";
import type { Equal, Expect, InferBuilderResult } from "./helpers";

const db = clickhouseClient({
  databaseUrl: "http://localhost:8123/workflow_typecheck",
});

const latestEntities = db.$with("latest_entities").as(
  db
    .select({
      workflowId: workflowEntity.workflowId,
      itemId: workflowEntity.itemId,
      userId: workflowEntity.userId,
      itemCode: workflowEntity.itemCode,
      createdAt: workflowEntity.createdAt,
      entityNote: workflowEntity.note,
      entityStatus: workflowEntity.status,
    })
    .from(workflowEntity)
    .where(ck.eq(workflowEntity._peerdb_is_deleted, 0))
    .orderBy(ck.desc(workflowEntity.createdAt))
    .limitBy([workflowEntity.workflowId], 1)
    .final(),
);

const latestStateEvents = db.$with("latest_state_events").as(
  db
    .select({
      workflowId: workflowEvent.workflowId,
      itemId: workflowEvent.itemId,
      userId: workflowEvent.userId,
      completedAt: workflowEvent.completedAt,
      correctionScore: workflowEvent.correctionScore,
      quantity: workflowEvent.quantity,
      eventNote: workflowEvent.note,
      eventStatus: workflowEvent.status,
    })
    .from(workflowEvent)
    .where(ck.eq(workflowEvent._peerdb_is_deleted, 0))
    .orderBy(ck.desc(workflowEvent.processedAt))
    .limitBy([workflowEvent.workflowId], 1)
    .final(),
);

const lifecycleQuery = db
  .with(latestEntities, latestStateEvents)
  .select({
    workflowId: latestEntities.workflowId,
    itemId: latestEntities.itemId,
    userId: latestEntities.userId,
    itemCode: latestEntities.itemCode,
    createdAt: latestEntities.createdAt,
    completedAt: latestStateEvents.completedAt,
    quantity: latestStateEvents.quantity,
    correctionScore: latestStateEvents.correctionScore,
    effectiveNote: fn
      .coalesce<string | null>(latestStateEvents.eventNote, latestEntities.entityNote)
      .as("effective_note"),
    effectiveStatus: fn
      .coalesce<number>(latestStateEvents.eventStatus, latestEntities.entityStatus)
      .as("effective_status"),
  })
  .from(latestEntities)
  .leftJoin(latestStateEvents, ck.eq(latestEntities.workflowId, latestStateEvents.workflowId))
  .orderBy(ck.desc(latestEntities.createdAt));

type _WorkflowLifecycleType = Expect<
  Equal<
    InferBuilderResult<typeof lifecycleQuery>,
    {
      workflowId: string;
      itemId: string;
      userId: string;
      itemCode: string;
      createdAt: Date;
      completedAt: Date | null;
      quantity: string | null;
      correctionScore: number | null;
      effectiveNote: string | null;
      effectiveStatus: number;
    }
  >
>;

const noNullsLifecycleQuery = db
  .withSettings({ join_use_nulls: 0 as const })
  .with(latestEntities, latestStateEvents)
  .select({
    workflowId: latestEntities.workflowId,
    completedAt: latestStateEvents.completedAt,
    quantity: latestStateEvents.quantity,
  })
  .from(latestEntities)
  .leftJoin(latestStateEvents, ck.eq(latestEntities.workflowId, latestStateEvents.workflowId));

type _WorkflowNoNullsLifecycleType = Expect<
  Equal<
    InferBuilderResult<typeof noNullsLifecycleQuery>,
    {
      workflowId: string;
      completedAt: Date | null;
      quantity: string;
    }
  >
>;
