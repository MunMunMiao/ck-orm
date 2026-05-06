import { createProbeDb } from "./probe-client";
import { probeTelemetry } from "./schema/probe";

export const buildNestedNullableInsertExample = () => {
  const probeDb = createProbeDb();

  const insert = probeDb.insert(probeTelemetry).values([
    {
      id: 1,
      probeId: "probe_alpha",
      missionId: 10,
      sampleId: "900001",
      signalStrength: "42.50000",
      status: 1,
      observedDay: new Date("2026-06-15T08:00:00.000Z"),
      missionDay: new Date("2026-06-15T08:00:00.000Z"),
      createdAt: new Date("2026-06-15T08:30:00.125Z"),
      deletedAt: null,
      tags: ["solar", "critical"],
      metadata: {
        alerts: ["battery"],
        risk: {
          level: "medium",
          score: 72,
        },
        samples: [{ channel: "main", id: "900001" }],
      },
      position: [121.4737, 31.2304],
      components: [
        {
          componentId: "panel_a",
          flagged: 0,
          reading: "12.30000",
        },
        {
          componentId: "panel_b",
          flagged: 1,
          reading: "30.20000",
        },
      ],
      ingestedAt: new Date("2026-06-15T08:30:01.000Z"),
      ingestVersion: "1",
    },
    {
      id: 2,
      probeId: "probe_beta",
      missionId: 10,
      sampleId: "900002",
      signalStrength: "7.12500",
      status: 0,
      observedDay: new Date("2026-06-16T08:00:00.000Z"),
      missionDay: new Date("2026-06-16T08:00:00.000Z"),
      createdAt: new Date("2026-06-16T09:00:00.000Z"),
      deletedAt: new Date("2026-06-16T10:00:00.000Z"),
      tags: [],
      metadata: {},
      position: [139.6917, 35.6895],
      components: [],
      ingestedAt: new Date("2026-06-16T09:00:01.000Z"),
      ingestVersion: "1",
    },
  ]);

  return {
    insert,
  };
};

export const runNestedNullableInsertExample = async () => {
  const { insert } = buildNestedNullableInsertExample();
  return insert.execute();
};
