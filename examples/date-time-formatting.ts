import { ck, ckSql, fn } from "./ck-orm";
import { createProbeDb } from "./probe-client";
import { probeTelemetry } from "./schema/probe";

export const buildProbeCalendarFilterExample = () => {
  const probeDb = createProbeDb();
  const observedDay = new Date("2026-06-15T08:00:00.000Z");
  const missionStartDay = new Date("2026-06-01T00:00:00.000Z");
  const missionEndDay = new Date("2026-06-30T00:00:00.000Z");

  const createdHour = fn.formatDateTime(probeTelemetry.createdAt, "%Y-%m-%d %H:00:00", "UTC").as("created_hour");

  const query = probeDb
    .select({
      probeId: probeTelemetry.probeId,
      sampleId: probeTelemetry.sampleId,
      observedDay: probeTelemetry.observedDay,
      createdHour,
    })
    .from(probeTelemetry)
    .where(
      ck.eq(probeTelemetry.observedDay, observedDay),
      ck.between(probeTelemetry.missionDay, missionStartDay, missionEndDay),
    )
    .orderBy(ck.asc(createdHour));

  return {
    query,
  };
};

export const buildDateTimeConversionProjectionExample = () => {
  const probeDb = createProbeDb();
  const rawTimestamp = ckSql`'2026-06-15 08:30:00.125'`;

  const query = probeDb.select({
    parsedAt: fn.parseDateTime64BestEffort(rawTimestamp, 3, "UTC").as("parsed_at"),
    roundedSecond: fn.toDateTime64(rawTimestamp, 3, "UTC").as("rounded_second"),
    displayDay: fn.formatDateTime(fn.toDateTime64(rawTimestamp, 3, "UTC"), "%Y-%m-%d", "UTC").as("display_day"),
  });

  return {
    query,
  };
};

export const runProbeCalendarFilterExample = async () => {
  const { query } = buildProbeCalendarFilterExample();
  return query.execute();
};
