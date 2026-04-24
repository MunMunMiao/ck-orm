import {
  type ClickHouseOrmInstrumentation,
  createLoggerInstrumentation,
  createTracingInstrumentation,
} from "./observability";
import { type ClickHouseOrmClient, createClickHouseOrmClient } from "./runtime/client";
import { type ClickHouseClientConfig, normalizeClientConfig, type ResolveJoinUseNulls } from "./runtime/config";
import { createSessionConcurrencyController } from "./runtime/session-concurrency";
import type { ClickHouseSettings } from "./runtime/settings";
import { createFetchClickHouseTransport } from "./runtime/transport";

export type {
  ClickHouseBaseQueryOptions,
  ClickHouseClientConfig,
  ClickHouseEndpointOptions,
  ClickHouseFetchConfigOptions,
  ClickHouseQueryOptions,
  ClickHouseStreamOptions,
  CreateTemporaryTableOptions,
  Session,
} from "./runtime/config";
export type {
  ClickHouseKnownSettingName,
  ClickHouseKnownSettings,
  ClickHouseSettings,
  ClickHouseSettingValue,
} from "./runtime/settings";

export const clickhouseClient = <TSchema, TSettings extends ClickHouseSettings | undefined = undefined>(
  config: ClickHouseClientConfig<TSchema> & {
    readonly clickhouse_settings?: TSettings;
  },
): ClickHouseOrmClient<TSchema, ResolveJoinUseNulls<TSettings, 1>> => {
  const { schema, logger, logLevel, tracing, instrumentation, ...clientOptions } = config;

  const normalizedConfig = normalizeClientConfig(clientOptions);
  const instrumentations = [
    ...(logger ? [createLoggerInstrumentation(logger, logLevel ?? "warn")] : []),
    ...(tracing === false || tracing === undefined
      ? []
      : [
          createTracingInstrumentation({
            ...tracing,
            dbName: tracing.dbName ?? normalizedConfig.database,
          }),
        ]),
    ...(instrumentation ?? []),
  ] satisfies ClickHouseOrmInstrumentation[];

  const client = createFetchClickHouseTransport(normalizedConfig);
  const sessionConcurrencyController = createSessionConcurrencyController(
    normalizedConfig.session_max_concurrent_requests,
  );
  const joinUseNulls = (config.clickhouse_settings?.join_use_nulls === 0 ? 0 : 1) as ResolveJoinUseNulls<TSettings, 1>;

  return createClickHouseOrmClient<TSchema, ResolveJoinUseNulls<TSettings, 1>>({
    schema,
    client,
    instrumentations,
    joinUseNulls,
    sessionConcurrencyController,
    defaultOptions: {
      clickhouse_settings: normalizedConfig.clickhouse_settings,
      session_id: normalizedConfig.session_id,
      http_headers: normalizedConfig.http_headers,
      role: normalizedConfig.role,
    },
  });
};
