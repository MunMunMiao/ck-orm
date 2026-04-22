import {
  type ClickHouseOrmInstrumentation,
  createLoggerInstrumentation,
  createTracingInstrumentation,
} from "./observability";
import { ClickHouseOrmClient, createOrmRunner } from "./runtime/client";
import { type ClickHouseClientConfig, normalizeClientConfig, type ResolveJoinUseNulls } from "./runtime/config";
import { FetchClickHouseTransport } from "./runtime/transport";

export type {
  ClickHouseBaseQueryOptions,
  ClickHouseClientConfig,
  ClickHouseEndpointOptions,
  ClickHouseFetchConfigOptions,
  ClickHouseQueryOptions,
  ClickHouseStreamOptions,
  SessionApi,
} from "./runtime/config";

export const clickhouseClient = <
  TSchema,
  TSettings extends Record<string, string | number | boolean> | undefined = undefined,
>(
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

  const client = new FetchClickHouseTransport(normalizedConfig);
  const joinUseNulls = (config.clickhouse_settings?.join_use_nulls === 0 ? 0 : 1) as ResolveJoinUseNulls<TSettings, 1>;

  let ormClient: ClickHouseOrmClient<TSchema, ResolveJoinUseNulls<TSettings, 1>>;
  const runner = createOrmRunner(() => ormClient);

  ormClient = new ClickHouseOrmClient<TSchema, ResolveJoinUseNulls<TSettings, 1>>({
    schema,
    client,
    instrumentations,
    runner,
    joinUseNulls,
    defaultOptions: {
      clickhouse_settings: normalizedConfig.clickhouse_settings,
      session_id: normalizedConfig.session_id,
      http_headers: normalizedConfig.http_headers,
      role: normalizedConfig.role,
    },
  });

  return ormClient;
};
