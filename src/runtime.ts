import { type ClickHouseORMPlugin, createLoggerPlugin } from "./observability";
import { type ClickHouseORMClient, createClickHouseORMClient } from "./runtime/client";
import { type ClickHouseClientConfig, normalizeClientConfig, type ResolveJoinUseNulls } from "./runtime/config";
import { createSessionConcurrencyController } from "./runtime/session-concurrency";
import type { ClickHouseSettings } from "./runtime/settings";
import { createFetchClickHouseTransport } from "./runtime/transport";

export type { ClickHouseORMClient } from "./runtime/client";
export type {
  ClickHouseBaseQueryOptions,
  ClickHouseClientConfig,
  ClickHouseEndpointOptions,
  ClickHouseFetchConfigOptions,
  ClickHouseQueryOptions,
  ClickHouseStreamOptions,
  CreateTableOptions,
  CreateTemporaryTableOptions,
  DropTableOptions,
  Session,
} from "./runtime/config";
export type {
  ClickHouseKnownSettingName,
  ClickHouseKnownSettings,
  ClickHouseSettings,
  ClickHouseSettingValue,
} from "./runtime/settings";

export const clickhouseClient = <const TSettings extends ClickHouseSettings | undefined = undefined>(
  config: ClickHouseClientConfig & {
    readonly clickhouse_settings?: TSettings;
  },
): ClickHouseORMClient<ResolveJoinUseNulls<TSettings, 1>> => {
  const { logger, logLevel, plugins, ...clientOptions } = config;

  const normalizedConfig = normalizeClientConfig(clientOptions);
  const allPlugins = [
    ...(logger ? [createLoggerPlugin(logger, logLevel ?? "warn")] : []),
    ...(plugins ?? []),
  ] satisfies ClickHouseORMPlugin[];

  const client = createFetchClickHouseTransport(normalizedConfig);
  const sessionConcurrencyController = createSessionConcurrencyController(
    normalizedConfig.session_max_concurrent_requests,
  );
  const joinUseNulls = (config.clickhouse_settings?.join_use_nulls === 0 ? 0 : 1) as ResolveJoinUseNulls<TSettings, 1>;

  return createClickHouseORMClient<ResolveJoinUseNulls<TSettings, 1>>({
    client,
    plugins: allPlugins,
    databaseName: normalizedConfig.database,
    serverAddress: normalizedConfig.url.hostname,
    serverPort: resolveServerPort(normalizedConfig.url),
    requestTimeoutMs: normalizedConfig.request_timeout,
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

const resolveServerPort = (url: URL): number | undefined => {
  if (url.port) {
    return Number(url.port);
  }
  const defaultPorts: Record<string, number | undefined> = {
    "http:": 80,
    "https:": 443,
  };
  return defaultPorts[url.protocol];
};
