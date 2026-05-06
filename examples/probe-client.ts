import { clickhouseClient } from "./ck-orm";

export const createProbeDb = () => {
  return clickhouseClient({
    host: "http://127.0.0.1:8123",
    database: "telemetry_lab",
    username: "default",
    password: "<password>",
    clickhouse_settings: {
      max_execution_time: 10,
    },
  });
};
