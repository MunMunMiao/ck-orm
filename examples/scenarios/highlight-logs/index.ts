// Highlight.io application logs — see ./README.md.
import { fn } from "../../ck-orm";
import { highlightLogs } from "../../schema/scenarios";

export { highlightLogs };

/** Count by severity_text — the "errors over time" dashboard widget. */
export const buildHighlightSeverityBreakdown = () => ({
  select: (db: ReturnType<typeof import("../../ck-orm").clickhouseClient>) =>
    db
      .select({
        severity: highlightLogs.severity_text,
        cnt: fn.count().as("cnt"),
      })
      .from(highlightLogs)
      .groupBy(highlightLogs.severity_text),
});
