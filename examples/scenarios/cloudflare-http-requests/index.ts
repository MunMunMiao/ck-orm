// Cloudflare HTTP analytics — see ./README.md.
import { ck, fn } from "../../ck-orm";
import { cloudflareHttpRequests } from "../../schema/scenarios";

export { cloudflareHttpRequests };

/** Total requests + bytes per country (zone dashboard). */
export const buildCloudflareTrafficByCountryExample = (zoneId: number) => ({
  select: (db: ReturnType<typeof import("../../ck-orm").clickhouseClient>) =>
    db
      .select({
        country: cloudflareHttpRequests.country,
        totalRequests: fn.sum(cloudflareHttpRequests.requests).as("total_requests"),
        totalBytes: fn.sum(cloudflareHttpRequests.bytes).as("total_bytes"),
      })
      .from(cloudflareHttpRequests)
      .where(ck.eq(cloudflareHttpRequests.zone_id, zoneId))
      .groupBy(cloudflareHttpRequests.country)
      .orderBy(ck.desc(fn.sum(cloudflareHttpRequests.requests))),
});
