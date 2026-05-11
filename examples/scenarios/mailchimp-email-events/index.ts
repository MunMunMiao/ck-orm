// Mailchimp-style email events — see ./README.md.
import { ck, fn } from "../../ck-orm";
import { mailchimpEmailEvents } from "../../schema/scenarios";

export { mailchimpEmailEvents };

/** Sent → delivered → opened → clicked → bounced funnel for one campaign. */
export const buildEmailFunnelExample = (campaignId: number) => ({
  select: (db: ReturnType<typeof import("../../ck-orm").clickhouseClient>) =>
    db
      .select({
        sent: fn.countIf(ck.eq(mailchimpEmailEvents.event_type, "sent")).as("sent"),
        delivered: fn.countIf(ck.eq(mailchimpEmailEvents.event_type, "delivered")).as("delivered"),
        opened: fn.countIf(ck.eq(mailchimpEmailEvents.event_type, "opened")).as("opened"),
        clicked: fn.countIf(ck.eq(mailchimpEmailEvents.event_type, "clicked")).as("clicked"),
        bounced: fn.countIf(ck.eq(mailchimpEmailEvents.event_type, "bounced")).as("bounced"),
      })
      .from(mailchimpEmailEvents)
      .where(ck.eq(mailchimpEmailEvents.campaign_id, campaignId)),
});
