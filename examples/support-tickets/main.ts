/**
 * Typed columns with runtime validation
 *
 * Ticket statuses and priorities are Zendesk's allowed values:
 * https://developer.zendesk.com/api-reference/ticketing/tickets/tickets/
 *
 * The email validator is a minimal Standard Schema v1 object written by hand:
 * https://standardschema.dev
 */
import type { ClickHouseORMClient, StandardSchemaV1 } from "ck-orm";
import { ck, ckTable, ckType, clickhouseClient, isClickHouseORMError } from "ck-orm";

export type TicketId = number & { readonly __brand: "TicketId" };

const ticketId = (value: number) => value as TicketId;

// Rejects anything that is not a string containing "@"; accepts and normalizes the rest.
const email: StandardSchemaV1<string, string> = {
  "~standard": {
    version: 1,
    vendor: "ck-orm-example",
    validate(value) {
      if (typeof value !== "string" || !value.includes("@")) {
        return {
          issues: [
            { message: `Expected an email address, got ${JSON.stringify(value)}` },
          ],
        };
      }
      return { value: value.trim().toLowerCase() };
    },
  },
};

const tickets = ckTable("tickets", {
  ticketId: ckType.uint32("ticket_id").$type<TicketId>(),
  status: ckType.enum8({ new: 1, open: 2, pending: 3, hold: 4, solved: 5, closed: 6 }),
  priority: ckType.enum8({ low: 1, normal: 2, high: 3, urgent: 4 }),
  requesterEmail: ckType.string("requester_email").$validator(email),
  account: ckType.json().$type<{ plan: "free" | "team" | "enterprise"; seats: number }>(),
});

export const run = (db: ClickHouseORMClient) =>
  db.runInSession(async (session) => {
    await session.createTemporaryTable(tickets);
    await session.insert(tickets).values([
      {
        ticketId: ticketId(101),
        status: "open",
        priority: "normal",
        requesterEmail: " Ana@Example.com ",
        account: { plan: "team", seats: 12 },
      },
      {
        ticketId: ticketId(102),
        status: "new",
        priority: "urgent",
        requesterEmail: "ben@example.com",
        account: { plan: "enterprise", seats: 400 },
      },
      {
        ticketId: ticketId(103),
        status: "pending",
        priority: "high",
        requesterEmail: "cy@example.com",
        account: { plan: "free", seats: 1 },
      },
      {
        ticketId: ticketId(104),
        status: "solved",
        priority: "urgent",
        requesterEmail: "dee@example.com",
        account: { plan: "team", seats: 5 },
      },
      {
        ticketId: ticketId(105),
        status: "open",
        priority: "urgent",
        requesterEmail: "eve@example.com",
        account: { plan: "free", seats: 2 },
      },
    ]);

    let rejected = "";
    try {
      // The validator rejects this row before any request is sent.
      await session.insert(tickets).values({
        ticketId: ticketId(106),
        status: "new",
        priority: "low",
        requesterEmail: "not-an-email",
        account: { plan: "free", seats: 1 },
      });
    } catch (error) {
      if (isClickHouseORMError(error) && error.kind === "client_validation") {
        rejected = `${error.kind}: ${error.message}`;
      } else {
        throw error;
      }
    }

    // Enum values sort by their numbers, so `urgent` (4) comes first.
    const queue = await session
      .select({
        ticketId: tickets.ticketId,
        status: tickets.status,
        priority: tickets.priority,
        requesterEmail: tickets.requesterEmail,
        plan: tickets.account.path("plan"),
      })
      .from(tickets)
      .where(ck.inArray(tickets.status, ["new", "open"]))
      .orderBy(ck.desc(tickets.priority), ck.asc(tickets.ticketId));

    return { queue, rejected };
  });

if (import.meta.main) {
  const db = clickhouseClient({
    databaseUrl: process.env.CLICKHOUSE_URL ?? "http://localhost:8123",
  });
  console.log(await run(db));
  // {
  //   queue: [
  //     { ticketId: 102, status: "new", priority: "urgent",
  //       requesterEmail: "ben@example.com", plan: "enterprise" },
  //     { ticketId: 105, status: "open", priority: "urgent",
  //       requesterEmail: "eve@example.com", plan: "free" },
  //     { ticketId: 101, status: "open", priority: "normal",
  //       requesterEmail: "ana@example.com", plan: "team" }
  //   ],
  //   rejected:
  //     'client_validation: [ck-orm] Standard Schema validation failed: ' +
  //     'Expected an email address, got "not-an-email"'
  // }
}
