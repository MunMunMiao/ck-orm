import { ckTable, ckType } from "../ck-orm";

export const customerInvoice = ckTable(
  "customer_invoices",
  {
    id: ckType.int32(),
    invoiceNumber: ckType.string("invoice_number"),
    userId: ckType.string("user_id"),
    channelId: ckType.int32("channel_id"),
    status: ckType.int16(),
    subtotalAmount: ckType.decimal("subtotal_amount", { precision: 18, scale: 5 }),
    feeAmount: ckType.decimal("fee_amount", { precision: 18, scale: 5 }),
    totalAmount: ckType.decimal("total_amount", { precision: 18, scale: 5 }),
    createdAt: ckType.int32("created_at"),
    updatedAt: ckType.int32("updated_at"),
    peerdbSyncedAt: ckType.dateTime64("_peerdb_synced_at", { precision: 9 }),
    peerdbIsDeleted: ckType.uint8("_peerdb_is_deleted"),
    peerdbVersion: ckType.uint64("_peerdb_version"),
  },
  (table) => ({
    engine: "ReplacingMergeTree",
    orderBy: [table.userId, table.createdAt, table.id],
    versionColumn: table.peerdbVersion,
  }),
);

export const orderRewardLog = ckTable(
  "order_reward_log",
  {
    id: ckType.int32(),
    userId: ckType.string("user_id"),
    membershipId: ckType.string("membership_id"),
    campaignId: ckType.int32("campaign_id"),
    orderId: ckType.int64("order_id"),
    productSku: ckType.string("product_sku"),
    quantity: ckType.decimal({ precision: 20, scale: 5 }),
    rewardPoints: ckType.decimal("reward_points", { precision: 20, scale: 5 }),
    channel: ckType.int32(),
    eventType: ckType.string("event_type"),
    status: ckType.int16(),
    region: ckType.string(),
    tags: ckType.array(ckType.string()),
    attributes: ckType.map(ckType.string(), ckType.string()),
    metadata: ckType.json<{
      regulatory?: string[];
      risk?: {
        score?: number;
        level?: string;
      };
      orders?: Array<{
        ticket: string;
        login: string;
      }>;
    }>(),
    createdAt: ckType.int32("created_at"),
    eventDate: ckType.int32("event_date"),
    peerdbSyncedAt: ckType.dateTime64("_peerdb_synced_at", { precision: 9 }),
    peerdbIsDeleted: ckType.uint8("_peerdb_is_deleted"),
    peerdbVersion: ckType.uint64("_peerdb_version"),
  },
  (table) => ({
    engine: "ReplacingMergeTree",
    orderBy: [table.userId, table.createdAt, table.id],
    versionColumn: table.peerdbVersion,
  }),
);

export const commerceSchema = {
  customerInvoice,
  orderRewardLog,
};

export type InvoiceModel = typeof customerInvoice.$inferSelect;
export type InvoiceInsertModel = typeof customerInvoice.$inferInsert;
export type OrderRewardLogModel = typeof orderRewardLog.$inferSelect;
export type OrderRewardLogInsertModel = typeof orderRewardLog.$inferInsert;
