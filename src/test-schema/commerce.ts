import { ckTable, ckType } from "../public_api";

export const customerInvoice = ckTable(
  "customer_invoices",
  {
    id: ckType.int32(),
    invoice_number: ckType.string(),
    user_id: ckType.string(),
    channel_id: ckType.int32(),
    status: ckType.int16(),
    subtotal_amount: ckType.decimal({ precision: 18, scale: 5 }),
    fee_amount: ckType.decimal({ precision: 18, scale: 5 }),
    total_amount: ckType.decimal({ precision: 18, scale: 5 }),
    created_at: ckType.int32(),
    updated_at: ckType.int32(),
    _peerdb_synced_at: ckType.dateTime64({ precision: 9 }),
    _peerdb_is_deleted: ckType.uint8(),
    _peerdb_version: ckType.uint64(),
  },
  (table) => ({
    engine: "ReplacingMergeTree",
    orderBy: [table.user_id, table.created_at, table.id],
    versionColumn: table._peerdb_version,
  }),
);

export const orderRewardLog = ckTable(
  "order_reward_log",
  {
    id: ckType.int32(),
    user_id: ckType.string(),
    membership_id: ckType.string(),
    campaign_id: ckType.int32(),
    order_id: ckType.int64(),
    product_sku: ckType.string(),
    quantity: ckType.decimal({ precision: 20, scale: 5 }),
    reward_points: ckType.decimal({ precision: 20, scale: 5 }),
    channel: ckType.int32(),
    event_type: ckType.string(),
    status: ckType.int16(),
    region: ckType.string(),
    created_at: ckType.int32(),
    event_date: ckType.int32(),
    _peerdb_synced_at: ckType.dateTime64({ precision: 9 }),
    _peerdb_is_deleted: ckType.uint8(),
    _peerdb_version: ckType.uint64(),
  },
  (table) => ({
    engine: "ReplacingMergeTree",
    orderBy: [table.user_id, table.created_at, table.id],
    versionColumn: table._peerdb_version,
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
