import { chTable, chType } from "../public_api";

export const customerInvoice = chTable(
  "customer_invoices",
  {
    id: chType.int32(),
    invoice_number: chType.string(),
    user_id: chType.string(),
    channel_id: chType.int32(),
    status: chType.int16(),
    subtotal_amount: chType.decimal(18, 5),
    fee_amount: chType.decimal(18, 5),
    total_amount: chType.decimal(18, 5),
    created_at: chType.int32(),
    updated_at: chType.int32(),
    _peerdb_synced_at: chType.dateTime64(9),
    _peerdb_is_deleted: chType.uint8(),
    _peerdb_version: chType.uint64(),
  },
  (table) => ({
    engine: "ReplacingMergeTree",
    orderBy: [table.user_id, table.created_at, table.id],
    versionColumn: table._peerdb_version,
  }),
);

export const orderRewardLog = chTable(
  "order_reward_log",
  {
    id: chType.int32(),
    user_id: chType.string(),
    membership_id: chType.string(),
    campaign_id: chType.int32(),
    order_id: chType.int64(),
    product_sku: chType.string(),
    quantity: chType.decimal(20, 5),
    reward_points: chType.decimal(20, 5),
    channel: chType.int32(),
    event_type: chType.string(),
    status: chType.int16(),
    region: chType.string(),
    created_at: chType.int32(),
    event_date: chType.int32(),
    _peerdb_synced_at: chType.dateTime64(9),
    _peerdb_is_deleted: chType.uint8(),
    _peerdb_version: chType.uint64(),
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
