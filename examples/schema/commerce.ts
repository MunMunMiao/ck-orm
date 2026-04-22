import { chTable, dateTime64, decimal, int16, int32, int64, string, uint8, uint64 } from "../ck-orm";

export const customerInvoice = chTable(
  "customer_invoices",
  {
    id: int32(),
    invoice_number: string(),
    user_id: string(),
    channel_id: int32(),
    status: int16(),
    subtotal_amount: decimal(18, 5),
    fee_amount: decimal(18, 5),
    total_amount: decimal(18, 5),
    created_at: int32(),
    updated_at: int32(),
    _peerdb_synced_at: dateTime64(9),
    _peerdb_is_deleted: uint8(),
    _peerdb_version: uint64(),
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
    id: int32(),
    user_id: string(),
    membership_id: string(),
    campaign_id: int32(),
    order_id: int64(),
    product_sku: string(),
    quantity: decimal(20, 5),
    reward_points: decimal(20, 5),
    channel: int32(),
    event_type: string(),
    status: int16(),
    region: string(),
    created_at: int32(),
    event_date: int32(),
    _peerdb_synced_at: dateTime64(9),
    _peerdb_is_deleted: uint8(),
    _peerdb_version: uint64(),
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
