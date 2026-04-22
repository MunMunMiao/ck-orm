import { chTable, dateTime64, float64, int16, int64, string, uint8, uint64 } from "../ck-orm";

export const shipmentOrder = chTable(
  "shipment_orders",
  {
    warehouse_id: string(),
    order_id: int64(),
    shipment_id: string(),
    user_id: string(),
    product_sku: string(),
    quantity: float64(),
    created_at: dateTime64(6),
    packed_at: dateTime64(6),
    status: int16(),
    priority: int16(),
    note: string(),
    _peerdb_synced_at: dateTime64(9),
    _peerdb_is_deleted: uint8(),
    _peerdb_version: uint64(),
  },
  (table) => ({
    engine: "ReplacingMergeTree",
    orderBy: [table.warehouse_id, table.created_at, table.order_id, table.shipment_id],
    versionColumn: table._peerdb_version,
  }),
);

export const shipmentEvent = chTable(
  "shipment_events",
  {
    warehouse_id: string(),
    event_id: int64(),
    order_id: int64(),
    shipment_id: string(),
    user_id: string(),
    product_sku: string(),
    processed_at: dateTime64(6),
    delivered_at: dateTime64(6),
    quantity: float64(),
    adjustment_score: float64(),
    status: int16(),
    note: string(),
    _peerdb_synced_at: dateTime64(9),
    _peerdb_is_deleted: uint8(),
    _peerdb_version: uint64(),
  },
  (table) => ({
    engine: "ReplacingMergeTree",
    orderBy: [table.warehouse_id, table.processed_at, table.event_id, table.shipment_id],
    versionColumn: table._peerdb_version,
  }),
);

export const fulfillmentSchema = {
  shipmentOrder,
  shipmentEvent,
};
