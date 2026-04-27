import { ckTable, ckType } from "../ck-orm";

export const shipmentOrder = ckTable(
  "shipment_orders",
  {
    warehouse_id: ckType.string(),
    order_id: ckType.int64(),
    shipment_id: ckType.string(),
    user_id: ckType.string(),
    product_sku: ckType.string(),
    quantity: ckType.float64(),
    created_at: ckType.dateTime64({ precision: 6 }),
    packed_at: ckType.dateTime64({ precision: 6 }),
    status: ckType.int16(),
    priority: ckType.int16(),
    note: ckType.string(),
    _peerdb_synced_at: ckType.dateTime64({ precision: 9 }),
    _peerdb_is_deleted: ckType.uint8(),
    _peerdb_version: ckType.uint64(),
  },
  (table) => ({
    engine: "ReplacingMergeTree",
    orderBy: [table.warehouse_id, table.created_at, table.order_id, table.shipment_id],
    versionColumn: table._peerdb_version,
  }),
);

export const shipmentEvent = ckTable(
  "shipment_events",
  {
    warehouse_id: ckType.string(),
    event_id: ckType.int64(),
    order_id: ckType.int64(),
    shipment_id: ckType.string(),
    user_id: ckType.string(),
    product_sku: ckType.string(),
    processed_at: ckType.dateTime64({ precision: 6 }),
    delivered_at: ckType.dateTime64({ precision: 6 }),
    quantity: ckType.float64(),
    adjustment_score: ckType.float64(),
    status: ckType.int16(),
    note: ckType.string(),
    _peerdb_synced_at: ckType.dateTime64({ precision: 9 }),
    _peerdb_is_deleted: ckType.uint8(),
    _peerdb_version: ckType.uint64(),
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
