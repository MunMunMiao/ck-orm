import { chTable, chType } from "../public_api";

export const shipmentOrder = chTable(
  "shipment_orders",
  {
    warehouse_id: chType.string(),
    order_id: chType.int64(),
    shipment_id: chType.string(),
    user_id: chType.string(),
    product_sku: chType.string(),
    quantity: chType.float64(),
    created_at: chType.dateTime64({ precision: 6 }),
    packed_at: chType.dateTime64({ precision: 6 }),
    status: chType.int16(),
    priority: chType.int16(),
    note: chType.string(),
    _peerdb_synced_at: chType.dateTime64({ precision: 9 }),
    _peerdb_is_deleted: chType.uint8(),
    _peerdb_version: chType.uint64(),
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
    warehouse_id: chType.string(),
    event_id: chType.int64(),
    order_id: chType.int64(),
    shipment_id: chType.string(),
    user_id: chType.string(),
    product_sku: chType.string(),
    processed_at: chType.dateTime64({ precision: 6 }),
    delivered_at: chType.dateTime64({ precision: 6 }),
    quantity: chType.float64(),
    adjustment_score: chType.float64(),
    status: chType.int16(),
    note: chType.string(),
    _peerdb_synced_at: chType.dateTime64({ precision: 9 }),
    _peerdb_is_deleted: chType.uint8(),
    _peerdb_version: chType.uint64(),
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
