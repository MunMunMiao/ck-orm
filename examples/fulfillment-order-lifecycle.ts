import { ck, clickhouseClient, fn } from "./ck-orm";
import { shipmentEvent, shipmentOrder } from "./schema/fulfillment";

const createFulfillmentDb = () => {
  return clickhouseClient({
    host: "http://127.0.0.1:8123",
    database: "demo_fulfillment",
    username: "default",
    password: "<password>",
    clickhouse_settings: {
      max_execution_time: 10,
    },
  });
};

export const createFulfillmentLifecycleQuery = () => {
  const fulfillmentDb = createFulfillmentDb();

  const latestOrders = fulfillmentDb.$with("latest_orders").as(
    fulfillmentDb
      .select({
        shipmentId: shipmentOrder.shipment_id,
        orderId: shipmentOrder.order_id,
        userId: shipmentOrder.user_id,
        productSku: shipmentOrder.product_sku,
        createdAt: shipmentOrder.created_at,
        orderNote: shipmentOrder.note,
        orderStatus: shipmentOrder.status,
      })
      .from(shipmentOrder)
      .where(ck.eq(shipmentOrder._peerdb_is_deleted, 0))
      .orderBy(ck.desc(shipmentOrder.created_at))
      .limitBy([shipmentOrder.shipment_id], 1)
      .final(),
  );

  const latestEvents = fulfillmentDb.$with("latest_events").as(
    fulfillmentDb
      .select({
        shipmentId: shipmentEvent.shipment_id,
        orderId: shipmentEvent.order_id,
        userId: shipmentEvent.user_id,
        deliveredAt: shipmentEvent.delivered_at,
        adjustmentScore: shipmentEvent.adjustment_score,
        quantity: shipmentEvent.quantity,
        eventNote: shipmentEvent.note,
        eventStatus: shipmentEvent.status,
      })
      .from(shipmentEvent)
      .where(ck.eq(shipmentEvent._peerdb_is_deleted, 0))
      .orderBy(ck.desc(shipmentEvent.processed_at))
      .limitBy([shipmentEvent.shipment_id], 1)
      .final(),
  );

  return fulfillmentDb
    .with(latestOrders, latestEvents)
    .select({
      shipmentId: latestOrders.shipmentId,
      orderId: latestOrders.orderId,
      userId: latestOrders.userId,
      productSku: latestOrders.productSku,
      createdAt: latestOrders.createdAt,
      deliveredAt: latestEvents.deliveredAt,
      quantity: latestEvents.quantity,
      adjustmentScore: latestEvents.adjustmentScore,
      effectiveNote: fn.coalesce(latestEvents.eventNote, latestOrders.orderNote).as("effective_note"),
      effectiveStatus: fn.coalesce(latestEvents.eventStatus, latestOrders.orderStatus).as("effective_status"),
    })
    .from(latestOrders)
    .leftJoin(latestEvents, ck.eq(latestOrders.shipmentId, latestEvents.shipmentId))
    .orderBy(ck.desc(latestOrders.createdAt));
};

export const loadFulfillmentLifecycleSnapshot = async () => {
  return createFulfillmentLifecycleQuery().execute({
    query_id: "fulfillment_lifecycle_snapshot",
  });
};
