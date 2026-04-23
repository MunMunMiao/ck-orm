import { ck, clickhouseClient } from "./ck-orm";
import { commerceSchema, orderRewardLog } from "./schema/commerce";
import { fulfillmentSchema, shipmentEvent } from "./schema/fulfillment";

const createCommerceDb = () => {
  return clickhouseClient({
    host: "http://127.0.0.1:8123",
    database: "demo_store",
    username: "default",
    password: "<password>",
    schema: commerceSchema,
  });
};

const createFulfillmentDb = () => {
  return clickhouseClient({
    host: "http://127.0.0.1:8123",
    database: "demo_fulfillment",
    username: "default",
    password: "<password>",
    schema: fulfillmentSchema,
  });
};

export const loadRewardOrdersWithShipmentSnapshot = async () => {
  const commerceDb = createCommerceDb();
  const fulfillmentDb = createFulfillmentDb();

  const rewardRows = await commerceDb
    .select({
      orderId: orderRewardLog.order_id,
      userId: orderRewardLog.user_id,
      membershipId: orderRewardLog.membership_id,
      rewardPoints: orderRewardLog.reward_points,
      productSku: orderRewardLog.product_sku,
      createdAt: orderRewardLog.created_at,
    })
    .from(orderRewardLog)
    .where(ck.eq(orderRewardLog._peerdb_is_deleted, 0))
    .orderBy(ck.desc(orderRewardLog.created_at))
    .limit(100)
    .final();

  const orderIds = [...new Set(rewardRows.map((row) => row.orderId))];
  if (orderIds.length === 0) {
    return [];
  }

  const shipmentRows = await fulfillmentDb
    .select({
      orderId: shipmentEvent.order_id,
      shipmentId: shipmentEvent.shipment_id,
      warehouseId: shipmentEvent.warehouse_id,
      productSku: shipmentEvent.product_sku,
      quantity: shipmentEvent.quantity,
      adjustmentScore: shipmentEvent.adjustment_score,
      deliveredAt: shipmentEvent.delivered_at,
      note: shipmentEvent.note,
    })
    .from(shipmentEvent)
    .where(ck.inArray(shipmentEvent.order_id, orderIds))
    .orderBy(ck.desc(shipmentEvent.processed_at))
    .limit(500)
    .final();

  const latestShipmentByOrderId = new Map<bigint, (typeof shipmentRows)[number]>();
  for (const row of shipmentRows) {
    if (!latestShipmentByOrderId.has(row.orderId)) {
      latestShipmentByOrderId.set(row.orderId, row);
    }
  }

  return rewardRows.map((row) => ({
    ...row,
    latestShipment: latestShipmentByOrderId.get(row.orderId) ?? null,
  }));
};
