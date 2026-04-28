import { ck, ckType, clickhouseClient, fn } from "./ck-orm";
import { commerceSchema, orderRewardLog } from "./schema/commerce";

const createCommerceDb = () => {
  return clickhouseClient({
    host: "http://127.0.0.1:8123",
    database: "demo_store",
    username: "default",
    password: "<password>",
    schema: commerceSchema,
    clickhouse_settings: {
      max_execution_time: 10,
    },
  });
};

export const buildJsonRegulatoryFilterExample = () => {
  const commerceDb = createCommerceDb();
  const regulatoryRegions = fn.jsonExtract(orderRewardLog.metadata, ckType.array(ckType.string()), "regulatory");
  const riskScore = fn.jsonExtract(orderRewardLog.metadata, ckType.nullable(ckType.float64()), "risk", "score");

  const query = commerceDb
    .select({
      userId: orderRewardLog.userId,
      orderId: orderRewardLog.orderId,
      regulatoryRegions: regulatoryRegions.as("regulatory_regions"),
      riskScore: riskScore.as("risk_score"),
    })
    .from(orderRewardLog)
    .where(ck.eq(orderRewardLog.peerdbIsDeleted, 0), ck.hasAny(regulatoryRegions, ["AU", "EU"]), ck.gte(riskScore, 80))
    .limit(100);

  return {
    query,
  };
};

export const buildArrayHelperProjectionExample = () => {
  const commerceDb = createCommerceDb();
  const regulatoryRegions = fn.jsonExtract(orderRewardLog.metadata, ckType.array(ckType.string()), "regulatory");
  const normalizedTags = fn.arrayConcat<string>(orderRewardLog.tags, fn.array("reward")).as("normalized_tags");

  const query = commerceDb
    .select({
      userId: orderRewardLog.userId,
      firstTag: fn.arrayElement<string>(orderRewardLog.tags, 1).as("first_tag"),
      maybeSecondTag: fn.arrayElementOrNull<string>(orderRewardLog.tags, 2).as("maybe_second_tag"),
      topTwoRegions: fn.arraySlice<string>(regulatoryRegions, 1, 2).as("top_two_regions"),
      flattenedRegions: fn.arrayFlatten<string>(fn.array(regulatoryRegions)).as("flattened_regions"),
      matchingRegions: fn.arrayIntersect<string>(regulatoryRegions, ["AU", "EU", "UK"]).as("matching_regions"),
      normalizedTags,
      tagPosition: fn.indexOf(orderRewardLog.tags, "vip").as("tag_position"),
      tagCount: fn.length(orderRewardLog.tags).as("tag_count"),
      hasTags: fn.notEmpty(orderRewardLog.tags).as("has_tags"),
    })
    .from(orderRewardLog)
    .where(ck.hasAll(normalizedTags, ["reward"]));

  return {
    query,
  };
};

export const buildArrayZipTupleElementScopeExample = () => {
  const commerceDb = createCommerceDb();
  const orderIds = ["900001", "900002", "900003"];
  const userIds = ["user_100", "user_200", "user_300"];

  const targetPairs = commerceDb.$with("target_pairs").as(
    commerceDb.select({
      pair: fn.arrayJoin(fn.arrayZip(orderIds, userIds)).as("pair"),
    }),
  );

  const targetOrders = commerceDb.$with("target_orders").as(
    commerceDb
      .with(targetPairs)
      .select({
        orderId: fn.tupleElement<string>(targetPairs.pair, 1).as("order_id"),
        userId: fn.tupleElement<string>(targetPairs.pair, 2).as("user_id"),
      })
      .from(targetPairs),
  );

  const scopedOrderPair = commerceDb
    .select({
      pair: fn.tuple(targetOrders.orderId, targetOrders.userId).as("pair"),
    })
    .from(targetOrders);

  const query = commerceDb
    .with(targetPairs, targetOrders)
    .select({
      orderId: orderRewardLog.orderId,
      userId: orderRewardLog.userId,
      rewardPoints: orderRewardLog.rewardPoints,
    })
    .from(orderRewardLog)
    .where(ck.inArray(fn.tuple(orderRewardLog.orderId, orderRewardLog.userId), scopedOrderPair.as("scoped_order_pair")))
    .orderBy(ck.desc(orderRewardLog.createdAt));

  return {
    query,
  };
};
