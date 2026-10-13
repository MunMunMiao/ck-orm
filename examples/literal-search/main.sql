CREATE TEMPORARY TABLE `probes`
(
  `probe_id` String,
  `seen_at` DateTime64(3, 'UTC')
)
ENGINE = Memory

insert into `probes` (`probe_id`, `seen_at`) format Values
('alpha%1',1769904000000), ('alphax',1769904000000), ('alpha%2',1764547200000)

select `probes`.`probe_id` as `probeId` from `probes` where (`probes`.`probe_id` like {orm_param1:String} and `probes`.`seen_at` >= {orm_param2:DateTime64(3, 'UTC')}) limit 5
FORMAT JSON

DROP TABLE IF EXISTS `probes`
