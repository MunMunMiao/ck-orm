CREATE TEMPORARY TABLE `probes`
(
  `probe_id` String,
  `signal` Decimal(20, 5),
  `status` Int16
)
ENGINE = Memory

insert into `probes` (`probe_id`, `signal`, `status`) format Values
('alpha',1.10001,1), ('alpha',2.20002,1), ('beta',9.99999,0)

select `probes`.`probe_id` as `probeId`, CAST(sum(`probes`.`signal`) AS Decimal(38, 5)) as `total` from `probes` where `probes`.`status` = {orm_param1:Int16} group by `probes`.`probe_id` order by CAST(sum(`probes`.`signal`) AS Decimal(38, 5)) DESC limit 20
FORMAT JSON

DROP TABLE IF EXISTS `probes`
