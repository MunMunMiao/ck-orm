CREATE TEMPORARY TABLE `probes`
(
  `probe_id` String
)
ENGINE = Memory

CREATE TEMPORARY TABLE `missions`
(
  `probe_id` String,
  `name` String
)
ENGINE = Memory

insert into `probes` (`probe_id`) format Values
('alpha'), ('beta')

insert into `missions` (`probe_id`, `name`) format Values
('alpha','Voyager')

select `probes`.`probe_id` as `probeId`, `missions`.`name` as `mission` from `probes` left join `missions` on `probes`.`probe_id` = `missions`.`probe_id` order by `probes`.`probe_id` ASC limit 10
FORMAT JSON

DROP TABLE IF EXISTS `missions`

DROP TABLE IF EXISTS `probes`
