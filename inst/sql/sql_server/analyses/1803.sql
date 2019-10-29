-- 1803	Number of distinct measurement occurrence concepts per person

--HINT DISTRIBUTE_ON_KEY(count_value)
with rawData(count_value) as
(
  select num_measurements as count_value
  from
	(
  	select m.person_id, COUNT_BIG(distinct m.measurement_concept_id) as num_measurements
  	from
  	@cdmDatabaseSchema.measurement m
  	group by m.person_id
	) t0
)

  select CAST(avg(1.0 * count_value) AS FLOAT) as avg_value,
    CAST(stdev(count_value) AS FLOAT) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count_big(*) as total
	into #overallstatstemp_1803
  from rawData;

with rawData(count_value) as
(
  select num_measurements as count_value
  from
	(
  	select m.person_id, COUNT_BIG(distinct m.measurement_concept_id) as num_measurements
  	from
  	@cdmDatabaseSchema.measurement m
  	group by m.person_id
	) t0
)

  select count_value, 
  	count_big(*) as total, 
	row_number() over (order by count_value) as rn
	into #statsViewtemp_1803
  FROM rawData
  group by count_value;

  CREATE INDEX IX_#statsViewtemp_1803 ON #statsViewTemp_1803(rn);


  select s.count_value, s.total, sum(p.total) as accumulated into #priorStatstemp_1803
  from #statsViewtemp_1803 s
  join #statsViewtemp_1803 p on p.rn <= s.rn
  group by s.count_value, s.total, s.rn;

select 1803 as analysis_id,
  o.total as count_value,
  o.min_value,
	o.max_value,
	o.avg_value,
	o.stdev_value,
	MIN(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
	MIN(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
	MIN(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
	MIN(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
	MIN(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
into #tempResults_1803
from #priorStatstemp_1803 p
CROSS JOIN #overallstatstemp_1803 o
GROUP BY o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;

--HINT DISTRIBUTE_ON_KEY(count_value)
select analysis_id, 
cast(null as varchar(255)) as stratum_1, cast(null as varchar(255)) as stratum_2, cast(null as varchar(255)) as stratum_3, cast(null as varchar(255)) as stratum_18, cast(null as varchar(255)) as stratum_5,
count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
into @scratchDatabaseSchema@schemaDelim@tempAchillesPrefix_dist_1803
from #tempResults_1803
;

truncate table #overallstatstemp_1803;
truncate table #statsViewtemp_1803;
truncate table #priorStatstemp_1803;
truncate table #tempResults_1803;
drop table #overallstatstemp_1803;
drop table #statsViewtemp_1803;
drop table #priorStatstemp_1803;
drop table #tempResults_1803;