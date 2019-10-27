-- 1003	Number of distinct condition era concepts per person

--HINT DISTRIBUTE_ON_KEY(count_value)
with rawData(count_value) as
(
  select COUNT_BIG(distinct ce1.condition_concept_id) as count_value
	from @cdmDatabaseSchema.condition_era ce1
	group by ce1.person_id
)


  select CAST(avg(1.0 * count_value) AS FLOAT) as avg_value,
    CAST(stdev(count_value) AS FLOAT) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count_big(*) as total
	into #overallStatstemp_1003
  from rawData;

with rawData(count_value) as
(
  select COUNT_BIG(distinct ce1.condition_concept_id) as count_value
	from @cdmDatabaseSchema.condition_era ce1
	group by ce1.person_id
)

  select count_value, 
  	count_big(*) as total, 
	row_number() over (order by count_value) as rn
	into #statsViewtemp_1003
  FROM rawData
  group by count_value;

  CREATE INDEX IX_#statsviewtemp_1003 ON #statsviewtemp_1003(rn);

  select s.count_value, s.total, sum(p.total) as accumulated into #priorStatstemp_1003
  from #statsViewtemp_1003 s
  join #statsViewtemp_1003 p on p.rn <= s.rn
  group by s.count_value, s.total, s.rn;

select 1003 as analysis_id,
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
into #tempResults_1003
from #priorStatstemp_1003 p
CROSS JOIN #overallStatstemp_1003 o
GROUP BY o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;

--HINT DISTRIBUTE_ON_KEY(count_value)
select analysis_id, 
cast(null as varchar(255)) as stratum_1, cast(null as varchar(255)) as stratum_2, cast(null as varchar(255)) as stratum_3, cast(null as varchar(255)) as stratum_4, cast(null as varchar(255)) as stratum_5,
count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
into @scratchDatabaseSchema@schemaDelim@tempAchillesPrefix_dist_1003
from #tempResults_1003
;
truncate table #overallStatstemp_1003;
truncate table #statsViewtemp_1003;
truncate table #priorStatstemp_1003;
truncate table #tempResults_1003;
drop table #overallStatstemp_1003;
drop table #statsViewtemp_1003;
drop table #priorStatstemp_1003;
drop table #tempResults_1003;
