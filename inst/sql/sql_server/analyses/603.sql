-- 603	Number of distinct procedure occurrence concepts per person

--HINT DISTRIBUTE_ON_KEY(count_value)
with rawData(count_value) as
(
  select COUNT_BIG(distinct po.procedure_concept_id) as count_value
	from @cdmDatabaseSchema.procedure_occurrence po
	group by po.person_id
)

  select CAST(avg(1.0 * count_value) AS FLOAT) as avg_value,
    CAST(stdev(count_value) AS FLOAT) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count_big(*) as total
	into #overallstatstemp_603
  from rawData;

  with rawData(count_value) as
(
  select COUNT_BIG(distinct po.procedure_concept_id) as count_value
	from @cdmDatabaseSchema.procedure_occurrence po
	group by po.person_id
)

  select count_value,
  count_big(*) as total,
  row_number() over (order by count_value) as rn into #statsviewtemp_603
  FROM rawData
  group by count_value;

  CREATE INDEX IX_#statsviewtemp_603 ON #statsviewtemp_603(rn);

  select s.count_value, s.total, sum(p.total) as accumulated into #priorstatstemp_603
  from #statsviewtemp_603 s
  join #statsviewtemp_603 p on p.rn <= s.rn
  group by s.count_value, s.total, s.rn;


select 603 as analysis_id,
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
into #tempResults_603
from #priorstatstemp_603 p
CROSS JOIN #overallstatstemp_603 o
GROUP BY o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value;



--HINT DISTRIBUTE_ON_KEY(count_value)
select analysis_id, 
cast(null as varchar(255)) as stratum_1, cast(null as varchar(255)) as stratum_2, cast(null as varchar(255)) as stratum_3, cast(null as varchar(255)) as stratum_4, cast(null as varchar(255)) as stratum_5,
count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
into @scratchDatabaseSchema@schemaDelim@tempAchillesPrefix_dist_603
from #tempResults_603;

truncate table #overallstatstemp_603;
truncate table #statsviewtemp_603;
truncate table #priorstatstemp_603;
truncate table #tempResults_603;
drop table #overallstatstemp_603;
drop table #statsviewtemp_603;
drop table #priorstatstemp_603;
drop table #tempResults_603;
