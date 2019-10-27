-- 903	Number of distinct drug era concepts per person

--HINT DISTRIBUTE_ON_KEY(count_value)
with rawData(count_value) as
(
  select COUNT_BIG(distinct de1.drug_concept_id) as count_value
	from @cdmDatabaseSchema.drug_era de1
	group by de1.person_id
)


  select CAST(avg(1.0 * count_value) AS FLOAT) as avg_value,
    CAST(stdev(count_value) AS FLOAT) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count_big(*) as total
	into #overallstatstemp_903
  from rawData;

with rawData(count_value) as
(
  select COUNT_BIG(distinct de1.drug_concept_id) as count_value
	from @cdmDatabaseSchema.drug_era de1
	group by de1.person_id
)

  select count_value, 
  	count_big(*) as total, 
	row_number() over (order by count_value) as rn
	into #statsviewtemp_903
  FROM rawData
  group by count_value;

  CREATE INDEX IX_#statsviewtemp_903 ON #statsviewtemp_903(rn);


  select s.count_value, s.total, sum(p.total) as accumulated into #priorstatstemp_903
  from #overallstatstemp_903 s
  join #overallstatstemp_903 p on p.rn <= s.rn
  group by s.count_value, s.total, s.rn
  ;

select 903 as analysis_id,
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
into #tempResults_903
from #priorstatstemp_903 p
CROSS JOIN #overallstatstemp_903 o
GROUP BY o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;

--HINT DISTRIBUTE_ON_KEY(count_value)
select analysis_id, 
cast(null as varchar(255)) as stratum_1, cast(null as varchar(255)) as stratum_2, cast(null as varchar(255)) as stratum_3, cast(null as varchar(255)) as stratum_9, cast(null as varchar(255)) as stratum_5,
count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
into @scratchDatabaseSchema@schemaDelim@tempAchillesPrefix_dist_903
from #tempResults_903;

truncate table #overallstatstemp_903;
truncate table #statsviewtemp_903;
truncate table #priorstatstemp_903;
truncate table #tempResults_903;
drop table #overallstatstemp_903;
drop table #statsviewtemp_903;
drop table #priorstatstemp_903;
drop table #tempResults_903;
