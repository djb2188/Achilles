-- 703	Number of distinct drug exposure concepts per person

--HINT DISTRIBUTE_ON_KEY(count_value)
with rawData(count_value) as
(
  select num_drugs as count_value
	from
	(
		select de1.person_id, COUNT_BIG(distinct de1.drug_concept_id) as num_drugs
		from
		@cdmDatabaseSchema.drug_exposure de1
		group by de1.person_id
	) t0
)

  select CAST(avg(1.0 * count_value) AS FLOAT) as avg_value,
    CAST(stdev(count_value) AS FLOAT) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count_big(*) as total
	into #overallstatstemp_703
  from rawData;

  with rawData(count_value) as
(
  select num_drugs as count_value
	from
	(
		select de1.person_id, COUNT_BIG(distinct de1.drug_concept_id) as num_drugs
		from
			@cdmDatabaseSchema.drug_exposure de1
		group by de1.person_id
	) t0
)

 select count_value,
  count_big(*) as total,
  row_number() over (order by count_value) as rn into #statsviewtemp_703
  FROM rawData
  group by count_value;

 CREATE INDEX IX_#statsviewtemp ON #statsviewtemp_703(rn);

 select s.count_value, s.total, sum(p.total) as accumulated into #priorstatstemp_703
  from #statsviewtemp_703 s
  join #statsviewtemp_703 p on p.rn <= s.rn
  group by s.count_value, s.total, s.rn;


select 703 as analysis_id,
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
into #tempResults_703
from #priorstatstemp_703 p
CROSS JOIN #overallstatstemp_703 o
GROUP BY o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;

--HINT DISTRIBUTE_ON_KEY(count_value)
select analysis_id, 
cast(null as varchar(255)) as stratum_1, cast(null as varchar(255)) as stratum_2, cast(null as varchar(255)) as stratum_3, cast(null as varchar(255)) as stratum_4, cast(null as varchar(255)) as stratum_5,
count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
into @scratchDatabaseSchema@schemaDelim@tempAchillesPrefix_dist_703
from #tempResults_703
;

truncate table #overallstatstemp_703;
truncate table #statsviewtemp_703;
truncate table #priorstatstemp_703;
truncate table #tempResults_703;
drop table #overallstatstemp_703;
drop table #statsviewtemp_703;
drop table #priorstatstemp_703;
drop table #tempResults_703;
