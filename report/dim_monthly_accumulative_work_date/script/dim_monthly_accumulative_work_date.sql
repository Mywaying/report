set hive.execution.engine=spark;
use ods ;
drop table if exists dim.dimtmp_monthly_accumulative_work_date;
create table if not exists dim.dimtmp_monthly_accumulative_work_date (
  yearmonth STRING COMMENT '年月',
  day TIMESTAMP COMMENT '日期',
  IsWorkDay bigint comment '是否工作日',
  CumWorkDay bigint comment '月连续累计工作日',
  CumWorkDay2 bigint comment '月间断累计工作日(非工作日不累计)',
  TolWorkDay bigint comment '月总工作日'
);

insert overwrite table dim.dimtmp_monthly_accumulative_work_date

select
aa.yearmonth --年月
,aa.day --日期
,aa.IsWorkDay --是否工作日
,aa.CumWorkDay --月连续累计工作日
,aa.CumWorkDay2 --月间断累计工作日(非工作日不累计)
,TolWorkDay --月总工作日
from 
(select 
substr(a.day,1,7) yearmonth
,cast(a.day as timestamp) as `day`
,sum(case when b.is_work_day=1  then b.is_work_day else 0 end) CumWorkDay
,sum(case when b.is_work_day=1  then a.is_work_day else 0 end) CumWorkDay2
,max(a.is_work_day)IsWorkDay
from
dim.dim_c_work_calendar a
left join 
dim.dim_c_work_calendar b
on substr(a.day,1,7)=substr(b.day,1,7) where b.day<=a.day
group by substr(a.day,1,7),a.day
order by day asc) as aa
left join 
(select 
substr(a.day,1,7) yearmonth
,sum(a.is_work_day) TolWorkDay
from
dim.dim_c_work_calendar a
where  a.is_work_day=1  
group by substr(a.day,1,7)) as bb
on aa.yearmonth=bb.yearmonth
order by day asc;

drop table if exists dim.dim_monthly_accumulative_work_date;
ALTER TABLE dim.dimtmp_monthly_accumulative_work_date RENAME TO dim.dim_monthly_accumulative_work_date;