drop table if exists dim.tmpdim_c_work_calendar ;
CREATE TABLE if not exists dim.tmpdim_c_work_calendar(
  `id` string, 
  `day` string, 
  `is_work_day` bigint, 
  `type` string, 
  `system` string, 
  `etl_date` timestamp
);

insert overwrite table dim.tmpdim_c_work_calendar 
select 
id,
to_date(`day`) , 
cast(isworkday as bigint), 
type, 
system, 
from_unixtime(unix_timestamp(),'yyyy-MM-dd') etl_date
from ods.ods_bpms_c_work_calendar;

drop table if exists dim.dim_c_work_calendar;
ALTER TABLE dim.tmpdim_c_work_calendar RENAME TO dim.dim_c_work_calendar;