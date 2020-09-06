
-- 将latest24state字段 拆分成多行
drop table if exists ods.tmp_ods_rcs_icrloaninfo_latest24state_split;
create table if not exists ods.tmp_ods_rcs_icrloaninfo_latest24state_split (
	reportno string comment "报告编号",
	serialno string comment "流水号",
	latest24state string comment "24个月还款状态",
	latest24state_value string comment "某个月的还款状态",
	latest24state_no bigint comment '还款月份'
	)stored as parquet;

insert overwrite table ods.`tmp_ods_rcs_icrloaninfo_latest24state_split`
select 
reportno
, serialno
, latest24state
, substr(latest24state, 1, 1) latest24state_value
, 1 latest24state_no
from ods.ods_rcs_icrloaninfo where latest24state <> "" and latest24state is not null 

union all

select 
reportno
, serialno
, latest24state
, substr(latest24state, 2, 1) latest24state_value
, 2 latest24state_no
from ods.ods_rcs_icrloaninfo where latest24state <> "" and latest24state is not null 

union all

select 
reportno
, serialno
, latest24state
, substr(latest24state, 3, 1) latest24state_value
, 3 latest24state_no
from ods.ods_rcs_icrloaninfo where latest24state <> "" and latest24state is not null 

union all

select 
reportno
, serialno
, latest24state
, substr(latest24state, 4, 1) latest24state_value
, 4 latest24state_no
from ods.ods_rcs_icrloaninfo where latest24state <> "" and latest24state is not null 

union all

select 
reportno
, serialno
, latest24state
, substr(latest24state, 5, 1) latest24state_value
, 5 latest24state_no
from ods.ods_rcs_icrloaninfo where latest24state <> "" and latest24state is not null 

union all

select 
reportno
, serialno
, latest24state
, substr(latest24state, 6, 1) latest24state_value
, 6 latest24state_no
from ods.ods_rcs_icrloaninfo where latest24state <> "" and latest24state is not null 

union all

select 
reportno
, serialno
, latest24state
, substr(latest24state, 7, 1) latest24state_value
, 7 latest24state_no
from ods.ods_rcs_icrloaninfo where latest24state <> "" and latest24state is not null 

union all

select 
reportno
, serialno
, latest24state
, substr(latest24state, 8, 1) latest24state_value
, 8 latest24state_no
from ods.ods_rcs_icrloaninfo where latest24state <> "" and latest24state is not null 

union all

select 
reportno
, serialno
, latest24state
, substr(latest24state, 9, 1) latest24state_value
, 9 latest24state_no
from ods.ods_rcs_icrloaninfo where latest24state <> "" and latest24state is not null 

union all

select 
reportno
, serialno
, latest24state
, substr(latest24state, 10, 1) latest24state_value
, 10 latest24state_no
from ods.ods_rcs_icrloaninfo where latest24state <> "" and latest24state is not null 

union all

select 
reportno
, serialno
, latest24state
, substr(latest24state, 11, 1) latest24state_value
, 11 latest24state_no
from ods.ods_rcs_icrloaninfo where latest24state <> "" and latest24state is not null 

union all

select 
reportno
, serialno
, latest24state
, substr(latest24state, 12, 1) latest24state_value
, 12 latest24state_no
from ods.ods_rcs_icrloaninfo where latest24state <> "" and latest24state is not null 

union all

select 
reportno
, serialno
, latest24state
, substr(latest24state, 13, 1) latest24state_value
, 13 latest24state_no
from ods.ods_rcs_icrloaninfo where latest24state <> "" and latest24state is not null 

union all

select 
reportno
, serialno
, latest24state
, substr(latest24state, 14, 1) latest24state_value
, 14 latest24state_no
from ods.ods_rcs_icrloaninfo where latest24state <> "" and latest24state is not null 

union all

select 
reportno
, serialno
, latest24state
, substr(latest24state, 15, 1) latest24state_value
, 15 latest24state_no
from ods.ods_rcs_icrloaninfo where latest24state <> "" and latest24state is not null 

union all

select 
reportno
, serialno
, latest24state
, substr(latest24state, 16, 1) latest24state_value
, 16 latest24state_no
from ods.ods_rcs_icrloaninfo where latest24state <> "" and latest24state is not null 

union all

select 
reportno
, serialno
, latest24state
, substr(latest24state, 17, 1) latest24state_value
, 17 latest24state_no
from ods.ods_rcs_icrloaninfo where latest24state <> "" and latest24state is not null 

union all

select 
reportno
, serialno
, latest24state
, substr(latest24state, 18, 1) latest24state_value
, 18 latest24state_no
from ods.ods_rcs_icrloaninfo where latest24state <> "" and latest24state is not null 

union all

select 
reportno
, serialno
, latest24state
, substr(latest24state, 19, 1) latest24state_value
, 19 latest24state_no
from ods.ods_rcs_icrloaninfo where latest24state <> "" and latest24state is not null 

union all

select 
reportno
, serialno
, latest24state
, substr(latest24state, 20, 1) latest24state_value
, 20 latest24state_no
from ods.ods_rcs_icrloaninfo where latest24state <> "" and latest24state is not null 

union all

select 
reportno
, serialno
, latest24state
, substr(latest24state, 21, 1) latest24state_value
, 21 latest24state_no
from ods.ods_rcs_icrloaninfo where latest24state <> "" and latest24state is not null 

union all

select 
reportno
, serialno
, latest24state
, substr(latest24state, 22, 1) latest24state_value
, 22 latest24state_no
from ods.ods_rcs_icrloaninfo where latest24state <> "" and latest24state is not null 

union all

select 
reportno
, serialno
, latest24state
, substr(latest24state, 23, 1) latest24state_value
, 23 latest24state_no
from ods.ods_rcs_icrloaninfo where latest24state <> "" and latest24state is not null 

union all

select 
reportno
, serialno
, latest24state
, substr(latest24state, 24, 1) latest24state_value
, 24 latest24state_no
from ods.ods_rcs_icrloaninfo where latest24state <> "" and latest24state is not null 
