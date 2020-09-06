set hive.execution.engine=spark;
-- 找出变更的公司数据，将date_to 修改
insert overwrite table dws.dws_sys_user_snap
select 
a.s_key
,a.user_id
,a.user_name
,a.user_status
,a.job_code
,a.job_name
,a.sub_department_id
,a.sub_department_name
,a.org_type
,a.zhuguan_id
,a.zhuguan_name
,a.department_id
,a.department_name
,a.sub_company_id
,a.sub_company_name
,a.company_name
,null company_type
,a.date_from
-- 修改变更数据的date_to
,case
  when to_date(a.date_to) = '2200-01-01' and b.user_id is null then date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1)
  else cast(a.date_to as string)
end as date_to
,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') etl_update_time
from dws.dws_sys_user_snap as a
left outer join dws.tmp_dws_sys_user_snap_all as b
on b.user_id = a.user_id and b.job_name = a.job_name and b.sub_company_id = a.sub_company_id and b.sub_department_id = a.sub_department_id
