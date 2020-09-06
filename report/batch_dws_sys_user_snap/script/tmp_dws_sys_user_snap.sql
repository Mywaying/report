set hive.execution.engine=spark;
drop table if exists dws.tmp_dws_sys_user_snap;
create table if not exists dws.tmp_dws_sys_user_snap (
	user_id STRING
	,   user_name STRING
	,   user_status bigint
	,   job_code STRING
	,   job_name STRING
	,   org_type STRING
	,   zhuguan_id STRING
	,   zhuguan_name STRING
	,	sub_department_name STRING
	,	sub_department_id STRING
	,   department_name STRING
	,	department_id STRING
	,	sub_company_name STRING
	,	sub_company_id STRING
	,	company_name STRING
	,	company_id STRING
	,   etl_update_time STRING) ;

insert overwrite table dws.tmp_dws_sys_user_snap
select
a.*
,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') etl_update_time
from 
(SELECT
distinct 
su.id_ user_id
,su.fullname_ user_name
,su.status_ user_status
,def.CODE_ job_code
,if(def.name_="渠道经理", '渠道经理岗', def.name_ ) job_name
,if(org.ORG_TYPE_ = 'JMS', '加盟业务', '自营业务') org_type
,zg.id_ zhuguan_id 
,zg.fullname_ zhuguan_name
,org.NAME_ sub_department_name
,org.CODE_ sub_department_id
,case when org.GRADE_ = 5 then org2.NAME_  
	 when org.GRADE_ in (99, 4) then org.NAME_ 
end department_name

,case when org.GRADE_ = 5 then org2.CODE_ 
	 when org.GRADE_ in (99, 4) then org.CODE_ 
end department_id

,case when org2.GRADE_ = 4 then org3.NAME_
	  when org2.GRADE_ in (1, 2, 3) then org2.NAME_ 
end sub_company_name

,case when org2.GRADE_ = 4 then org3.CODE_
	  when org2.GRADE_ in (1, 2, 3) then org2.CODE_
end sub_company_id

,case when org3.GRADE_ = 3 then org4.NAME_
      when org3.GRADE_ in (0, 1) then org2.NAME_
      when org3.GRADE_ = 2 then org3.NAME_
end company_name

,case when org3.GRADE_ = 3 then org4.CODE_
      when org3.GRADE_ in (0, 1) then org2.CODE_
      when org3.GRADE_ = 2 then org3.CODE_
end company_id
FROM
	ods.ods_bpms_sys_user su
left join ods.ods_bpms_sys_org_user sou on su.id_ = sou.USER_ID_
left join ods.ods_bpms_sys_org_rel orgrel on sou.REL_ID_ = orgrel.ID_
left JOIN ods.ods_bpms_sys_org_reldef def ON orgrel.rel_def_id_ = def.id_
LEFT JOIN ods.ods_bpms_sys_org org ON orgrel.org_id_ = org.id_ 
left join ods.ods_bpms_sys_org org2 on org.PARENT_ID_ = org2.ID_
left join ods.ods_bpms_sys_org org3 on org2.PARENT_ID_ = org3.ID_
left join ods.ods_bpms_sys_org org4 on org3.PARENT_ID_ = org4.ID_
left join (
	SELECT
		orgrel.ORG_ID_
	, def.name_ job_name_
	,concat_ws(',', collect_list(su.id_)) id_
	,concat_ws(',', collect_list(su.fullname_)) fullname_

	FROM
		ods.ods_bpms_sys_org_rel orgrel
	INNER JOIN ods.ods_bpms_sys_org_reldef def ON orgrel.rel_def_id_ = def.id_
	INNER JOIN ods.ods_bpms_sys_org org ON orgrel.org_id_ = org.id_ AND def.NAME_ like '%主管%'
	left join ods.ods_bpms_sys_org_user sou on sou.REL_ID_ = orgrel.ID_
	LEFT JOIN ods.ods_bpms_sys_user su on su.id_ = sou.USER_ID_
  where su.status_ = 1
  group by orgrel.ORG_ID_, def.name_
) zg on zg.ORG_ID_ = org.ID_
where def.name_ is not null and org.CODE_ is not null  and  su.id_ is not null 
and (
   (def.create_time >= date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1) and def.create_time < from_unixtime(unix_timestamp(),'yyyy-MM-dd'))
or (def.update_time >= date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1) and def.update_time < from_unixtime(unix_timestamp(),'yyyy-MM-dd'))
or (org.create_time >= date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1) and org.create_time < from_unixtime(unix_timestamp(),'yyyy-MM-dd'))
or (org.update_time >= date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1) and org.update_time < from_unixtime(unix_timestamp(),'yyyy-MM-dd'))
or (su.create_time_ >= date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1) and su.create_time_ < from_unixtime(unix_timestamp(),'yyyy-MM-dd'))
or (su.update_time >= date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1) and su.update_time < from_unixtime(unix_timestamp(),'yyyy-MM-dd'))
or (sou.create_time >= date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1) and sou.create_time < from_unixtime(unix_timestamp(),'yyyy-MM-dd'))
or (sou.update_time >= date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1) and sou.update_time < from_unixtime(unix_timestamp(),'yyyy-MM-dd'))
or (orgrel.create_time >= date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1) and orgrel.create_time < from_unixtime(unix_timestamp(),'yyyy-MM-dd'))
or (orgrel.update_time >= date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1) and orgrel.update_time < from_unixtime(unix_timestamp(),'yyyy-MM-dd'))

)
) as a
