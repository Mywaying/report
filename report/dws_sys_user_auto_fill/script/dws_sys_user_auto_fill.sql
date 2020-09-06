set hive.execution.engine=spark;
drop table if exists dws.dwstmp_sys_user_auto_fill;
create table if not exists dws.dwstmp_sys_user_auto_fill (
  user_id STRING comment '用户id'
  ,   user_name STRING comment '用户姓名'
  ,   user_status bigint comment '用户状态： 1 正常， 0 禁用'
  ,   job_code STRING comment '岗位编码'
  ,   job_name STRING comment '岗位'
  ,   org_type STRING comment '组织类型'
  ,   zhuguan_id STRING comment '主管id'
  ,   zhuguan_name STRING comment '主管姓名'
  , sub_department_name STRING comment '子部门id'
  , sub_department_id STRING comment '子部门名称'
  ,   department_name STRING comment '部门名称'
  , department_id STRING comment '部门id'
  , sub_company_name STRING comment '分公司id'
  , sub_company_id STRING comment '分公司名称'
  , company_name STRING comment '公司名称'
  , company_id STRING comment '公司id'
  , s_key bigint
  , etl_update_time STRING
  ) ;


with tmp_t as (
  SELECT
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
  ,case when org.GRADE_ = '5' then org2.NAME_  
     when org.GRADE_ in ('99', '4') then org.NAME_ 
  end department_name

  ,case when org.GRADE_ = '5' then org2.CODE_ 
     when org.GRADE_ in ('99', '4') then org.CODE_ 
  end department_id

  ,case when org2.GRADE_ = '4' then org3.NAME_
      when org2.GRADE_ in ('1', '2', '3') then org2.NAME_ 
  end sub_company_name

  ,case when org2.GRADE_ = '4' then org3.CODE_
      when org2.GRADE_ in ('1', '2', '3') then org2.CODE_
  end sub_company_id

  ,case when org3.GRADE_ = '3' then org4.NAME_
        when org3.GRADE_ in ('0', '1') then org2.NAME_
        when org3.GRADE_ = '2' then org3.NAME_
  end company_name

  ,case when org3.GRADE_ = '3' then org4.CODE_
        when org3.GRADE_ in ('0', '1') then org2.CODE_
        when org3.GRADE_ = '2' then org3.CODE_
  end company_id
  FROM
    ods.ods_bpms_sys_user su
  left join ods.ods_bpms_sys_org_user sou on su.id_ = sou.USER_ID_
  left join ods.ods_bpms_sys_org_rel orgrel on sou.REL_ID_ = orgrel.ID_
  left join ods.ods_bpms_sys_org_reldef def ON orgrel.rel_def_id_ = def.id_
  left join ods.ods_bpms_sys_org org ON orgrel.org_id_ = org.id_
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
    left join ods.ods_bpms_sys_user su on su.id_ = sou.USER_ID_
    where su.status_ = 1
    group by orgrel.ORG_ID_, def.name_
  ) zg on zg.ORG_ID_ = org.ID_
  where org.CODE_ is not null  and  su.id_ is not null
)

insert overwrite table dws.dwstmp_sys_user_auto_fill 
select 
distinct a.user_id,a.user_name,a.user_status,a.job_code,a.job_name,a.org_type,a.zhuguan_id ,a.zhuguan_name,a.sub_department_name,a.sub_department_id,a.department_name,a.department_id,a.sub_company_name,a.sub_company_id,a.company_name,a.company_id
,row_number() over (order by a.user_id, a.user_status, a.job_code, a.sub_department_id, a.department_id, a.sub_company_id) s_key
,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') etl_update_time
from 
(
  select * from tmp_t

  union all 

  select 
  tmp_t.user_id
  ,tmp_t.user_name
  ,tmp_t.user_status
  ,tmp_t.job_code
  ,tmp_t.job_name
  ,tmp_t.org_type
  ,tmp_t.zhuguan_id 
  ,tmp_t.zhuguan_name
  ,tmp_t.sub_department_name
  ,tmp_t.sub_department_id
  ,tmp_t.department_name
  ,tmp_t.department_id
  ,dc.company_name_3_level sub_company_name
  ,dc.company_id_3_level sub_company_id
  ,tmp_t.company_name
  ,tmp_t.company_id
  from tmp_t 
  left join dim.dim_company dc on tmp_t.company_name = dc.company_name_2_level
  left join ods.ods_bpms_sys_org so on tmp_t.sub_company_id = so.code_
  where tmp_t.company_name = tmp_t.sub_company_name and tmp_t.sub_department_name not like "%市场%" 
) as a;
drop table if exists dws.dws_sys_user_auto_fill;
ALTER TABLE dws.dwstmp_sys_user_auto_fill RENAME TO dws.dws_sys_user_auto_fill;