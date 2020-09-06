set hive.execution.engine=spark;
drop table if exists dws.tmp_dim_employe_org;
create table if not exists dws.tmp_dim_employe_org (
  user_id STRING comment '用户id'
  ,   user_name STRING comment '用户姓名'
  ,   user_status bigint comment '用户状态： 1 正常， 0 禁用'
  ,   job_code STRING comment '岗位编码'
  ,   job_name STRING comment '岗位'
  ,   org_type STRING comment '组织类型'
  ,   zhuguan_id STRING comment '主管id'
  ,   zhuguan_name STRING comment '主管姓名'
  , sub_department_name STRING comment '子部门名称'
  , sub_department_id STRING comment '子部门id'
  ,   department_name STRING comment '部门名称'
  , department_id STRING comment '部门id'
  , sub_company_name STRING comment '分公司名称'
  , sub_company_id STRING comment '分公司id'
  , company_name STRING comment '公司名称'
  , company_id STRING comment '公司id'
  , etl_update_time STRING
  , s_key bigint
  ) ;


with tmp_main_data as (
  SELECT
  distinct 
  su.id_ user_id
  ,su.fullname_ user_name
  ,su.status_ user_status
  ,nvl(orgrel.rel_code_, 'unknown') job_code
  ,nvl(orgrel.rel_name_, 'unknown') job_name
  ,if(org.ORG_TYPE_ = 'JMS', '加盟业务', '自营业务') org_type
  ,zg.id_ zhuguan_id 
  ,zg.fullname_ zhuguan_name
  ,nvl(org.NAME_, 'unknown')sub_department_name
  ,nvl(org.CODE_, 'unknown') sub_department_id
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
  join ods.ods_bpms_sys_org_user sou on su.id_ = sou.USER_ID_
  left join ods.ods_bpms_sys_org_rel orgrel on sou.REL_ID_ = orgrel.ID_
  left join ods.ods_bpms_sys_org org ON sou.org_id_ = org.id_
  left join ods.ods_bpms_sys_org org2 on org.PARENT_ID_ = org2.ID_
  left join ods.ods_bpms_sys_org org3 on org2.PARENT_ID_ = org3.ID_
  left join ods.ods_bpms_sys_org org4 on org3.PARENT_ID_ = org4.ID_
  left join (
    SELECT
      orgrel.ORG_ID_
    ,concat_ws(',', collect_list(su.id_)) id_
    ,concat_ws(',', collect_list(su.fullname_)) fullname_
    FROM
      ods.ods_bpms_sys_org_rel orgrel
    INNER JOIN ods.ods_bpms_sys_org_reldef def ON orgrel.rel_def_id_ = def.id_
    INNER JOIN ods.ods_bpms_sys_org org ON orgrel.org_id_ = org.id_ AND def.NAME_ like '%主管%'
    left join ods.ods_bpms_sys_org_user sou on sou.REL_ID_ = orgrel.ID_
    left join ods.ods_bpms_sys_user su on su.id_ = sou.USER_ID_
    where su.status_ = 1
    group by orgrel.ORG_ID_
  ) zg on zg.ORG_ID_ = org.ID_
  where org.CODE_ is not null 
)

insert overwrite table dws.tmp_dim_employe_org
select 
a.*
,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') etl_update_time
,row_number() over (order by a.user_id, a.user_status, a.job_code, a.sub_department_id, a.department_id, a.sub_company_id) s_key
from tmp_main_data a
where a.department_id is not null;