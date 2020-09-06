set hive.execution.engine=spark;
drop table if exists dws.dimtmp_employe_org;
create table if not exists dws.dimtmp_employe_org (
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
  , rn bigint
  ) ;

with tmp_his_user_data as (
  select 
  distinct bao.sales_user_id, bao.sales_user_name, bao.sales_branch_id, bao.branch_id  
  from ods.ods_bpms_biz_apply_order bao
  left join dws.tmp_dim_employe_org deo 
      on bao.sales_user_id = deo.user_id 
    and bao.sales_branch_id = deo.sub_department_id 
    and bao.branch_id = deo.sub_company_id
  where deo.s_key is null 
),

-- 订单主表中的用户历史数据，无法直接关联获取。
tmp_his_data as (
  select 
  thud.sales_user_id user_id
  ,thud.sales_user_name user_name
  ,0 user_status
  ,'qdjlg' job_code
  ,'渠道经理岗' job_name
  , '' org_type
  , zg.id_ zhuguan_id
  , zg.fullname_ zhuguan_name
  , nvl(org.name_, 'unknown') sub_department_name
  , nvl(thud.sales_branch_id, 'unknown') sub_department_id
  , case when org.GRADE_ = '5' then org2.NAME_  
       when org.GRADE_ in ('99', '4') then org.NAME_ 
    end department_name
  , case when org.GRADE_ = '5' then org2.CODE_ 
       when org.GRADE_ in ('99', '4') then org.CODE_ 
    end department_id
  , org3.NAME_ sub_company_name
  , org3.CODE_ sub_company_id
  , case when org4.NAME_ = "总部" and org3.NAME_ like "%公司%" then org3.NAME_
    end company_name
  , case when org4.NAME_ = "总部" and org3.NAME_ like "%公司%" then org3.CODE_ 
    end company_id
  , from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') etl_update_time
  from tmp_his_user_data thud
  left join ods.ods_bpms_sys_org org on thud.sales_branch_id = org.code_ 
  left join ods.ods_bpms_sys_org org2 on org.PARENT_ID_ = org2.ID_
  left join ods.ods_bpms_sys_org org3 on thud.branch_id = org3.code_
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
  where  thud.sales_user_id is not null and thud.sales_user_id <> ""
  -- org.name_ is not null and
),

tmp_unknown_data as (
select 
"unknown" user_id
,"unknown" user_name
,"unknown" user_status
,"unknown" job_code
,"unknown" job_name
,"unknown" org_type
,"unknown" zhuguan_id
,"unknown" zhuguan_name
,"unknown" sub_department_name
,"unknown" sub_department_id
,"unknown" department_name
,"unknown" department_id
,"unknown" sub_company_name
,"unknown" sub_company_id
,"unknown" company_name
,"unknown" company_id
,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')  etl_update_time
,0 s_key
,1 rn
)

insert overwrite table dws.dimtmp_employe_org
select
rk.*
from 
(
  select 
  ad.*
  , ROW_NUMBER() OVER(PARTITION BY ad.user_id, ad.user_status, ad.job_code, ad.sub_department_id, ad.department_id, ad.sub_company_id ORDER BY ad.sub_department_id ) rn
  from(
    select 
    a.* 
    ,row_number() over (order by a.user_id, a.user_status, a.job_code, a.sub_department_id, a.department_id, a.sub_company_id) + ok_max s_key
    from tmp_his_data a
    cross join (select nvl(max(s_key), 0) ok_max from dws.tmp_dim_employe_org) b

    union all

    select * from dws.tmp_dim_employe_org

  ) ad
) rk
where rk.rn = 1

union all 

select * from tmp_unknown_data
;

drop table if exists dws.dim_employe_org;
ALTER TABLE dws.dimtmp_employe_org RENAME TO dws.dim_employe_org;
