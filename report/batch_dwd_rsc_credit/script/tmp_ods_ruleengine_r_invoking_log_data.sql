set hive.execution.engine=spark;
drop table if exists ods.tmp_ods_ruleengine_r_invoking_log_data;
create table if not exists ods.tmp_ods_ruleengine_r_invoking_log_data (
	apply_no string comment "订单编号",
	idNo string comment "id编号",
	idType string comment "id类型",
	microLendTimes double comment "",
	microQueryTimes double comment "小贷查询次数",
	mobileNo double comment "手机号码",
	name string comment "客户名称",
	microFinance string comment "",
	request_time timestamp comment "数据查询时间",
	rn bigint 
	);

with 
tmp_microFinance_clean as (
    select 
    b.apply_no, t3.idNo, t3.idType, t3.microLendTimes, t3.microQueryTimes, t3.mobileNo, t3.name, b.microFinance, b.request_time
    from(
        select 
        exp.splitted microFinance
        ,a.apply_no
        ,a.request_time
        from(
            select  
            get_json_object(a.access_param, '$.microFinanceList') as ttt
            ,get_json_object(a.access_param, '$.applyNo') apply_no
            ,b.request_time 
            from ods.ods_ruleengine_r_invoking_log_data a
            left join ods.ods_ruleengine_r_invoking_log  b on a.id = b.id
            where a.access_param like "%microFinanceList%" and a.callback_param like "%成功%"
        ) as a
        lateral view explode(split(regexp_replace(regexp_replace(ttt,'\\[\\{',''),'}]',''),'},\\{')) exp as splitted
    ) b
    lateral view json_tuple(concat('{',microFinance,'}'),'idNo','idType','microLendTimes','microQueryTimes', 'mobileNo', 'name') t3 
    as idNo, idType, microLendTimes, microQueryTimes, mobileNo, name
    where t3.idNo is not null 
)

insert overwrite table ods.`tmp_ods_ruleengine_r_invoking_log_data`
select 
tmc.*
,row_number() over(partition by tmc.apply_no, tmc.idNo order by request_time desc) rn
from tmp_microFinance_clean tmc

