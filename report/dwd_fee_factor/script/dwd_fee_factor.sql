set hive.execution.engine=spark;
drop table if exists dwd.dwdtmp_fee_factor;
create table if not exists dwd.dwdtmp_fee_factor (
  `city_name` string COMMENT '城市',
  `channel_name` string COMMENT '渠道',
  `product_name` string COMMENT '产品',
  `capital_name` string COMMENT '资方机构',
  `loan_node_name` string COMMENT '放款节点',
  `product_term` string COMMENT '产品期限',
  `charge_way_name` string COMMENT '收费方式',
  `template_name` string COMMENT '使用模板',
  `jfjs` string COMMENT '计费基数',
  `qdj_b` double COMMENT '渠道价/笔（费率值）',
  `dsfy_b` double  COMMENT '代收返佣/笔（最大值）',
  `cqfl_d` double  COMMENT '超期费率/天（最小值）',
  `qdj_d` double COMMENT '渠道价/天（费率值）',
  `dsfy_d` double  COMMENT '代收返佣/天（最大值）',
  `zjsfts` double  COMMENT '最低收费天数',
  `wscdsfdfyfw` double  COMMENT '我司承担税费的返佣范围',
  `zdsfje_qd` double  COMMENT '最低收费金额（对渠道）',
  `qdj_b_m` double COMMENT '渠道价/笔（最小值）',
  `qdj_d_m` double COMMENT '渠道价/天（最小值）',
  `qdj_b_xtmrz` double COMMENT '渠道价/笔（系统默认值）',
  `cqfl_d_m` double COMMENT '超期费率/天',
  `cqfl_d_xtmrz` double COMMENT '超期费率/天（系统默认值）',
  `qdk_d_xtmrz` double COMMENT  '渠道价/天（系统默认值）',
  `start_time` timestamp  COMMENT '生效时间',
  `end_time` timestamp  COMMENT '失效时间',
  `fullname_` string  COMMENT '创建人',
  `update_time` timestamp  COMMENT '修改时间',
  `franchisee_name` string COMMENT '业务类型',
  `qdj_bx_b` double COMMENT '渠道价（保险）/笔（%）',
  `bffcbl` double COMMENT '保费分成比例（%）',
  `etl_update_time` timestamp,
  `fwfl_b` double COMMENT '服务费率/笔（%）',
  `fwf_b` double COMMENT '服务费/笔（元）',
  `rzcbl_d` double COMMENT '融资成本率/天（%）',
  `htpzdrzcbl_d` double COMMENT '后台配置的融资成本率/天（%）'
);
with tmp_biz_base_fee_factor_value_rel as (
	select fee_factor_id, fee_metadata_id, parameter_metadata_id, max_value, min_value, value, version
	from (
		select
		a.*
		,row_number() over(partition by a.fee_factor_id, a.fee_metadata_id order by cast(a.version as int) desc ) rank
		from ods.ods_bpms_biz_base_fee_factor_value_rel a
	) ffvr
	where rank = 1
),

tmp_metadata_factor_value as (
	SELECT
	ffvr.fee_factor_id
	,ffvr.version
	,max(case when bfm.fee_metadata_name = '渠道价/笔（%）' then ffvr.`value` end) qdj_b -- 渠道价/笔_费率值
	,max(case when bfm.fee_metadata_name = '渠道价/笔（%）' then ffvr.min_value end) qdj_b_m -- 渠道价/笔_最小值
	,max(case when bfm.fee_metadata_name like '%代收返佣/笔%' then ffvr.max_value end) dsfy_b-- 代收返佣/笔（最大值） 
	,max(case when bfm.fee_metadata_name like '%超期费率/天%' then ffvr.min_value end) cqfl_d -- 超期费率/天（最小值）
	,max(case when bfm.fee_metadata_name = '渠道价/天（%）' then ffvr.`value` end) qdj_d -- 渠道价/天_费率值 
	,max(case when bfm.fee_metadata_name = '渠道价/天（%）' then ffvr.min_value end) qdj_d_m -- 渠道价/天_最小值）
	,max(case when bfm.fee_metadata_name like '%代收返佣/天%' then max_value end) dsfy_d -- 代收返佣/天（最大值）
	,max(case when bfm.fee_metadata_name = '计费基数' then ff.fee_metadata_name end) jfjs -- 计费基数
	,max(case when bfm.fee_metadata_name = '最低收费天数' then ffvr.`value` end) zjsfts -- 最低收费天数
	,max(case when bfm.fee_metadata_name = '我司承担税费的返佣范围' then ffvr.`value` end) wscdsfdfyfw --  我司承担税费的返佣范围
	,max(case when bfm.fee_metadata_name = '最低收费金额（对渠道）' then ffvr.`value` end) zdsfje_qd  -- 最低收费金额（对渠道）
	,max(case when bfm.fee_metadata_name = '渠道价/笔（系统默认值）（%）' then ffvr.`value` end ) qdj_b_xtmrz -- '渠道价/笔（系统默认值）（%）'
	,max(case when bfm.fee_metadata_name = '超期费率/天（%）' then ffvr.`value` end ) cqfl_d_m -- 超期费率/天
	,max(case when bfm.fee_metadata_name = '超期费率/天（系统默认值）（%）' then ffvr.`value` end ) cqfl_d_xtmrz -- 超期费率/天（系统默认值）（%）
	,max(case when bfm.fee_metadata_name = '渠道价/天（系统默认值）（%）' then ffvr.`value` end ) qdk_d_xtmrz -- 渠道价/天（系统默认值）（%）
	,max(case when bfm.fee_metadata_name = '渠道价（保险）/笔（%）' then ffvr.`value` end ) qdj_bx_b -- 渠道价（保险）/笔（%）
	,max(case when bfm.fee_metadata_name = '保费分成比例（%）' then ffvr.`value` end ) bffcbl -- 保费分成比例（%）
	,max(case when bfm.fee_metadata_name = '服务费率/笔（%）' then ffvr.`value` end ) fwfl_b -- 服务费率/笔（%）
	,max(case when bfm.fee_metadata_name = '服务费/笔（元）' then ffvr.`value` end ) fwf_b -- 服务费/笔（元）
	,max(case when bfm.fee_metadata_name = '融资成本率/天（%）' then ffvr.`value` end ) rzcbl_d -- 融资成本率/天（%）
	,max(case when bfm.fee_metadata_name = '后台配置的融资成本率/天（%）' then ffvr.`value` end ) htpzdrzcbl_d -- 后台配置的融资成本率/天（%）
	from ods.ods_bpms_biz_base_fee_metadata bfm
	join tmp_biz_base_fee_factor_value_rel ffvr on ffvr.fee_metadata_id = bfm.id
	left join(
		SELECT id, fee_metadata_name 
		from ods.ods_bpms_biz_base_fee_metadata  bfm
		join (SELECT DISTINCT parameter_metadata_id from ods.ods_bpms_biz_base_fee_factor_value_rel 
			  where fee_metadata_id = '10000047471353') bb on bfm.id = bb.parameter_metadata_id
	) as ff on ffvr.parameter_metadata_id = ff.id

	where(
			 bfm.fee_metadata_name like '%渠道价/笔%'
		or bfm.fee_metadata_name like '%代收返佣/笔%'
		or bfm.fee_metadata_name like '%超期费率/天%'
		or bfm.fee_metadata_name like '%渠道价/天%'
		or bfm.fee_metadata_name like '%代收返佣/天%'
		or bfm.fee_metadata_name = '计费基数'
		or bfm.fee_metadata_name = '最低收费天数'
		or bfm.fee_metadata_name = '我司承担税费的返佣范围'
		or bfm.fee_metadata_name = '最低收费金额（对渠道）'
		or bfm.fee_metadata_name = '渠道价/笔（系统默认值）（%）'
		or bfm.fee_metadata_name = '%超期费率/天%'
		or bfm.fee_metadata_name = '超期费率/天（系统默认值）（%）'
		or bfm.fee_metadata_name = '渠道价/天（系统默认值）（%）'
		or bfm.fee_metadata_name = '渠道价（保险）/笔（%）'
		or bfm.fee_metadata_name = '保费分成比例（%）'
        or bfm.fee_metadata_name = '服务费率/笔（%）'
        or bfm.fee_metadata_name = '服务费/笔（元）'
        or bfm.fee_metadata_name = '融资成本率/天（%）'
        or bfm.fee_metadata_name = '后台配置的融资成本率/天（%）'
	)
	group by ffvr.fee_factor_id, ffvr.version
),

tmp_ftr as (
	select a.* 
  	from (
  		select
		a.*
		,row_number() over(partition by a.fee_factor_id order by cast(a.version as int) desc ) rank
		from ods.ods_bpms_biz_base_fee_factor_template_rel a
  	) a
  	where rank = 1  		
),

tmp_bffcbl as (
	SELECT
	       a.id
	,a.company_name  -- '城市'
	,a.channel_name -- '渠道'
	,a.product_name -- '产品'
	,a.capital_name -- '资方机构'
	,a.loan_node_name -- '放款节点'
	,a.product_term -- '产品期限'
	,a.charge_way_name -- '收费方式'
	,template.template_name -- '使用模板'
	,c.jfjs -- '计费基数'
	,c.bffcbl -- '保费分成比例'
    ,c.fwfl_b -- '服务费率/笔（%）'
    ,c.fwf_b -- '服务费/笔（元）'
    ,c.rzcbl_d -- '融资成本率/天（%）'
    ,c.htpzdrzcbl_d -- '后台配置的融资成本率/天（%）'
	FROM ods.ods_bpms_biz_base_fee_factor a
	JOIN tmp_ftr ftr on a.id = ftr.fee_factor_id
    left join ods.ods_bpms_biz_base_fee_template template on ftr.fee_template_id = template.id and template_type = '保额保费费率'
	join tmp_metadata_factor_value c on ftr.fee_factor_id = c.fee_factor_id and ftr.version = c.version
	left join ods.ods_bpms_sys_user su on ftr.create_user_id = su.id_
	where a.franchisee_name is not null
)

insert overwrite table dwd.dwdtmp_fee_factor
SELECT
a.company_name city_name -- '城市'
,case when (a.channel_name='普通渠道' and a.franchisee_code='self') or (a.channel_name='无' and a.franchisee_code='franchisee') then null else a.channel_name end channel_name -- '渠道'
,a.product_name -- '产品'
,a.capital_name -- '资方机构'
,a.loan_node_name -- '放款节点'
,a.product_term -- '产品期限'
,a.charge_way_name -- '收费方式'
,template.template_name -- '使用模板'
,c.jfjs -- '计费基数'
,c.qdj_b -- '渠道价/笔_费率值'
,c.dsfy_b -- '代收返佣/笔（最大值）'
,c.cqfl_d -- '超期费率/天（最小值）'
,c.qdj_d -- '渠道价/天_费率值'
,c.dsfy_d -- '代收返佣/天（最大值）'
,c.zjsfts -- '最低收费天数'
,c.wscdsfdfyfw -- '我司承担税费的返佣范围'
,c.zdsfje_qd -- '最低收费金额（对渠道）'
,c.qdj_b_m  -- 渠道价/笔_最小值
,c.qdj_d_m -- 渠道价/天_最小值）
,c.qdj_b_xtmrz -- '渠道价/笔（系统默认值）（%）'
,c.cqfl_d_m -- 超期费率/天
,c.cqfl_d_xtmrz -- 超期费率/天（系统默认值）（%）
,c.qdk_d_xtmrz -- 渠道价/天（系统默认值）（%）
,ftr.start_time  -- '生效时间'
,ftr.end_time -- '失效时间'
,su.fullname_ -- '创建人'
,a.update_time -- '修改时间'
,a.franchisee_name -- '业务类型'
,c.qdj_bx_b -- '渠道价（保险）/笔'
,tb.bffcbl -- '保费分成比例'
,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') etl_update_time
,tb.fwfl_b -- '服务费率/笔（%）'
,tb.fwf_b -- '服务费/笔（元）'
,tb.rzcbl_d -- '融资成本率/天（%）'
,tb.htpzdrzcbl_d -- '后台配置的融资成本率/天（%）'
FROM ods.ods_bpms_biz_base_fee_factor a
JOIN tmp_ftr ftr on a.id = ftr.fee_factor_id
join ods.ods_bpms_biz_base_fee_template template on ftr.fee_template_id = template.id and template_type = '产品费率'  --保额保费费率
left join tmp_metadata_factor_value c on ftr.fee_factor_id = c.fee_factor_id and ftr.version = c.version
left join ods.ods_bpms_sys_user su on ftr.create_user_id = su.id_
left join tmp_bffcbl tb 
	on tb.id = a.id
where a.franchisee_name is not null 

;

drop table if exists dwd.dwd_fee_factor;
ALTER TABLE dwd.dwdtmp_fee_factor RENAME TO dwd.dwd_fee_factor;