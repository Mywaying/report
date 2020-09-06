use dwd;
drop table if exists dwd.dwd_rsc_credit;
create table if not exists dwd.dwd_rsc_credit(
	apply_no string comment "订单编号",
	report_no string comment "征信报告编号",
	customer_no string comment "客户编号",
	relation_name string comment "关系人类型",
	customer_name string comment "姓名",
	id_card_no string comment "身份证",
	age bigint comment "年龄",
	sex string comment "性别",
	marital_status_name string comment "婚姻状态",
	2year_m4_above_max bigint comment "逾期情况（两年内）- M4+单笔最高次数",
	2year_m4_above_count bigint comment "逾期情况（两年内）- M4+次数",
	1year_m4_above_max bigint comment "逾期情况（一年内）- M4+单笔最高次数",
	1year_m4_above_count bigint comment "逾期情况（一年内）- M4+次数",
	1year_m3_above_max bigint comment "逾期情况（一年内）- M3+单笔最高次数",
	1year_m3_above_count bigint comment "逾期情况（一年内）- M3+次数",
	1year_m2_above_max bigint comment "逾期情况（一年内）- M2+单笔最高次数",
	1year_m2_above_count bigint comment "逾期情况（一年内）- M2+次数",
	1year_m1_above_max bigint comment "逾期情况（一年内）- M1+单笔最高次数",
	1year_m1_above_count bigint comment "逾期情况（一年内）- M1+次数",
	1year_m1_all_count bigint comment "逾期情况（一年内）- M1+还款记录总期数",
	6month_m4_above_max bigint comment "逾期情况（半年内）- M4+单笔最高次数",
	6month_m4_above_count bigint comment "逾期情况（半年内）- M4+次数",
	6month_m3_above_max bigint comment "逾期情况（半年内）- M3+单笔最高次数",
	6month_m3_above_count bigint comment "逾期情况（半年内）- M3+次数",
	6month_m2_above_max bigint comment "逾期情况（半年内）- M2+单笔最高次数",
	6month_m2_above_count bigint comment "逾期情况（半年内）- M2+次数",
	6month_m1_above_max bigint comment "逾期情况（半年内）- M1+单笔最高次数",
	6month_m1_above_count bigint comment "逾期情况（半年内）- M1+次数",
	6month_m1_all_count bigint comment "逾期情况（半年内）- M1+还款记录总期数",
	3month_m3_above_max bigint comment "逾期情况（三个月内）- M3+单笔最高次数",
	3month_m3_above_count bigint comment "逾期情况（三个月内）- M3+次数",
	3month_m2_above_max bigint comment "逾期情况（三个月内）- M2+单笔最高次数",
	3month_m2_above_count bigint comment "逾期情况（三个月内）- M2+次数",
	3month_m1_above_max bigint comment "逾期情况（三个月内）- M1+单笔最高次数",
	3month_m1_above_count bigint comment "逾期情况（三个月内）- M1+次数",
	3month_m1_all_count bigint comment "逾期情况（三个月内）- M1+还款记录总期数",
	overdue_max bigint comment "逾期情况 - 当前最高逾期",
	icr_exception_count bigint comment "贷款异常笔数" ,
	liability_credit_amount_recent double comment "临期征信负债" ,
	liability_credit_amount double comment "征信负债" ,
	credit_limit_amount double comment "授信总额" ,
	used_credit_amount double  comment "已用额度" ,
	latest_6month_used_avg_amount double comment "最近6个月平均使用额度" ,
	add_loan_count bigint comment "新增贷款笔数" ,
	add_loan_amount double comment "新增贷款金额" ,
	add_loancard_count bigint comment "新增信用卡" ,
	add_loancard_amount double comment "新增授信额度" ,
	6month_loan_org_query_count bigint comment "机构贷前查询数_半年内" ,
	6month_loan_org_f_query_count bigint comment "机构泛贷前查询数_半年内" ,
	6month_creidtcard_query_count bigint comment "发卡审批查询数_半年内" ,
	3month_loan_org_query_count bigint comment "机构贷前查询数_3个月内" ,
	3month_loan_org_f_query_count bigint comment "机构泛贷前查询数_3个月内" ,
	3month_creidtcard_query_count bigint comment "发卡审批查询数_3个月内",
	customer_credit_parse_status string comment "征信解析状态_用户",
	customer_is_employed string comment "自雇状态_用户",
	is_actual_borrower_name string comment "是否主借款人",
    phone string comment "联系方式",
    address string comment "家庭地址",
    id_card_type_name string comment "证件类型",
    create_time string comment "客户信息创建时间"
);

with tmp_all_latest24state_split as(
	select cast(reportno as string), serialno, latest24state, latest24state_value, latest24state_no
	from ods.tmp_ods_rcs_icrloaninfo_latest24state_split  -- 贷款信息 还款情况
	union all 
	select cast(reportno as string), serialno, latest24state, latest24state_value, latest24state_no 
	from ods.tmp_ods_rcs_icrloancardinfo_latest24state_split -- 信用卡信息 还款情况
),

tmp_ods_bpms_biz_query_credit as(
	select * from 
	(select 
    b.*, row_number() over(partition by b.customer_no order by b.create_time desc ) rank
    from ods.ods_bpms_biz_query_credit b 
    ) a
    where rank = 1
),

tmp_1y_m1_all as(
	-- M1+还款记录总期数
	select 
	cast(reportno as string) reportno
	,count(*) 1year_m1_all_count
	from tmp_all_latest24state_split a
	-- 找出 >= 1 的所有 serialno
	join (
		select serialno
		from tmp_all_latest24state_split
		where cast(latest24state_value as bigint) >= 1
		and latest24state_no > 12
		group by serialno
	) b on a.serialno = b.serialno
	where a.latest24state_no > 12 and a.latest24state_value <> "/"
	group by a.reportno
),

tmp_6m_m1_all as(
	-- M1+还款记录总期数
	select 
	cast(reportno as string) reportno
	,count(*) 6month_m1_all_count
	from tmp_all_latest24state_split a
	-- 找出 >= 1 的所有 serialno
	join (
		select serialno
		from tmp_all_latest24state_split
		where cast(latest24state_value as bigint) >= 1
		and latest24state_no > 18
		group by serialno
	) b on a.serialno = b.serialno
	where a.latest24state_no > 18 and a.latest24state_value <> "/"
	group by a.reportno
),


tmp_3m_m1_all as(
	-- M1+还款记录总期数
	select 
	cast(reportno as string) reportno
	,count(*) 3month_m1_all_count
	from tmp_all_latest24state_split a
	-- 找出 >= 1 的所有 serialno
	join (
		select serialno
		from tmp_all_latest24state_split
		where cast(latest24state_value as bigint) >= 1
		and latest24state_no > 21
		group by serialno
	) b on a.serialno = b.serialno
	where a.latest24state_no > 21 and a.latest24state_value <> "/"
	group by a.reportno
),



-- 逾期情况（两年内）---------------------------------------------------------
tmp_year_m as(
	select 
	cast(reportno as string) reportno
	,count(case when cast(latest24state_value as bigint) >= 4 then cast(latest24state_value as bigint) end ) 2year_m4_above_count -- M4+次数

	-- 逾期情况（一年内）---------------------------------------------------------
	,count(case when cast(latest24state_value as bigint) >= 4 and latest24state_no > 12 then cast(latest24state_value as bigint) end ) 1year_m4_above_count -- M4+次数
	,count(case when cast(latest24state_value as bigint) >= 3 and latest24state_no > 12 then cast(latest24state_value as bigint) end ) 1year_m3_above_count -- M3+次数
	,count(case when cast(latest24state_value as bigint) >= 2 and latest24state_no > 12 then cast(latest24state_value as bigint) end ) 1year_m2_above_count -- M2+次数
	,count(case when cast(latest24state_value as bigint) >= 1 and latest24state_no > 12 then cast(latest24state_value as bigint) end ) 1year_m1_above_count -- M1+次数

	-- 逾期情况（半年内）---------------------------------------------------------
	,count(case when cast(latest24state_value as bigint) >= 4 and latest24state_no > 18 then cast(latest24state_value as bigint) end ) 6month_m4_above_count -- M4+次数
	,count(case when cast(latest24state_value as bigint) >= 3 and latest24state_no > 18 then cast(latest24state_value as bigint) end ) 6month_m3_above_count -- M3+次数
	,count(case when cast(latest24state_value as bigint) >= 2 and latest24state_no > 18 then cast(latest24state_value as bigint) end ) 6month_m2_above_count -- M2+次数
	,count(case when cast(latest24state_value as bigint) >= 1 and latest24state_no > 18 then cast(latest24state_value as bigint) end ) 6month_m1_above_count -- M1+次数

	-- 逾期情况（三个月内）---------------------------------------------------------
	,count(case when cast(latest24state_value as bigint) >= 3 and latest24state_no > 21 then cast(latest24state_value as bigint) end ) 3month_m3_above_count -- M3+次数
	,count(case when cast(latest24state_value as bigint) >= 2 and latest24state_no > 21 then cast(latest24state_value as bigint) end ) 3month_m2_above_count -- M2+次数
	,count(case when cast(latest24state_value as bigint) >= 1 and latest24state_no > 21 then cast(latest24state_value as bigint) end ) 3month_m1_above_count -- M1+次数
	from tmp_all_latest24state_split
	group by reportno
),

tmp_year_sum as(
	select 
	reportno
	,max(2year_m4_above_sum) 2year_m4_above_max -- M4+单笔最高次数
	-- 逾期情况（一年内）---------------------------------------------------------
	,max(1year_m4_above_sum) 1year_m4_above_max -- M4+单笔最高次数
	,max(1year_m3_above_sum) 1year_m3_above_max -- M3+单笔最高次数
	,max(1year_m2_above_sum) 1year_m2_above_max -- M2+单笔最高次数
	,max(1year_m1_above_sum) 1year_m1_above_max -- M1+单笔最高次数
	-- 逾期情况（半年内）---------------------------------------------------------
	,max(6month_m4_above_sum) 6month_m4_above_max -- M4+单笔最高次数
	,max(6month_m3_above_sum) 6month_m3_above_max -- M3+单笔最高次数
	,max(6month_m2_above_sum) 6month_m2_above_max -- M2+单笔最高次数
	,max(6month_m1_above_sum) 6month_m1_above_max -- M1+单笔最高次数
	-- 逾期情况（三个月内）---------------------------------------------------------
	,max(3month_m3_above_sum) 3month_m3_above_max -- M3+单笔最高次数
	,max(3month_m2_above_sum) 3month_m2_above_max -- M2+单笔最高次数
	,max(3month_m1_above_sum) 3month_m1_above_max -- M1+单笔最高次数

	from (
		select 
		cast(reportno as string) reportno, serialno
		,sum(case when cast(latest24state_value as bigint) >= 4 then 1 end ) 2year_m4_above_sum
		-- 逾期情况（一年内）---------------------------------------------------------
		,sum(case when cast(latest24state_value as bigint) >= 4 and latest24state_no > 12 then 1 end ) 1year_m4_above_sum -- M4+单笔最高次数
		,sum(case when cast(latest24state_value as bigint) >= 3 and latest24state_no > 12 then 1 end ) 1year_m3_above_sum -- M3+单笔最高次数
		,sum(case when cast(latest24state_value as bigint) >= 2 and latest24state_no > 12 then 1 end ) 1year_m2_above_sum -- M2+单笔最高次数
		,sum(case when cast(latest24state_value as bigint) >= 1 and latest24state_no > 12 then 1 end ) 1year_m1_above_sum -- M1+单笔最高次数
			-- 逾期情况（半年内）---------------------------------------------------------
		,sum(case when cast(latest24state_value as bigint) >= 4 and latest24state_no > 18 then 1 end ) 6month_m4_above_sum -- M4+单笔最高次数
		,sum(case when cast(latest24state_value as bigint) >= 3 and latest24state_no > 18 then 1 end ) 6month_m3_above_sum -- M3+单笔最高次数
		,sum(case when cast(latest24state_value as bigint) >= 2 and latest24state_no > 18 then 1 end ) 6month_m2_above_sum -- M2+单笔最高次数
		,sum(case when cast(latest24state_value as bigint) >= 1 and latest24state_no > 18 then 1 end ) 6month_m1_above_sum -- M1+单笔最高次数
			-- 逾期情况（三个月内）---------------------------------------------------------
		,sum(case when cast(latest24state_value as bigint) >= 3 and latest24state_no > 21 then 1 end ) 3month_m3_above_sum -- M3+单笔最高次数
		,sum(case when cast(latest24state_value as bigint) >= 2 and latest24state_no > 21 then 1 end ) 3month_m2_above_sum -- M2+单笔最高次数
		,sum(case when cast(latest24state_value as bigint) >= 1 and latest24state_no > 21 then 1 end ) 3month_m1_above_sum -- M1+单笔最高次数
		from tmp_all_latest24state_split
		group by reportno, serialno
	) as a
	group by reportno
),


-- 逾期情况 ---------------------------------------------------------
tmp_overdue_max as(
	select 
	cast(a.reportno as string) reportno
	,max(a.overdue_max) overdue_max -- 当前最高逾期
	from(
		select 
		a.reportno
		,max(cast(a.latest24state_value as bigint)) overdue_max -- M1+单笔最高次数
		from ods.tmp_ods_rcs_icrloaninfo_latest24state_split a
		join ods.ods_rcs_icrloaninfo b on a.reportno = b.reportno and a.serialno = b.serialno
		where b.state <> "结清"
		group by a.reportno

		union all

		select 
		a.reportno
		,max(cast(a.latest24state_value as bigint)) overdue_max -- M1+单笔最高次数
		from ods.tmp_ods_rcs_icrloancardinfo_latest24state_split a
		join ods.ods_rcs_icrloancardinfo b on a.reportno = b.reportno and a.serialno = b.serialno
		where b.state <> "销户"
		group by a.reportno
	) as a 
	group by a.reportno
	
),

-- 负债情况 ---------------------------------------------------------
-- 贷款异常笔数
tmp_liability_exception_count as(
	select 
	cast(report_no as string) report_no
	,sum(icr_exception_count) icr_exception_count -- 贷款异常笔数
	from( 
		select 
		reportno report_no
		,count(*) icr_exception_count-- 贷款异常笔数
		from ods.ods_rcs_icrloaninfo 
		where class5state in ("可疑","关注","损失","次级","异常")
		group by reportno

		union all

		select 
		reportno report_no
		,count(*) icr_exception_count -- 贷款异常笔数
		from ods.ods_rcs_icrguarantee
		where class5state in ("可疑","关注","损失","次级","异常")
		group by reportno
	) as s
	group by  report_no
),

tmp_icrundestoryloancard as(
	select 
	cast(reportno as string) report_no
	,sum(usedcreditlimit) used_credit_amount -- 已用额度
	,sum(creditlimit) credit_limit_amount -- 授信总额
	,sum(latest6monthusedavgamount) latest_6month_used_avg_amount -- 最近6个月平均使用额度
	from ods.ods_rcs_icrundestoryloancard
	group by reportno
),


tmp_icrguarantee as(
	select 
	cast(reportno as string) report_no
	,sum(a.guaranteebalance)  guarantee_balance -- 担保贷款本金余额
	from ods.ods_rcs_icrguarantee a
	join tmp_ods_bpms_biz_query_credit obbqc on a.reportno = cast(obbqc.report_no as string)
	join ods.ods_bpms_biz_customer_rel obbcr on obbcr.customer_no = obbqc.customer_no
	join ods.ods_bpms_biz_apply_order_common bao on bao.apply_no = obbcr.apply_no
	join ods.ods_bpms_biz_fee_summary bfs on bao.apply_no = bfs.apply_no
	join ods.ods_rcs_reportbaseinfo orrb on orrb.reportno = a.reportno
	where datediff(bfs.borrowing_due_date, orrb.querytime) <= bfs.product_term
	group by a.reportno
),

tmp_liability_credit_nearterm as(
	select 
	cast(a.reportno as string) report_no
	,sum(case when datediff(bfs.borrowing_due_date, orrb.querytime) <= bfs.product_term 
		 then a.balance end) recent_balance -- 临期本金余额
	,sum(a.balance) balance -- 本金余额
	from ods.ods_rcs_icrloaninfo a
	join tmp_ods_bpms_biz_query_credit obbqc on a.reportno = cast(obbqc.report_no as string)
	join ods.ods_bpms_biz_customer_rel obbcr on obbcr.customer_no = obbqc.customer_no
	join ods.ods_bpms_biz_apply_order_common bao on bao.apply_no = obbcr.apply_no
	join ods.ods_bpms_biz_fee_summary bfs on bao.apply_no = bfs.apply_no
	join ods.ods_rcs_reportbaseinfo orrb on orrb.reportno = a.reportno
	where a.guaranteeType in ("信用/免担保","保证","农户联保")
	group by a.reportno
),

tmp_icrloaninfo as(
	select 
	cast(a.reportno as string) report_no
	,sum(case when datediff(regexp_replace(orrbo.reportcreatetime, "/", "-"), regexp_replace(a.opendate, "/", "-")) <= 90 then 1 end) add_loan_count -- 新增贷款笔数
	,sum(case when datediff(regexp_replace(orrbo.reportcreatetime, "/", "-"), regexp_replace(a.opendate, "/", "-")) <= 90 then a.creditlimitamount end) add_loan_amount -- 新增贷款金额
	from ods.ods_rcs_icrloaninfo a 
	join ods.ods_rcs_reportbaseinfo orrbo on a.reportno = orrbo.reportno
	group by a.reportno
),

tmp_icrloancardinfo as(
	select 
	cast(a.reportno as string) report_no
	,sum(case when datediff(regexp_replace(orrbo.reportcreatetime, "/", "-"), regexp_replace(a.opendate, "/", "-")) <= 90 then 1 end) add_loancard_count -- 新增信用卡
	,sum(case when datediff(regexp_replace(orrbo.reportcreatetime, "/", "-"), regexp_replace(a.opendate, "/", "-")) <= 90 then a.creditlimitamount end) add_loancard_amount -- 新增授信额度
	from ods.ods_rcs_icrloancardinfo a 
	join ods.ods_rcs_reportbaseinfo orrbo on a.reportno = orrbo.reportno
	group by a.reportno
),

tmp_icrrecorddetail as(
	select
	cast(ori.reportno as string) reportno
	-- 机构查询情况（半年内）--------------
	,count(distinct 
		case when datediff(regexp_replace(orrbo.reportcreatetime, "/", "-"), regexp_replace(ori.querydate, "/", "-")) <=180 
		and ori.querier not like "%本人%"
		and ori.queryreason in ("贷款审批", "担保资格审查")  
	 	then ori.querier end 
	 	,case when datediff(regexp_replace(orrbo.reportcreatetime, "/", "-"), regexp_replace(ori.querydate, "/", "-")) <=180 
		and ori.querier not like "%本人%"
		and ori.queryreason in ("贷款审批", "担保资格审查")  
	 	then ori.queryreason end 
	) 6month_loan_org_query_count-- 机构贷前查询数_半年内

	,count(distinct 
		case when datediff(regexp_replace(orrbo.reportcreatetime, "/", "-"), regexp_replace(ori.querydate, "/", "-")) <=180 
		and ori.querier not like "%本人%"
		and ori.queryreason in ("货款审批","担保资格审查","资信审查","保前审查")  
	 	then ori.querier end 
	 	,case when datediff(regexp_replace(orrbo.reportcreatetime, "/", "-"), regexp_replace(ori.querydate, "/", "-")) <=180 
		and ori.querier not like "%本人%"
		and ori.queryreason in ("货款审批","担保资格审查","资信审查","保前审查") 
	 	then ori.queryreason end 
	) 6month_loan_org_f_query_count-- 机构泛贷前查询数_半年内

	,count(distinct 
		case when datediff(regexp_replace(orrbo.reportcreatetime, "/", "-"), regexp_replace(ori.querydate, "/", "-")) <=180 
		and ori.querier not like "%本人%"
		and ori.queryreason = "信用卡审批"
	 	then ori.querier end 
	 	,case when datediff(regexp_replace(orrbo.reportcreatetime, "/", "-"), regexp_replace(ori.querydate, "/", "-")) <=180 
		and ori.querier not like "%本人%"
		and ori.queryreason = "信用卡审批"
	 	then ori.queryreason end 
	) 6month_creidtcard_query_count-- 发卡审批查询数

	-- 机构查询情况（三个月内）----------------------------
	,count(distinct 
		case when datediff(regexp_replace(orrbo.reportcreatetime, "/", "-"), regexp_replace(ori.querydate, "/", "-")) <=90 
		and ori.querier not like "%本人%"
		and ori.queryreason in ("贷款审批", "担保资格审查")  
	 	then ori.querier end 
	 	,case when datediff(regexp_replace(orrbo.reportcreatetime, "/", "-"), regexp_replace(ori.querydate, "/", "-")) <=90 
		and ori.querier not like "%本人%"
		and ori.queryreason in ("贷款审批", "担保资格审查")  
	 	then ori.queryreason end 
	) 3month_loan_org_query_count-- 机构贷前查询数_半年内

	,count(distinct 
		case when datediff(regexp_replace(orrbo.reportcreatetime, "/", "-"), regexp_replace(ori.querydate, "/", "-")) <=90 
		and ori.querier not like "%本人%"
		and ori.queryreason in ("货款审批","担保资格审查","资信审查","保前审查")  
	 	then ori.querier end 
	 	,case when datediff(regexp_replace(orrbo.reportcreatetime, "/", "-"), regexp_replace(ori.querydate, "/", "-")) <=90 
		and ori.querier not like "%本人%"
		and ori.queryreason in ("货款审批","担保资格审查","资信审查","保前审查") 
	 	then ori.queryreason end 
	) 3month_loan_org_f_query_count-- 机构泛贷前查询数_半年内

	,count(distinct 
		case when datediff(regexp_replace(orrbo.reportcreatetime, "/", "-"), regexp_replace(ori.querydate, "/", "-")) <=90 
		and ori.querier not like "%本人%"
		and ori.queryreason = "信用卡审批"
	 	then ori.querier end 
	 	,case when datediff(regexp_replace(orrbo.reportcreatetime, "/", "-"), regexp_replace(ori.querydate, "/", "-")) <=90 
		and ori.querier not like "%本人%"
		and ori.queryreason = "信用卡审批"
	 	then ori.queryreason end 
	) 3month_creidtcard_query_count-- 发卡审批查询数

	from ods.ods_rcs_icrrecorddetail ori
	join ods.ods_rcs_reportbaseinfo orrbo on ori.reportno = orrbo.reportno
	group by ori.reportno
),

tmp_customer_credit_parse_status as (
   -- 客户粒度
   select 
   br.apply_no
   ,br.customer_no
   ,if(sum(case when a.parse_way <> "Y" then 1 end) >= 1, "解析失败", "解析成功") customer_credit_parse_status -- 征信解析状态
   from ods.ods_bpms_biz_customer_rel br 
   left join ods.ods_bpms_biz_query_credit  a on br.customer_no = a.customer_no
   group by br.apply_no, br.customer_no
),

tmp_customer_is_employed as (
   -- 客户粒度
   select 
   br.apply_no
   ,br.customer_no
   ,if(sum(case when a.is_self_employed = "Y" then 1 end) >= 1, "自雇", "受薪") customer_is_employed -- 征信解析状态
   from ods.ods_bpms_biz_customer_rel br 
   left join ods.ods_bpms_biz_business_info a on br.customer_no = a.customer_no
   group by br.apply_no, br.customer_no
)

insert overwrite table dwd.`dwd_rsc_credit`
select 
bao.apply_no -- 订单编号
,cast(obbqc.report_no as string) -- 征信报告编号
,obbcr.customer_no -- 客户编号
,obbcr.relation_name -- 关系人类型
,obbcr.name customer_name -- 姓名
,obbcr.id_card_no -- 身份证
,obbcr.age -- 年龄
,case when obbcr.sex = "M" then "男"
	  when obbcr.sex = "F" then "女"
 end sex -- 性别
,obbcr.marital_status_name -- 婚姻状态

-- 逾期情况（两年内）-----------
,tys.2year_m4_above_max -- M4+单笔最高次数
,tym.2year_m4_above_count -- M4+次数

-- 逾期情况（一年内）-------------
,tys.1year_m4_above_max -- M4+单笔最高次数
,tym.1year_m4_above_count -- M4+次数
,tys.1year_m3_above_max -- M3+单笔最高次数
,tym.1year_m3_above_count -- M3+次数
,tys.1year_m2_above_max -- M2+单笔最高次数
,tym.1year_m2_above_count -- M2+次数
,tys.1year_m1_above_max -- M1+单笔最高次数
,tym.1year_m1_above_count -- M1+次数
,t1m1a.1year_m1_all_count -- M1+还款记录总期数

-- 逾期情况（半年内）------------
,tys.6month_m4_above_max -- M4+单笔最高次数
,tym.6month_m4_above_count -- M4+次数
,tys.6month_m3_above_max -- M3+单笔最高次数
,tym.6month_m3_above_count -- M3+次数
,tys.6month_m2_above_max -- M2+单笔最高次数
,tym.6month_m2_above_count -- -- M2+次数
,tys.6month_m1_above_max -- M1+单笔最高次数
,tym.6month_m1_above_count -- M1+次数
,t6m1a.6month_m1_all_count -- M1+还款记录总期数

-- 逾期情况（三个月内）---------------
,tys.3month_m3_above_max -- M3+单笔最高次数
,tym.3month_m3_above_count -- M3+次数
,tys.3month_m2_above_max -- M2+单笔最高次数
,tym.3month_m2_above_count -- -- M2+次数
,tys.3month_m1_above_max -- M1+单笔最高次数
,tym.3month_m1_above_count -- M1+次数
,t3m1a.3month_m1_all_count -- M1+还款记录总期数

-- 逾期情况 ---------------------------
,tom.overdue_max -- 当前最高逾期

-- 负债情况 ---------------------------
,tlec.icr_exception_count -- 贷款异常笔数
-- 贷款信息.本金余额(临期) + 对外贷款担保信息.担保贷款本金余额 + 未销户贷记卡信息汇总.已用额度
,nvl(tlcn.recent_balance, 0) + nvl(tig.guarantee_balance, 0) + nvl(tisld.used_credit_amount, 0)  liability_credit_amount_recent -- 临期征信负债
-- 贷款信息.本金余额 + 对外贷款担保信息.担保贷款本金余额 + 未销户贷记卡信息汇总.已用额度
,nvl(tlcn.balance, 0) + nvl(tig.guarantee_balance, 0) + nvl(tisld.used_credit_amount, 0)  liability_credit_amount -- 征信负债
,tisld.credit_limit_amount -- 授信总额
,tisld.used_credit_amount -- 已用额度
,tisld.latest_6month_used_avg_amount -- 最近6个月平均使用额度
,tili.add_loan_count -- 新增贷款笔数
,tili.add_loan_amount -- 新增贷款金额
,tilci.add_loancard_count -- 新增信用卡
,tilci.add_loancard_amount -- 新增授信额度
,tird.6month_loan_org_query_count-- 机构贷前查询数_半年内
,tird.6month_loan_org_f_query_count-- 机构泛贷前查询数_半年内
,tird.6month_creidtcard_query_count-- 发卡审批查询数_半年内
,tird.3month_loan_org_query_count-- 机构贷前查询数_3个月内
,tird.3month_loan_org_f_query_count-- 机构泛贷前查询数_3个月内
,tird.3month_creidtcard_query_count-- 发卡审批查询数_3个月内
,tccps.customer_credit_parse_status -- 征信解析状态
,tcie.customer_is_employed -- 自雇状态
,obbcr.is_actual_borrower_name -- 是否主借款人
,obbcr.phone -- 联系方式
,obbcr.address -- 家庭地址
,obbcr.id_card_type_name -- 证件类型
,obbcr.create_time -- 客户信息创建时间
from ods.ods_bpms_biz_apply_order_common bao
left join ods.ods_bpms_biz_customer_rel_common obbcr on bao.apply_no = obbcr.apply_no 
left join tmp_ods_bpms_biz_query_credit obbqc on obbcr.customer_no = obbqc.customer_no  
left join ods.ods_rcs_reportbaseinfo orrbo on cast(obbqc.report_no as string) = cast(orrbo.reportno as string)
left join tmp_year_m tym on cast(obbqc.report_no as string) = cast(tym.reportno as string)
left join tmp_year_sum tys on cast(obbqc.report_no as string) = cast(tys.reportno as string)
left join tmp_1y_m1_all t1m1a on cast(obbqc.report_no as string) = cast(t1m1a.reportno as string)
left join tmp_6m_m1_all t6m1a on cast(obbqc.report_no as string) = cast(t6m1a.reportno as string)
left join tmp_3m_m1_all t3m1a on cast(obbqc.report_no as string) = cast(t3m1a.reportno as string)
left join tmp_overdue_max tom on cast(obbqc.report_no as string) = cast(tom.reportno as string)
left join tmp_liability_exception_count tlec on tlec.report_no = cast(obbqc.report_no as string)
left join tmp_icrundestoryloancard tisld on tisld.report_no = cast(obbqc.report_no as string)
left join tmp_icrguarantee tig on cast(obbqc.report_no as string) = tig.report_no
left join tmp_liability_credit_nearterm tlcn on cast(obbqc.report_no as string) = tlcn.report_no
left join tmp_icrloaninfo tili on cast(tili.report_no as string) = cast(obbqc.report_no as string)
left join tmp_icrloancardinfo tilci on cast(tilci.report_no as string) = cast(obbqc.report_no as string)
left join tmp_icrrecorddetail tird on cast(tird.reportno as string) = cast(obbqc.report_no as string)
left join tmp_customer_credit_parse_status tccps on tccps.apply_no = bao.apply_no and tccps.customer_no = obbcr.customer_no
left join tmp_customer_is_employed tcie on tcie.apply_no = bao.apply_no and tcie.customer_no = obbcr.customer_no;