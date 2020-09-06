drop table if exists dwd.tmp_order_cost_capital_fr;

create table dwd.tmp_order_cost_capital_fr(
	apply_no string comment '订单编号',
	product_name string comment '产品名称',
	cost_capital double comment '资金成本',
	ftp_pre_rate double comment '前置费率',
	ftp_pre_amount double comment '前置金额'
);

with tmp_amt as (
	select
	bao.apply_no
	,bfs.borrowing_amount realease_amount -- 借款金额
	,bfs.borrowing_value_date -- 客户起息日
	,bfs.platform_value_date -- 平台起息日
	,if(bao.service_type = "JMS", "是", "否") is_jms -- 是否加盟商业务
	from ods.ods_bpms_biz_apply_order bao
	left join ods.ods_bpms_biz_isr_mixed bim on bao.apply_no = bim.apply_no
	left join ods.ods_bpms_biz_fee_summary bfs on bao.apply_no = bfs.apply_no
),

tmp_slq as (
  SELECT
  t1.`apply_no`,
  if(max(t2.`fin_date`) is not null, '是', '否') AS advance_before_shulou,  -- 赎楼前垫资是否归还
  sum(t1.`adv_amt`)  AS  floorAdvance_adv_amt, -- 赎楼前垫资金额
  min(t1.`fin_date`)  AS  floorAdvance_fin_date, -- 赎楼前垫资放款时间
  max(t2.`fin_date`) AS  floorAdvance_ret_date -- 赎楼前垫资归还时间
  FROM ods.`ods_bpms_fd_advance` t1
  LEFT JOIN ods.`ods_bpms_fd_advance_ret` t2 ON t1.id=t2.`apply_id`
  WHERE t1.`adv_type`='floorAdvance' and t1.status <> "ZZ"
  group by t1.`apply_no`
),

tmp_dqgh as (
  SELECT
  t1.`apply_no`,
  if(max(t2.`fin_date`) is not null, '是', '否') AS advance_return, --到期归还垫资是否归还
  sum(t1.`adv_amt`)  AS  expireAdvance_adv_amt, -- 到期归还垫资金额
  min(t1.`fin_date`)  AS expireAdvance_fin_date, -- 到期归还垫资时间
  max(t2.`fin_date`)  AS expireAdvance_ret_date -- 到期归还垫资归还时间
  FROM ods.`ods_bpms_fd_advance` t1
  LEFT JOIN ods.`ods_bpms_fd_advance_ret` t2 ON t1.id=t2.`apply_id`
  WHERE t1.`adv_type`='expireAdvance' and  t1.status <> "ZZ"
  group by t1.`apply_no`
),

tmp_matter_record as (
	select
	bomrd.apply_no
	,bomrd.handle_time
	from(
	    select
	    b.*, row_number() over(partition by b.apply_no, b.matter_key order by b.handle_time desc) rank
	    from ods.ods_bpms_biz_order_matter_record b
	    where b.matter_key = "paymentArrival"
    ) as  bomrd
 	where bomrd.matter_key = "paymentArrival" and bomrd.rank = 1
),

tmp_cc01 as (
  select
  cct.apply_no
  ,max(cct.trans_day) rebate_date_hk -- （客户）借款回款-回款日期
  ,SUM(cct.`trans_money`) receive_amount_hk  -- 回款金额
  from ods.ods_bpms_c_cost_trade cct
  where cct.trans_type = "CC01"
  group by cct.apply_no
),

tmp_cc02 as (
  select
  cct.apply_no
  ,max(cct.trans_day) return_date -- 还款时间（归还平台）
  from ods.ods_bpms_c_cost_trade cct
  where cct.trans_type = "CC02"
  group by cct.apply_no
),

tmp_return_date as (
	select
	a.apply_no
	,nvl(b.return_date, c.handle_time) return_date
	from ods.ods_bpms_biz_apply_order a
	left join tmp_cc02 b on a.apply_no = b.apply_no
	left join tmp_matter_record c on a.apply_no = c.apply_no
),

tmp_cd01 as (
  select
  apply_no
  ,max(cct.payer_acct) payer_acct
  ,max(cct.payer_bank_name) payer_bank_name
  ,max(cct.payer_acct_no) payer_acct_no
  ,max(cct.payee_acct) payee_acct
  ,max(cct.payee_bank_name) payee_bank_name
  ,max(cct.payee_acct_no) payee_acct_no
  from ods.ods_bpms_c_cost_trade cct
  where cct.trans_type = "CD01"  -- 划拨借款
  group by cct.apply_no
),

tmp_amount_use_day as (
	select
	bao.apply_no
	,case when datediff(nvl(regexp_replace(cast(tmp_slq.floorAdvance_ret_date as string), "/", "-"), date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1)), regexp_replace(cast(tmp_slq.floorAdvance_fin_date as string), "/", "-")) is null then 0
	      when datediff(nvl(regexp_replace(cast(tmp_slq.floorAdvance_ret_date as string), "/", "-"), date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1)), regexp_replace(cast(tmp_slq.floorAdvance_fin_date as string), "/", "-")) in (0, 1) then 0
	      else datediff(nvl(regexp_replace(cast(tmp_slq.floorAdvance_ret_date as string), "/", "-"), date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1)), regexp_replace(cast(tmp_slq.floorAdvance_fin_date as string), "/", "-"))
	 end dz_slq_use_days -- 赎楼前垫资用款天数

	,case when datediff(nvl(regexp_replace(cast(tmp_dqgh.expireAdvance_ret_date as string), "/", "-"), date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1)), regexp_replace(cast(tmp_dqgh.expireAdvance_fin_date as string), "/", "-")) is null then 0
	      when datediff(nvl(regexp_replace(cast(tmp_dqgh.expireAdvance_ret_date as string), "/", "-"), date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1)), regexp_replace(cast(tmp_dqgh.expireAdvance_fin_date as string), "/", "-")) in (0, 1) then 0
	      else datediff(nvl(regexp_replace(cast(tmp_dqgh.expireAdvance_ret_date as string), "/", "-"), date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1)), regexp_replace(cast(tmp_dqgh.expireAdvance_fin_date as string), "/", "-"))
	 end dz_dqgh_use_days -- 到期归还垫资用款天数

	,case when trd.return_date is null then datediff(nvl(to_date(regexp_replace(cast(trd.return_date as string), "/", "-")), date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1)), regexp_replace(cast(bfs.borrowing_value_date as string), "/", "-"))+1
      	  when trd.return_date is not null then datediff(nvl(to_date(regexp_replace(cast(trd.return_date as string), "/", "-")), date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1)), regexp_replace(cast(bfs.borrowing_value_date as string), "/", "-"))
    end use_days -- 实际借款期限
	from ods.ods_bpms_biz_apply_order bao
	left join ods.ods_bpms_biz_fee_summary bfs on bao.apply_no = bfs.apply_no
	left join tmp_slq on bao.apply_no = tmp_slq.apply_no
	left join tmp_dqgh on bao.apply_no = tmp_dqgh.apply_no
	left join tmp_return_date trd on bao.apply_no = trd.apply_no
),


tmp_ftp as (
	select
	a.apply_no
	,(nvl(cfcr.ftp, 0))  ftp
	,nvl(cfcr.pre_rate, 0) + nvl(cfcr.pre_amount/tmp_amt.realease_amount, 0)  pre_rate
	,nvl(cfcr.pre_rate, 0) ftp_pre_rate  -- 前置费率
	,nvl(cfcr.pre_amount, 0) ftp_pre_amount -- 前置金额
	from ods.ods_bpms_biz_apply_order_common a
	left join ods.ods_bpms_sys_org b on a.branch_id = b.code_
	left join tmp_amt on a.apply_no = tmp_amt.apply_no
	join ods.ods_bpms_c_fund_cost_rule cfcr
	     on cfcr.city_name = b.name_
		and cfcr.biz_type = if(tmp_amt.is_jms = "是", "加盟业务", "直营业务")
		and cfcr.institution = trim(a.partner_insurance_name) -- 合作机构/垫资类型
		and cfcr.ftp_type = "分润"
	    and a.transaction_type = cfcr.product_type
	where tmp_amt.platform_value_date >= to_date(from_unixtime(UNIX_TIMESTAMP(cfcr.begin_time, 'yyyy/M/d'))) and tmp_amt.platform_value_date < to_date(from_unixtime(UNIX_TIMESTAMP(cfcr.end_time, 'yyyy/M/d')))
),

tmp_slq_ftp as (
	select
	a.apply_no
	,(nvl(cfcr.ftp, 0)) ftp
	from ods.ods_bpms_biz_apply_order_common a
	left join ods.ods_bpms_sys_org b on a.branch_id = b.code_
	left join tmp_amt on a.apply_no = tmp_amt.apply_no
	left join tmp_slq on tmp_slq.apply_no = a.apply_no
	join ods.ods_bpms_c_fund_cost_rule cfcr
	     on cfcr.city_name = b.name_
		and cfcr.biz_type = if(tmp_amt.is_jms = "是", "加盟业务", "直营业务")
		and cfcr.institution = "赎楼前垫资" -- 合作机构/垫资类型
		and cfcr.ftp_type = "分润"
        and a.transaction_type = cfcr.product_type
	where tmp_slq.floorAdvance_fin_date >= to_date(from_unixtime(UNIX_TIMESTAMP(cfcr.begin_time, 'yyyy/M/d'))) and tmp_slq.floorAdvance_fin_date < to_date(from_unixtime(UNIX_TIMESTAMP(cfcr.end_time, 'yyyy/M/d')))
),

tmp_dqgh_ftp as (
	select
	a.apply_no
	,(nvl(cfcr.ftp, 0))  ftp
	from ods.ods_bpms_biz_apply_order_common a
	left join ods.ods_bpms_sys_org b on a.branch_id = b.code_
	left join tmp_amt on a.apply_no = tmp_amt.apply_no
	left join tmp_dqgh on tmp_dqgh.apply_no = a.apply_no
	join ods.ods_bpms_c_fund_cost_rule cfcr
	     on cfcr.city_name = b.name_
		and cfcr.biz_type = if(tmp_amt.is_jms = "是", "加盟业务", "直营业务")
		and cfcr.institution = "到期归还垫资" -- 合作机构/垫资类型
		and cfcr.ftp_type = "分润"
        and a.transaction_type = cfcr.product_type
	where tmp_dqgh.expireAdvance_fin_date >= to_date(from_unixtime(UNIX_TIMESTAMP(cfcr.begin_time, 'yyyy/M/d'))) and tmp_dqgh.expireAdvance_fin_date < to_date(from_unixtime(UNIX_TIMESTAMP(cfcr.end_time, 'yyyy/M/d')))
),

tmp_old_ftp as (
	select
	a.apply_no
	,ioof.ftp
	from ods.ods_bpms_biz_apply_order a
	left join tmp_amt on a.apply_no = tmp_amt.apply_no
	join dim.dim_order_organization_ftp ioof on trim(a.partner_insurance_name) = ioof.organization_name and tmp_amt.is_jms = ioof.is_jms
	where tmp_amt.platform_value_date >= cast(ioof.start_date as timestamp) and tmp_amt.platform_value_date < cast(ioof.end_date as timestamp)
	),

tmp_old_slq_ftp as (
	select
	a.apply_no
	,ioof.ftp
	from ods.ods_bpms_biz_apply_order a
	left join tmp_slq on tmp_slq.apply_no = a.apply_no
	left join tmp_amt on a.apply_no = tmp_amt.apply_no
	join dim.dim_order_organization_ftp ioof on ioof.organization_name = "垫资" and tmp_amt.is_jms = ioof.is_jms
	where tmp_slq.floorAdvance_fin_date >= cast(ioof.start_date as timestamp) and tmp_slq.floorAdvance_fin_date < cast(ioof.end_date as timestamp)
),

tmp_old_dqgh_ftp as (
	select
	a.apply_no
	,ioof.ftp
	from ods.ods_bpms_biz_apply_order a
	left join tmp_amt on a.apply_no = tmp_amt.apply_no
	left join tmp_dqgh on tmp_dqgh.apply_no = a.apply_no
	join dim.dim_order_organization_ftp ioof on ioof.organization_name = "垫资" and tmp_amt.is_jms = ioof.is_jms
	where tmp_dqgh.expireAdvance_fin_date >= cast(ioof.start_date as timestamp) and tmp_dqgh.expireAdvance_fin_date < cast(ioof.end_date as timestamp)
) ,

tmp_cost_capital as (
-- -----------------------------无前置费------------------------------------------
	select
	bao.apply_no
	,bao.product_name
	,0 ftp_pre_rate
	,0 ftp_pre_amount
	,cast(
		case when tmp_cc01.rebate_date_hk is not null and trd.return_date is not null
			 then
			 	if(
			 		(
				 		  nvl(tmp_ftp.ftp/360 -- FTP
				 		  *(case when bfs.platform_value_date is not null and bfs.platform_value_date <> ""
				 					then if(datediff(regexp_replace(cast(trd.return_date as string), "/", "-"), regexp_replace(cast(bfs.platform_value_date as string), "/", "-"))>1, datediff(regexp_replace(cast(trd.return_date as string), "/", "-"), regexp_replace(bfs.platform_value_date, "/", "-")), 1)
					 	  		else 0 end )
					 	  , 0)
					 	  -- 赎楼前垫资 计算

					 	  +nvl(tmp_slq_ftp.ftp/360
					 	  *t1.dz_slq_use_days
					 	  , 0)
					 	  -- 到期归还垫资
					 	  +nvl(tmp_dqgh_ftp.ftp/360
					 	  *t1.dz_dqgh_use_days
					 	  , 0)
				    )
				    > tmp_ftp.ftp/360*3
				    ,(
				 		  nvl(tmp_ftp.ftp/360 -- FTP
				 		  *(case when bfs.platform_value_date is not null and bfs.platform_value_date <> ""
				 		  			then if(datediff(regexp_replace(cast(trd.return_date as string), "/", "-"), regexp_replace(cast(bfs.platform_value_date as string), "/", "-"))>1, datediff(regexp_replace(cast(trd.return_date as string), "/", "-"), regexp_replace(cast(bfs.platform_value_date as string), "/", "-")), 1)
				 		  		else 0 end )
					 	  , 0)
					 	  -- 赎楼前垫资 计算
					 	  +nvl(tmp_slq_ftp.ftp/360
					 	  *t1.dz_slq_use_days
					 	   , 0)
					 	  -- 到期归还垫资
					 	  +nvl(tmp_dqgh_ftp.ftp/360
					 	  *t1.dz_dqgh_use_days
					 	  , 0)
				    )
				    ,tmp_ftp.ftp/360*3
			 	)*tmp_amt.realease_amount

		    else
			   if(
				   (
				 	 tmp_ftp.ftp/360
					  -- 实际借款期限 - 赎楼前垫资用款天数 - 到期归还垫资用款天数
			  			*(nvl(t1.use_days, 0) - nvl(t1.dz_slq_use_days, 0) - nvl(t1.dz_dqgh_use_days, 0))

					 	  -- 赎楼前垫资 计算
					 	+nvl(tmp_slq_ftp.ftp/360, 0)*t1.dz_slq_use_days

					 	  -- 到期归还垫资
				 	    +nvl(tmp_dqgh_ftp.ftp/360, 0) * t1.dz_dqgh_use_days
				   )
				   >tmp_ftp.ftp/360*3
				   ,(
				 	 tmp_ftp.ftp/360
					  -- 实际借款期限 - 赎楼前垫资用款天数 - 到期归还垫资用款天数
			  			*(nvl(t1.use_days, 0) - nvl(t1.dz_slq_use_days, 0) - nvl(t1.dz_dqgh_use_days, 0))

					 	  -- 赎楼前垫资 计算
					 	+nvl(tmp_slq_ftp.ftp/360, 0)*t1.dz_slq_use_days

					 	  -- 到期归还垫资
				 	    +nvl(tmp_dqgh_ftp.ftp/360, 0) * t1.dz_dqgh_use_days
				   )
				   ,tmp_ftp.ftp/360*3
				)*tmp_amt.realease_amount
		end as double )
	as cost_capital

	from ods.ods_bpms_biz_apply_order bao
	left join tmp_cc01 on bao.apply_no = tmp_cc01.apply_no
	left join tmp_cc02 on bao.apply_no = tmp_cc02.apply_no
	left join ods.ods_bpms_biz_fee_summary bfs on bao.apply_no = bfs.apply_no
	left join tmp_amount_use_day t1 on bao.apply_no = t1.apply_no
	left join tmp_slq_ftp on bao.apply_no = tmp_slq_ftp.apply_no
	left join tmp_dqgh_ftp on bao.apply_no = tmp_dqgh_ftp.apply_no
	left join tmp_ftp on bao.apply_no = tmp_ftp.apply_no
	left join tmp_amt on bao.apply_no = tmp_amt.apply_no
	left join tmp_return_date trd on bao.apply_no = trd.apply_no
	where  bfs.borrowing_value_date >= '2019-06-01'
	and tmp_ftp.pre_rate = 0

	union all

	-- -----------------------------有前置费------------------------------------------
	select
	bao.apply_no
	,bao.product_name
	,tmp_ftp.ftp_pre_rate
	,tmp_ftp.ftp_pre_amount
	,cast(
		case when tmp_cc01.rebate_date_hk is not null and trd.return_date is not null
			 then  (
				 		  nvl(tmp_ftp.ftp/360 -- FTP
				 		  *(case when bfs.platform_value_date is not null and bfs.platform_value_date <> ""
				 		  			then if(datediff(regexp_replace(cast(trd.return_date as string), "/", "-"), regexp_replace(cast(bfs.platform_value_date as string), "/", "-"))>1, datediff(regexp_replace(cast(trd.return_date as string), "/", "-"), regexp_replace(cast(bfs.platform_value_date as string), "/", "-")), 1)
				 		  		else 0 end )
					 	  ,0)
					 	  -- 赎楼前垫资 计算
					 	  +nvl(tmp_slq_ftp.ftp/360
					 	  *t1.dz_slq_use_days
					 	  , 0)
					 	  -- 到期归还垫资
					 	  +nvl(tmp_dqgh_ftp.ftp/360
					 	  *t1.dz_dqgh_use_days
					 	  , 0)
				    )*tmp_amt.realease_amount
			 		+ nvl(tmp_ftp.pre_rate*tmp_amt.realease_amount, 0)

		    else
			   (
			 	 tmp_ftp.ftp/360
				  -- 实际借款期限 - 赎楼前垫资用款天数 - 到期归还垫资用款天数
		  			*(nvl(t1.use_days, 0) - nvl(t1.dz_slq_use_days, 0) - nvl(t1.dz_dqgh_use_days, 0))

				 	  -- 赎楼前垫资 计算
				 	+nvl(tmp_slq_ftp.ftp/360, 0)*t1.dz_slq_use_days

				 	  -- 到期归还垫资
			 	    +nvl(tmp_dqgh_ftp.ftp/360, 0) * t1.dz_dqgh_use_days

			   )*tmp_amt.realease_amount
			   + nvl(tmp_ftp.pre_rate*tmp_amt.realease_amount, 0)
		end as double )
	as cost_capital

	from ods.ods_bpms_biz_apply_order bao
	left join tmp_cc01 on bao.apply_no = tmp_cc01.apply_no
	left join tmp_cc02 on bao.apply_no = tmp_cc02.apply_no
	left join ods.ods_bpms_biz_fee_summary bfs on bao.apply_no = bfs.apply_no
	left join tmp_amount_use_day t1 on bao.apply_no = t1.apply_no
	left join tmp_slq_ftp on bao.apply_no = tmp_slq_ftp.apply_no
	left join tmp_dqgh_ftp on bao.apply_no = tmp_dqgh_ftp.apply_no
	left join tmp_ftp on bao.apply_no = tmp_ftp.apply_no
	left join tmp_amt on bao.apply_no = tmp_amt.apply_no
	left join tmp_return_date trd on bao.apply_no = trd.apply_no
	where  bfs.borrowing_value_date >= '2019-06-01'
	and tmp_ftp.pre_rate > 0

	union all

	-- -----------------------------20190601之前逻辑------------------------------------------
	select
	bao.apply_no
	,bao.product_name
	,0 ftp_pre_rate
	,0 ftp_pre_amount
	,cast(
		case when tmp_cc01.rebate_date_hk is not null and trd.return_date is not null
			 then  (

				 		  nvl(tmp_old_ftp.ftp/360 -- FTP
				 		  *(case when bfs.platform_value_date is not null and bfs.platform_value_date <> ""
				 		  			then if(datediff(regexp_replace(cast(trd.return_date as string), "/", "-"), regexp_replace(cast(bfs.platform_value_date as string), "/", "-"))>1, datediff(regexp_replace(cast(trd.return_date as string), "/", "-"), regexp_replace(cast(bfs.platform_value_date as string), "/", "-")), 1)
				 		  		else 0 end )
					 	  , 0)
					 	  -- 赎楼前垫资 计算
					 	  +nvl(tmp_old_slq_ftp.ftp/360
					 	  	*t1.dz_slq_use_days
					 	  , 0)
					 	  -- 到期归还垫资
					 	  +nvl(tmp_old_dqgh_ftp.ftp/360
					 	  *t1.dz_dqgh_use_days
					 	  , 0)
				    )*tmp_amt.realease_amount


		    else
			   (
			 	   tmp_old_ftp.ftp/360
				  -- 实际借款期限 - 赎楼前垫资用款天数 - 到期归还垫资用款天数
		  			*(nvl(t1.use_days, 0) - nvl(t1.dz_slq_use_days, 0) - nvl(t1.dz_dqgh_use_days, 0))

				 	  -- 赎楼前垫资 计算
				 	+nvl(tmp_old_slq_ftp.ftp/360, 0)*t1.dz_slq_use_days

				 	  -- 到期归还垫资
			 	    +nvl(tmp_old_dqgh_ftp.ftp/360, 0) * t1.dz_dqgh_use_days

			   )*tmp_amt.realease_amount
		end  as double )
	as cost_capital

	from ods.ods_bpms_biz_apply_order bao
	left join tmp_cc01 on bao.apply_no = tmp_cc01.apply_no
	left join tmp_cc02 on bao.apply_no = tmp_cc02.apply_no
	left join ods.ods_bpms_biz_fee_summary bfs on bao.apply_no = bfs.apply_no
	left join tmp_amount_use_day t1 on bao.apply_no = t1.apply_no
	left join tmp_old_slq_ftp on bao.apply_no = tmp_old_slq_ftp.apply_no
	left join tmp_old_dqgh_ftp on bao.apply_no = tmp_old_dqgh_ftp.apply_no
	left join tmp_old_ftp on bao.apply_no = tmp_old_ftp.apply_no
	left join tmp_amt on bao.apply_no = tmp_amt.apply_no
	left join tmp_return_date trd on bao.apply_no = trd.apply_no
	where bfs.borrowing_value_date < '2019-06-01'
),

tmp_opinion_desc as(
select
tmp.PROC_INST_ID_
,min(case when tmp.TASK_KEY_ = 'CostConfirm' then tmp.COMPLETE_TIME_ end ) costconfirm_time -- 缴费确认时间
,min(case when tmp.TASK_KEY_ = 'AgreeLoanMark' then tmp.COMPLETE_TIME_ end ) agreeloanmark_time  --  同贷信息登记时间
,min(case when tmp.TASK_KEY_ = 'UserTask5' then tmp.COMPLETE_TIME_ end ) send_loan_command_time_v1 -- 发送放款指令时间
,min(case when tmp.TASK_KEY_ = 'SendLoanCommand' then tmp.COMPLETE_TIME_ end ) send_loan_command_time -- 发送放款指令时间
,min(case when tmp.TASK_KEY_ = 'TransferMark' then tmp.COMPLETE_TIME_ end ) transfer_mark_time -- 放款登记时间
from (
  select
    b.*, row_number() over(partition by b.PROC_INST_ID_, b.TASK_KEY_ order by b.COMPLETE_TIME_ desc ) rank
    from ods.ods_bpms_bpm_check_opinion b
    where TASK_KEY_ in ('CostConfirm', 'AgreeLoanMark', 'UserTask5', 'SendLoanCommand', 'TransferMark')
) tmp

where rank = 1
group by tmp.PROC_INST_ID_
),

tmp_csc1 as
(
  select * from
  (
	  select a.apply_no, a.trans_day,  --交易日
	  ROW_NUMBER() OVER(PARTITION BY apply_no ORDER BY a.trans_day asc) rank
	  from ods.ods_bpms_c_cost_trade a
	  where a.trans_type = "CSC1"
  ) as a
  where a.rank = 1
)

insert overwrite table dwd.tmp_order_cost_capital_fr
select
bao.apply_no
,bao.product_name
,tcc.cost_capital
,cast(tcc.ftp_pre_rate as double) ftp_pre_rate  -- 前置费率
,cast(tcc.ftp_pre_amount as double)  ftp_pre_amount -- 前置金额
from ods.ods_bpms_biz_apply_order bao
left join tmp_cost_capital tcc on bao.apply_no = tcc.apply_no
left join tmp_opinion_desc opid on bao.flow_instance_id = opid.PROC_INST_ID_
LEFT JOIN ods.ods_bpms_biz_fee_summary bfs on bao.apply_no = bfs.apply_no
LEFT JOIN ods.ods_bpms_biz_isr_mixed_common bim on bao.apply_no = bim.apply_no
left join tmp_csc1 tc1 on bao.apply_no = tc1.apply_no
left join ods.ods_bpms_bpm_check_opinion_common_ex_wx t666 on t666.apply_no=bao.apply_no
left join ods.ods_bpms_c_fund_module_common cfm on cfm.apply_no=bao.apply_no;