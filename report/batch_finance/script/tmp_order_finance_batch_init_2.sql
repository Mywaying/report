set hive.execution.engine=spark;
drop table if exists dwd.tmp_dwd_order_finance_2;
CREATE TABLE dwd.tmp_dwd_order_finance_2 (   
   apply_no STRING COMMENT '订单编号',
   basefeecalculatedaily_fee_value DOUBLE COMMENT '公司服务费',
   agency_difference DOUBLE COMMENT '实收渠道费',
   agency_channel DOUBLE COMMENT '代收渠道费',
   should_plat_cost DOUBLE COMMENT '平台服务费',
   ought_amount_plat_interest DOUBLE COMMENT '平台利息',
   use_money_day bigint comment '用款天数',
   cost_capital DOUBLE COMMENT '资金成本',
   ftp_pre_rate double comment '前置费率',
   ftp_pre_amount double comment '前置金额'
);
with t1 as (
	select 
	a.apply_no
	,case when datediff(nvl(regexp_replace(a.floorAdvance_ret_date, "/", "-"), from_unixtime(unix_timestamp(),'yyyy-MM-dd')), regexp_replace(a.floorAdvance_fin_date, "/", "-")) is null then 0
	      when datediff(nvl(regexp_replace(a.floorAdvance_ret_date, "/", "-"), from_unixtime(unix_timestamp(),'yyyy-MM-dd')), regexp_replace(a.floorAdvance_fin_date, "/", "-")) in (0, 1) then 0
	      else datediff(nvl(regexp_replace(a.floorAdvance_ret_date, "/", "-"), from_unixtime(unix_timestamp(),'yyyy-MM-dd')), regexp_replace(a.floorAdvance_fin_date, "/", "-"))
	 end dz_slq_use_days -- 赎楼前垫资用款天数

	,case when datediff(nvl(regexp_replace(a.expireAdvance_ret_date, "/", "-"), from_unixtime(unix_timestamp(),'yyyy-MM-dd')), regexp_replace(a.expireAdvance_fin_date, "/", "-")) is null then 0
	      when datediff(nvl(regexp_replace(a.expireAdvance_ret_date, "/", "-"), from_unixtime(unix_timestamp(),'yyyy-MM-dd')), regexp_replace(a.expireAdvance_fin_date, "/", "-")) in (0, 1) then 0
	      else datediff(nvl(regexp_replace(a.expireAdvance_ret_date, "/", "-"), from_unixtime(unix_timestamp(),'yyyy-MM-dd')), regexp_replace(a.expireAdvance_fin_date, "/", "-"))
	 end dz_dqgh_use_days -- 到期归还垫资用款天数

	,case when a.max_payment_time is null then datediff(nvl(to_date(regexp_replace(a.max_payment_time, "/", "-")), from_unixtime(unix_timestamp(),'yyyy-MM-dd')), regexp_replace(a.loan_time_jc, "/", "-"))+1   
      	  when a.max_payment_time is not null then datediff(nvl(to_date(regexp_replace(a.max_payment_time, "/", "-")), from_unixtime(unix_timestamp(),'yyyy-MM-dd')), regexp_replace(a.loan_time_jc, "/", "-"))
    end use_days -- 实际借款期限
	from dwd.tmp_dwd_order_finance a
),

tmp_biz_fee_detial as (
  select 
  bfd.apply_no
  ,max(case when bfd.fee_define_no = "rakebackFeeCalculateDaily" then bfd.fee_value end) rakebackFeeCalculateDaily -- 返佣费-按天计息
  ,max(case when bfd.fee_define_no = "rakebackFeeFixedTerm" then bfd.fee_value end) rakebackFeeFixedTerm -- 返佣费-固定期限
  ,max(case when bfd.fee_define_no = "rakebackFee" then bfd.fee_value end) rakeBackFee -- 返佣费
  from ods_bpms_biz_fee_detial bfd
  group by bfd.apply_no
)

insert overwrite table dwd.`tmp_dwd_order_finance_2` 
select 
a.`apply_no`
,case when a.product_term_and_charge_way in ("固定期限", "固定金额", "固定费率") 
			then (CASE WHEN a.loan_time_jc is not null  THEN a.`ddfwf_ys` ELSE null END )  
      when a.product_term_and_charge_way in ("按天计息", "区间计息")
		    then (CASE WHEN a.loan_time_jc is not null THEN a.`ddfwf_ys`
			       WHEN a.loan_time_jc is not null THEN a.`ddfwf_ys`
				   WHEN a.loan_time_jc is not null  and (a.max_payment_time  is null )  
				   		THEN (a.`ddfwf_ys`*(DATEDIFF(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), regexp_replace(a.loan_time_jc, "/", "-"))+1)/a.`fixed_term`) 
			       WHEN a.loan_time_jc is not null  and (a.max_payment_time  is null ) 
			       		THEN (a.`ddfwf_ys`*(DATEDIFF(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), regexp_replace(a.loan_time_jc, "/", "-"))+1)/a.`fixed_term`)
				 ELSE 0 END )
 end baseFeeCalculateDaily_fee_value -- 公司服务费

,(CASE WHEN a.loan_time_jc is not null OR a.max_payment_time is not null 
	THEN nvl(tbfd.rakebackFeeCalculateDaily, 0) + nvl(tbfd.rakebackFeeFixedTerm, 0) + nvl(tbfd.rakeBackFee, 0)
	ELSE null  END ) agency_difference -- 实收渠道费
,(CASE WHEN a.loan_time_jc is not null OR a.max_payment_time is not null THEN a.`agency_channel`  ELSE null END) agency_channel -- 代收渠道费
,case when a.product_term_and_charge_way in ("固定期限", "固定金额", "固定费率")
		then (CASE WHEN a.loan_time_jc is not null  then a.`should_plat_cost` else null end) 
	  when a.product_term_and_charge_way in ("按天计息", "区间计息")
	  	then (CASE WHEN  a.advance_return='是' AND a.loan_time_jc is not null  and a.max_payment_time is not null THEN a.`should_plat_cost` 
				   WHEN  a.advance_return='是' AND a.loan_time_jc is not null  and a.max_payment_time is not null THEN a.`should_plat_cost` 
			       WHEN  a.advance_return='是' AND a.loan_time_jc is not null  and a.max_payment_time is null 

					     THEN (a.`should_plat_cost`
					     	 *(case when a.max_payment_time is null then datediff(nvl(to_date(regexp_replace(a.max_payment_time, "/", "-")),from_unixtime(unix_timestamp(),'yyyy-MM-dd')),regexp_replace(a.loan_time_jc, "/", "-"))+1   
					                when a.max_payment_time is not null then datediff(nvl(to_date(regexp_replace(a.max_payment_time, "/", "-")),from_unixtime(unix_timestamp(),'yyyy-MM-dd')),regexp_replace(a.loan_time_jc, "/", "-"))
					            end)/a.`product_term`)

			       WHEN  a.advance_return='是' AND a.loan_time_jc is not null and a.max_payment_time is null 

			    		 THEN (a.`should_plat_cost`
			    		 	 *(case when a.max_payment_time is null then datediff(nvl(to_date(regexp_replace(a.max_payment_time, "/", "-")), from_unixtime(unix_timestamp(),'yyyy-MM-dd')), regexp_replace(a.loan_time_jc, "/", "-"))+1   
						            when a.max_payment_time is not null then datediff(nvl(to_date(regexp_replace(a.max_payment_time, "/", "-")), from_unixtime(unix_timestamp(),'yyyy-MM-dd')), regexp_replace(a.loan_time_jc, "/", "-"))
						       end)/a.`product_term`)

				   WHEN (a.advance_return='否' AND (a.`partner_insurance_name` like '%大桔网%' OR a.`partner_insurance_name` like '%中广核富盈%' ) )
						AND a.loan_time_jc is not null and a.max_payment_time is not null  THEN  a.`should_plat_cost` 

				   WHEN (a.advance_return='否' AND (a.`partner_insurance_name` like '%大桔网%' OR a.`partner_insurance_name`  like '%中广核富盈%' ) )
					    AND a.loan_time_jc is not null and a.max_payment_time is not null THEN a.`should_plat_cost`  

				   WHEN (a.advance_return='否' AND (a.`partner_insurance_name` like '%大桔网%' OR a.`partner_insurance_name`  like '%中广核富盈%' ) )
						AND a.loan_time_jc is not null and a.max_payment_time is null   

						THEN (a.`should_plat_cost`
							*(case when a.max_payment_time is null then datediff(nvl(to_date(regexp_replace(a.max_payment_time, "/", "-")), from_unixtime(unix_timestamp(),'yyyy-MM-dd')), regexp_replace(a.loan_time_jc, "/", "-"))+1   
					      		   when a.max_payment_time is not null then datediff(nvl(to_date(regexp_replace(a.max_payment_time, "/", "-")), from_unixtime(unix_timestamp(),'yyyy-MM-dd')), regexp_replace(a.loan_time_jc, "/", "-"))
					          end)/a.`product_term`) 

				   WHEN (a.advance_return='否' AND (a.`partner_insurance_name` like '%大桔网%' OR a.`partner_insurance_name`  like '%中广核富盈%' ) )
						AND a.loan_time_jc is not null and a.max_payment_time  is null 

						THEN (a.`should_plat_cost`
							*(case when a.max_payment_time is null then datediff(nvl(to_date(regexp_replace(a.max_payment_time, "/", "-")), from_unixtime(unix_timestamp(),'yyyy-MM-dd')), regexp_replace(a.loan_time_jc, "/", "-"))+1   
					      		   when a.max_payment_time is not null then datediff(nvl(to_date(regexp_replace(a.max_payment_time, "/", "-")), from_unixtime(unix_timestamp(),'yyyy-MM-dd')), regexp_replace(a.loan_time_jc, "/", "-"))
					      	  end)/a.`product_term`) 

				   WHEN a.advance_return='否' AND a.`partner_insurance_name` not like '%大桔网%' and a.`partner_insurance_name` not like '%中广核富盈%' 
						AND  a.loan_time_jc is not null OR a.max_payment_time is not null  THEN a.`should_plat_cost` 

			       ELSE null 

			 END)
end should_plat_cost -- 平台服务费

,case when a.product_term_and_charge_way in ("固定期限", "固定金额", "固定费率")
		then (CASE 
			-- 已放款未还款
			WHEN a.loan_time_jc is not null and  a.max_payment_time is null then (a.`ought_amount_plat_interest`*a.`fixed_term`/a.product_term) 

			-- 已还款
			when a.loan_time_jc is not null and a.max_payment_time is not null and a.return_date is not null  -- 已归还平台
			then a.ought_amount_plat_interest

			when a.loan_time_jc is not null and a.max_payment_time is not null and a.return_date is null  -- 未归还平台
			then a.ought_amount_plat_interest*t1.use_days/a.product_term
			else 0 END) 
	  when a.product_term_and_charge_way in ("按天计息", "区间计息")
	  	then (CASE WHEN a.advance_return='是' AND a.loan_time_jc is not null and a.max_payment_time is not null  
				    	THEN a.`ought_amount_plat_interest`
			   
			       WHEN a.advance_return='是' AND a.loan_time_jc is not null  and a.max_payment_time is null 
			    
					    THEN (a.`ought_amount_plat_interest`
					    	*(case when a.max_payment_time is null then datediff(nvl(to_date(regexp_replace(a.max_payment_time, "/", "-")), from_unixtime(unix_timestamp(),'yyyy-MM-dd') ), regexp_replace(a.loan_time_jc, "/", "-"))+1   
					      when a.max_payment_time is not null then datediff(nvl(to_date(regexp_replace(a.max_payment_time, "/", "-")), from_unixtime(unix_timestamp(),'yyyy-MM-dd')), regexp_replace(a.loan_time_jc, "/", "-"))
					      end)/a.`product_term`)
			   
			    WHEN a.advance_return='是' AND a.loan_time_jc is not null and a.max_payment_time  is null 

					    THEN (a.`ought_amount_plat_interest`
					    	*(case when a.max_payment_time is null then datediff(nvl(to_date(regexp_replace(a.max_payment_time, "/", "-")), from_unixtime(unix_timestamp(),'yyyy-MM-dd')), regexp_replace(a.loan_time_jc, "/", "-"))+1   
					      when a.max_payment_time is not null then datediff(nvl(to_date(regexp_replace(a.max_payment_time, "/", "-")), from_unixtime(unix_timestamp(),'yyyy-MM-dd')), regexp_replace(a.loan_time_jc, "/", "-"))
					      end)/a.`product_term`)

				WHEN a.advance_return='否' AND a.loan_time_jc is not null and a.max_payment_time is not null 
						THEN a.`ought_amount_plat_interest`
				
				WHEN a.advance_return='否' AND a.loan_time_jc is not null and a.max_payment_time is null  
				
						THEN (a.`ought_amount_plat_interest`
							*(case when a.max_payment_time is null then datediff(nvl(to_date(regexp_replace(a.max_payment_time, "/", "-")), from_unixtime(unix_timestamp(),'yyyy-MM-dd')), regexp_replace(a.loan_time_jc, "/", "-"))+1   
					      when a.max_payment_time is not null then datediff(nvl(to_date(regexp_replace(a.max_payment_time, "/", "-")), from_unixtime(unix_timestamp(),'yyyy-MM-dd')), regexp_replace(a.loan_time_jc, "/", "-"))
					      end)/a.`product_term`) 

				WHEN a.advance_return='否' AND a.loan_time_jc is not null and a.max_payment_time is not null 
						THEN a.`ought_amount_plat_interest`
				
				WHEN a.advance_return='否' AND a.loan_time_jc is not null and a.max_payment_time is null  

					THEN (a.`ought_amount_plat_interest`
						*(case when a.max_payment_time is null then datediff(nvl(to_date(regexp_replace(a.max_payment_time, "/", "-")), from_unixtime(unix_timestamp(),'yyyy-MM-dd')), regexp_replace(a.loan_time_jc, "/", "-"))+1   
				      when a.max_payment_time is not null then datediff(nvl(to_date(regexp_replace(a.max_payment_time, "/", "-")), from_unixtime(unix_timestamp(),'yyyy-MM-dd')), regexp_replace(a.loan_time_jc, "/", "-"))
				      end)/a.`product_term`) 

				ELSE null 
			END) 

end ought_amount_plat_interest  -- 平台利息
,
case when a.product_term_and_charge_way in ("固定期限", "固定金额", "固定费率") 
		then (case 
				when a.loan_time_jc is not null and a.max_payment_time is null 
					then  datediff(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), to_date(regexp_replace(a.borrowing_value_date, "/", "-"))) -- 已放款未还款   当天-起息日 
				when a.loan_time_jc is not null and a.max_payment_time is not null 
					then datediff(a.max_payment_time, to_date(regexp_replace(a.borrowing_value_date, "/", "-"))) -- 已还款 回款日-起息日
				else 0 end) 
	 when a.product_term_and_charge_way in ("按天计息", "区间计息")
	 	then (case when a.max_payment_time is null 
	 				then datediff(nvl(to_date(regexp_replace(a.max_payment_time, "/", "-")), from_unixtime(unix_timestamp(),'yyyy-MM-dd')), regexp_replace(a.loan_time_jc, "/", "-"))+1   
			     when a.max_payment_time is not null 
			     	then datediff(nvl(to_date(regexp_replace(a.max_payment_time, "/", "-")), from_unixtime(unix_timestamp(),'yyyy-MM-dd')), regexp_replace(a.loan_time_jc, "/", "-"))
			 end)
end use_money_day -- 用款天数
,ofc.cost_capital -- 资金成本
,ofc.ftp_pre_rate -- 前置费率
,ofc.ftp_pre_amount -- 前置金额
from dwd.tmp_dwd_order_finance a
left join ods.ods_orders_finance_common ofc on a.apply_no = ofc.apply_no
left join t1 on t1.apply_no=a.apply_no
left join tmp_biz_fee_detial tbfd on a.apply_no = tbfd.apply_no
where a.product_term_and_charge_way in ("固定期限", "固定金额", "固定费率", "按天计息", "区间计息")