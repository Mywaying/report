set hive.auto.convert.join = true;
with t1 as (
	select 
	a.apply_no
	,case when datediff(nvl(regexp_replace(a.floorAdvance_ret_date, "/", "-"), from_unixtime(unix_timestamp(),'yyyy-MM-dd')), regexp_replace(a.floorAdvance_fin_date, "/", "-")) is null then 0
	      when datediff(nvl(regexp_replace(a.floorAdvance_ret_date, "/", "-"), from_unixtime(unix_timestamp(),'yyyy-MM-dd')), regexp_replace(a.floorAdvance_fin_date, "/", "-")) in (0, 1) then 1
	      else datediff(nvl(regexp_replace(a.floorAdvance_ret_date, "/", "-"), from_unixtime(unix_timestamp(),'yyyy-MM-dd')), regexp_replace(a.floorAdvance_fin_date, "/", "-"))
	 end dz_slq_use_days -- 赎楼前垫资用款天数

	,case when datediff(nvl(regexp_replace(a.expireAdvance_ret_date, "/", "-"), from_unixtime(unix_timestamp(),'yyyy-MM-dd')), regexp_replace(a.expireAdvance_fin_date, "/", "-")) is null then 0
	      when datediff(nvl(regexp_replace(a.expireAdvance_ret_date, "/", "-"), from_unixtime(unix_timestamp(),'yyyy-MM-dd')), regexp_replace(a.expireAdvance_fin_date, "/", "-")) in (0, 1) then 1
	      else datediff(nvl(regexp_replace(a.expireAdvance_ret_date, "/", "-"), from_unixtime(unix_timestamp(),'yyyy-MM-dd')), regexp_replace(a.expireAdvance_fin_date, "/", "-"))
	 end dz_dqgh_use_days -- 到期归还垫资用款天数

	,case when a.max_payment_time is null then datediff(nvl(to_date(regexp_replace(a.max_payment_time, "/", "-")), from_unixtime(unix_timestamp(),'yyyy-MM-dd')), regexp_replace(a.fk_date, "/", "-"))+1   
      	  when a.max_payment_time is not null then datediff(nvl(to_date(regexp_replace(a.max_payment_time, "/", "-")), from_unixtime(unix_timestamp(),'yyyy-MM-dd')), regexp_replace(a.fk_date, "/", "-"))
    end use_days -- 实际借款期限
	from dwd.tmp_dwd_order_finance a
),

ioof as ( 
	select 
	a.apply_no
	,ioof.ftp
	from dwd.tmp_dwd_order_finance a
	join dim.dim_order_organization_ftp ioof on a.partner_insurance_name = ioof.organization_name and a.is_jms = ioof.is_jms 
	where a.fk_date >= ioof.start_date and a.fk_date < ioof.end_date 
	),

ioof_dz_slq as (
	select 
	a.apply_no
	,ioof.ftp
	from dwd.tmp_dwd_order_finance a
	join dim.dim_order_organization_ftp ioof on ioof.organization_name = "垫资" and a.is_jms = ioof.is_jms 
	where a.floorAdvance_fin_date >= ioof.start_date and a.floorAdvance_fin_date < ioof.end_date 
),

ioof_dz_dqgh as (
	select 
	a.apply_no
	,ioof.ftp
	from dwd.tmp_dwd_order_finance a
	join dim.dim_order_organization_ftp ioof on ioof.organization_name = "垫资" and a.is_jms = ioof.is_jms 
	where a.expireAdvance_fin_date >= ioof.start_date and a.expireAdvance_fin_date < ioof.end_date 
) 


-- insert overwrite table dwd.`tmp_dwd_order_finance_2` 
select 
a.`apply_no`
,case when a.product_term_and_charge_way in ("固定期限", "固定金额", "固定费率") 
			then (CASE WHEN a.fk_date is not null  THEN a.`ddfwf_ys` ELSE null END )  
      when a.product_term_and_charge_way in ("按天计息", "区间计息")
		    then (CASE WHEN a.fk_date is not null THEN a.`ddfwf_ys`
			       WHEN a.fk_date is not null THEN a.`ddfwf_ys`
				   WHEN a.fk_date is not null  and (a.max_payment_time  is null )  
				   		THEN (a.`ddfwf_ys`*(DATEDIFF(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), regexp_replace(a.fk_date, "/", "-"))+1)/(cast(a.`fixed_term` as smallint)) ) 
			       WHEN a.fk_date is not null  and (a.max_payment_time  is null ) 
			       		THEN (a.`ddfwf_ys`*(DATEDIFF(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), regexp_replace(a.fk_date, "/", "-"))+1)/(cast(a.`fixed_term` as smallint)) )
				 ELSE 0 END )
 end baseFeeCalculateDaily_fee_value -- 公司服务费

,(CASE WHEN a.fk_date is not null OR a.max_payment_time is not null THEN a.`agency_difference`  ELSE null  END ) agency_difference -- 实收渠道费
,(CASE WHEN a.fk_date is not null OR a.max_payment_time is not null THEN a.`agency_channel`  ELSE null END) agency_channel -- 代收渠道费
,case when a.product_term_and_charge_way in ("固定期限", "固定金额", "固定费率")
		then (CASE WHEN a.fk_date is not null  then a.`should_plat_cost` else null end) 
	  when a.product_term_and_charge_way in ("按天计息", "区间计息")
	  	then (CASE WHEN  a.advance_return='是' AND a.fk_date is not null  and a.max_payment_time is not null THEN a.`should_plat_cost` 
				   WHEN  a.advance_return='是' AND a.fk_date is not null  and a.max_payment_time is not null THEN a.`should_plat_cost` 
			       WHEN  a.advance_return='是' AND a.fk_date is not null  and a.max_payment_time is null 

					     THEN (a.`should_plat_cost`
					     	 *(case when a.max_payment_time is null then datediff(nvl(to_date(regexp_replace(a.max_payment_time, "/", "-")),from_unixtime(unix_timestamp(),'yyyy-MM-dd')),regexp_replace(a.fk_date, "/", "-"))+1   
					                when a.max_payment_time is not null then datediff(nvl(to_date(regexp_replace(a.max_payment_time, "/", "-")),from_unixtime(unix_timestamp(),'yyyy-MM-dd')),regexp_replace(a.fk_date, "/", "-"))
					            end)/(cast(a.`product_term` as smallint)) )

			       WHEN  a.advance_return='是' AND a.fk_date is not null and a.max_payment_time is null 

			    		 THEN (a.`should_plat_cost`
			    		 	 *(case when a.max_payment_time is null then datediff(nvl(to_date(regexp_replace(a.max_payment_time, "/", "-")), from_unixtime(unix_timestamp(),'yyyy-MM-dd')), regexp_replace(a.fk_date, "/", "-"))+1   
						            when a.max_payment_time is not null then datediff(nvl(to_date(regexp_replace(a.max_payment_time, "/", "-")), from_unixtime(unix_timestamp(),'yyyy-MM-dd')), regexp_replace(a.fk_date, "/", "-"))
						       end)/(cast(a.`product_term` as smallint)) )

				   WHEN (a.advance_return='否' AND (a.`partner_insurance_name` like '%大桔网%' OR a.`partner_insurance_name` like '%中广核富盈%' ) )
						AND a.fk_date is not null and a.max_payment_time is not null  THEN  a.`should_plat_cost` 

				   WHEN (a.advance_return='否' AND (a.`partner_insurance_name` like '%大桔网%' OR a.`partner_insurance_name`  like '%中广核富盈%' ) )
					    AND a.fk_date is not null and a.max_payment_time is not null THEN a.`should_plat_cost`  

				   WHEN (a.advance_return='否' AND (a.`partner_insurance_name` like '%大桔网%' OR a.`partner_insurance_name`  like '%中广核富盈%' ) )
						AND a.fk_date is not null and a.max_payment_time is null   

						THEN (a.`should_plat_cost`
							*(case when a.max_payment_time is null then datediff(nvl(to_date(regexp_replace(a.max_payment_time, "/", "-")), from_unixtime(unix_timestamp(),'yyyy-MM-dd')), regexp_replace(a.fk_date, "/", "-"))+1   
					      		   when a.max_payment_time is not null then datediff(nvl(to_date(regexp_replace(a.max_payment_time, "/", "-")), from_unixtime(unix_timestamp(),'yyyy-MM-dd')), regexp_replace(a.fk_date, "/", "-"))
					          end)/(cast(a.`product_term` as smallint)) ) 

				   WHEN (a.advance_return='否' AND (a.`partner_insurance_name` like '%大桔网%' OR a.`partner_insurance_name`  like '%中广核富盈%' ) )
						AND a.fk_date is not null and a.max_payment_time  is null 

						THEN (a.`should_plat_cost`
							*(case when a.max_payment_time is null then datediff(nvl(to_date(regexp_replace(a.max_payment_time, "/", "-")), from_unixtime(unix_timestamp(),'yyyy-MM-dd')), regexp_replace(a.fk_date, "/", "-"))+1   
					      		   when a.max_payment_time is not null then datediff(nvl(to_date(regexp_replace(a.max_payment_time, "/", "-")), from_unixtime(unix_timestamp(),'yyyy-MM-dd')), regexp_replace(a.fk_date, "/", "-"))
					      	  end)/(cast(a.`product_term` as smallint)) ) 

				   WHEN a.advance_return='否' AND a.`partner_insurance_name` not like '%大桔网%' and a.`partner_insurance_name` not like '%中广核富盈%' 
						AND  a.fk_date is not null OR a.max_payment_time is not null  THEN a.`should_plat_cost` 

			       ELSE null 

			 END)
end should_plat_cost -- 平台服务费

,case when a.product_term_and_charge_way in ("固定期限", "固定金额", "固定费率")
		then (CASE 
			-- 已放款未还款
			WHEN a.fk_date is not null and  a.max_payment_time is null then (a.`ought_amount_plat_interest`*(cast(a.`fixed_term` as smallint)) /(cast(a.product_term as smallint)) ) 

			-- 已还款
			when a.fk_date is not null and a.max_payment_time is not null and a.return_date is not null  -- 已归还平台
			then a.ought_amount_plat_interest

			when a.fk_date is not null and a.max_payment_time is not null and a.return_date is null  -- 未归还平台
			then a.ought_amount_plat_interest*t1.use_days/(cast(a.product_term as smallint)) 
			else 0 END) 
	  when a.product_term_and_charge_way in ("按天计息", "区间计息")
	  	then (CASE WHEN a.advance_return='是' AND a.fk_date is not null and a.max_payment_time is not null  
				    	THEN a.`ought_amount_plat_interest`
				
				   WHEN a.advance_return='是' AND a.fk_date is not null and a.max_payment_time is not null 
						THEN a.`ought_amount_plat_interest`
			   
			       WHEN a.advance_return='是' AND a.fk_date is not null  and a.max_payment_time is null 
			    
					    THEN (a.`ought_amount_plat_interest`
					    	*(case when a.max_payment_time is null then datediff(nvl(to_date(regexp_replace(a.max_payment_time, "/", "-")), from_unixtime(unix_timestamp(),'yyyy-MM-dd') ), regexp_replace(a.fk_date, "/", "-"))+1   
					      when a.max_payment_time is not null then datediff(nvl(to_date(regexp_replace(a.max_payment_time, "/", "-")), from_unixtime(unix_timestamp(),'yyyy-MM-dd')), regexp_replace(a.fk_date, "/", "-"))
					      end)/(cast(a.`product_term` as smallint)) )
			   
			    WHEN a.advance_return='是' AND a.fk_date is not null and a.max_payment_time  is null 

					    THEN (a.`ought_amount_plat_interest`
					    	*(case when a.max_payment_time is null then datediff(nvl(to_date(regexp_replace(a.max_payment_time, "/", "-")), from_unixtime(unix_timestamp(),'yyyy-MM-dd')), regexp_replace(a.fk_date, "/", "-"))+1   
					      when a.max_payment_time is not null then datediff(nvl(to_date(regexp_replace(a.max_payment_time, "/", "-")), from_unixtime(unix_timestamp(),'yyyy-MM-dd')), regexp_replace(a.fk_date, "/", "-"))
					      end)/(cast(a.`product_term` as smallint)) )

				WHEN a.advance_return='否' AND a.fk_date is not null and a.max_payment_time is not null 
						THEN a.`ought_amount_plat_interest`
				
				WHEN a.advance_return='否' AND a.fk_date is not null and a.max_payment_time is null  
				
						THEN (a.`ought_amount_plat_interest`
							*(case when a.max_payment_time is null then datediff(nvl(to_date(regexp_replace(a.max_payment_time, "/", "-")), from_unixtime(unix_timestamp(),'yyyy-MM-dd')), regexp_replace(a.fk_date, "/", "-"))+1   
					      when a.max_payment_time is not null then datediff(nvl(to_date(regexp_replace(a.max_payment_time, "/", "-")), from_unixtime(unix_timestamp(),'yyyy-MM-dd')), regexp_replace(a.fk_date, "/", "-"))
					      end)/(cast(a.`product_term` as smallint)) ) 

				WHEN a.advance_return='否' AND a.fk_date is not null and a.max_payment_time is not null 
						THEN a.`ought_amount_plat_interest`
				
				WHEN a.advance_return='否' AND a.fk_date is not null and a.max_payment_time is null  

					THEN (a.`ought_amount_plat_interest`
						*(case when a.max_payment_time is null then datediff(nvl(to_date(regexp_replace(a.max_payment_time, "/", "-")), from_unixtime(unix_timestamp(),'yyyy-MM-dd')), regexp_replace(a.fk_date, "/", "-"))+1   
				      when a.max_payment_time is not null then datediff(nvl(to_date(regexp_replace(a.max_payment_time, "/", "-")), from_unixtime(unix_timestamp(),'yyyy-MM-dd')), regexp_replace(a.fk_date, "/", "-"))
				      end)/(cast(a.`product_term` as smallint)) ) 

				ELSE null 
			END) 

end ought_amount_plat_interest  -- 平台利息

,
case when a.product_term_and_charge_way in ("固定期限", "固定金额", "固定费率") 
		then (case 
				when a.fk_date is not null and a.max_payment_time is null 
					then  datediff(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), to_date(regexp_replace(a.borrowing_value_date, "/", "-"))) -- 已放款未还款   当天-起息日 
				when a.fk_date is not null and a.max_payment_time is not null 
					then datediff(a.max_payment_time, to_date(regexp_replace(a.borrowing_value_date, "/", "-"))) -- 已还款 回款日-起息日
				else 0 end) 
	 when a.product_term_and_charge_way in ("按天计息", "区间计息")
	 	then (case when a.max_payment_time is null 
	 				then datediff(nvl(to_date(regexp_replace(a.max_payment_time, "/", "-")), from_unixtime(unix_timestamp(),'yyyy-MM-dd')), regexp_replace(a.fk_date, "/", "-"))+1   
			       when a.max_payment_time is not null 
			     	then datediff(nvl(to_date(regexp_replace(a.max_payment_time, "/", "-")), from_unixtime(unix_timestamp(),'yyyy-MM-dd')), regexp_replace(a.fk_date, "/", "-"))
	          end)

end use_money_day -- 用款天数


,-- 客户已回款、 归还平台
case when a.max_payment_time is not null and a.return_date is not null 
	 then if(
	 		  (
		 		  ioof.ftp/360 -- FTP
		 		  *if(datediff(regexp_replace(a.return_date, "/", "-"), regexp_replace(a.platform_value_date, "/", "-"))>1, datediff(regexp_replace(a.return_date, "/", "-"), regexp_replace(a.platform_value_date, "/", "-")), 1)
			 	  -- 赎楼前垫资 计算
			 	  +nvl(ioof_dz_slq.ftp/360 , 0)
			 	  *t1.dz_slq_use_days
			 	  -- 到期归还垫资 
			 	  +nvl(ioof_dz_dqgh.ftp/360, 0)
			 	  *t1.dz_dqgh_use_days

		 	   ) > (ioof.ftp/360*3)

		 	 ,(    ioof.ftp/360 -- FTP
		 		  *if(datediff(regexp_replace(a.return_date, "/", "-"), regexp_replace(a.platform_value_date, "/", "-"))>1, datediff(regexp_replace(a.return_date, "/", "-"), regexp_replace(a.platform_value_date, "/", "-")), 1)
			 	  -- 赎楼前垫资 计算
			 	  +nvl(ioof_dz_slq.ftp/360 , 0)
			 	  *t1.dz_slq_use_days
			 	  -- 到期归还垫资 
			 	  +nvl(ioof_dz_dqgh.ftp/360, 0)
			 	  *t1.dz_dqgh_use_days
	           )

		 	 ,(ioof.ftp/360*3)
		    )*a.realease_amount


    else
	 if(
	 	( ioof.ftp/360
		  -- 实际借款期限 - 赎楼前垫资用款天数 - 到期归还垫资用款天数
  			*(nvl(t1.use_days, 0) - nvl(t1.dz_slq_use_days, 0) - nvl(t1.dz_dqgh_use_days, 0))

		 	  -- 赎楼前垫资 计算
		 	+nvl(ioof_dz_slq.ftp/360, 0)*t1.dz_slq_use_days

		 	  -- 到期归还垫资 
	 	    +nvl(ioof_dz_dqgh.ftp/360, 0) * t1.dz_dqgh_use_days

		) > (ioof.ftp/360*3)

		,( ioof.ftp/360
		 	-- 实际借款期限 - 赎楼前垫资用款天数 - 到期归还垫资用款天数
  			*(nvl(t1.use_days, 0) - nvl(t1.dz_slq_use_days, 0) - nvl(t1.dz_dqgh_use_days, 0))
		 	  -- 赎楼前垫资 计算
		 	+nvl(ioof_dz_slq.ftp/360, 0) * t1.dz_slq_use_days
		 	  -- 到期归还垫资 
		 	+nvl(ioof_dz_dqgh.ftp/360, 0) * t1.dz_dqgh_use_days
		  )

		,(ioof.ftp/360*3)

	   )*a.realease_amount
end   as cost_capital

from dwd.tmp_dwd_order_finance a
left join ioof on a.apply_no = ioof.apply_no
left join ioof_dz_slq on a.apply_no = ioof_dz_slq.apply_no
left join ioof_dz_dqgh on a.apply_no = ioof_dz_dqgh.apply_no

left join t1 on t1.apply_no=a.apply_no
where a.product_term_and_charge_way in ("固定期限", "固定金额", "固定费率", "按天计息", "区间计息") 