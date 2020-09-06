use dws;
CREATE TABLE if not exists dws.tmp_insurance_info (apply_no STRING,   product_name STRING,   tail_release_node_name STRING,   city_name STRING,   seller_name STRING,   release_amount DOUBLE,   ori_loan_bank_name STRING,   new_loan_bank_name STRING,   check_time TIMESTAMP,   check_user_name STRING,   check_status_name STRING,   check_option STRING,   litigation_one_count BIGINT,   litigation_all_count BIGINT,   litigation_company_count BIGINT,   credit_parse_false_count BIGINT,   query_4_10_count BIGINT,   query_10_count BIGINT,   loan_1_3_count BIGINT,   loan_3_count BIGINT,   black BIGINT,   credit_over BIGINT,   queries_count BIGINT,   overdue_count BIGINT,   uncleared_count BIGINT,   unusual_count BIGINT,   hit_count BIGINT,   m1hit_count BIGINT,   credit_over_count BIGINT,   unsecured_count BIGINT,   non_bank_count BIGINT,   borrower_individual_count BIGINT,   property_individual_count BIGINT,   housing_type_count BIGINT,   property_over_count BIGINT,   age_upper_count BIGINT,   age_lower_count BIGINT,   mainland_count BIGINT,   liabilities_over_count BIGINT,   newloan_7_count BIGINT,   flooramount_105_count BIGINT,   aomount_upper_count BIGINT,   newloan_individual_count BIGINT,   newloan_non_agency_count BIGINT ) STORED AS parquet;
insert overwrite table dws.tmp_insurance_info
select
bao.`apply_no`,
bao.`product_name`,
bim.`tail_release_node_name`,
bao.`city_name`,
bao.`seller_name`,
(case
   when bao.product_type='现金类产品' then cast (cfm.con_funds_cost as double)
   when bao.product_name like'买付保%' then cast (bfs.guarantee_amount  as double)
   else nvl(bnl.biz_loan_amount, 0)
 end) release_amount,
bol.ori_loan_bank_name,
bnl.new_loan_bank_name,
nvl(t666.mancheck_time,t666.interview_time_min) check_time,
nvl(t666.mancheck_user_name,t666.interview_user_name_min) check_user_name,
nvl(t666.mancheck_status_name,t666.interview_status_name_min) check_status_name,
nvl(t666.mancheck_option,t666.interview_option_min) check_option,
T_litigation_one_count.`litigation_one_count`,
T_litigation_all_count.`litigation_all_count`,
T_litigation_company_count.`litigation_company_count`,
T_credit_parse_false_count.`credit_parse_false_count`,
T_query_4_10_count.`query_4_10_count`,
T_query_10_count.`query_10_count`,
T_loan_1_3_count.`loan_1_3_count`,
T_loan_3_count.`loan_3_count`,
T_black.`black`,
T_credit_over.`credit_over`,
T_queries_count.`queries_count`,
T_overdue_count.`overdue_count`,
T_uncleared_count.`uncleared_count`,
T_unusual_count.`unusual_count`,
T_hit_count.`hit_count`,
T_m1hit_count.`m1hit_count`,
T_credit_over_count.`credit_over_count`,
T_unsecured_count.`unsecured_count`,
T_non_bank_count.`non_bank_count`,
T_borrower_individual_count.`borrower_individual_count`,
T_property_individual_count.`property_individual_count`,
T_housing_type_count.`housing_type_count`,
T_property_over_count.`property_over_count`,
T_age_upper_count.`age_upper_count`,
T_age_lower_count.`age_lower_count`,
T_mainland_count.`mainland_count`,
T_liabilities_over_count.`liabilities_over_count`,
T_newloan_7_count.`newloan_7_count`,
T_flooramount_105_count.`flooramount_105_count`,
T_aomount_upper_count.`aomount_upper_count`,
T_newloan_individual_count.`newloan_individual_count`,
T_newloan_non_agency_count.`newloan_non_agency_count`
from ods.ods_bpms_biz_apply_order_common bao
left join ods.ods_bpms_c_fund_module_common cfm on cfm.apply_no=bao.apply_no
LEFT JOIN ods.ods_bpms_biz_isr_mixed_common bim on bim.apply_no=bao.apply_no
LEFT JOIN ods.ods_bpms_biz_fee_summary bfs on bfs.apply_no=bao.apply_no
left join (select * from ods.ods_bpms_biz_ori_loan_common where rn=1) bol on bol.apply_no=bao.apply_no
left join (select * from ods.ods_bpms_biz_new_loan_common where rn=1) bnl on bnl.apply_no=bao.apply_no
left join ods.ods_bpms_bpm_check_opinion_common_ex_wx t666 on t666.apply_no=bao.apply_no
left join (select apply_no,count(*) litigation_one_count from ods.ods_bpms_biz_hit_rule_common where rule_name_tag in (166,167,169,173,176,179,188,240,243,329)  group by apply_no) T_litigation_one_count on T_litigation_one_count.apply_no=bao.apply_no
left join (select apply_no,count(*) litigation_all_count from ods.ods_bpms_biz_hit_rule_common where rule_name_tag in (189,190,191,192,193,194,196,197,198,201,245,248,268)  group by apply_no) T_litigation_all_count on T_litigation_all_count.apply_no=bao.apply_no
left join (select apply_no,count(*) litigation_company_count from ods.ods_bpms_biz_hit_rule_common where rule_name_tag in (237,266)  group by apply_no) T_litigation_company_count on T_litigation_company_count.apply_no=bao.apply_no
left join (select apply_no,count(*) credit_parse_false_count from ods.ods_bpms_biz_hit_rule_common where rule_name_tag in (106)  group by apply_no) T_credit_parse_false_count on T_credit_parse_false_count.apply_no=bao.apply_no
left join (select apply_no,count(*) query_4_10_count from ods.ods_bpms_biz_hit_rule_common where rule_name_tag in (241,253)  group by apply_no) T_query_4_10_count on T_query_4_10_count.apply_no=bao.apply_no
left join (select apply_no,count(*) query_10_count from ods.ods_bpms_biz_hit_rule_common where rule_name_tag in (242,259)  group by apply_no) T_query_10_count on T_query_10_count.apply_no=bao.apply_no
left join (select apply_no,count(*) loan_1_3_count from ods.ods_bpms_biz_hit_rule_common where rule_name_tag in (134,247,262)  group by apply_no) T_loan_1_3_count on T_loan_1_3_count.apply_no=bao.apply_no
left join (select apply_no,count(*) loan_3_count from ods.ods_bpms_biz_hit_rule_common where rule_name_tag in (295)  group by apply_no) T_loan_3_count on T_loan_3_count.apply_no=bao.apply_no
left join (select apply_no,count(*) black from ods.ods_bpms_biz_hit_rule_common where rule_name_tag in (199)  group by apply_no) T_black on T_black.apply_no=bao.apply_no
left join (select apply_no,count(*) credit_over from ods.ods_bpms_biz_hit_rule_common where rule_name_tag in (77,162)  group by apply_no) T_credit_over on T_credit_over.apply_no=bao.apply_no
left join (select apply_no,count(*) queries_count from ods.ods_bpms_biz_hit_rule_common where rule_name_tag in (67)  group by apply_no) T_queries_count on T_queries_count.apply_no=bao.apply_no
left join (select apply_no,count(*) overdue_count from ods.ods_bpms_biz_hit_rule_common where rule_name_tag in (256,263,267,269,282)  group by apply_no) T_overdue_count on T_overdue_count.apply_no=bao.apply_no
left join (select apply_no,count(*) uncleared_count from ods.ods_bpms_biz_hit_rule_common where rule_name_tag in (29)  group by apply_no) T_uncleared_count on T_uncleared_count.apply_no=bao.apply_no
left join (select apply_no,count(*) unusual_count from ods.ods_bpms_biz_hit_rule_common where rule_name_tag in (251,257,261,265)  group by apply_no) T_unusual_count on T_unusual_count.apply_no=bao.apply_no
left join (select apply_no,count(*) hit_count from ods.ods_bpms_biz_hit_rule_common where rule_name_tag in (180,341)  group by apply_no) T_hit_count on T_hit_count.apply_no=bao.apply_no
left join (select apply_no,count(*) m1hit_count from ods.ods_bpms_biz_hit_rule_common where rule_name_tag in (171,178)  group by apply_no) T_m1hit_count on T_m1hit_count.apply_no=bao.apply_no
left join (select apply_no,count(*) credit_over_count from ods.ods_bpms_biz_hit_rule_common where rule_name_tag in (135)  group by apply_no) T_credit_over_count on T_credit_over_count.apply_no=bao.apply_no
left join (select apply_no,count(*) unsecured_count from ods.ods_bpms_biz_hit_rule_common where rule_name_tag in (216)  group by apply_no) T_unsecured_count on T_unsecured_count.apply_no=bao.apply_no
left join (select apply_no,count(*) non_bank_count from ods.ods_bpms_biz_hit_rule_common where rule_name_tag in (255,236)  group by apply_no) T_non_bank_count on T_non_bank_count.apply_no=bao.apply_no
left join (select apply_no,count(*) borrower_individual_count from ods.ods_bpms_biz_hit_rule_common where rule_name_tag in (213)  group by apply_no) T_borrower_individual_count on T_borrower_individual_count.apply_no=bao.apply_no
left join (select apply_no,count(*) property_individual_count from ods.ods_bpms_biz_hit_rule_common where rule_name_tag in (220)  group by apply_no) T_property_individual_count on T_property_individual_count.apply_no=bao.apply_no
left join (select apply_no,count(*) housing_type_count from ods.ods_bpms_biz_hit_rule_common where rule_name_tag in (164,250,258)  group by apply_no) T_housing_type_count on T_housing_type_count.apply_no=bao.apply_no
left join (select apply_no,count(*) property_over_count from ods.ods_bpms_biz_hit_rule_common where rule_name_tag in (249,254)  group by apply_no) T_property_over_count on T_property_over_count.apply_no=bao.apply_no
left join (select apply_no,count(*) age_upper_count from ods.ods_bpms_biz_hit_rule_common where rule_name_tag in (246)  group by apply_no) T_age_upper_count on T_age_upper_count.apply_no=bao.apply_no
left join (select apply_no,count(*) age_lower_count from ods.ods_bpms_biz_hit_rule_common where rule_name_tag in (260)  group by apply_no) T_age_lower_count on T_age_lower_count.apply_no=bao.apply_no
left join (select apply_no,count(*) mainland_count from ods.ods_bpms_biz_hit_rule_common where rule_name_tag in (238)  group by apply_no) T_mainland_count on T_mainland_count.apply_no=bao.apply_no
left join (select apply_no,count(*) liabilities_over_count from ods.ods_bpms_biz_hit_rule_common where rule_name_tag in (149)  group by apply_no) T_liabilities_over_count on T_liabilities_over_count.apply_no=bao.apply_no
left join (select apply_no,count(*) newloan_7_count from ods.ods_bpms_biz_hit_rule_common where rule_name_tag in (264)  group by apply_no) T_newloan_7_count on T_newloan_7_count.apply_no=bao.apply_no
left join (select apply_no,count(*) flooramount_105_count from ods.ods_bpms_biz_hit_rule_common where rule_name_tag in (252)  group by apply_no) T_flooramount_105_count on T_flooramount_105_count.apply_no=bao.apply_no
left join (select apply_no,count(*) aomount_upper_count from ods.ods_bpms_biz_hit_rule_common where rule_name_tag in (163,317,323,342,343,344,345,346)  group by apply_no) T_aomount_upper_count on T_aomount_upper_count.apply_no=bao.apply_no
left join (select apply_no,count(*) newloan_individual_count  from ods.ods_bpms_biz_hit_rule_common where rule_name_tag in (212)  group by apply_no) T_newloan_individual_count  on T_newloan_individual_count .apply_no=bao.apply_no
left join (select apply_no,count(*) newloan_non_agency_count from ods.ods_bpms_biz_hit_rule_common where rule_name_tag in (308)  group by apply_no) T_newloan_non_agency_count on T_newloan_non_agency_count.apply_no=bao.apply_no;
insert overwrite table dws.dws_insurance_info select
a.apply_no --业务编号
,a.product_name --业务品种
,a.tail_release_node_name --放款节点
,a.city_name --分公司
,a.seller_name --客户姓名
,a.release_amount --放款金额
,a.ori_loan_bank_name --原贷款机构
,a.new_loan_bank_name --新贷款机构
,a.check_time --审查日期
,a.check_user_name --审查人员
,a.check_status_name --审查结果
,a.check_option --审查意见
,nvl(a.litigation_one_count,0) --法诉单篇
,nvl(a.litigation_all_count,0) --法诉全文
,nvl(a.litigation_company_count,0) --公司法诉
,nvl(a.credit_parse_false_count,0) --征信解析失败
,nvl(a.query_4_10_count,0) --小贷查询4-10次
,nvl(a.query_10_count,0) --小贷查询超10次
,nvl(a.loan_1_3_count,0) --小贷放款1-3笔
,nvl(a.loan_3_count,0) --小贷放款超3笔
,nvl(a.black,0) --黑名单
,nvl(a.credit_over,0) --信用卡使用率超
,nvl(a.queries_count,0) --征信查询次数
,nvl(a.overdue_count,0) --当前逾期
,nvl(a.uncleared_count,0) --到期贷款未结清
,nvl(a.unusual_count,0) --状态异常
,nvl(a.hit_count,0) --还款命中规则 
,nvl(a.m1hit_count,0) --M1还款命中规则
,nvl(a.credit_over_count,0) --征信过期
,nvl(a.unsecured_count,0) --无抵押贷款
,nvl(a.non_bank_count,0) --原贷非银机构
,nvl(a.borrower_individual_count,0) --原贷借款人非个人
,nvl(a.property_individual_count,0) --产权人非个人
,nvl(a.housing_type_count,0) --房产类型
,nvl(a.property_over_count,0) --产权人数超限
,nvl(a.age_upper_count,0) --年龄上限
,nvl(a.age_lower_count,0) --年龄超下限
,nvl(a.mainland_count,0) --非大陆
,nvl(a.liabilities_over_count,0) --超负债
,nvl(a.newloan_7_count,0) --新贷款金额超评估价7成
,nvl(a.flooramount_105_count,0) --赎楼金额超剩余本金1.05倍
,nvl(a.aomount_upper_count,0) --金额超上限
,nvl(a.newloan_individual_count,0) --新贷借款人非个人
,nvl(a.newloan_non_agency_count,0) --新贷非银机构
,nvl(query_4_10_count,0)+nvl(loan_1_3_count,0),
nvl(query_4_10_count,0)+nvl(newloan_non_agency_count,0),
nvl(loan_1_3_count,0)+nvl(newloan_non_agency_count,0),
nvl(query_4_10_count,0)+nvl(m1hit_count,0),
nvl(query_4_10_count,0)+nvl(liabilities_over_count,0),
nvl(loan_1_3_count,0)+nvl(m1hit_count,0),
nvl(loan_1_3_count,0)+nvl(liabilities_over_count,0),
nvl(m1hit_count,0)+nvl(liabilities_over_count,0),
nvl(m1hit_count,0)+nvl(newloan_non_agency_count,0),
nvl(liabilities_over_count,0)+nvl(newloan_non_agency_count,0)
from tmp_insurance_info a;
drop table if exists tmp_insurance_info;