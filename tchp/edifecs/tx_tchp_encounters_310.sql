CREATE OR REPLACE VIEW srh_edifecs_dev.tx_tchp_encounters_310
AS
SELECT 
 '310' as c_310_record
, 'SR' || lpad(ird.ride_id, 16, '0') as c_310_key
, 'S' as c_2320_sbr01_payerresponsibilitysequence
, '18' as c_2320_sbr02_individual_relationship_code
, '' as c_2320_sbr03_policynumber
, '' as c_310_productidnumber
, '' as c_310_delegatedbenefitadminorgid
, '' as c_2320_sbr04_insuredgroupname
, '' as c_2320_sbr05_insurancetype
, '11' as c_2320_sbr09_claimfilingindicator
, '' as c_310_claimdisposition
, '' as c_310_riskassessmentcode_1
, '' as c_310_riskassessmentcode_2
, '' as c_310_riskassessmentcode_3
, '' as c_310_riskassessmentcode_4
, '' as c_310_riskassessmentcode_5
, '' as c_310_riskassessmentcode_6
, '' as c_310_riskassessmentcode_7
, '' as c_310_riskassessmentcode_8
, '' as c_310_riskassessmentcode_9
, '' as c_310_riskassessmentcode_10
, '' as c_310_riskassessmentcode_11
, '' as c_310_riskassessmentcode_12
, '' as c_310_riskassessmentcode_13
, '' as c_310_riskassessmentcode_14
, '' as c_310_riskassessmentcode_15
, '' as c_310_riskassessmentcode_16
, '' as c_310_riskassessmentcode_17
, '' as c_310_riskassessmentcode_18
, '' as c_310_riskassessmentcode_19
, '' as c_310_riskassessmentcode_20
, '' as c_310_riskassessmentcode_21
, '' as c_310_riskassessmentcode_22
, '' as c_310_riskassessmentcode_23
, '' as c_310_riskassessmentcode_24
, '' as c_2320_claimleveldenial_1
, '' as c_2320_claimleveldenial_2
, '' as c_2320_claimleveldenial_3
, '' as c_2320_claimleveldenial_4
, '' as c_2320_claimleveldenial_5
, '' as c_2320_claimleveldenial_6
, '' as c_2320_claimleveldenial_7
, '' as c_2320_claimleveldenial_8
, '' as c_2320_claimleveldenial_9
, '' as c_2320_claimleveldenial_10
, '' as c_2320_cas01_claimadjustmentgroup_1
, '' as c_2320_cas02_claimadjustmentreason_1
, '' as c_2320_cas03_claimadjustmentamount_1
, '' as c_2320_cas04_claimadjustmentquantity_1
, '' as c_2320_cas05_claimadjustmentreason_1
, '' as c_2320_cas06_claimadjustmentamount_1
, '' as c_2320_cas07_claimadjustmentquantity_1
, '' as c_2320_cas08_claimadjustmentreason_1
, '' as c_2320_cas09_claimadjustmentamount_1
, '' as c_2320_cas10_claimadjustmentquantity_1
, '' as c_2320_cas11_claimadjustmentreason_1
, '' as c_2320_cas12_claimadjustmentamount_1
, '' as c_2320_cas12_claimadjustmentquantity_1
, '' as c_2320_cas14_claimadjustmentreason_1
, '' as c_2320_cas15_claimadjustmentamount_1
, '' as c_2320_cas16_claimadjustmentquantity_1
, '' as c_2320_cas17_claimadjustmentreason_1
, '' as c_2320_cas18_claimadjustmentamount_1
, '' as c_2320_cas19_claimadjustmentquantity_1
, '' as c_2320_cas01_claimadjustmentgroup_2
, '' as c_2320_cas02_claimadjustmentreason_2
, '' as c_2320_cas03_claimadjustmentamount_2
, '' as c_2320_cas04_claimadjustmentquantity_2
, '' as c_2320_cas05_claimadjustmentreason_2
, '' as c_2320_cas06_claimadjustmentamount_2
, '' as c_2320_cas07_claimadjustmentquantity_2
, '' as c_2320_cas08_claimadjustmentreason_2
, '' as c_2320_cas09_claimadjustmentamount_2
, '' as c_2320_cas10_claimadjustmentquantity_2
, '' as c_2320_cas11_claimadjustmentreason_2
, '' as c_2320_cas12_claimadjustmentamount_2
, '' as c_2320_cas12_claimadjustmentquantity_2
, '' as c_2320_cas14_claimadjustmentreason_2
, '' as c_2320_cas15_claimadjustmentamount_2
, '' as c_2320_cas16_claimadjustmentquantity_2
, '' as c_2320_cas17_claimadjustmentreason_2
, '' as c_2320_cas18_claimadjustmentamount_2
, '' as c_2320_cas19_claimadjustmentquantity_2
, '' as c_2320_cas01_claimadjustmentgroup_3
, '' as c_2320_cas02_claimadjustmentreason_3
, '' as c_2320_cas03_claimadjustmentamount_3
, '' as c_2320_cas04_claimadjustmentquantity_3
, '' as c_2320_cas05_claimadjustmentreason_3
, '' as c_2320_cas06_claimadjustmentamount_3
, '' as c_2320_cas07_claimadjustmentquantity_3
, '' as c_2320_cas08_claimadjustmentreason_3
, '' as c_2320_cas09_claimadjustmentamount_3
, '' as c_2320_cas10_claimadjustmentquantity_3
, '' as c_2320_cas11_claimadjustmentreason_3
, '' as c_2320_cas12_claimadjustmentamount_3
, '' as c_2320_cas12_claimadjustmentquantity_3
, '' as c_2320_cas14_claimadjustmentreason_3
, '' as c_2320_cas15_claimadjustmentamount_3
, '' as c_2320_cas16_claimadjustmentquantity_3
, '' as c_2320_cas17_claimadjustmentreason_3
, '' as c_2320_cas18_claimadjustmentamount_3
, '' as c_2320_cas19_claimadjustmentquantity_3
, '' as c_2320_cas01_claimadjustmentgroup_4
, '' as c_2320_cas02_claimadjustmentreason_4
, '' as c_2320_cas03_claimadjustmentamount_4
, '' as c_2320_cas04_claimadjustmentquantity_4
, '' as c_2320_cas05_claimadjustmentreason_4
, '' as c_2320_cas06_claimadjustmentamount_4
, '' as c_2320_cas07_claimadjustmentquantity_4
, '' as c_2320_cas08_claimadjustmentreason_4
, '' as c_2320_cas09_claimadjustmentamount_4
, '' as c_2320_cas10_claimadjustmentquantity_4
, '' as c_2320_cas11_claimadjustmentreason_4
, '' as c_2320_cas12_claimadjustmentamount_4
, '' as c_2320_cas12_claimadjustmentquantity_4
, '' as c_2320_cas14_claimadjustmentreason_4
, '' as c_2320_cas15_claimadjustmentamount_4
, '' as c_2320_cas16_claimadjustmentquantity_4
, '' as c_2320_cas17_claimadjustmentreason_4
, '' as c_2320_cas18_claimadjustmentamount_4
, '' as c_2320_cas19_claimadjustmentquantity_4
, '' as c_2320_cas01_claimadjustmentgroup_5
, '' as c_2320_cas02_claimadjustmentreason_5
, '' as c_2320_cas03_claimadjustmentamount_5
, '' as c_2320_cas04_claimadjustmentquantity_5
, '' as c_2320_cas05_claimadjustmentreason_5
, '' as c_2320_cas06_claimadjustmentamount_5
, '' as c_2320_cas07_claimadjustmentquantity_5
, '' as c_2320_cas08_claimadjustmentreason_5
, '' as c_2320_cas09_claimadjustmentamount_5
, '' as c_2320_cas10_claimadjustmentquantity_5
, '' as c_2320_cas11_claimadjustmentreason_5
, '' as c_2320_cas12_claimadjustmentamount_5
, '' as c_2320_cas12_claimadjustmentquantity_5
, '' as c_2320_cas14_claimadjustmentreason_5
, '' as c_2320_cas15_claimadjustmentamount_5
, '' as c_2320_cas16_claimadjustmentquantity_5
, '' as c_2320_cas17_claimadjustmentreason_5
, '' as c_2320_cas18_claimadjustmentamount_5
, '' as c_2320_cas19_claimadjustmentquantity_5
, 'D' as c_2320_amt01_amountqualifier_1
, sum(coalesce(ird.trip_cost, 0) + coalesce(ird.adjustment_amount, 0)) as c_2320_amt02_cobamount_1
, '' as c_2320_amt01_amountqualifier_2
, '' as c_2320_amt02_cobamount_2
, '' as c_2320_amt01_amountqualifier_3
, '' as c_2320_amt02_cobamount_3
, 'Y' as c_2320_oi03_assignbenefitsindicator
, '' as c_2320_oi04_patientsignature
, 'Y' as c_2320_oi06_releaseofinformation
, '' as c_2320_mia01_adjudquantity
, '' as c_2320_mia03_adjudquantity
, '' as c_2320_mia04_adjudamount
, '' as c_2320_mia05_remarkcode
, '' as c_2320_mia06_adjudamount
, '' as c_2320_mia07_adjudamount
, '' as c_2320_mia08_adjudamount
, '' as c_2320_mia09_adjudamount
, '' as c_2320_mia10_adjudamount
, '' as c_2320_mia11_adjudamount
, '' as c_2320_mia12_adjudamount
, '' as c_2320_mia13_adjudamount
, '' as c_2320_mia14_adjudamount
, '' as c_2320_mia15_adjudquantity
, '' as c_2320_mia16_adjudamount
, '' as c_2320_mia17_adjudamount
, '' as c_2320_mia18_adjudamount
, '' as c_2320_mia19_adjudamount
, '' as c_2320_mia20_remarkcode
, '' as c_2320_mia21_remarkcode
, '' as c_2320_mia22_remarkcode
, '' as c_2320_mia23_remarkcode
, '' as c_2320_mia24_adjudamount
, '' as c_2320_moa01_adjudamount
, '' as c_2320_moa02_adjudamount
, '' as c_2320_moa03_remarkcode
, '' as c_2320_moa04_remarkcode
, '' as c_2320_moa05_remarkcode
, '' as c_2320_moa06_remarkcode
, '' as c_2320_moa07_remarkcode
, '' as c_2320_moa08_adjudamount
, '' as c_2320_moa09_adjudamount
, 'IL' as c_2330a_nm101_personrole
, '1' as c_2330a_nm102_personindicator
,  ird.passenger_last_name as c_2330a_nm103_lastname 
, ird.passenger_first_name as c_2330a_nm104_firstname 
, '' as c_2330a_nm105_middlename
, '' as c_2330a_nm107_suffix
, 'MI' as c_2330a_nm108_personidentifierqualifier
, p.alt_id as c_2330a_nm109_personidentifier -- medical_id here 
,  p.street1 as c_2330a_n301_addressline1 --  as c_2330a_n301_addressline1
, p.street2 as c_2330a_n302_addressline2
, p.city as c_2330a_n401_city
, p.state as c_2330a_n402_state
, rpad(p.zipcode, 9, '9') as c_2330a_n403_postalcode
, '' as c_2330a_n404_country
, 'PR' as c_2330b_nm101_organizationrole
, '2' as c_2330b_nm102_personindicator
, 'SAFERIDE, INC' as c_2330b_nm103_name
, 'PI' as c_2330b_nm108_organizationidqualifier
, '87726' as c_2330b_nm109_organizationidentifier -- plan code here 
, '18302 Talavera Ridge' as c_2330b_n301_addressline1
, '' as c_2330b_n302_addressline2
, 'SAN ANTONIO' as c_2330b_n401_city
, 'TX' as c_2330b_n402_state
, '782570000' as c_2330b_n403_postalcode
, '' as c_2330b_n404_country
, '573' as c_2330b_dtp01_datetimequalifier
, 'D8' as c_2330b_dtp02_datetimeformat
, max(to_char(ird.reporting_end_date::date, 'YYYYMMDD')) as c_2330b_dtp03_claimpaiddate
, '' as c_310_overpaymentid
, '' as c_2330b_ref01_organizationidqualifier_1
, '' as c_2330b_ref01_organizationidentifier_1
, '' as c_2330b_ref01_organizationidqualifier_2
, '' as c_2330b_ref01_organizationidentifier_2
, '' as c_2330b_ref01_organizationidqualifier_2
, '' as c_2330b_ref01_organizationidentifier_2
, '' as c_2330b_ref01_organizationidqualifier_3
, '' as c_2330b_ref01_organizationidentifier_3
, 'F8' as c_2330b_ref01_organizationidqualifier_3
, 'SR' || lpad(ird.ride_id, 16, '0') as c_2330b_ref01_organizationidentifier_3
, '' as c_310_reportbegindate
, '' as c_310_reportenddate
, '' as c_310_paymentyear
, '' as c_2330b_ref01_adjudicateddrgqualifier
, '' as c_2330b_ref02_adjudicateddrg
, '' as c_2330b_ref04_01_referenceidentificationqualifier
, '' as c_2330b_ref04_02_drggrouperversion
, '' as c_310_rate_increase_indicator
, '' as c_310_bundle_indicator
, '' as c_310_bundle_claim_number
, '' as c_claimapprovedamount
, '' as c_310_paymentcheckdate
, '' as c_310_filler_03
, '' as c_310_filler_04
, '' as c_310_filler_05
, '' as c_310_filler_06
, '' as c_310_filler_07
, '' as c_310_filler_08
, '' as c_310_filler_09
, '' as c_310_filler_10
, '' as c_310_filler_11
, '' as c_310_filler_12
, '' as c_310_filler_13
, '' as c_310_filler_14
, '' as c_310_filler_15
, '' as c_310_recipientaidcategory
, '' as c_2330a_ref01_othersubscriberssnqualifier
, '' as c_2330a_ref02_othersubscriber_identifier_ssn
, '' as c_835_bpr04_paymentmethodcode
, '' as c_835_header_ref_ev_remittance_number
, '' as c_835_2100_ref_6p_outlier_code
, '' as c_835_2100_amt01_amount_qualifier_code
, '' as c_835_2100_amt02_supplemental_information_amount
, '' as c_835_2100_qty01_supplemental_quantity_qualifier
, '' as c_835_2100_qty02_supplemental_quantity
, '' as c_835_2100_amt01_amount_qualifier_code_2
, '' as c_835_2100_amt02_supplemental_information_amount_2
, '' as c_835_2100_amt01_amount_qualifier_code_3
, '' as c_835_2100_amt02_supplemental_information_amount_3
, '' as c_check_number
from
 srh_edifecs_dev.careprovider_invoice_ride_details ird
  inner join src_rds_saferide.health_sub_plans hsp on ird.health_sub_plan_name = hsp."name" and hsp.supports_wrap = 0
  inner join src_rds_saferide.rides r
    on r.id = ird.ride_id
  inner join src_rds_saferide.passengers p
    on p.id = r.passengerid
  left join src_rds_saferide.vehicle_company vc
    on vc.id = r.vehiclownerid   
  left join src_rds_saferide.drivers d
    on d.id = r.driverid
     left join srh_ie2_ds.mongo_prod_db.member_data md
    on md._id = p.id

WHERE      
1=1
and ird.zero_cost_ride = 0 and ird.transport in ('AMB','WCV')
AND ird.hospital_group_id = 238

group by
  'SR' || lpad(ird.ride_id, 16, '0')
  ,ird.passenger_last_name 
  , ird.passenger_first_name 
  , p.alt_id  --medicaid_id
  , p.street1 
  , p.street2 
  , p.city 
  , p.state 
  , rpad(p.zipcode, 9, '9') 
  ,'SR' || lpad(ird.ride_id, 16, '0')

WITH NO SCHEMA BINDING;