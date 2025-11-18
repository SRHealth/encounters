CREATE OR REPLACE VIEW srh_edifecs_dev.tx_tchp_encounters_150
AS
SELECT
 '150' as c_150_record
, 'SR' || lpad(ird.ride_id, 16, '0') as c_150_key
, 'P' as c_2000b_sbr01_subscriberpayerresponsibilitysequence
, '18' as c_2000b_sbr02_subscriberrelationship
, '' as c_2000b_sbr03_subscriberpolicynumber
, '' as c_2000b_sbr04_insuredgroupname
, 'MC' as c_2000b_sbr09_claimfilingindicator
, 'IL' as c_2010ba_nm101_subscriberpersonrole
, '1' as c_2010ba_nm102_subscriberpersonindicator
, ird.passenger_last_name  as c_2010ba_nm103_subscriberlastname
, ird.passenger_first_name as c_2010ba_nm104_subscriberfirstname
, '' as c_2010ba_nm105_subscribermiddlename
, '' as c_2010ba_nm107_subscribersuffix
, 'MI' as c_2010ba_nm108_subscriberidentifierqualifer
, ird.passenger_last_name as c_2010ba_nm109_subscriberidentifier -- medical_id
, '' as c_2010ba_ref01_subscriberssnqualifier
, '' as c_2010ba_ref02_subscriber_identifier_ssn
, '' as c_2010ba_ref01_propertyandcasualty_qualifier
, '' as c_2010ba_ref02_propertyandcasualty_identifier
, p.street1 as c_2010ba_n301_subscriberaddressline1
, p.street2  as c_2010ba_n302_subscriberaddressline2
, p.city  as c_2010ba_n401_subscribercity
, p.state as c_2010ba_n402_subscriberstate
, rpad(p.zipcode, 9, '9') as c_2010ba_n403_subscriberpostalcode
, '' as c_2010ba_n404_subscribercountry
, 'D8' as c_2010ba_dmg01_datequalifer
, ird.passenger_dob as c_2010ba_dmg02_dateofbirth
, case 
    when p.gender = 'male' then 'M'
    when p.gender = 'female' then 'F'
    else 'U'
  end as c_2010ba_dmg03_gender
, '' as c_2010ba_ref01_subscriberidentifierqualifier_1
, '' as c_2010ba_ref02_subscriberidentifier_1
, '' as c_2010ba_ref01_subscriberidentifierqualifer_2
, '' as c_2010ba_ref02_subscriberidentifier_2
, 'PR' as c_2010bb_nm101_payer_organizationrole
, '2' as c_2010bb_nm102_payerpersonindicator
, 'TMHP' as c_2010bb_nm103_payername
, 'PI' as c_2010bb_nm108_payerorganizationidqualifier
, '617591011MTPT' as c_2010bb_nm109_payerorganizationidentifier
, '' as c_2010bb_n301_payeraddressline1
, '' as c_2010bb_n302_payeraddressline2
, '' as c_2010bb_n401_payercity
, '' as c_2010bb_n402_payerstate
, '' as c_2010bb_n403_payerpostalcode
, '' as c_2010bb_n404_payercountry
, '' as c_2010bb_ref01_payerorganizationidqualifier_1
, '' as c_2010bb_ref02_payerorganizationidentifier_1
, '' as c_2010bb_ref01_provideridqualifier_1
, '' as c_2010bb_ref02_billingprovideridentifier_1
, '' as c_2010bb_ref01_billingprovideridqualifier_2
, '' as c_2010bb_ref02_billingprovideridentifier_2
, '' as c_2010bb_ref01_billingprovideridqualifier_3
, '' as c_2010bb_ref02_provideridentifier_3
, '' as c_2010bb_ref01_billingprovideridqualifier_4
, '' as c_2010bb_ref02_billingprovideridentifier_4
, '' as c_150_patient_last_name -- p.lastname as c_150_patient_last_name
, '' as c_150_patient_first_name -- p.firstname as c_150_patient_first_name
, '' as c_150_patient_middle_name
, '' as c_150_patient_ssn
,'' as c_150_patient_member_id -- p.medicalid as c_150_patient_member_id
, '' as c_150_patient_gender
, '' as c_150_patient_dob -- to_char(p.dateofbirth, 'YYYYMMDD') as c_150_patient_dob
,'' as c_150_patient_address_line1 -- p.street1 as c_150_patient_address_line1
,'' as c_150_patient_address_line2
,'' as c_150_patient_address_city
,'' as c_150_patient_address_state
,''  as c_150_patient_address_zip
, '' as c_150_pcp_id_qual
, '' as c_150_pcp_id
, '' as c_150_pcp_group_identifier
, '' as c_150_pdp_ipa_pmg_type
, '' as c_150_pdp_ipa_pmg_id
, '' as c_150_pcp_open_indic
, '' as c_150_pcp_eligibility_ind
, '' as c_150_cos
, '' as c_150_service_category_type
, '' as c_150_rendering_prov_eff_date
, '' as c_150_rendering_prov_term_date
, '' as c_150_rendering_prov_dea_id
, '' as c_150_rendering_prov_gender
, '' as c_150_provider_par_non_par
, '' as c_care_plan_option_indicator
, '' as c_group_indentifier
, '' as c_care_type_code
, '' as c_financial_arrangement_code
, '' as c_150_filler_05
, '' as c_2000b_sbr05_insurancetypecode
, '' as c_2000b_pat09_pregnancyindicator
, '' as c_150_filler_6
, '' as c_150_filler_7
, '' as c_150_filler_8
, '' as c_150_filler_9
, '' as c_150_filler_10
, '' as c_150_filler_11
, '' as c_150_filler_12
, '' as c_150_filler_13
, '' as c_150_filler_14
, '' as c_150_filler_15
, '' as c_2000b_pat06_patientdeathdate
, '' as c_2000c_pat01_patientrelationship
, '' as c_2000c_pat06_patientdeathdate
, '' as c_2000c_pat08_patientweight
, '' as c_2000c_pat09_pregnancyindicator
, '' as c_150_subscriberregioncode
, '' as c_150_subscriberotherinsurancecoverage
, '' as c_150_purchasedindicator
, '' as c_150_behavioralhealth_cos
, '' as c_150_medicarecode
, '' as c_150_otherinsurancecode
, '' as c_150_pcc_internalproviderid
, '' as c_150_pcc_internal_provider_id_type
, '' as c_150_filler_16
, '' as c_150_filler_17
, '' as c_150_pcp_provider_id_address_location_code
, '' as c_150_filler_18
, '' as c_150_filler_19
, '' as c_150_pcc_provider_id_address_location_code
, '' as c_financial_arrangement_code_2
FROM       
srh_edifecs_dev.careprovider_invoice_ride_details ird
  
  inner join src_rds_saferide.rides r
    on r.id = ird.ride_id
  inner join src_rds_saferide.passengers p
    on p.id = r.passengerid
    left join srh_ie2_ds.mongo_prod_db.member_data md
    on md._id = p.id

WHERE      
1=1
and ird.zero_cost_ride = 0 and ird.transport in ('AMB','WCV')
AND ird.hospital_group_id = 238

WITH NO SCHEMA BINDING
;