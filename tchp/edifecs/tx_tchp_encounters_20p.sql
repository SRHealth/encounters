
CREATE OR REPLACE VIEW srh_edifecs_dev.tx_tchp_encounters_20p
AS 

SELECT 
 '20P' as c_20p_record
, 'SR' || lpad(ird.ride_id, 16, '0') as c_20p_key
, 'R' || lpad(ird.ride_id, 12, '0')||'CD0' as c_2300p_clm01_claimnumber
, coalesce(ird.trip_cost, 0) + coalesce(ird.adjustment_amount, 0) as c_2300p_clm02_totalclaimcharge
, '99' as c_2300p_clm05_01_placeofservice
, 'B' as c_2300p_clm05_02_billtypequalifier
, '1' as c_2300p_clm05_03_claimfrequencycode
, 'Y' as c_2300p_clm06_providersignature
, 'A' as c_2300p_clm07_medicareassignment
, 'Y' as c_2300p_clm08_benefitassignmentindicator
, 'I' as c_2300p_clm09_releaseofinformation
, '' as c_2300p_clm10_patientsignaturesource
, '' as c_2300p_clm11_01_relatedcausescode
, '' as c_2300p_clm11_02_relatedcausescode
, '' as c_2300p_clm11_04_accidentstate
, '' as c_2300p_clm11_05_accidentcountry
, '' as c_2300p_clm12_specialprogramindicator
, '' as c_2300p_clm20_delayreason
, '' as c_2300p_dtp01_datetimequalifier_1
, '' as c_2300p_dtp02_formatqualifier_1
, '' as c_2300p_dtp03_datetime_1
, '' as c_2300p_dtp01_datetimequalifier_2
, '' as c_2300p_dtp02_formatqualifier_2
, '' as c_2300p_dtp03_datetime_2
, '' as c_2300p_dtp01_datetimequalifier_3
, '' as c_2300p_dtp02_formatqualifier_3
, '' as c_2300p_dtp03_datetime_3
, '' as c_2300p_dtp01_datetimequalifier_4
, '' as c_2300p_dtp02_formatqualifier_4
, '' as c_2300p_dtp03_datetime_4
, '435' as c_2300p_dtp01_datetimequalifier_5
, 'D8' as c_2300p_dtp02_formatqualifier_5
, to_char(ird.ride_start_date, 'YYYYMMDD') as c_2300p_dtp03_datetime_5 -- DOS
, '' as c_2300p_dtp01_datetimequalifier_6
, '' as c_2300p_dtp02_formatqualifier_6
, '' as c_2300p_dtp03_datetime_6
, '' as c_2300p_dtp01_datetimequalifier_7
, '' as c_2300p_dtp02_formatqualifier_7
, '' as c_2300p_dtp03_datetime_7  --circle back later Claim Received Date
, '' as c_2300p_cn101_contracttypecode
, '' as c_2300p_cn102_monetaryamount --paid_amount
, '' as c_2300p_cn103_contractpercentage
, '' as c_2300p_cn104_contractcode
, '' as c_2300p_cn105_termsdiscountpercent
, '' as c_2300p_cn106_contractversionidentifier
, '' as c_2300p_amt01_amountqualifier
, '' as c_2300p_amt02_patientamountpaid
, '' as c_2300p_ref01_claimreferencenumberqualifier_1
, '' as c_2300p_ref02_claimreferencenumber_1
, 'G1' as c_2300p_ref01_claimreferencenumberqualifier_2
, 'SR' || lpad(ird.ride_id, 16, '0') as c_2300p_ref02_claimreferencenumber_2
, '' as c_2300p_ref01_claimreferencenumberqualifier_3
, '' as c_2300p_ref02_claimreferencenumber_3
, '' as c_2300p_ref01_claimreferencenumberqualifier_4
, '' as c_2300p_ref02_claimreferencenumber_4
, '' as c_2300p_ref01_claimreferencenumberqualifier_5
, '' as c_2300p_ref02_claimreferencenumber_5
, '' as c_2300p_ref01_claimreferencenumberqualifier_6
, '' as c_2300p_ref02_claimreferencenumber_6
, '' as c_2300p_ref01_claimreferencenumberqualifier_7
, '' as c_2300p_ref02_claimreferencenumber_7
, '' as c_2300p_ref01_claimreferencenumberqualifier_8
, '' as c_2300p_ref02_claimreferencenumber_8
, '' as c_2300p_nte01_claimnotetype
, '' as c_2300p_nte02_claimnote -- NTE segment here 
, '' as c_20p_ambulance_transport_count
, '' as c_2300p_cr101_ambpatientweightqualifier
, '' as c_2300p_cr102_ambulancepatientweight
, '' as c_2300p_cr104_ambulancetransportreason
, '' as c_2300p_cr105_ambulancemeasure 
, '' as c_2300p_cr106_ambulancedistance
, '' as c_2300p_cr109_ambulanceroundtripdesc --circle back Placeholder take the logic from 40p
, '' as c_2400p_cr110_stretcherpurpose
, '' as c_2300p_cr110_stretcherpurposedescription
, '' as c_2300p_crc01_servicecertificationcategory
, '' as c_2300p_crc02_servicecertificationindicator
, '' as c_2300p_crc03_conditionindicatorcode
, '' as c_2300p_crc04_conditionindicatorcode
, '' as c_2300p_crc05_conditionindicatorcode
, '' as c_2300p_crc06_conditionindicatorcode
, '' as c_2300p_crc07_conditionindicatorcode
, 'ABK' as c_2300p_hi01_01_dxtype_1
, 'Z753' as c_2300p_hi01_02_dxcode_1 -- Principal Diagnosis Code
, '' as c_2300p_hi02_01_dxtype_1
, '' as c_2300p_hi02_02_dxcode_1
, '' as c_2300p_hi03_01_dxtype_1
, '' as c_2300p_hi03_02_dxcode_1
, '' as c_2300p_hi04_01_dxtype_1
, '' as c_2300p_hi04_02_dxcode_1
, '' as c_2300p_hi05_01_dxtype_1
, '' as c_2300p_hi05_02_dxcode_1
, '' as c_2300p_hi06_01_dxtype_1
, '' as c_2300p_hi06_02_dxcode_1
, '' as c_2300p_hi07_01_dxtype_1
, '' as c_2300p_hi07_02_dxcode_1
, '' as c_2300p_hi08_01_dxtype_1
, '' as c_2300p_hi08_02_dxcode_1
, '' as c_2300p_hi09_01_dxtype_1
, '' as c_2300p_hi09_02_dxcode_1
, '' as c_2300p_hi10_01_dxtype_1
, '' as c_2300p_hi10_02_dxcode_1
, '' as c_2300p_hi11_01_dxtype_1
, '' as c_2300p_hi11_02_dxcode_1
, '' as c_2300p_hi12_01_dxtype_1
, '' as c_2300p_hi12_02_dxcode_1
--Referring provider Not applicable to Saferide
, '' as c_2310ap_nm101_providerrole
, '' as c_2310ap_nm102_personindicator
, '' as c_2310ap_nm103_lastname
, '' as c_2310ap_nm104_firstname
, '' as c_2310ap_nm105_middlename
, '' as c_2310ap_nm107_suffix
, '' as c_2310ap_nm108_provideridentifierqualifer
, '' as c_2310ap_nm109_provideridentifier
, '' as c_2310ap_ref01_provideridentifierqualifer_1
, '' as c_2310ap_ref02_provideridentifier_1
, '' as c_2310ap_ref01_provideridentifierqualifer_2
, '' as c_2310ap_ref02_provideridentifier_2  
--Rendering provider Driver is missing in the rides entered 
, '82' as c_2310bp_nm101_providerrole
, '1' as c_2310bp_nm102_personindicator
--circle back to lyft and uber drivers logic later
, coalesce(d.lastname ) as c_2310bp_nm103_lastname --, lrd.lyft_driver_name, urd.uber_driver_name
, coalesce(d.firstname) as c_2310bp_nm104_firstname --, lrd.lyft_driver_name, urd.uber_driver_name
, '' as c_2310bp_nm105_middlename
, '' as c_2310bp_nm107_suffix
, 'XX' as c_2310bp_nm108_provideridentifierqualifer
,  ''  as   c_2310bp_nm109_provideridentifier --Driver's license?
, 'PE' as c_2310bp_prv01_providercode
, 'PXC' as c_2310bp_prv02_providercodequalifer
, '343900000X' as c_2310bp_prv03_providertaxonomy
, '' as c_2310bp_ref01_provideridentifierqualifer_1
, '' as c_2310bp_ref02_provideridentifier_1 --???
, '' as c_2310bp_ref01_provideridentifierqualifer_2
, '' as c_2310bp_ref02_provideridentifier_2
, '' as c_20p_renderingprovideraddress1
, '' as c_20p_renderingprovideraddress2
, '' as c_20p_renderingprovidercity
, '' as c_20p_renderingproviderstate
, '' as c_20p_renderingproviderzip
--Service Facility What do we populate here??
, '77' as c_2310cp_nm101_providerrole
, '2' as c_2310cp_nm102_personindicator
, '' as c_2310cp_nm103_lastname
, 'XX' as c_2310cp_nm108_provideridentifierqualifer
, '' as c_2310cp_nm109_provideridentifier
, '' as c_2310cp_n301_addressline1
, '' as c_2310cp_n302_addressline2
, '' as c_2310cp_n401_city
, '' as c_2310cp_n402_state
, '' as c_2310cp_n403_postalcode
, '' as c_2310cp_n404_country
, 'G2' as c_2310cp_ref01_provideridentifierqualifer
, '' as c_2310cp_ref02_provideridentifier
--Supervising provider only needed for special cases like GMR, meals and lodging....
, '' as c_2310dp_nm101_providerrole
, '1' as c_2310dp_nm102_personindicator
, '' as c_2310dp_nm103_lastname
, '' as c_2310dp_nm104_firstname
, '' as c_2310dp_nm105_middlename
, '' as c_2310dp_nm107_suffix
, 'XX' as c_2310dp_nm108_provideridentifierqualifer
, '' as c_2310dp_nm109_provideridentifier
, '' as c_2310dp_ref01_provideridentifierqualifer
, '' as c_2310dp_ref02_provideridentifier
---Pickup location
, 'PW' as c_2310ep_nm101_providerrole
, '2' as c_2310ep_nm102_personindicator
, trim((case when LENGTH(ird.ride_from_address) - LENGTH(REPLACE(ird.ride_from_address, ',', '')) > 3 then  concat(split_part(ird.ride_from_address,',', 1) , split_part(ird.ride_from_address,',', 2 )) else split_part(ird.ride_from_address,',',1) end))  as c_2310ep_n301_addressline1
, '' as c_2310ep_n302_addressline2
, trim((case when LENGTH(ird.ride_from_address) - LENGTH(REPLACE(ird.ride_from_address, ',', '')) > 3 then  split_part(ird.ride_from_address,',',3)  else split_part(ird.ride_from_address,',',2) end)) as c_2310ep_n401_city
, ird.ride_from_state as c_2310ep_n402_state
, rpad(r.fromzipcode, 9, '9') as c_2310ep_n403_postalcode
, '' as c_2310ep_n404_country
, '' as c_2310ep_ref01_provideridentifierqualifer
, '' as c_2310ep_ref02_provideridentifier
--Drop off location
, '45' as c_2310fp_nm101_providerrole
, '2' as c_2310fp_nm102_personindicator
, '' as c_2310fp_nm103_org_lastname
, case when LENGTH(ird.ride_to_address) - LENGTH(REPLACE(ird.ride_to_address, ',', '')) > 3 then  concat(split_part(ird.ride_to_address,',', 1) , split_part(ird.ride_to_address,',', 2 )) else split_part(ird.ride_to_address,',',1) end  as c_2310fp_n301_addressline1
, '' as c_2310fp_n302_addressline2
, trim((case when LENGTH(ird.ride_to_address) - LENGTH(REPLACE(ird.ride_to_address, ',', '')) > 3 then  split_part(ird.ride_to_address,',',3)  else split_part(ird.ride_to_address,',',2) end))  as c_2310fp_n401_city
, ird.ride_to_state as c_2310fp_n402_state
, rpad(r.tozipcode, 9, '9') as c_2310fp_n403_postalcode
, '' as c_2310fp_n404_country
, '' as c_2310fp_ref01_provideridentifierqualifer
, '' as c_2310fp_ref02_provideridentifier
, '' as c_2300p_pwk01_attachmentreporttype
, '' as c_2300p_pwk02_attachmenttransmissioncode
, '' as c_2300p_pwk06_attachmentcontrolnumber
, '' as c_2300p_ref01_investigationaldeviceexemptionnumber_qualifier
, '' as c_2300p_ref02_investigationaldeviceexemptionnumber
, '' as c_2300p_ref01_serviceauthorizationexceptioncode_qualifier
, '' as c_2300p_ref02_serviceauthorizationexceptioncode
, '' as c_2300p_ref01_mammographycertificationnumber_qualifier
, '' as c_2300p_ref01_mammographycertificationnumber
, '' as c_2300p_cr208_patientconditioncode
, '' as c_2300p_cr210_patientdescription
, '' as c_2300p_cr211_patientdescription
, '' as c_2300p_hi101_2_anesthesiarelatedsurgicalprocedure
, '' as c_2300p_hi102_2_anesthesiarelatedsurgicalprocedure
, '' as c_2300p_dtp01_datetimequalifier_7
, '' as c_2300p_dtp02_formatqualifier_7
, '' as c_2300p_dtp03_datetime_7
, '' as c_2300p_dtp01_datetimequalifier_8
, '' as c_2300p_dtp02_formatqualifier_8
, '' as c_2300p_dtp03_datetime_8
, '' as c_2300p_dtp01_datetimequalifier_9
, '' as c_2300p_dtp02_formatqualifier_9
, '' as c_2300p_dtp03_datetime_9
, '' as c_2300p_dtp01_datetimequalifier_10
, '' as c_2300p_dtp02_formatqualifier_10
, '' as c_2300p_dtp03_datetime_10
, '' as c_2300p_dtp01_datetimequalifier_11
, '' as c_2300p_dtp02_formatqualifier_11
, '' as c_2300p_dtp03_datetime_11
, '' as c_2300p_dtp01_datetimequalifier_12
, '' as c_2300p_dtp02_formatqualifier_12
, '' as c_2300p_dtp03_datetime_12
, '' as c_2300p_dtp01_datetimequalifier_13
, '' as c_2300p_dtp02_formatqualifier_13
, '' as c_2300p_dtp03_datetime_13
, '' as c_2300p_dtp01_datetimequalifier_14
, '' as c_2300p_dtp02_formatqualifier_14
, '' as c_2300p_dtp03_datetime_14
, '' as c_2300p_dtp01_datetimequalifier_15
, '' as c_2300p_dtp02_formatqualifier_15
, '' as c_2300p_dtp03_datetime_15
, '' as c_2300p_dtp01_datetimequalifier_16
, '' as c_2300p_dtp02_formatqualifier_16
, '' as c_2300p_dtp03_datetime_16
, '' as c_2300p_hcp01_pricingmethodology
, '' as c_2300p_hcp02_repricedallowedamount
, '' as c_2300p_hcp03_repricedsavingamount
, '' as c_2300p_hcp04_repricingorganizationidentifier
, '' as c_2300p_hcp05_repricingperdiemamount
, '' as c_2300p_hcp06_repricedapprovedambulatorypatientgroupcode
, '' as c_2300p_hcp07_repricedapprovedambulatorypatientgroupamount
, '' as c_2300p_hcp13_rejectreasoncode
, '' as c_2300p_hcp14_policycompliancecode
, '' as c_2300p_hcp15_exceptioncode
, '' as c_2300p_hi01_01_conditioncodequalifier
, '' as c_2300p_hi01_02_conditioncode_
, '' as c_2300p_hi02_01_conditioncodequalifier
, '' as c_2300p_hi02_02_conditioncode_
, '' as c_2300p_hi03_01_conditioncodequalifier
, '' as c_2300p_hi03_02_conditioncode_
, '' as c_2300p_hi04_01_conditioncodequalifier
, '' as c_2300p_hi04_02_conditioncode_
, '' as c_2300p_hi05_01_conditioncodequalifier
, '' as c_2300p_hi05_02_conditioncode_
, '' as c_2300p_hi06_01_conditioncodequalifier
, '' as c_2300p_hi06_02_conditioncode_
, '' as c_2300p_hi07_01_conditioncodequalifier
, '' as c_2300p_hi07_02_conditioncode_
, '' as c_2300p_hi08_01_conditioncodequalifier
, '' as c_2300p_hi08_02_conditioncode_
, '' as c_2300p_hi09_01_conditioncodequalifier
, '' as c_2300p_hi09_02_conditioncode_
, '' as c_2300p_hi10_01_conditioncodequalifier
, '' as c_2300p_hi10_02_conditioncode_
, '' as c_2300p_hi11_01_conditioncodequalifier
, '' as c_2300p_hi11_02_conditioncode_
, '' as c_2300p_hi12_01_conditioncodequalifier
, '' as c_2300p_hi12_02_conditioncode_
, '' as c_2300p_ref01_fileinformation_qualifier
, '' as c_2300i_ref01_fileinformation
, '' as c_claim_processor_receiver_date
, '' as c_innetworkindicator_
, '' as c_patientcontrolnumber
, '' as c_20p_filler_04
, '' as c_20p_filler_05
, '' as c_20p_filler_06
, '' as c_20p_filler_07
, '' as c_20p_filler_08
, '' as c_20p_filler_09
, '' as c_20p_filler_10
, '' as c_20p_filler_11
, '' as c_20p_filler_12
, '' as c_20p_filler_13
, '' as c_20p_filler_14
, '' as c_20p_filler_15
, '' as c_2300p_hi13_01_conditioncodequalifier
, '' as c_2300p_hi13_02_conditioncode_
, '' as c_2300p_hi14_01_conditioncodequalifier
, '' as c_2300p_hi14_02_conditioncode_
, '' as c_2300p_hi15_01_conditioncodequalifier
, '' as c_2300p_hi15_02_conditioncode_
, '' as c_2300p_hi16_01_conditioncodequalifier
, '' as c_2300p_hi16_02_conditioncode_
, '' as c_2300p_hi17_01_conditioncodequalifier
, '' as c_2300p_hi17_02_conditioncode_
, '' as c_2300p_hi18_01_conditioncodequalifier
, '' as c_2300p_hi18_02_conditioncode_
, '' as c_2300p_hi19_01_conditioncodequalifier
, '' as c_2300p_hi19_02_conditioncode_
, '' as c_2300p_hi20_01_conditioncodequalifier
, '' as c_2300p_hi20_02_conditioncode_
, '' as c_2300p_hi21_01_conditioncodequalifier
, '' as c_2300p_hi21_02_conditioncode_
, '' as c_2300p_hi22_01_conditioncodequalifier
, '' as c_2300p_hi22_02_conditioncode_
, '' as c_2300p_hi23_01_conditioncodequalifier
, '' as c_2300p_hi23_02_conditioncode_
, '' as c_2300p_hi24_01_conditioncodequalifier
, '' as c_2300p_hi24_02_conditioncode_
--Placeholder: circle back
, '' as c_2300p_ref01_fileinformation --SUB-123456789;IPAD-XXX.XXX.XXX;USER-XXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
, '' as c_2300p_ref01_fileinformation3 --AL1-Rendering Provider Address_line_1;AL2-Rendering Provider Address_line_2
, '' as c_2300p_ref01_fileinformation4 --CY-Rendering Provider Address_City;ST-Rendering Provider State;ZC-Rendering Porvider Zip Code;EI-Rendering Provider TIN 
, '' as c_2300p_ref01_fileinformation5 --circle back to location --TRPN-<ASPUFEPAPER (Paper Claim) or ASPUFEELEC (Electronic Claim)>;SNWK-<'I' or 'O'>;SLOC-<location code 999>
, '' as c_2300p_ref01_fileinformation6 --HICN-<HICN>;CCIN-<'Y' or 'N'>
, '' as c_2300p_ref01_fileinformation7 --SCC-xxx;OCC-xxx;DCC-xxx;SNI-xx,xx,xx,xx;PYM-xx;48H-x;RG-xx;TVI-x
, '' as c_2300p_ref01_fileinformation8 --VIN-xxxxxxxxxxxxxxxxx;HCTXMY-xxxxxxxxxxxxxxx;HCTAX-xxxxxxxxx ??
, '' as c_2300p_ref01_fileinformation9
, '' as c_2300p_ref01_fileinformation10
, '' as c_renderingproviderspecialty
, '' as c_ambulancespecialneedind
, '' as c_ambulanceattendenttype
, '' as c_ambulanceindividualsaccompanying
, '' as c_ambulancebeneficiarypickup
, '' as c_ambulancetriprequesteddate
, '' as c_nonemg
, '' as c_tbsv
, '' as c_ssrc
, '' as c_ipad
, '' as c_user
, '' as c_20p_otheraccident
, '' as c_2310ap_renderingprovider_taxid
, '' as c_20p_servicing_provider_type
, '' as c_2310ap_renderingprovider_specialtydesc
, '' as c_alternateclaimid
, '' as c_2300p_crc01_servicecertificationcategory2
, '' as c_2300p_crc02_servicecertificationindicator2
, '' as c_2300p_crc03_conditionindicatorcode2
, '' as c_2300p_crc04_conditionindicatorcode2
, '' as c_2300p_crc05_conditionindicatorcode2
, '' as c_2300p_crc06_conditionindicatorcode2
, '' as c_2300p_crc07_conditionindicatorcode2
, '' as c_2300p_crc01_servicecertificationcategory3
, '' as c_2300p_crc02_servicecertificationindicator3
, '' as c_2300p_crc03_conditionindicatorcode3
, '' as c_2300p_crc04_conditionindicatorcode3
, '' as c_2300p_crc05_conditionindicatorcode3
, '' as c_2300p_crc06_conditionindicatorcode3
, '' as c_2300p_crc07_conditionindicatorcode3
, '' as c_2300p_crc01_servicecertificationcategory4
, '' as c_2300p_crc02_servicecertificationindicator4
, '' as c_2300p_crc03_conditionindicatorcode4
, '' as c_2300p_crc04_conditionindicatorcode4
, '' as c_2300p_crc05_conditionindicatorcode4
, '' as c_2300p_crc06_conditionindicatorcode4
, '' as c_2300p_crc07_conditionindicatorcode4
, '' as c_2300p_crc01_servicecertificationcategory5
, '' as c_2300p_crc02_servicecertificationindicator5
, '' as c_2300p_crc03_conditionindicatorcode5
, '' as c_2300p_crc04_conditionindicatorcode5
, '' as c_2300p_crc05_conditionindicatorcode5
, '' as c_2300p_crc06_conditionindicatorcode5
, '' as c_2300p_crc07_conditionindicatorcode5
, '' as c_2300p_crc01_servicecertificationcategory6
, '' as c_2300p_crc02_servicecertificationindicator6
, '' as c_2300p_crc03_conditionindicatorcode6
, '' as c_2300p_crc04_conditionindicatorcode6
, '' as c_2300p_crc05_conditionindicatorcode6
, '' as c_2300p_crc06_conditionindicatorcode6
, '' as c_2300p_crc07_conditionindicatorcode6
, '' as c_2300p_crc01_servicecertificationcategory7
, '' as c_2300p_crc02_servicecertificationindicator7
, '' as c_2300p_crc03_conditionindicatorcode7
, '' as c_2300p_crc04_conditionindicatorcode7
, '' as c_2300p_crc05_conditionindicatorcode7
, '' as c_2300p_crc06_conditionindicatorcode7
, '' as c_2300p_crc07_conditionindicatorcode7
, '' as c_2300p_crc01_servicecertificationcategory8
, '' as c_2300p_crc02_servicecertificationindicator8
, '' as c_2300p_crc03_conditionindicatorcode8
, '' as c_2300p_crc04_conditionindicatorcode8
, '' as c_2300p_crc05_conditionindicatorcode8
, '' as c_2300p_crc06_conditionindicatorcode8
, '' as c_2300p_crc07_conditionindicatorcode8
, '' as c_835_header_ref_f2_drg_indicator
, '' as c_2300p_dtp01_datetimequalifier_17
, '' as c_2300p_dtp02_formatqualifier_17
, '' as c_2300p_dtp03_datetime_17
, '' as c_ccdt
, '' as c_ccar_client_id
, '' as c_epsdt
, '' as c_servicerenderinglocation
, '' as c_servicefacilitylocation
, '' as c_servicefacilitystatelicensenumber
, '' as c_servicesupervisinglocation
, '' as c_servicesupervisingstatelicensenumber
, '' as c_servicereferringlocation
, '' as c_2300p_pwk01_02_attachmentreporttype
, '' as c_2300p_pwk02_02_attachmenttransmissioncode
, '' as c_2300p_pwk06_02_attachmentcontrolnumber
, '' as c_2300p_pwk01_03_attachmentreporttype
, '' as c_2300p_pwk02_03_attachmenttransmissioncode
, '' as c_2300p_pwk06_03_attachmentcontrolnumber
, '' as c_2300p_pwk01_04_attachmentreporttype
, '' as c_2300p_pwk02_04_attachmenttransmissioncode
, '' as c_2300p_pwk06_04_attachmentcontrolnumber
, '' as c_2300p_pwk01_05_attachmentreporttype
, '' as c_2300p_pwk02_05_attachmenttransmissioncode
, '' as c_2300p_pwk06_05_attachmentcontrolnumber
, '' as c_2300p_pwk01_06_attachmentreporttype
, '' as c_2300p_pwk02_06_attachmenttransmissioncode
, '' as c_2300p_pwk06_06_attachmentcontrolnumber

from srh_edifecs_dev.careprovider_invoice_ride_details ird

inner join src_rds_saferide.rides r
    on r.id = ird.ride_id
left join src_rds_saferide.drivers d
    on d.id = r.driverid
left join src_rds_saferide.vehicle_company vc
    on vc.id = r.vehiclownerid 
join src_rds_saferide.passengers p on p.id = ird.passenger_id 
left join srh_ie2_ds.mongo_prod_db.member_data   md on md._id = ird.passenger_id
left join srh_ie2_ds.srh_mapping.tmhp_zip_county zcp on zcp.zipcode = p.zipcode 
left join srh_ie2_ds.srh_mapping.tmhp_zip_county zco on zco.zipcode = r.fromzipcode
left join srh_ie2_ds.srh_mapping.tmhp_zip_county zcd on zcd.zipcode = r.tozipcode
left join src_rds_saferide.vehicle_types vt
    on r.reqVehicleType = vt.id  

WHERE      
1=1
and ird.zero_cost_ride = 0 and ird.transport in ('AMB','WCV')
AND ird.hospital_group_id = 238

WITH NO SCHEMA BINDING;