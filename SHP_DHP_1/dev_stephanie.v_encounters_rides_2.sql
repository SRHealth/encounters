CREATE VIEW dev_stephanie.v_encounters_rides_2 AS 

with cte_encounters_submitted as
(
select
distinct   
  e.authorization_number
  ,cast(
    CASE
      -- If the string starts with numbers followed by "S0001", extract the numeric part
      WHEN e.authorization_number ~ '^[0-9]+S0001$' 
        THEN SUBSTRING(e.authorization_number FROM 1 FOR POSITION('S' IN e.authorization_number) - 1)
    end 
   as varchar(32)) as ride_id
--     ,len(e.authorization_number) as char_length

from
  sandbox_bi.encounters e

where
  e.created_at >= '2025-01-01'
  and len(e.authorization_number) < 16

),
cte_encounters_prospective as
(
select
  rs.ride_id
from  
  srh_dw.ride_summary rs
  left join cte_encounters_submitted cte
	 on rs.ride_id = cast(cte.ride_id as int)
where
  rs.hospital_group_id in (106, 135) -- TX Medicaid plans only 
  and rs.ride_create_datetime >= '2025-01-01'
  and cte.ride_id is null
),
cte_v as

(
select
  v.safe_ride_vehicle_id_c

  ,v.vehicle_vin_c
  ,v.created_date
  ,row_number() over (partition by v.safe_ride_vehicle_id_c order by v.created_date desc) as rn
from
  salesforce_01.vehicle_compliance_c v

)

select  
'Ride' as Encounter_Type
,case  
    when rs.vehicle_category_adj = 'Rideshare' then 'TNC Demand Response'
	when rs.vehicle_type_nick_name IN ('GND','AMB','WCV','BUS','WCX') and rs.ride_mode = 'Public' then 'Mass Transit'
	when rs.vehicle_category_adj = 'NEMT' then 'Demand Response'
	when rs.vehicle_category_adj = 'Mileage Reimbursement' AND LOWER(mt88_s.first_name) = LOWER(rs.passenger_first_name) then 'ITP - Self'
	when rs.vehicle_category_adj = 'Mileage Reimbursement' then 'ITP - Other'   
  end as scenario
,rs.hospital_group_id as payerName
,case	
	when rs.vehicle_type_nick_name = 'LFT' then 'LYFT HEALTHCARE INC'
	when rs.vehicle_type_nick_name = 'UBR' then 'UBER HEALTH LLC'
	when scenario = 'Mass Transit' then 'DART'
	when scenario like '%ITP%' then NULL
	else rs.vehicle_company_name 
  end as vehicleCompanyName
,dp.shp_plan_id as planCode
,cast(case
	when scenario like '%TNC%' then NULL 
	when scenario like '%ITP%' then NULL
	else s.npi_identifier_c 
  end as varchar(30)) as vehicleCompanyNPI
,case
	when rs.vehicle_type_nick_name = 'LFT' then 'A422529401'
	when rs.vehicle_type_nick_name = 'UBR' then 'A423114401'
	else NULL 
  end as vehicleCompanyAPI
,case  
	when scenario = 'TNC Demand Response' or scenario like '%ITP%' then '347C00000X'
	when scenario = 'Demand Response' then '343800000X'
	when scenario = 'Mass Transit' then '347B00000X'
  end as vehicleCompanyTaxonomy
,case 
    when rs.vehicle_type_nick_name = 'LFT' then '842337700'
	when rs.vehicle_type_nick_name = 'UBR' then '452647441'
	when scenario = 'Demand Response' then REPLACE(s.ein_c, '-', '') 
	when scenario like '%ITP%' then NULL 
	when scenario = 'Mass Transit' then '813037449'
  end as vehicleCompanyTaxId
,case 
	when scenario like '%ITP%' then 'T'+concat(dp.shp_plan_id, 
						case when rs.hospital_group_id=106 then mt88_s.mti
        				when rs.hospital_group_id=135 then mt88_d.mti 
        				end)
	when scenario = 'Mass Transit' then 'T'+concat(dp.shp_plan_id,'0003690')
	else NULL
  end as billingProviderMTI
,case 
	when rs.vehicle_type_nick_name = 'LFT' then '185 Berry Street'
	when rs.vehicle_type_nick_name = 'UBR' then '1515 3rd St' 
	when scenario like '%ITP%' then dc.mailing_address_street_s
	when scenario = 'Demand Response' then s.billing_street
	when scenario = 'Mass Transit' then '106 JEFFERSON ST'
	end as billingAddress
,case when scenario = 'Mass Transit' then '3RD FL'else null end as billingAddress2
,case 
	when scenario like '%TNC%' then 'San Francisco'
	when scenario like '%ITP%' then dc.mailing_address_city_s
	when scenario = 'Demand Response' then s.billing_city
	when scenario = 'Mass Transit' then 'SAN ANTONIO'
	end as billingCity
,case 
	when scenario like '%TNC%' then 'CA'
	when scenario like '%ITP%' then upper(dc.mailing_address_state_code_s)
	when scenario = 'Demand Response' then s.billing_state_code
	when scenario = 'Mass Transit' then 'TX' 
	end as billingState 
,case 
	when rs.vehicle_type_nick_name = 'LFT' then '94107'
	when rs.vehicle_type_nick_name = 'UBR' then '94158'
	when scenario like '%ITP%' then dc.mailing_address_postal_code_s
	when scenario = 'Demand Response' then s.billing_postal_code
	when scenario = 'Mass Transit' then '78205'
	end as billingZip
,billingAddress as billingPayToAddress
,billingAddress2 as billingPayToAddress2
,billingCity as billingPayToCity
,billingState as billingPayToState 
,billingZip as billingPayToZip
,case  
	when scenario = 'ITP - Self' then 'A0090'
	when scenario = 'ITP - Other' then 'A0080'
	when scenario = 'TNC Demand Response' then 'A0120'
	when scenario = 'Demand Response' then 'A0100'
	when scenario = 'Mass Transit' then 'T2004'
  end as procCode
,COALESCE(ap1.provider_name, ap2.provider_name) as careProviderName
,COALESCE(ap1.provider_tpi, ap2.provider_tpi) as careProviderTPI
,case when careProviderName = 'Total Rehab Kids' then '1578834172' 
	else COALESCE(ap1.provider_npi, ap2.provider_npi) end as careProviderNPI
,case 
	when COALESCE(ap1.provider_taxonomy_code, ap2.provider_taxonomy_code) =''
		and careProviderName like '%Dialysis%' or careProviderName like '%FRESENIUS%' or careProviderName like '%KIDNEY%' or careProviderName like '%Kidney%' then '261QE0700X'
		when careProviderName = 'Livingston Hearing Aid Center' then '237600000X'
		when careProviderName = 'MHMR SERVICES FOR THE CONCHO VALLEY' then '193200000X'
		when careProviderName = 'Total Rehab Kids' then '261QR0400X'
	else COALESCE(ap1.provider_taxonomy_code, ap2.provider_taxonomy_code) end as careProviderTaxonomy
,COALESCE(ap1.provider_tax_id, ap2.provider_tax_id) as careProviderTaxId
,case 
	when COALESCE(ap1.provider_street, ap2.provider_street) IS NULL 
	or COALESCE(ap1.provider_street, ap2.provider_street) = ''
	then COALESCE(LEFT(ap1.provider_address,20), LEFT(ap2.provider_address,20))  
	else COALESCE(ap1.provider_street, ap2.provider_street) end as careProviderAddress
,case 
	when COALESCE(ap1.provider_city, ap2.provider_city) IS NULL 
	or COALESCE(ap1.provider_street, ap2.provider_street) = '' 
	then COALESCE(reverse(split_part(reverse(ap1.provider_address), ' ', 3)),reverse(split_part(reverse(ap2.provider_address), ' ', 3)))
	else COALESCE(ap1.provider_city, ap2.provider_city) end as careProviderCity
,case
	when COALESCE(ap1.provider_state, ap2.provider_state) IS NULL 
	or COALESCE(ap1.provider_street, ap2.provider_street) = '' 
	then COALESCE(LEFT(RIGHT(ap1.provider_address,8),2),LEFT(RIGHT(ap2.provider_address,8),2))
	else COALESCE(ap1.provider_state, ap2.provider_state) end as careProviderState
,case 
	when COALESCE(ap1.provider_zipcode, ap2.provider_zipcode) IS NULL 
	or COALESCE(ap1.provider_street, ap2.provider_street) = '' 
	then COALESCE(RIGHT(ap1.provider_address,5),RIGHT(ap2.provider_address,5))
	else COALESCE(ap1.provider_zipcode, ap2.provider_zipcode) end as careProviderZipcode
,case 
	when scenario like '%TNC%' then 'TTNCLASTNAME' 
	when scenario = 'Mass Transit' then NULL  
	when (scenario = 'Demand Response' or scenario like '%ITP%') then 
												case when rs.hospital_group_id=106 then mt88_s.last_name
        										when rs.hospital_group_id=135 then mt88_d.last_name 
        										end
	end as driverLastname
,case 
	when scenario like '%TNC%' then rs.driver_first_name 
	when scenario = 'Mass Transit' then NULL
	when (scenario = 'Demand Response' or scenario like '%ITP%') then 
												case when rs.hospital_group_id=106 then mt88_s.first_name
        										when rs.hospital_group_id=135 then mt88_d.first_name 
        										end  
	end as driverFirstname
,case 
	when scenario like '%TNC%' then 'TTNCDRIVER'
	when scenario = 'Mass Transit' then NULL
	when (scenario = 'Demand Response' or scenario like '%ITP%') then 'T'+concat(dp.shp_plan_id,
												case when rs.hospital_group_id=106 then mt88_s.mti
        										when rs.hospital_group_id=135 then mt88_d.mti 
        										end)
	end as driverMTI
,case 
	when (scenario like '%TNC%' or scenario = 'Mass Transit') then NULL
	when (scenario = 'Demand Response' or scenario like '%ITP%') then 
												case when rs.hospital_group_id=106 then mt88_s.dl
        										when rs.hospital_group_id=135 then mt88_d.dl 
        										end 
	end as driverLicense
,case
	when (scenario like '%TNC%' or scenario = 'Mass Transit') then NULL
	when (scenario = 'Demand Response' or scenario like '%ITP%') then right('00'+
												case when rs.hospital_group_id=106 then mt88_s.tin
        										when rs.hospital_group_id=135 then mt88_d.tin 
        										end,9)
	end as driverSSN
,rs.passenger_medical_id as patientMedicalId
,rs.passenger_last_name as insuredLast
,rs.passenger_first_name as insuredFirst
,NULL as insuredMI
,rs.passenger_current_address_1 as insuredAddress
,NULL as insuredAddress2
,rs.passenger_city as insuredCity
,left(case when rs.passenger_current_state='Texas' then 'TX' else rs.passenger_current_state end,2) as insuredState
,rs.passenger_zipcode as insuredZip
,rs.passenger_dob as insuredDOB
,case when dp.passenger_gender ='Female' then 'F' when dp.passenger_gender ='Male' then 'M' else NULL end as insuredSex
,insuredLast as patLast
,insuredFirst as patFirst
,insuredAddress as patientAddress
,NULL as patientAddress2
,insuredCity as patientCity
,insuredState as patientState
,insuredZip as patientZip
,insuredDOB as patDOB
,case when dp.passenger_gender ='Female' then 'F' when dp.passenger_gender ='Male' then 'M' else NULL end as patSex
,rs.estimated_start_datetime_local_timezone as pickupDate
,rs.estimated_start_datetime_local_timezone as appointmentDate
,concat(rs.ride_id,'S0001') as authorizationNumber1
,NULL as authorizationNumber2
,'1' as Frequency
,case 
	when scenario like '%TNC%' or scenario = 'Mass Transit' then rs.estimated_start_datetime_local_timezone
	when rs.claim_submitted_datetime IS NULL then rs.estimated_start_datetime_local_timezone
	else CONVERT_TIMEZONE('US/Central', rs.claim_submitted_datetime) 
	end as claimReceiptDate  
-- ,case
-- 	when scenario like '%TNC%' or scenario = 'Mass Transit' then claimReceiptDate
-- 	when rs.claim_approved_datetime is null then b.adjudication_date
-- 	else CONVERT_TIMEZONE('US/Central', rs.claim_approved_datetime)
-- 	end as claimAdjudicationDate
,CASE
	when rs.vehicle_type_nick_name = 'LFT' then tb.shp_lyft_payment_date::date
	when rs.vehicle_type_nick_name = 'UBR' then tb.shp_uber_payment_date::date
	when rs.ride_mode = 'Public' and rs.claim_paid_datetime is null then si.created_at
    WHEN rs.claim_submitted_datetime >= c.ride_start_date 
         AND rs.claim_submitted_datetime < c.ride_end_date + 1
    THEN c.payment_date::date
    ELSE trunc(rs.claim_paid_datetime)::date END as claimAdjudicationDate -- Edited on 6/5/25
,coalesce(cm.hhsc_county_code,'999') as memberCountyCode
,COALESCE(cf.hhsc_county_code,'999') as pickupCountyCode
,COALESCE(ct.hhsc_county_code,'999') as dropoffCountyCode
,COALESCE(a.tmhp_code_1,'00') as specialNeeds1
,COALESCE(a.tmhp_code_2,'00')  as specialNeeds2
,COALESCE(a.tmhp_code_3,'00') as specialNeeds3
,COALESCE(a.tmhp_code_4,'00') as specialNeeds4
,'DB' as paymentMethod
,left(case 
	when scenario like '%TNC%' or scenario = 'Mass Transit' then NULL
	when scenario = 'Demand Response' then cte_v.vehicle_vin_c
	when scenario like '%ITP%' then dc.vehicle_vin_c
	end,17) as VIN
,NULL as SharedServicesRefID
-- ,case
-- 	when rs.vehicle_type_nick_name = 'LFT' then tb.shp_lyft_payment_date
-- 	when rs.vehicle_type_nick_name IN ('UBR','UBRN') then tb.shp_uber_payment_date
-- 	when rs.claim_paid_datetime is null then b.payment_issued
-- 	else CONVERT_TIMEZONE('US/Central', rs.claim_paid_datetime) 
-- 	end as claimPaymentDate 
,CASE
	when rs.vehicle_type_nick_name = 'LFT' then tb.shp_lyft_payment_date
	when rs.vehicle_type_nick_name = 'UBR' then tb.shp_uber_payment_date
	when rs.ride_mode = 'Public' and rs.claim_paid_datetime is null then si.created_at
    WHEN rs.claim_submitted_datetime >= c.ride_start_date 
         AND rs.claim_submitted_datetime < c.ride_end_date + 1
    THEN c.payment_date
    ELSE trunc(rs.claim_paid_datetime) END as claimPaymentDate -- Edited on 6/5/25
,case when datediff(hour, cast(rs.ride_create_datetime as timestamp), cast(rs.estimated_start_datetime as timestamp)) < 48 then 'Y' else 'N' end as lessThan48Hour
,case when datediff(year, cast(rs.passenger_dob as timestamp), cast(rs.estimated_start_datetime as timestamp)) < 18 then '00' 
	  when datediff(year, cast(rs.passenger_dob as timestamp), cast(rs.estimated_start_datetime as timestamp)) >= 18 AND datediff(year, cast(rs.passenger_dob as timestamp), cast(rs.estimated_start_datetime as timestamp)) <= 20 then '06' 
	  when datediff(year, cast(rs.passenger_dob as timestamp), cast(rs.estimated_start_datetime as timestamp)) > 20 then '07'
	end as riskGroup
,NULL as tripVerified
,case when rs.health_sub_plan_id=207 then 'V' end as OtherEventIndicator

-- from address
,TRIM(SPLIT_PART(rs.ride_from_address,',',1)) as fromAddress1
,TRIM(SPLIT_PART(rs.ride_from_address,',',2)) as fromCity
,rs.ride_from_state as fromState
,left(rs.ride_from_zipcode,5) as fromZipcode 
-- ,case 
--      when rs.ride_from_address like '%,%' 
--        then SPLIT_PART(rs.ride_from_address, ',', 1) 
--     when len(rs.ride_from_address) > 6 and rs.ride_from_address not like '%,%'   
--     then substring(rs.ride_from_address
--                  , 1
--                    , charindex(
--                        split_part(rs.ride_from_address, ' ', regexp_count(rs.ride_from_address, ' ')) || ' ' || split_part(rs.ride_from_address, ' ', regexp_count(rs.ride_from_address, ' ') + 1)
--                        ,rs.ride_from_address
--                        ) - 1
--              )
--     else rs.ride_from_address
--     end as fromAddress1
-- ,case
--  	when rs.ride_from_address like '%,%' 
--  		then SPLIT_PART(rs.ride_from_address, ',', 2)
--     when len(rs.ride_from_address) > 6 and rs.ride_from_address not like '%,%'  
--  		then reverse(split_part(reverse(rs.ride_from_address), ' ', 3))
--     else SPLIT_PART(rs.ride_from_address, ',', 2)
--  		end as fromCity
-- ,case 
--     when substring(
--                trim(SPLIT_PART(rs.ride_from_address, ',', 3))
--                , 1
--                , charindex(
--                  split_part(trim(SPLIT_PART(rs.ride_from_address, ',', 3)), ' ', regexp_count(trim(SPLIT_PART(rs.ride_from_address, ',', 3)), ' ') + 1) 
--                  ,trim(SPLIT_PART(rs.ride_from_address, ',', 3))
--                  ) - 1
--                ) = 'Texas' then 'TX'
--    when rs.ride_from_address like '%,%'  
--      then substring(
--                trim(SPLIT_PART(rs.ride_from_address, ',', 3))
--                , 1
--                , charindex(
--                  split_part(trim(SPLIT_PART(rs.ride_from_address, ',', 3)), ' ', regexp_count(trim(SPLIT_PART(rs.ride_from_address, ',', 3)), ' ') + 1) 
--                  ,trim(SPLIT_PART(rs.ride_from_address, ',', 3))
--                  ) - 1
--                )
--    when len(rs.ride_from_address) > 6 and rs.ride_from_address not like '%,%'
--    then split_part(rs.ride_from_address, ' ', regexp_count(rs.ride_from_address, ' '))
--    else null 
--   end as fromState
-- ,case 
--    when rs.ride_from_address like '%,%'
--      then split_part(trim(SPLIT_PART(rs.ride_from_address, ',', 3)), ' ', regexp_count(trim(SPLIT_PART(rs.ride_from_address, ',', 3)), ' ') + 1) 
--    when len(rs.ride_from_address) > 6 and rs.ride_from_address not like '%,%'
--     then split_part(rs.ride_from_address, ' ', regexp_count(rs.ride_from_address, ' ') + 1)
--     else null 
--   end as fromZipcode
  
-- to address 
,TRIM(SPLIT_PART(rs.ride_to_address,',',1)) as toAddress1
,TRIM(SPLIT_PART(rs.ride_from_address,',',2)) as toCity
,rs.ride_to_state as toState
,left(rs.ride_to_zipcode,5) as toZipcode
-- ,case 
--      when rs.ride_to_address like '%,%' 
--        then SPLIT_PART(rs.ride_to_address, ',', 1) 
--     when len(rs.ride_to_address) > 6 and rs.ride_to_address not like '%,%'   
--     then substring(rs.ride_to_address
--                  , 1
--                    , charindex(
--                        split_part(rs.ride_to_address, ' ', regexp_count(rs.ride_to_address, ' ')) || ' ' || split_part(rs.ride_to_address, ' ', regexp_count(rs.ride_to_address, ' ') + 1)
--                        ,rs.ride_to_address
--                        ) - 1
--              )
--     else rs.ride_to_address
--     end as toAddress1
--  ,case
--  	when rs.ride_to_address like '%,%' 
--  		then SPLIT_PART(rs.ride_to_address, ',', 2)
--     when len(rs.ride_to_address) > 6 and rs.ride_to_address not like '%,%'  
--  		then reverse(split_part(reverse(rs.ride_to_address), ' ', 3))
--     else SPLIT_PART(rs.ride_to_address, ',', 2)
--  		end as toCity
-- ,case 
--     when substring(
--                trim(SPLIT_PART(rs.ride_to_address, ',', 3))
--                , 1
--                , charindex(
--                  split_part(trim(SPLIT_PART(rs.ride_to_address, ',', 3)), ' ', regexp_count(trim(SPLIT_PART(rs.ride_to_address, ',', 3)), ' ') + 1) 
--                  ,trim(SPLIT_PART(rs.ride_to_address, ',', 3))
--                  ) - 1
--                ) = 'Texas' then 'TX'
--    when rs.ride_to_address like '%,%'  
--      then substring(
--                trim(SPLIT_PART(rs.ride_to_address, ',', 3))
--                , 1
--                , charindex(
--                  split_part(trim(SPLIT_PART(rs.ride_to_address, ',', 3)), ' ', regexp_count(trim(SPLIT_PART(rs.ride_to_address, ',', 3)), ' ') + 1) 
--                  ,trim(SPLIT_PART(rs.ride_to_address, ',', 3))
--                  ) - 1
--                )
--    when len(rs.ride_to_address) > 6 and rs.ride_to_address not like '%,%'
--    then split_part(rs.ride_to_address, ' ', regexp_count(rs.ride_to_address, ' '))
--    else null 
--   end as toState
-- ,case 
--    when rs.ride_to_address like '%,%'
--      then split_part(trim(SPLIT_PART(rs.ride_to_address, ',', 3)), ' ', regexp_count(trim(SPLIT_PART(rs.ride_to_address, ',', 3)), ' ') + 1) 
--    when len(rs.ride_to_address) > 6 and rs.ride_to_address not like '%,%'
--     then split_part(rs.ride_to_address, ' ', regexp_count(rs.ride_to_address, ' ') + 1)
--     else null 
--   end as toZipcode
  
,ROUND(rs.ride_cost_actual,2) as finalCost 
,NULL as payingICN
,ROUND(rs.ride_distance,2) as units
,rs.estimated_start_datetime_local_timezone as pickupDate2
,case when datediff(day, cast(rs.passenger_dob as timestamp), cast(rs.estimated_start_datetime as timestamp)) < 6572 then '1' end as attendants


-- other data attributes

,case when rs.hospital_group_id=106 then mt88_s.mti 
	  when rs.hospital_group_id=135 then mt88_d.mti
	end as mti
,case when rs.hospital_group_id=106 then mt88_s.dl_val_indicator 
	  when rs.hospital_group_id=135 then mt88_d.dl_val_indicator
	end as dl_val_indicator
,case when rs.hospital_group_id=106 then mt88_s.dl_effective_date::date 
	  when rs.hospital_group_id=135 then mt88_d.dl_effective_date::date 
	end as dl_effective_date
,case when rs.hospital_group_id=106 then mt88_s.effective_date::date 
	  when rs.hospital_group_id=135 then mt88_d.effective_date::date
	end as effective_date
,case when rs.hospital_group_id=106 then mt88_s.specialty_code 
	  when rs.hospital_group_id=135 then mt88_d.specialty_code
	end as specialty_code
,rs.vehicle_company_name
,rs.ride_mode
,rs.vehicle_id as vehicle_id
,rs.driver_id as driver_id
-- ,case when rs.ride_distance = 0 then m.distance
-- 	else rs.ride_distance end as units_raw
,rs.ride_cost_actual as total_cost
,si.created_at as invoice_created_at



from  
sandbox_bi.rides_invoiced si

inner join srh_dw.ride_summary rs 
	on si.ride_id = rs.ride_id 
   
--  inner join sandbox_bi.rides_invoiced si
-- 	on rs.ride_id = si.ride_id

 inner join cte_encounters_prospective cte_ep
  on cte_ep.ride_id = rs.ride_id
  
  
 left join dev_stephanie.v_shp_dhp_metadata_assistance_needs a 
	on si.ride_id = a.ride_id
 
-- left join srh_mapping.tmhp_ride_assistance_detail ad
-- 	left join dev_stephanie.tmhp_ride_assistance_detail ad 
--     on rs.ride_id = ad.ride_id and ad.assistance_id_order = 1
-- left join srh_mapping.tmhp_ride_assistance_detail ad2
-- 	left join dev_stephanie.tmhp_ride_assistance_detail ad2 
--     on rs.ride_id = ad2.ride_id and ad2.assistance_id_order = 2
-- left join srh_mapping.tmhp_ride_assistance_detail ad3
-- 	left join dev_stephanie.tmhp_ride_assistance_detail ad3 
--     on rs.ride_id = ad3.ride_id and ad3.assistance_id_order = 3
-- left join srh_mapping.tmhp_ride_assistance_detail ad4
-- 	left join dev_stephanie.tmhp_ride_assistance_detail ad4 
--     on rs.ride_id = ad4.ride_id and ad4.assistance_id_order = 4
	
--   left join sandbox_bi.tmhp_special_needs sn
-- 	on ad.tmhp_assitance_id = sn.srh_assistance_id
-- left join sandbox_bi.tmhp_special_needs sn2
-- 	on ad2.tmhp_assitance_id = sn2.srh_assistance_id
-- left join sandbox_bi.tmhp_special_needs sn3 
-- 	on ad3.tmhp_assitance_id = sn3.srh_assistance_id
-- left join sandbox_bi.tmhp_special_needs sn4 
-- 	on ad4.tmhp_assitance_id = sn4.srh_assistance_id	
	
left join dms_rds_saferide.approved_providers ap1
    on rs.pickup_provider_id = ap1.id
left join dms_rds_saferide.approved_providers ap2
    on rs.dropoff_provider_id = ap2.id
    
LEFT JOIN srh_encounters.nemt_medicaid_calendar c 
  ON date(rs.claim_submitted_datetime) BETWEEN c.ride_start_date AND c.ride_end_date

left join sandbox_bi.tmhp_277_import_raw t
	on rs.ride_id = left(right(t.claim_reference_id,13),8) 
	
left join srh_encounters.shp_billing_schedule b
	on si.created_at = b.invoice_date
	
left join srh_encounters.tnc_billing_schedule tb
	on DATE(rs.estimated_start_datetime_local_timezone) = tb.ride_date 

-- left join dms_rds_saferide.ride_metadata rm
-- 	on rm.ride_id = rs.ride_id
	
-- left join srh_encounters.mileage_zero m
-- 	on rs.ride_id = m.ride_id

left join srh_dw.dim_passenger dp
    on rs.passenger_id = dp.passenger_id

left join sandbox_bi.tmhp_zipcodes cm
	on dp.current_zipcode = cm.zipcode
left join sandbox_bi.tmhp_zipcodes cf 
	on rs.ride_from_zipcode = cf.zipcode
left join sandbox_bi.tmhp_zipcodes ct
	on rs.ride_to_zipcode = ct.zipcode

-- left join dms_rds_saferide.ride_metadata_view rv
-- 	on rs.ride_id = rv.ride_id

left join salesforce_01.account s
	on rs.vehicle_company_id = s.safe_ride_vo_id_c and is_deleted = FALSE 
	
left join salesforce_01.driver_compliance_c dc
	on rs.driver_mti_number = dc.mti_c

left join sandbox_bi.mt88_shp_production_20250724 mt88_s
	on rs.driver_mti_number = mt88_s.mti

left join sandbox_bi.mt88_dhp_production_20250727 mt88_d
	on rs.driver_mti_number = mt88_d.mti
	
left join cte_v
 	on rs.vehicle_id = cte_v.safe_ride_vehicle_id_c and cte_v.rn = 1
 	

WHERE 
    t.claim_reference_id IS NULL
    AND si.created_at >= '2024-08-01'  
    AND dp.shp_plan_id <> 'S4' 
-- 	AND (rs.driver_id IS NULL OR rs.driver_id NOT IN (
--             38447, 37860, 9760, 6280, 6098, 26909, 16260, 20899, 5744, 30526, 31907, 
--             6853, 16258, 16276, 22078, 25852, 5926, 10794, 33206, 33313, 22274, 
--             42309, 38771, 31084, 40597, 32811, 22697, 32154, 31903, 41597, 9269, 
--             30568, 41594, 6792, 32589, 41419, 31222, 31228, 30052, 32149, 31220, 
--             33292
--         ))

-- limit 10 

WITH NO SCHEMA BINDING 
;