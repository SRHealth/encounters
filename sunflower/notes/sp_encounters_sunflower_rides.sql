CALL srh_encounters.sp_encounters_sunflower_rides();

DROP procedure srh_encounters.sp_encounters_sunflower_rides();  

-- DELETE from srh_encounters.sunflower_encountered_rides;

-- DELETE from srh_encounters.sunflower_encountered_member_date_rides;

select 
etl_datetime::date 
,file_name
,count(*) as encounters 
,sum(daily_grand_total) as total_sum 
 
from srh_encounters.sunflower_encountered_member_date_rides e

where 
1=1
-- and e.etl_datetime::date = '2025-09-06'

group by 1,2
;



CREATE OR REPLACE PROCEDURE srh_encounters.sp_encounters_sunflower_rides_expenses()
LANGUAGE plpgsql
AS $$

DECLARE
    ride_count INTEGER;
    encounter_count INTEGER;
    etl_datetime_out TIMESTAMP;
  
BEGIN

insert into srh_encounters.sunflower_encountered_rides
(
	   primary_ride_id,
	   encounter_id,
       ride_id,
       expense_id,
       ride_datetime,
       ride_date,
       ride_leg_type,
       trip_expense_cost,
       mileage,
       transport,
       ride_from_address,
       ride_to_address,
       procedure_code,
       vehicle_type,
       vehicle_company_name,
       member_id,
       health_sub_plan_name,
       passenger_last_name,
       passenger_first_name,
       passenger_gender,
       passenger_current_address_1,
       passenger_current_address_2,
       passenger_current_city,
       passenger_current_state,
       passenger_current_zipcode,
       passenger_dob,
       ride_from_state,
       ride_from_zipcode,
       ride_to_state,
       ride_to_zipcode,
       claim_submitted_datetime,
       claim_paid_datetime,
       claimadjudicationdate,
       provider_name,
       provider_kmms_id,
       provider_tax_id,
       serv_loc_street_address_1,
       serv_loc_street_address_2,
       serv_loc_city,
       serv_loc_state,
       serv_loc_zip,
       serv_loc_zip_4_code,
       base_mile_limit,
       etl_datetime,
       file_name

)

WITH deduplicated_invoice_rides AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY ride_id ORDER BY trip_expense_cost DESC) as rn
    FROM srh_ie2_datalake.srh_invoices.sunflower_rides_expenses
),

sunflower_prn AS (
    select sp.*, 
           ROW_NUMBER() OVER (PARTITION BY sp.provider_name ORDER BY sp.provider_kmms_id ASC) AS rn
    FROM dev_stephanie.v_sunflower_prn_providers sp
), 

raw_rides AS (
    SELECT
        i.ride_id, 
        i.expense_id,
        i.ride_datetime,
        i.ride_datetime::date AS ride_date, 
        case when r.appointment_datetime is null then 'B' else 'A' end as ride_leg_type,
        i.trip_expense_cost,
        round(i.mileage,2) AS mileage,
        i.transport,
        i.ride_from_address,
        i.ride_to_address,
        spc.procedure_code,
        spc.vehicle_type,
        case 
        	when r.vehicle_company_name is null then 'SAFERIDE INC.'
        	when r.vehicle_company_name ilike '%GMR Validation%' then 'SAFERIDE INC.'
        	else r.vehicle_company_name
        end AS vehicle_company_name,
        i.member_id,
        i.health_sub_plan_name,
        r.passenger_last_name,
        r.passenger_first_name,
        r.passenger_gender,
        r.passenger_current_address_1,
		r.passenger_current_address_2,
		r.passenger_current_city,
		r.passenger_current_state,
		r.passenger_current_zipcode,
		r.passenger_dob,
		r.ride_from_state,
		r.ride_from_zipcode,
		r.ride_to_state,
		r.ride_to_zipcode,
        r.claim_submitted_datetime,
        r.claim_paid_datetime,
        CASE
            WHEN i.transport IN ('LFT','UBR') THEN
                CASE 
                    WHEN EXTRACT(DAY FROM i.ride_datetime) <= 15 
                    THEN DATEADD(month, 1, DATE_TRUNC('month', i.ride_datetime))
                    ELSE DATEADD(day, 9, DATEADD(month, 1, DATE_TRUNC('month', i.ride_datetime)))
                END
            WHEN c.payment_date IS NOT NULL 
            THEN c.payment_date
            WHEN r.claim_paid_datetime IS NULL 
            THEN i.invoice_date
            ELSE r.claim_paid_datetime::date 
        END AS claimAdjudicationDate,
		COALESCE(sp.provider_name, 'SAFERIDE INC.') AS provider_name,
		COALESCE(sp.provider_kmms_id, 'SAFERIDE INC.') AS provider_kmms_id,
		COALESCE(sp.provider_tax_id, '813037449') AS provider_tax_id,
		COALESCE(sp.serv_loc_street_address_1, '106 JEFFERSON ST') AS serv_loc_street_address_1,
		COALESCE(sp.serv_loc_street_address_2, '3RD FLOOR') AS serv_loc_street_address_2,
		COALESCE(sp.serv_loc_city, 'SAN ANTONIO') AS serv_loc_city,
		COALESCE(sp.serv_loc_state, 'TX') AS serv_loc_state,
		COALESCE(sp.serv_loc_zip, '78205') AS serv_loc_zip,
		COALESCE(sp.serv_loc_zip_4_code, '1005') AS serv_loc_zip_4_code

	FROM 
	    deduplicated_invoice_rides i  
	
	    inner join srh_dw.ride_summary  r 
	    	on i.ride_id = r.ride_id
	    inner join srh_encounters.sunflower_procedure_code_mapping AS spc 
	           ON i.transport = spc.vehicle_type	
	    left join salesforce_01.account s 
	    	on r.vehicle_company_id = s.safe_ride_vo_id_c
	    left join sunflower_prn sp -- 3 Providers mising 
	    	on s.medicaid_approved_number_c = sp.provider_kmms_id AND sp.rn = 1
	    left join srh_encounters.nemt_medicaid_calendar c
	       ON r.claim_submitted_datetime BETWEEN c.ride_start_date AND c.ride_end_date
	    left join  dev_stephanie.sunflower_spend_down_rides_2 spe 
		    ON spe.ride_date = i.ride_datetime::date
		    AND spe.member_id = i.member_id
		    AND coalesce(spe.vehicle_company_name,'SAFERIDE INC.') = coalesce(r.vehicle_company_name,'SAFERIDE INC.')
	WHERE 
		i.rn = 1
	    and i.ride_datetime >= '2025-01-01'
	    and i.ride_datetime < '2025-05-01'
	    and i.expense_id is null -- Rides only
	    and spe.ride_id is null -- Exclude spend down
	    AND NOT EXISTS ( -- Exclude previous encountered
	    SELECT 1 
	    FROM srh_encounters.sunflower_encountered_rides tt
	    WHERE tt.ride_id = i.ride_id
		)
	    AND (r.vehicle_company_name IS NULL -- Exclude providers erroring 
     	OR r.vehicle_company_name NOT IN (
         'Arrive Medical Transportation LLC (KS)',
         'Sunnyflower Transportation, LLC (KS)',
         'Quest Services, Inc. (KS)'
     ))
    
    
union all 


-- Expenses Union 
select 
        i.ride_id, 
        i.expense_id,
        i.ride_datetime,
        i.ride_datetime::date AS ride_date, 
        case when r.appointment_datetime is null then 'B' else 'A' end as ride_leg_type,
        i.trip_expense_cost,
        round(i.mileage,2) AS mileage,
        i.transport,
        i.ride_from_address,
        i.ride_to_address,
		v.procedure_code,
		v.vehicle_type,
        'SAFERIDE INC.' AS vehicle_company_name,
        i.member_id,
        i.health_sub_plan_name,
        p.lastname as passenger_last_name,
        p.firstname as passenger_first_name,
        p.passenger_gender,
        p.current_address_1 as passenger_current_address_1,
        p.current_address_2 as passenger_current_address_2,
        p.current_city as passenger_current_city,
        p.current_state as passenger_current_state,
        p.current_zipcode as passenger_current_zipcode,
        p.dateofbirth as passenger_dob,
		r.ride_from_state,
		r.ride_from_zipcode,
		r.ride_to_state,
		r.ride_to_zipcode,
        null as claim_submitted_datetime,
        i.invoice_date as claim_paid_datetime,
        i.invoice_date as claim_adjudication_date,
        'SAFERIDE INC.' as provider_name,
        '30005195050001' AS provider_kmms_id,
		'813037449' AS provider_tax_id,
		'106 JEFFERSON ST' AS serv_loc_street_address_1,
		'3RD FLOOR' AS serv_loc_street_address_2,
		'SAN ANTONIO' AS serv_loc_city,
		'TX' AS serv_loc_state,
		'78205' AS serv_loc_zip,
		'1005' AS serv_loc_zip_4_code

	from srh_ie2_datalake.srh_invoices.sunflower_rides_expenses i  

	inner join srh_dw.ride_summary  r 
	    on i.ride_id = r.ride_id
	inner join stg_tahoe.expenses_raw e 
		on i.expense_id = e.expense_id       
	inner join srh_dw.dim_passenger p
		on e.report_passenger_id = p.passenger_id 
	inner join dev_stephanie.sunflower_procedure_code v 
		on case when e.expense_merchant ilike '%Meal%' then 'Meals'
	    	  when e.expense_merchant = 'SafeRide Health' then 'Meals'
	          when e.expense_merchant ilike '%Lodging%' then 'Lodging'
	          when e.expense_merchant ilike '%Hotel%' then 'Lodging'   
	        else i.expense_category end = v.vehicle_type
	left join  dev_stephanie.sunflower_spend_down_rides_2 spe 
		    ON spe.ride_date = i.ride_datetime::date
		    AND spe.member_id = i.member_id
		    AND coalesce(spe.vehicle_company_name,'SAFERIDE INC.') = coalesce(r.vehicle_company_name,'SAFERIDE INC.')
	 
	WHERE
	i.expense_id is not null 
	and i.ride_datetime >= '2025-01-01'
	and i.ride_datetime < '2025-05-01'
	and spe.ride_id is null -- Exclude spend down
	AND NOT EXISTS ( -- Exclude previous encountered
    SELECT 1 
    FROM srh_encounters.sunflower_encountered_rides tt
    WHERE tt.ride_id = i.ride_id
	)
),

-- 2) Aggregate daily data per passenger/date/company.
daily_rollup AS (
    SELECT
        member_id,
        ride_date,
        provider_name,
        procedure_code,
        COUNT(DISTINCT ride_id)         AS daily_ride_count,
        SUM(mileage)       				AS total_daily_distance,
        SUM(trip_expense_cost)           AS daily_grand_total,
        MIN(MIN(ride_id)) OVER (
            PARTITION BY member_id, ride_date, provider_name
        )                               AS primary_ride_id -- minimal ride_id across all proc_codes
        ,row_number() over (order by ride_date asc) rn
    FROM raw_rides
    GROUP BY
        member_id,
        ride_date,
        provider_name,
        procedure_code
)

SELECT
   dr.primary_ride_id 
  ,'SR' || dr.primary_ride_id || TO_CHAR(dr.ride_date,'YYMMDD') as encounter_id 
  ,rr.*
  ,CASE WHEN dr.daily_ride_count = 1 THEN 10 ELSE 20 END AS base_mile_limit
  ,getdate() as etl_datetime
  ,'KC19837P' || TO_CHAR(getdate(), 'YYYYMMDDHHMMSS') as file_name
FROM
  raw_rides rr
  inner join daily_rollup dr
    on 1=1
    and dr.member_id = rr.member_id
    and dr.ride_date = rr.ride_date
    and dr.provider_name = rr.provider_name
    and dr.procedure_code = rr.procedure_code
WHERE 
  1=1
  and dr.rn <= 5000
;


delete 
from srh_encounters.sunflower_encountered_member_date_rides
WHERE etl_datetime = (SELECT MAX(etl_datetime) FROM srh_encounters.sunflower_encountered_rides)
;

insert into srh_encounters.sunflower_encountered_member_date_rides
(
       member_id,
       ride_date,
       provider_name,
       procedure_code,
       file_name,
       daily_ride_count,
       total_daily_distance,
       daily_grand_total,
       primary_ride_id,
       rn,
       etl_datetime
)
SELECT
  member_id,
  ride_date,
  provider_name,
  procedure_code,
  file_name,
  COUNT(DISTINCT ride_id)         AS daily_ride_count,
  SUM(mileage)       				AS total_daily_distance,
  SUM(trip_expense_cost)           AS daily_grand_total,
  MIN(MIN(ride_id)) OVER (
            PARTITION BY member_id, ride_date, provider_name
        )                               AS primary_ride_id,
  row_number() over (order by ride_date asc) as rn,
  t.etl_datetime as etl_datetime
FROM 
  srh_encounters.sunflower_encountered_rides t
WHERE
  t.etl_datetime = (SELECT MAX(etl_datetime) FROM srh_encounters.sunflower_encountered_rides t)
GROUP BY
  member_id,
  ride_date,
  provider_name,
  procedure_code,
  file_name,
  etl_datetime
;


END
;
$$
;