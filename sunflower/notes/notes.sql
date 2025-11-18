-- No Spend Down in Expenses 
select * 

	from srh_ie2_datalake.srh_invoices.sunflower_rides_expenses i  

	inner join srh_dw.ride_summary  r 
	    on i.ride_id = r.ride_id

	WHERE
	i.expense_id is not null
	and r.health_sub_plan_name = 'Medically Needy Population'
	;

select 
m.etl_datetime
,m.file_name
,count(*) 
,sum(daily_grand_total)

from srh_encounters.sunflower_encountered_member_date_rides m

group by 1,2

order by m.etl_datetime desc ;


call srh_encounters.sp_encounters_sunflower_rides();

-- Attested Encounters Form 
select 
e.file_name || '.dat'
,min(to_date(e.pickupdate,'YYYYMMDD')) as min_ride_date
,max(to_date(e.pickupdate,'YYYYMMDD')) as max_ride_date
,min(to_date(e.claimpaymentdate,'YYYYMMDD')) as min_payment_date
,max(to_date(e.claimpaymentdate,'YYYYMMDD')) as max_payment_date
,count(distinct e.authorizationnumber1) as encounter_count 

from srh_encounters.v_sunflower_encounters_ems_2 e

where 
e.insuredcity is not null 

group by 1 
;



-- Submitted Encounters
select 
-- e.file_name
count(*)
,sum(e.allowedamt) 

from srh_encounters.srh_sunflower_encounters_submitted e

-- group by 1
;

select 
status_category
,count(distinct r.primary_ride_id)

from 
srh_encounters.sunflower_encountered_member_date_rides r

inner join dev_stephanie.sunflower_manual_response_20250513 m
	on r.primary_ride_id = right(left(m.claim_id,10),8)

where 
m.claim_id is not null 

group by 1
;

select 
-- r.primary_ride_id
-- ,r.ride_date
m.tsnsts
,count(distinct r.primary_ride_id) 

from srh_encounters.sunflower_encountered_member_date_rides r

inner join srh_spectrum_external.srh_sunflower_encounters_responses m 
	on r.primary_ride_id = right(left(m.patientacntnbr,10),8)

where 
m.patientacntnbr is not null 

group by 1
;


select * from srh_spectrum_external.srh_sunflower_encounters_responses limit 10;


select 
r.tsnsts as encounter_status 
,count(*)

from srh_spectrum_external.srh_sunflower_encounters_responses r

group by 
1;



-- ############## Checks for Encounters ###############



WITH deduplicated_invoice_rides AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY ride_id ORDER BY trip_expense_cost DESC) as rn
    FROM srh_ie2_datalake.srh_invoices.sunflower_rides_expenses
),

sunflower_prn AS (
    select sp.*, 
           ROW_NUMBER() OVER (PARTITION BY sp.provider_name ORDER BY sp.provider_kmms_id ASC) AS rn
    FROM dev_stephanie.v_sunflower_prn_providers sp
)

    SELECT
	count(i.ride_id)
	,sum(i.trip_expense_cost) as totalcost 

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
	WHERE 
		i.rn = 1
	    AND (
	        i.ride_datetime < '2025-01-01'    -- All of 2024 and earlier
	        OR i.ride_datetime >= '2025-05-01' -- May 2025 and later
	    )
	    and i.expense_id is null -- Rides only
	    and r.health_sub_plan_name <> 'Medically Needy Population' -- Exclude Spend down
	   	AND NOT EXISTS ( -- Exclude previous encountered
	    SELECT 1 
	    FROM srh_encounters.sunflower_encountered_rides tt
	    WHERE tt.ride_id = i.ride_id
	    and tt.etl_datetime::date <> '2025-10-09'
		)
		AND (r.vehicle_company_name IS NULL -- Exclude providers erroring 
     	OR r.vehicle_company_name NOT IN (
         'Arrive Medical Transportation LLC (KS)',
         'Sunnyflower Transportation, LLC (KS)',
         'Quest Services, Inc. (KS)'
     ))
     ;


WITH deduplicated_invoice_rides AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY ride_id ORDER BY trip_expense_cost DESC) as rn
    FROM srh_ie2_datalake.srh_invoices.sunflower_rides_expenses
),

sunflower_prn AS (
    select sp.*, 
           ROW_NUMBER() OVER (PARTITION BY sp.provider_name ORDER BY sp.provider_kmms_id ASC) AS rn
    FROM dev_stephanie.v_sunflower_prn_providers sp
)
 
-- Expenses Union 
select 
	count(i.ride_id)
	,sum(i.trip_expense_cost) as totalcost 


	from srh_ie2_datalake.srh_invoices.sunflower_rides_expenses i  
	
	inner join srh_encounters.sunflower_expense_id_translation t
		on i.expense_id = t.expense_id

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
	 
	WHERE
	i.expense_id is not null
    AND (
        i.dos < '2025-01-01'    -- All of 2024 and earlier
        OR i.dos >= '2025-05-01' -- May 2025 and later
    ) 
	and r.health_sub_plan_name <> 'Medically Needy Population' -- Exclude Spend down 
	AND NOT EXISTS ( -- Exclude previous encountered
    SELECT 1 
    FROM srh_encounters.sunflower_encountered_rides tt
    WHERE tt.ride_id = (CASE 
        WHEN i.dos::date = i.ride_datetime::date THEN i.ride_id 
        ELSE t.expense_id_adj::bigint 
    END)
    and tt.etl_datetime::date <> '2025-10-09'
	)
;