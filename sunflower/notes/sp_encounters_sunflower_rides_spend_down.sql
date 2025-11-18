call srh_encounters.sp_encounters_sunflower_rides_spend_down();


delete from srh_encounters.sunflower_encountered_rides_expenses_spend_down;

delete from srh_encounters.sunflower_encountered_member_date_rides_expenses_spend_down;

delete from srh_encounters.sunflower_encounters_spend_down_ems;


CREATE OR REPLACE PROCEDURE srh_encounters.sp_encounters_sunflower_rides_spend_down()
LANGUAGE plpgsql
AS $$

DECLARE
    ride_count INTEGER;
    encounter_count INTEGER;
    etl_datetime_out TIMESTAMP;
    v_max_etl_datetime TIMESTAMP;
  
BEGIN

-- SECTION 1: Original stored procedure logic for rides_expenses
insert into srh_encounters.sunflower_encountered_rides_expenses_spend_down
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

    select 
    r.estimated_start_datetime_local_timezone::date as invoice_date,
    coalesce(i.dos, r.estimated_start_datetime_local_timezone) as dos,
    s.ride_id,
    coalesce(i.member_id, r.passenger_medical_id) as member_id,
    coalesce(i.health_sub_plan_name, r.health_sub_plan_name) as health_sub_plan_name,
    coalesce(i.ride_from_address, r.ride_from_address) as ride_from_address,
    coalesce(i.ride_to_address, r.ride_to_address) as ride_to_address,
    coalesce(i.mileage, r.ride_distance) as mileage,
    coalesce(i.ride_status, r.ride_status) as ride_status,
    coalesce(i.transport, r.vehicle_type_nick_name) as transport,
    -- i.license_fee_type as license_fee_type,
    i.expense_id as expense_id,
    -- i.expense_category as expense_category,
    s.base_code_cost + coalesce(s.mileage_code_cost,0) as trip_expense_cost,
    -- i.cancellation_reason as cancellation_reason,
    -- i.cancellation_datetime as cancellation_datetime,
    coalesce(i.ride_datetime, r.estimated_start_datetime_local_timezone) as ride_datetime
    -- i.license_fee_cost as license_fee_cost
    -- s.encounter_id,
    -- s.base_code_cost,
    -- s.mileage_code_cost,
    -- null as confirmed_receipt_cost

    from dev_stephanie.spend_down_20251014 s

    left join srh_ie2_datalake.srh_invoices.sunflower_rides_expenses i 
        on s.ride_id = i.ride_id

    inner join srh_dw.ride_summary r 
        on s.ride_id = r.ride_id 	

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
        substring(REGEXP_REPLACE(TRIM(SPLIT_PART(r.passenger_current_address_2,',',1)), '[^a-z A-Z0-9]', '') ,1,54) as passenger_current_address_2,
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
        COALESCE(sp.provider_kmms_id, '30005195050001') AS provider_kmms_id,
        COALESCE(sp.provider_tax_id, '813037449') AS provider_tax_id,
        COALESCE(sp.serv_loc_street_address_1, '106 JEFFERSON ST') AS serv_loc_street_address_1,
        COALESCE(sp.serv_loc_street_address_2, '3RD FLOOR') AS serv_loc_street_address_2,
        COALESCE(sp.serv_loc_city, 'SAN ANTONIO') AS serv_loc_city,
        COALESCE(sp.serv_loc_state, 'TX') AS serv_loc_state,
        COALESCE(sp.serv_loc_zip, '78205') AS serv_loc_zip,
        COALESCE(sp.serv_loc_zip_4_code, '1005') AS serv_loc_zip_4_code
    FROM 
        deduplicated_invoice_rides i  
        inner join srh_dw.ride_summary r 
            on i.ride_id = r.ride_id
        inner join srh_encounters.sunflower_procedure_code_mapping AS spc 
            ON i.transport = spc.vehicle_type    
        left join salesforce_01.account s 
            on r.vehicle_company_id = s.safe_ride_vo_id_c
        left join sunflower_prn sp 
            on s.medicaid_approved_number_c = sp.provider_kmms_id AND sp.rn = 1
        left join srh_encounters.nemt_medicaid_calendar c
            ON r.claim_submitted_datetime BETWEEN c.ride_start_date AND c.ride_end_date
    WHERE 
        i.expense_id is null -- Rides only
    
    union all 
    
    -- Expenses Union 
    select 
        case when i.dos::date = i.ride_datetime::date then i.ride_id else t.expense_id_adj::bigint end as ride_id,  
        i.expense_id,
        i.dos as ride_datetime,
        i.dos::date AS ride_date, 
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
        substring(REGEXP_REPLACE(TRIM(SPLIT_PART(p.current_address_2,',',1)), '[^a-z A-Z0-9]', '') ,1,54) as current_address_2,
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
        inner join srh_encounters.sunflower_expense_id_translation t
            on i.expense_id = t.expense_id
        inner join srh_dw.ride_summary r 
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
), 
daily_rollup AS (
    SELECT
        member_id,
        ride_date,
        provider_name,
        procedure_code,
        COUNT(DISTINCT ride_id)         AS daily_ride_count,
        SUM(mileage)                    AS total_daily_distance,
        SUM(trip_expense_cost)          AS daily_grand_total,
        MIN(MIN(ride_id)) OVER (
            PARTITION BY member_id, ride_date, provider_name
        )                               AS primary_ride_id,
        row_number() over (order by ride_date asc) rn
    FROM raw_rides
    GROUP BY
        member_id,
        ride_date,
        provider_name,
        procedure_code
)
SELECT
    dr.primary_ride_id,
    'SR' || dr.primary_ride_id || TO_CHAR(dr.ride_date,'YYMMDD') as encounter_id,
    rr.*,
    CASE WHEN dr.daily_ride_count = 1 THEN 10 ELSE 20 END AS base_mile_limit,
    getdate() as etl_datetime,
    'KC19837P' || TO_CHAR(getdate(), 'YYYYMMDDHHMMSS') as file_name
FROM
    raw_rides rr
    inner join daily_rollup dr
        on 1=1
        and dr.member_id = rr.member_id
        and dr.ride_date = rr.ride_date
        and dr.provider_name = rr.provider_name
        and dr.procedure_code = rr.procedure_code
WHERE 
    1=1;

-- SECTION 2: Delete and insert member_date_rides_expenses
delete 
from srh_encounters.sunflower_encountered_member_date_rides_expenses_spend_down
WHERE etl_datetime = (SELECT MAX(etl_datetime) FROM srh_encounters.sunflower_encountered_member_date_rides_expenses_spend_down);

insert into srh_encounters.sunflower_encountered_member_date_rides_expenses_spend_down
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
    SUM(mileage)                    AS total_daily_distance,
    SUM(trip_expense_cost)          AS daily_grand_total,
    MIN(MIN(ride_id)) OVER (
        PARTITION BY member_id, ride_date, provider_name
    )                               AS primary_ride_id,
    row_number() over (order by ride_date asc) as rn,
    t.etl_datetime as etl_datetime
FROM 
    srh_encounters.sunflower_encountered_rides_expenses_spend_down t
WHERE
    t.etl_datetime = (SELECT MAX(etl_datetime) FROM srh_encounters.sunflower_encountered_rides_expenses_spend_down t)
GROUP BY
    member_id,
    ride_date,
    provider_name,
    procedure_code,
    file_name,
    etl_datetime;

-- SECTION 3: Generate final encounters output (from the view logic)
-- Get the most recent ETL datetime to use for filtering
SELECT MAX(etl_datetime) INTO v_max_etl_datetime
FROM srh_encounters.sunflower_encountered_member_date_rides_expenses_spend_down;


INSERT INTO srh_encounters.sunflower_encounters_spend_down_ems
WITH transformed_data AS (
    -- Proc Codes without Mileage (pass through unchanged) 
    SELECT 
        member_id,
        ride_date,
        provider_name,
        procedure_code,
        file_name,
        total_daily_distance AS units,
        daily_grand_total AS totalallowedamt,
        primary_ride_id,
        etl_datetime
    FROM srh_encounters.sunflower_encountered_member_date_rides_expenses_spend_down
    WHERE procedure_code IN ('A0190', 'A0110', 'A0180')
        AND etl_datetime = v_max_etl_datetime
    
    UNION ALL
    
    -- All other Proc Codes where distance <= 10 or 20 (pass through unchanged) 
    SELECT 
        member_id,
        ride_date,
        provider_name,
        procedure_code,
        file_name,
        daily_ride_count AS units,
        daily_grand_total AS totalallowedamt,
        primary_ride_id,
        etl_datetime
    FROM srh_encounters.sunflower_encountered_member_date_rides_expenses_spend_down
    WHERE procedure_code NOT IN ('A0190', 'A0110', 'A0180') 
        AND ((daily_ride_count = 1 AND total_daily_distance <= 10) 
            OR (daily_ride_count > 1 AND total_daily_distance <= 20))
        AND etl_datetime = v_max_etl_datetime
    
    UNION ALL
    
    -- All other Proc Codes where distance > 10 or 20 (split with mileage cap)
    SELECT 
        member_id,
        ride_date,
        provider_name,
        procedure_code,
        file_name,
        daily_ride_count AS units,
        CAST(
            CAST(daily_grand_total AS DECIMAL(38,10)) * 
            (CAST(CASE WHEN daily_ride_count = 1 THEN 10.0 ELSE 20.0 END AS DECIMAL(38,10)) / 
             CAST(total_daily_distance AS DECIMAL(38,10)))
        AS DECIMAL(18,2)) AS totalallowedamt,
        primary_ride_id,
        etl_datetime
    FROM srh_encounters.sunflower_encountered_member_date_rides_expenses_spend_down
    WHERE procedure_code NOT IN ('A0190', 'A0110', 'A0180') 
        AND ((daily_ride_count = 1 AND total_daily_distance > 10) 
            OR (daily_ride_count > 1 AND total_daily_distance > 20))
        AND etl_datetime = v_max_etl_datetime
    
    UNION ALL
    
    -- All other Proc Codes where distance > 10 or 20 (create A0425 with remaining miles) 
    SELECT 
        member_id,
        ride_date,
        provider_name,
        'A0425' AS procedure_code,
        file_name,
        total_daily_distance - (CASE WHEN daily_ride_count = 1 THEN 10 ELSE 20 END) AS units,
        CAST(
            CAST(daily_grand_total AS DECIMAL(38,10)) * 
            ((CAST(total_daily_distance AS DECIMAL(38,10)) - 
              CAST(CASE WHEN daily_ride_count = 1 THEN 10.0 ELSE 20.0 END AS DECIMAL(38,10))) / 
             CAST(total_daily_distance AS DECIMAL(38,10)))
        AS DECIMAL(18,2)) AS totalallowedamt,
        primary_ride_id,
        etl_datetime
    FROM srh_encounters.sunflower_encountered_member_date_rides_expenses_spend_down
    WHERE procedure_code NOT IN ('A0190', 'A0110', 'A0180') 
        AND ((daily_ride_count = 1 AND total_daily_distance > 10) 
            OR (daily_ride_count > 1 AND total_daily_distance > 20))
        AND etl_datetime = v_max_etl_datetime
),
grand_totals AS (
    SELECT 
        primary_ride_id,
        SUM(totalallowedamt) AS grand_total
    FROM transformed_data
    GROUP BY primary_ride_id
),
numbered_data AS (
    SELECT 
        t.*,
        ROW_NUMBER() OVER (
            PARTITION BY t.primary_ride_id 
            ORDER BY 
                t.member_id,
                t.ride_date,
                t.provider_name,
                CASE 
                    WHEN t.procedure_code = 'A0425' THEN 1
                    ELSE 0
                END,
                t.procedure_code
        ) AS row_num
    FROM transformed_data t
),
deduplicated_rides AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY ride_id ORDER BY etl_datetime DESC) as rn
    FROM srh_encounters.sunflower_encountered_rides_expenses_spend_down
)
SELECT 
    'Sunflower' as scenario,
    'KANSAS MEDICAL ASSISTANCE PROGRAM' as payerName,
    REPLACE(r.provider_name, ',', '') as vehicleCompanyName,
    '3397152' planCode,
    r.provider_kmms_id as vehicleCompanyAPI,
    '343900000X' as vehicleCompanyTaxonomy,
    REPLACE(r.provider_tax_id, '-', '') as vehicleCompanyTaxId,
    '' as billingProviderMTI,
    REPLACE(r.serv_loc_street_address_1, ',', '') as billingAddress,
    REGEXP_REPLACE(r.serv_loc_street_address_2, '[^a-zA-Z0-9 ]', '') as billingAddress2,
    r.serv_loc_city as billingCity,
    case
        when r.serv_loc_state = 'Kansas' then 'KS'
        else r.serv_loc_state
    end as billingState,
    left(r.serv_loc_zip || r.serv_loc_zip_4_code || '0000', 9) as billingZip,
    rtrim(n.procedure_code, ' ') as procCode,
    null as careProviderName,
    null as careProviderTPI,
    null as careProviderNPI,
    null as careProviderTaxonomy,
    null as careProviderTaxId,
    null as careProviderAddress,
    null as careProviderCity,
    null as careProviderState,
    null as careProviderZipcode,
    '' as driverLastName,
    '' as driverFirstname,
    '' as driverMTI,
    '' as driverLicense,
    '' as driverSSN,
    n.member_id as patientMedicalId,
    r.passenger_last_name as insuredLast,
    r.passenger_first_name as insuredFirst,
    '' as insuredMI,
    REPLACE(r.passenger_current_address_1, ',', ' ') as insuredAddress,
    REPLACE(r.passenger_current_address_2, '#', ' ') as insuredAddress2,
    r.passenger_current_city as insuredCity,
    case
        when r.passenger_current_state = 'Kansas' then 'KS'
        else r.passenger_current_state
    end as insuredState,
    SUBSTRING(r.passenger_current_zipcode, 1, 5) as insuredZip,
    TO_CHAR(r.passenger_dob, 'YYYYMMDD') as insuredDOB,
    case
        when LOWER(r.passenger_gender) = 'female' then 'F'
        when LOWER(r.passenger_gender) = 'male' then 'M'
        when LOWER(r.passenger_gender) = 'other' then 'U'
        else r.passenger_gender
    end as insuredSex,
    TO_CHAR(n.ride_date, 'YYYYMMDD') as pickupDate,
    TO_CHAR(n.ride_date, 'YYYYMMDD') as appointmentDate,
    'SR' || LPAD(n.primary_ride_id, 8, '0') || TO_CHAR(n.ride_date, 'YYMMDD') as authorizationNumber1,
    '' as authorizationNumber2,
    1 as Frequency,
    TO_CHAR(TRUNC(COALESCE(r.claim_submitted_datetime, n.ride_date)), 'YYYYMMDD') as claimReceiptDate,
    TO_CHAR(r.claimAdjudicationDate, 'YYYYMMDD') as claimAdjudicationDate,
    '50' as claimReceiptDateQualifier,
    '' as pickupCountyCode,
    '' as dropoffCountyCode,
    '' as specialNeeds1,
    '' as specialNeeds2,
    '' as specialNeeds3,
    '' as specialNeeds4,
    '' as paymentMethod,
    '' as VIN,
    '' as SharedServicesRefID,
    TO_CHAR(r.claimAdjudicationDate, 'YYYYMMDD') as claimPaymentDate,
    '' as lessThan48Hour,
    '' as riskGroup,
    g.grand_total as totalAllowedAmt, 
    n.totalallowedamt as allowedAmt,
    substring(REGEXP_REPLACE(TRIM(SPLIT_PART(r.ride_from_address, ',', 1)), '[^a-z A-Z0-9]', ''), 1, 54) as fromAddress1,
    REGEXP_REPLACE(TRIM(SPLIT_PART(r.ride_from_address, ',', 2)), '[^a-z A-Z0-9]', '') as fromCity,
    r.ride_from_state as fromState,
    r.ride_from_zipcode as fromZipcode,
    substring(REGEXP_REPLACE(TRIM(SPLIT_PART(r.ride_to_address, ',', 1)), '[^a-z A-Z0-9]', ''), 1, 54) as toAddress1,
    REGEXP_REPLACE(TRIM(SPLIT_PART(r.ride_to_address, ',', 2)), '[^a-z A-Z0-9]', '') as toCity,
    r.ride_to_state as toState,
    r.ride_to_zipcode as toZipcode,
    n.totalallowedamt as finalCost,
    g.grand_total as "primary paid",
    null as "member paid",
    TO_CHAR(n.units, 'FM999999') as units,
    TO_CHAR(n.ride_date, 'YYYYMMDD') as "pickupDate.1",
    null as attendants,
    to_char(TRUNC(current_date), 'MM/DD/YYYY') as file_submitted_date,
    'SR' || LPAD(n.primary_ride_id, 8, '0') || TO_CHAR(n.ride_date, 'YYMMDD') || 
        CASE 
            WHEN n.row_num = 1 THEN '' 
            ELSE n.row_num::VARCHAR 
        END as ref_6R_authorization_number, 
    n.file_name
from numbered_data n
    inner join grand_totals g   
        on n.primary_ride_id = g.primary_ride_id
    left join deduplicated_rides r  
        on n.primary_ride_id = r.ride_id and r.rn = 1
WHERE 1=1
ORDER BY  
    n.primary_ride_id,
    n.row_num;

-- Get count of inserted records for logging
GET DIAGNOSTICS ride_count = ROW_COUNT;

RAISE NOTICE 'Sunflower encounters processing completed successfully';
RAISE NOTICE 'Records inserted into sunflower_encounters_ems: %', ride_count;


END
;
$$
;
