create view srh_encounters.v_sunflower_encounters_ems_2 as
WITH transformed_data AS (
    -- Proc Codes without Mileage, Meals, Lodging, Bus (pass through unchanged) 
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
    FROM srh_encounters.sunflower_encountered_member_date_rides
    WHERE procedure_code IN ('A0190', 'A0110', 'A0180')
    
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
    FROM srh_encounters.sunflower_encountered_member_date_rides
    WHERE procedure_code NOT IN ('A0190', 'A0110', 'A0180') 
      AND ((daily_ride_count = 1 AND total_daily_distance <= 10) 
           OR (daily_ride_count > 1 AND total_daily_distance <= 20))
    
    UNION ALL
    
    -- All other Proc Codes where distance > 10 or 20 (split with mileage cap)
    SELECT 
        member_id,
        ride_date,
        provider_name,
        procedure_code,
        file_name,
        daily_ride_count AS units,  -- Keep original count as units
        CAST(
            CAST(daily_grand_total AS DECIMAL(18,2)) * 
            (CAST(CASE WHEN daily_ride_count = 1 THEN 10.0 ELSE 20.0 END AS DECIMAL(10,2)) / 
             CAST(total_daily_distance AS DECIMAL(18,2)))
        AS DECIMAL(18,2)) AS totalallowedamt,
        primary_ride_id,
        etl_datetime
    FROM srh_encounters.sunflower_encountered_member_date_rides
    WHERE procedure_code NOT IN ('A0190', 'A0110', 'A0180') 
      AND ((daily_ride_count = 1 AND total_daily_distance > 10) 
           OR (daily_ride_count > 1 AND total_daily_distance > 20))
    
    UNION ALL
    
    -- All other Proc Codes where distance > 10 or 20 (create A0425 with remaining miles) 
    SELECT 
        member_id,
        ride_date,
        provider_name,
        'A0425' AS procedure_code,
        file_name,
        total_daily_distance - (CASE WHEN daily_ride_count = 1 THEN 10 ELSE 20 END) AS units,  -- Remaining miles after threshold
        CAST(
            CAST(daily_grand_total AS DECIMAL(18,2)) * 
            ((CAST(total_daily_distance AS DECIMAL(18,2)) - 
              CAST(CASE WHEN daily_ride_count = 1 THEN 10.0 ELSE 20.0 END AS DECIMAL(10,2))) / 
             CAST(total_daily_distance AS DECIMAL(18,2)))
        AS DECIMAL(18,2)) AS totalallowedamt,
        primary_ride_id,
        etl_datetime
    FROM srh_encounters.sunflower_encountered_member_date_rides
    WHERE procedure_code NOT IN ('A0190', 'A0110', 'A0180') 
      AND ((daily_ride_count = 1 AND total_daily_distance > 10) 
           OR (daily_ride_count > 1 AND total_daily_distance > 20))
),
-- Calculate grand total per primary_ride_id
grand_totals AS (
    SELECT 
        primary_ride_id,
        SUM(totalallowedamt) AS grand_total
    FROM transformed_data
    GROUP BY primary_ride_id
),
-- Add row numbers for each unique combination
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
)
-- Final output with grand total and concatenated primary_ride_id
SELECT 
    n.member_id,
    n.ride_date,
    n.provider_name,
    n.procedure_code,
    n.file_name,
    n.units,
    n.totalallowedamt,
    n.primary_ride_id,
    n.primary_ride_id || n.row_num AS ref_6R,
    n.etl_datetime,
    g.grand_total
    
FROM numbered_data n
INNER JOIN grand_totals g ON n.primary_ride_id = g.primary_ride_id
-- INNER JOIN srh_encounters.sunflower_encountered_rides r ON n.primary_ride_id = r.ride_id

WHERE
n.etl_datetime::date = '2025-10-09'

ORDER BY 
    n.primary_ride_id,
    n.row_num
    
WITH NO SCHEMA BINDING;