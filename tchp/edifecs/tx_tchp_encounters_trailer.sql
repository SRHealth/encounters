CREATE OR REPLACE VIEW srh_edifecs_dev.tx_tchp_encounters_trailer AS
SELECT
'01' as c_01_trailer_record
, 'TRT' as c_trt_record
, count(distinct c_150_key) as c_total_professional
, '' as c_total_institutional
, '' as c_total_dental
, count(distinct c_150_key) as c_total_claim_count

FROM
  srh_edifecs_dev.tx_encounters_150

WITH NO SCHEMA BINDING;