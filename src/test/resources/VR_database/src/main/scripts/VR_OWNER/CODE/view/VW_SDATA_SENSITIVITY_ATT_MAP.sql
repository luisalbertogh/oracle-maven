--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:VW_SDATA_SENSITIVITY_ATT_MAP runOnChange:TRUE  failOnError:TRUE splitStatements:TRUE


  CREATE OR REPLACE FORCE VIEW "VW_SDATA_SENSITIVITY_ATT_MAP" 
(
   ID,
   ASOFDATE,
   SENTIVITY,
   RISK_FATOR_SENSITIVITY_NAME,
   RISK_FACTOR_CHANGE,
   RISK_FACTOR_CHANGE_UNITS,
   RISK_CLASS_METHOD,
   MEASUREMENT_UNIT,
   CATEGORY_ID,
   CATEGORY_NAME
)
AS
     SELECT ROWNUM AS ID,
            trunc(sysdate, 'month') AS asofdate,
            CASE
			   WHEN upper(s.sensitivity_name) like 'DUMMY%' then null
               WHEN satt.rfs_extension IS NULL THEN s.sensitivity_name
               ELSE s.sensitivity_name || ' ' || rfs_extension
            END
               AS sentivity,
            satt.rfs_sensitivity_name AS RISK_FATOR_SENSITIVITY_NAME,
            s.RISK_FACTOR_CHANGE,
            s.RISK_FACTOR_CHANGE_UNITS,
            CASE
               WHEN satt.rfs_extension IS NULL THEN s.RISK_CLASS_METHOD
               ELSE NULL
            END
               AS RISK_CLASS_METHOD,
            s.measurement_unit,
            s.CATEGORY_ID,
            cat.category_name
       FROM sdata_sensitivity s
            JOIN sdata_sensitivity_attributes satt
               ON S.SENSITIVITY_NAME = SATT.SENSITIVITY_NAME
            LEFT OUTER JOIN (SELECT DISTINCT category_id, category_name
                               FROM sdata_limit_category) cat
               ON cat.CATEGORY_ID = S.CATEGORY_ID
      WHERE satt.status = 'Active'
   ORDER BY 3;
