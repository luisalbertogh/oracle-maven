--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:VW_SDATA_DESK_SENSITIVITY_MAP runOnChange:TRUE  failOnError:TRUE splitStatements:TRUE


  CREATE OR REPLACE FORCE VIEW "VW_SDATA_DESK_SENSITIVITY_MAP" 
(
   ID,
   ASOFDATE,
   DESK_CODE,
   SENSITIVITY_ID,
   LIMIT,
   PRIME_FLAG
)
AS
     SELECT ROWNUM AS ID,
            trunc(sysdate, 'month') AS asofdate,
            ds.desk_rpl_code AS desk_code,
            CASE
               WHEN ds.extension_group IS NULL THEN ds.sensitivity_name
               ELSE ds.sensitivity_name || ' ' || rfse.extension_name
            END AS sensitivity_id,
            ds.LIMIT,
			ds.PRIME_FLAG
       FROM sdata_desk_sensitivity ds
            JOIN sdata_desk desk
               ON desk.desk_rpl_code = ds.desk_rpl_code
            LEFT OUTER JOIN sdata_rfs_extension rfse
               ON ds.extension_group = rfse.extension_group and rfse.status='Active'
      WHERE ds.status = 'Active' AND desk.desk_source <> 'Non-VTD'
            AND desk.start_date <=trunc(sysdate, 'month')
            AND (desk.end_date IS NULL
                 OR desk.end_date >=last_day(trunc(sysdate, 'month')));
