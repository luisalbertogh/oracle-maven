--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:VIEW_VW_SDATA_VRI_DESK_MAPPING runOnChange:TRUE  failOnError:TRUE splitStatements:TRUE


CREATE OR REPLACE FORCE VIEW VW_SDATA_VRI_DESK_MAPPING
(
   ID,
   DESK_ID,
   DESK_NAME,
   AGENCY_ID,
   DESK_DESCRIPTION,
   ASOFDATE,
   METRICS_MEETING,
   REGION,
   COUNTRY,
   PVF,
   BUSINESS,
   DIVISION,
   CF_DESK_FLAG,
   DESK_UBR_ID,
   EXEMP_EXCL,
   INTENT,
   CONTROL_TYPE,
   DESK_BUSINESS,
   DESK_DIVISION,
   VRO_LIQUIDITY_PROFILE,
   VRO_CREDIT_NONCREDIT,
   DESK_SOURCE,
   METRICS_REPORTABLE,
   VOLCKER_REPORTABLE,
   STATUS,
   DESK_START_DATE,
   DESK_END_DATE,
   ATTRIBUTE_EFFECTIVE_DATE,
   UPDATED_BY,
   UPDATED_ON
)
AS
   SELECT ROWNUM AS ID,
          "DESK_ID",
          "DESK_NAME",
          "AGENCY_ID",
          "DESK_DESCRIPTION",
          "ASOFDATE",
          "METRICS_MEETING",
          "REGION",
          "COUNTRY",
          "PVF",
          "BUSINESS",
          "DIVISION",
          "CF_DESK_FLAG",
          "DESK_UBR_ID",
          "EXEMP_EXCL",
          "INTENT",
          "CONTROL_TYPE",
          "DESK_BUSINESS",
          "DESK_DIVISION",
          "VRO_LIQUIDITY_PROFILE",
          "VRO_CREDIT_NONCREDIT",
          "DESK_SOURCE",
          "METRICS_REPORTABLE",
          "VOLCKER_REPORTABLE",
          "STATUS",
          "DESK_START_DATE",
          "DESK_END_DATE",
          "ATTRIBUTE_EFFECTIVE_DATE",
          "UPDATED_BY",
          "UPDATED_ON"
     FROM (SELECT distinct
                  desk.desk_rpl_code AS desk_id,
                  desk.desk_full_name AS desk_name,
                  case when  desk.desk_source = 'Non-VTD' then null else regulator_code end AS agency_id,
                  NVL (deskatt.desk_descr, desk.desk_full_name) AS desk_description,
                  TO_DATE (month_id, 'yyyymm') AS asofdate,
                  deskatt.brm_meeting AS metrics_meeting,
                  deskatt.region,
                  deskatt.country,
                  deskatt.pvf,
                  deskatt.cf_business AS business,
                  deskatt.cf_division AS division,
                  DECODE (deskatt.cf_reportable_flag, 'Y', 'Yes', NULL)
                     AS cf_desk_flag,
                  desk.desk_id AS desk_ubr_id,
                  deskatt.exemp_excl_type AS exemp_excl,
                  (SELECT INTENTS
                     FROM sdata_EXEMP_EXCL_type
                    WHERE EXEMP_EXCL_code = deskatt.EXEMP_EXCL_TYPE)
                     AS INTENT,
                  deskatt.CONTROL_TYPE AS CONTROL_TYPE,   
                  deskatt.business AS desk_business,
                  deskatt.division AS desk_division,
                  deskatt.liquidity_profile AS vro_liquidity_profile,
                  deskatt.credit_rates_ind AS vro_credit_noncredit,
                  desk.desk_source,
                  NVL (deskatt.metrics_reportable, 'N') AS metrics_reportable,
                  NVL (
                     deskatt.VOLCKER_RELEVANT,
                     CASE
                        WHEN desk.desk_source = 'Non-VTD' THEN 'N'
                        ELSE 'Y'
                     END)
                     AS VOLCKER_REPORTABLE,
                  desk.status,
                  desk.start_date AS desk_start_date,
                  desk.end_date AS desk_end_date,
                  deskatt.start_date AS attribute_effective_date,
                  deskatt.update_user AS updated_by,
                  deskatt.update_datetime AS updated_on
             FROM sdata_month mon
                  LEFT OUTER JOIN sdata_desk desk
                     ON TO_CHAR (desk.start_date, 'yyyymm') <= mon.month_id
                        AND TO_CHAR (NVL (desk.end_date, '31-Dec-2099'),
                                     'yyyymm') >= mon.month_id
                  LEFT OUTER JOIN sdata_desk_attributes deskatt
                     ON (desk.desk_rpl_code = deskatt.desk_rpl_code
                         AND desk.desk_source = deskatt.desk_source
                         AND TO_CHAR (deskatt.start_date, 'yyyymm') <=
                                mon.month_id
                         AND TO_CHAR (NVL (deskatt.end_date, '31-Dec-2099'),
                                      'yyyymm') >= mon.month_id)
                  LEFT OUTER JOIN (SELECT desk_rpl_code, 'FED' AS regulator_code
                                     FROM sdata_desk
                                    WHERE desk_source <> 'Non-VTD'
                                   UNION
                                   SELECT CODE1, CODE2
                                     FROM SDATA_DECODE_LIST
                                    WHERE LIST_NAME = 'VRI_DESK_REG_OVERRIDE' AND decode1 = 'Include'
                                   UNION
                                   SELECT DISTINCT
                                          deskle.desk_rpl_code,
                                          reg.regulator_code
                                     FROM sdata_desk_legal_entity_map deskle
                                          JOIN sdata_desk desk
                                             ON deskle.desk_rpl_code =
                                                   desk.desk_rpl_code
                                                AND desk.desk_source <>
                                                       'Non-VTD'
                                          JOIN sdata_legal_entity_regulator lereg
                                             ON deskle.legal_entity_id =
                                                   lereg.legal_entity_id
                                          JOIN sdata_regulator reg
                                             ON lereg.regulator_id =
                                                   reg.regulator_id
                                   MINUS
                                   SELECT CODE1, CODE2
                                     FROM SDATA_DECODE_LIST
                                    WHERE LIST_NAME = 'VRI_DESK_REG_OVERRIDE'
                                          AND decode1 = 'Exclude') deskreg
                     ON     desk.desk_rpl_code = deskreg.desk_rpl_code
            WHERE vri_elapsed_month = 'Y') vri_desk_mapping;