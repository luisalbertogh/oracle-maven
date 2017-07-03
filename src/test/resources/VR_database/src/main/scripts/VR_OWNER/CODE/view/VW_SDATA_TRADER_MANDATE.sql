--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:VIEW_VW_SDATA_TRADER_MANDATE runOnChange:TRUE  failOnError:TRUE splitStatements:TRUE


  CREATE OR REPLACE FORCE VIEW "VW_SDATA_TRADER_MANDATE" ("DESK_FULL_NAME", "DESK_RPL_CODE", "DESK_ID", "EXEMPTION_EXCLUSION", "VTD_NVTD", "DESK_TYPE", "ASOFDATE") AS 
  SELECT distinct "DESK_FULL_NAME",
          "DESK_RPL_CODE",
          "DESK_ID",
          "EXEMPTION_EXCLUSION",
          "VTD_NVTD",
          "DESK_TYPE",
          "ASOFDATE"
     FROM (SELECT desk.desk_full_name,
                  desk.desk_rpl_code,
                  desk.desk_id,
                  deskatt.exemp_excl_type AS exemption_exclusion,
                  CASE
                     WHEN desk.desk_source = 'Non-VTD' THEN 'Not_TD'
                     ELSE 'Confirmed_TD'
                  END
                     AS vtd_nvtd,
                   bh.nodetype as desk_type,  
                  TO_DATE (month_id, 'yyyymm') AS asofdate
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
                   LEFT OUTER JOIN brds_vw_hierarchy bh on desk.desk_rpl_code = bh.rplcode
            WHERE vri_elapsed_month = 'Y') vri_desk_mapping;
