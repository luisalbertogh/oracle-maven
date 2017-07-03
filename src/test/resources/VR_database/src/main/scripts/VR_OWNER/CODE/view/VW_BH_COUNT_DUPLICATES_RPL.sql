--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:VIEW_VW_BH_COUNT_DUPLICATES_RPL runOnChange:TRUE  failOnError:TRUE splitStatements:TRUE


  CREATE OR REPLACE FORCE VIEW "VW_BH_COUNT_DUPLICATES_RPL" ("ASOFDATE", "BOOK_ID", "SOURCE_SYSTEM", "TRADING_UNIT", "VOLCKER_TRADING_DESK_FULL", "LOWEST_LEVEL_RPL_CODE", "LOWEST_LEVEL_RPL_FULL_NAME", "LOWEST_LEVEL_RPL", "CHARGE_REPORTING_UNIT_CODE", "CHARGE_REPORTING_UNIT", "CHARGE_REPORTING_PARENT_CODE", "CHARGE_REPORTING_PARENT", "UBR_LEVEL_1_ID", "UBR_LEVEL_1_NAME", "UBR_LEVEL_1_RPL_CODE", "UBR_LEVEL_2_ID", "UBR_LEVEL_2_NAME", "UBR_LEVEL_2_RPL_CODE", "UBR_LEVEL_3_ID", "UBR_LEVEL_3_NAME", "UBR_LEVEL_3_RPL_CODE", "UBR_LEVEL_4_ID", "UBR_LEVEL_4_NAME", "UBR_LEVEL_4_RPL_CODE", "UBR_LEVEL_5_ID", "UBR_LEVEL_5_NAME", "UBR_LEVEL_5_RPL_CODE", "UBR_LEVEL_6_ID", "UBR_LEVEL_6_NAME", "UBR_LEVEL_6_RPL_CODE", "UBR_LEVEL_7_ID", "UBR_LEVEL_7_NAME", "UBR_LEVEL_7_RPL_CODE", "UBR_LEVEL_8_ID", "UBR_LEVEL_8_NAME", "UBR_LEVEL_8_RPL_CODE", "UBR_LEVEL_9_ID", "UBR_LEVEL_9_NAME", "UBR_LEVEL_9_RPL_CODE", "UBR_LEVEL_10_ID", "UBR_LEVEL_10_NAME", "UBR_LEVEL_10_RPL_CODE", "UBR_LEVEL_11_ID", "UBR_LEVEL_11_NAME", "UBR_LEVEL_11_RPL_CODE", "UBR_LEVEL_12_ID", "UBR_LEVEL_12_NAME", "UBR_LEVEL_12_RPL_CODE", "UBR_LEVEL_13_ID", "UBR_LEVEL_13_NAME", "UBR_LEVEL_13_RPL_CODE", "UBR_LEVEL_14_ID", "UBR_LEVEL_14_NAME", "UBR_LEVEL_14_RPL_CODE", "DESK_LEVEL_1_ID", "DESK_LEVEL_1_NAME", "DESK_LEVEL_1_RPL_CODE", "DESK_LEVEL_2_ID", "DESK_LEVEL_2_NAME", "DESK_LEVEL_2_RPL_CODE", "DESK_LEVEL_3_ID", "DESK_LEVEL_3_NAME", "DESK_LEVEL_3_RPL_CODE", "DESK_LEVEL_4_ID", "DESK_LEVEL_4_NAME", "DESK_LEVEL_4_RPL_CODE", "DESK_LEVEL_5_ID", "DESK_LEVEL_5_NAME", "DESK_LEVEL_5_RPL_CODE", "PORTFOLIO_ID", "PORTFOLIO_NAME", "PORTFOLIO_RPL_CODE", "BUSINESS", "SUB_BUSINESS", "NUM_BOOKS", "REGION", "SUBREGION", "GLOBAL_TRADER_BOOK_ID") AS 
  SELECT b1.asofdate,
          b1.book_id,
          b1.source_system_id AS source_system,
          b2.volcker_trading_desk AS trading_unit,
          b2.volcker_trading_desk_full,
          b2.lowest_level_rpl_code,
          b2.lowest_level_rpl_full_name,
          b2.lowest_level_rpl,

          b2.charge_reporting_unit_code,
          b2.charge_reporting_unit,
          b2.charge_reporting_parent_code,
          b2.charge_reporting_parent,
          b2.ubr_level_1_id,
          b2.ubr_level_1_name,
          b2.ubr_level_1_rpl_code,
          b2.ubr_level_2_id,
          b2.ubr_level_2_name,
          b2.ubr_level_2_rpl_code,
          b2.ubr_level_3_id,
          b2.ubr_level_3_name,
          b2.ubr_level_3_rpl_code,
          b2.ubr_level_4_id,
          b2.ubr_level_4_name,
          b2.ubr_level_4_rpl_code,
          b2.ubr_level_5_id,
          b2.ubr_level_5_name,
          b2.ubr_level_5_rpl_code,
          b2.ubr_level_6_id,
          b2.ubr_level_6_name,
          b2.ubr_level_6_rpl_code,
          b2.ubr_level_7_id,
          b2.ubr_level_7_name,
          b2.ubr_level_7_rpl_code,
          b2.ubr_level_8_id,
          b2.ubr_level_8_name,
          b2.ubr_level_8_rpl_code,
          b2.ubr_level_9_id,
          b2.ubr_level_9_name,
          b2.ubr_level_9_rpl_code,
          b2.ubr_level_10_id,
          b2.ubr_level_10_name,
          b2.ubr_level_10_rpl_code,
          b2.ubr_level_11_id,
          b2.ubr_level_11_name,
          b2.ubr_level_11_rpl_code,
          b2.ubr_level_12_id,
          b2.ubr_level_12_name,
          b2.ubr_level_12_rpl_code,
          b2.ubr_level_13_id,
          b2.ubr_level_13_name,
          b2.ubr_level_13_rpl_code,
          b2.ubr_level_14_id,
          b2.ubr_level_14_name,
          b2.ubr_level_14_rpl_code,
          b2.desk_level_1_id,
          b2.desk_level_1_name,
          b2.desk_level_1_rpl_code,
          b2.desk_level_2_id,
          b2.desk_level_2_name,
          b2.desk_level_2_rpl_code,
          b2.desk_level_3_id,
          b2.desk_level_3_name,
          b2.desk_level_3_rpl_code,
          b2.desk_level_4_id,
          b2.desk_level_4_name,
          b2.desk_level_4_rpl_code,
          b2.desk_level_5_id,
          b2.desk_level_5_name,
          b2.desk_level_5_rpl_code,

          b2.portfolio_id,
          b2.portfolio_name,
          b2.portfolio_rpl_code,
          b2.business,
          b2.sub_business,
          COUNT(B1.BOOK_ID) OVER (PARTITION by B1.ASOFDATE, B1.BOOK_ID, B1.SOURCE_SYSTEM_ID) AS NUM_BOOKS, 
          b2.region,
          b2.subregion,
          b2.global_trader_book_id
     FROM    (  SELECT b.asofdate,
                       b.book_id,
                       ss3.source_system_id,
                       NVL (MAX (ss2.source_system_id), '[NULL]') original_ss
                  FROM book_hierarchy_rpl b
                       LEFT JOIN source_system ss
                          ON ss.source_system_id = b.source_system
                       LEFT JOIN source_system ss2
                          ON ss2.source_system_crds_name =
                                ss.source_system_crds_name
                             AND ss2.region_id = ss.region_id
                       LEFT JOIN source_system ss3
                          ON NVL (ss2.source_system_id, ss3.source_system_id) =
                                ss3.source_system_id
              GROUP BY b.asofdate, b.book_id, ss3.source_system_id) b1
          JOIN
             (SELECT b.*, NVL (sss2.source_system_id,
'[NULL]') original_ss
                FROM book_hierarchy_rpl b
                     LEFT JOIN source_system sss
                        ON sss.source_system_id = b.source_system
                     LEFT JOIN source_system sss2
                        ON sss2.source_system_crds_name =
                              sss.source_system_crds_name
                           AND sss2.region_id = sss.region_id) b2
          ON     b2.asofdate = b1.asofdate
             AND b2.book_id = b1.book_id
             AND b2.original_ss = b1.original_ss;
