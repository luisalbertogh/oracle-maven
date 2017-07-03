--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:VIEW_VW_BRDS_REP_CONFLICT runOnChange:TRUE  failOnError:TRUE splitStatements:TRUE


  CREATE OR REPLACE FORCE VIEW "VW_BRDS_REP_CONFLICT" ("ID", "ASOFDATE", "BOOK_ID", "GLOBAL_TRADER_BOOK_ID", "VOLCKER_TRADING_DESK", "CHARGE_REPORTING_UNIT", "CHARGE_REPORTING_PARENT", 
  -- GBSVR-33754 Start: CFBU decommissioning
  -- GBSVR-33754 End:   CFBU decommissioning
  "DATA_SOURCE", "SOURCE_SYSTEM_ID", "COMMENTS") AS 
  SELECT rownum as ID, asofdate, book_id,global_trader_book_id,volcker_trading_desk,charge_reporting_unit,
charge_reporting_parent, 
-- GBSVR-33754 Start: CFBU decommissioning
-- GBSVR-33754 End:   CFBU decommissioning
data_source, source_system_id, comments 
FROM (
    SELECT bh_intermediary_id, asofdate, book_id,global_trader_book_id,volcker_trading_desk,
           charge_reporting_unit_code charge_reporting_unit,
           charge_reporting_parent_code charge_reporting_parent,
           -- GBSVR-33754 Start: CFBU decommissioning
           -- GBSVR-33754 End:   CFBU decommissioning
           'BRDS' data_source, source_system source_system_id, comments
      FROM bh_conflicts
     WHERE status='PENDING'
    UNION ALL
    SELECT bh.bh_intermediary_id, bh.asofdate, bh.book_id, bh.global_trader_book_id, bh.volcker_trading_desk,
           bh.charge_reporting_unit_code charge_reporting_unit,
           bh.charge_reporting_parent_code charge_reporting_parent,
           -- GBSVR-33754 Start: CFBU decommissioning
           -- GBSVR-33754 End:   CFBU decommissioning
           'MANUAL' data_source, bh.source_system source_system_id, bh.comments
      FROM BH_STAGING bh, BH_INTERMEDIARY bh_i, BH_CONFLICTS bh_c
     WHERE bh.bh_intermediary_id = bh_i.id
       AND bh_c.bh_intermediary_id = bh_i.id
       AND status='PENDING'
)
order by asofdate desc, bh_intermediary_id desc, data_source;
