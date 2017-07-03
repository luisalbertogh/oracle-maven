--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:VIEW_VW_BH_COUNT_DUPLICATES runOnChange:TRUE  failOnError:TRUE splitStatements:TRUE


  CREATE OR REPLACE FORCE VIEW "VW_BH_COUNT_DUPLICATES" ("ASOFDATE", "BOOK_ID", "BUSINESS", "SUB_BUSINESS", "TRADING_UNIT", "NUM_BOOKS") AS 
  SELECT DISTINCT
          BOOK_HIERARCHY_RPL.ASOFDATE,
          BOOK_HIERARCHY_RPL.BOOK_ID,
          DECODE (
             COUNT (
                BOOK_HIERARCHY_RPL.BOOK_ID)
             OVER (
                PARTITION BY BOOK_HIERARCHY_RPL.ASOFDATE,
                             BOOK_HIERARCHY_RPL.BOOK_ID,
                             BOOK_HIERARCHY_RPL.SOURCE_SYSTEM
                ORDER BY BOOK_HIERARCHY_RPL.ASOFDATE),
             1, BOOK_HIERARCHY_RPL.BUSINESS,
             NULL)
             BUSINESS,
          DECODE (
             COUNT (
                BOOK_HIERARCHY_RPL.BOOK_ID)
             OVER (
                PARTITION BY BOOK_HIERARCHY_RPL.ASOFDATE,
                             BOOK_HIERARCHY_RPL.BOOK_ID,
                             BOOK_HIERARCHY_RPL.SOURCE_SYSTEM
                ORDER BY BOOK_HIERARCHY_RPL.ASOFDATE),
             1, BOOK_HIERARCHY_RPL.SUB_BUSINESS,
             NULL)
             SUB_BUSINESS,
          DECODE (
             COUNT (
                BOOK_HIERARCHY_RPL.BOOK_ID)
             OVER (
                PARTITION BY BOOK_HIERARCHY_RPL.ASOFDATE,
                             BOOK_HIERARCHY_RPL.BOOK_ID,
                             BOOK_HIERARCHY_RPL.SOURCE_SYSTEM
                ORDER BY BOOK_HIERARCHY_RPL.ASOFDATE),
             1, VOLCKER_TRADING_DESK,
             NULL)
             TRADING_UNIT,
          COUNT (
             BOOK_HIERARCHY_RPL.BOOK_ID)
          OVER (
             PARTITION BY BOOK_HIERARCHY_RPL.ASOFDATE,
                          BOOK_HIERARCHY_RPL.BOOK_ID,
                          BOOK_HIERARCHY_RPL.SOURCE_SYSTEM
             ORDER BY BOOK_HIERARCHY_RPL.ASOFDATE)
             NUM_BOOKS
     FROM BOOK_HIERARCHY_RPL;
