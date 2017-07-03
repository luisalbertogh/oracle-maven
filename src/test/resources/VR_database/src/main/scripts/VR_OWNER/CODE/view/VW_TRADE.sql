--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:VIEW_VW_TRADE runOnChange:TRUE  failOnError:TRUE splitStatements:TRUE


  CREATE OR REPLACE FORCE VIEW "VW_TRADE" ("TRADE_ID", "TRADE_VERSION", "BUCKET_ID", "TRADE_DATE", "TRADE_MOD_DATE", "TRADE_TYPE", "BOOK_ID", "INSTRUMENT_ID", "INSTRUMENT_TYPE", "CURRENCY_ID", "BUY_OR_SELL", "QUANTITY", "INT_OR_EXT", "CORPACT", "PORT_ID", "NUMOFDAYS", "SOURCE_SYSTEM_ID", "ASOFDATE") AS 
  SELECT leg.TRADE_ID,
  leg.TRADE_VERSION,
  leg.BUCKET_ID,
  leg.TRADE_DATE,
  leg.TRADE_MOD_DATE,
  leg.TRADE_TYPE,
  leg.BOOK_ID,
  leg.INSTRUMENT_ID,
  leg.INSTRUMENT_TYPE,
  leg.CURRENCY_ID,
  leg.BUY_OR_SELL,
  leg.QUANTITY,
  leg.INT_OR_EXT,
  leg.CORPACT,
  leg.PORT_ID,
  leg.NUMOFDAYS,
  leg.SOURCE_SYSTEM_ID,
  leg.ASOFDATE
FROM TRADE leg
INNER JOIN paraLlel_config c
ON leg.source_system_id=c.source_system_id
AND c.metric_id        ='IA'
AND c.origin           ='LEGACY'
AND c.cobdate          =
  (SELECT MAX(b.cobdate) FROM parallel_config b WHERE b.cobdate<=leg.asofdate and b.METRIC_ID='IA' and b.SOURCE_SYSTEM_ID=leg.SOURCE_SYSTEM_ID  )
UNION ALL
SELECT refact.TRADE_ID,
  refact.TRADE_VERSION,
  refact.BUCKET_ID,
  refact.TRADE_DATE,
  refact.TRADE_MOD_DATE,
  refact.TRADE_TYPE,
  refact.BOOK_ID,
  refact.INSTRUMENT_ID,
  refact.INSTRUMENT_TYPE,
  refact.CURRENCY_ID,
  refact.BUY_OR_SELL,
  refact.QUANTITY,
  refact.INT_OR_EXT,
  refact.CORPACT,
  refact.PORT_ID,
  refact.NUMOFDAYS,
  refact.SOURCE_SYSTEM_ID,
  refact.ASOFDATE
FROM new_TRADE refact
INNER JOIN paraLlel_config c
ON refact.source_system_id=c.source_system_id
AND c.metric_id           ='IA'
AND c.origin              ='REFACTORING'
AND c.cobdate             =
  (SELECT MAX(b.cobdate) FROM parallel_config b WHERE b.cobdate<=refact.asofdate and b.METRIC_ID='IA' and b.SOURCE_SYSTEM_ID=refact.SOURCE_SYSTEM_ID  );
