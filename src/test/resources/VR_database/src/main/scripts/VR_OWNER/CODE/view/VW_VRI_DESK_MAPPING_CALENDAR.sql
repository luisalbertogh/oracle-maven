--liquibase formatted sql
--changeset P17.02.11:GBSVR-34022 ${vr_owner_user}_CODE:VIEW_VW_VRI_DESK_MAPPING_CALENDAR runOnChange:TRUE  failOnError:TRUE splitStatements:TRUE


CREATE OR REPLACE FORCE VIEW "VW_VRI_DESK_MAPPING_CALENDAR" ("DESK_ID", "REGION", "COUNTRY", "COBDATE") AS 
  select d.desk_id, d.region, d.country, c.cobdate
from vri_desk_mapping d
inner join vri_trading_day_calendar c
   on to_char(d.asofdate, 'YYYYMM') = to_char(c.cobdate, 'YYYYMM')
  and (nvl(d.region, 'XX') || '-' || nvl(d.country, 'XX')) = (nvl(c.region, 'XX') || '-' || nvl(c.country, 'XX'))
  and (d.desk_id = c.desk_id or c.desk_id is null)
  and d.agency_id = 'FED'
group by d.desk_id, d.region, d.country, c.cobdate
  -- NTDs have TradingDay = N and PnL = 0 in the VRI Calendar table (Desk specific NTD tagging takes priority)
having  min(case when c.desk_id is not null and c.tradingday = 'N' and c.pnl = 0 then 1 
            when c.desk_id is not null and (c.tradingday = 'Y' or c.pnl != 0) then 2
            when c.desk_id is null and c.tradingday = 'N' and c.pnl = 0 then 3  
            else 999 end) in (1, 3)
order by d.desk_id, d.region, d.country, c.cobdate;