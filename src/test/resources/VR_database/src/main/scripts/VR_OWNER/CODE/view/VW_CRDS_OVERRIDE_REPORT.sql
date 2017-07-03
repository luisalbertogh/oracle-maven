--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:VIEW_VW_CRDS_OVERRIDE_REPORT runOnChange:TRUE  failOnError:TRUE splitStatements:TRUE


  CREATE OR REPLACE FORCE VIEW "VW_CRDS_OVERRIDE_REPORT" ("ID", "ASOFDATE", "SOURCE_SYSTEM_CRDS", "COUNTER_PARTY_ID", "COUNTER_PARTY_NAME", "VOLCKER_TRADING_DESK", "BOOK_ID", "CLASSIFICATION", "REASON", "CREATE_USER", "CREATE_DATE", "MODIFY_USER", "MODIFY_DATE", "ACTION", "POSITIONID", "TRADEID") AS 
  (
select 
rownum as ID,
ASOFDATE,
 SOURCE_SYSTEM_CRDS,
 COUNTER_PARTY_ID,
 COUNTER_PARTY_NAME,
 VOLCKER_TRADING_DESK,
 BOOK_ID,       
 CLASSIFICATION,    
 REASON,                   
 CREATE_USER,               
 CREATE_DATE,            
 MODIFY_USER,                
 MODIFY_DATE,  
 ACTION,                   
 POSITIONID ,          
 TRADEID    
from crds_override)
order by SOURCE_SYSTEM_CRDS;
