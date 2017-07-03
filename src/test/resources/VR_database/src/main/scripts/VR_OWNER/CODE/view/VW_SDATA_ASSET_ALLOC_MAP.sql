--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:VW_SDATA_ASSET_ALLOC_MAP runOnChange:TRUE  failOnError:TRUE splitStatements:TRUE


  CREATE OR REPLACE FORCE VIEW "VW_SDATA_ASSET_ALLOC_MAP" 
  (
   ID,
   ASOFDATE,
   ASSET_ALLOCATION,
   MAPPING_VALUE,
   MEASUREMENT_UNIT
)
AS
     SELECT ROWNUM AS ID,
            trunc(sysdate, 'MONTH') asofdate,
            asset_allocation,
            mapping_value,
            measurement_unit
       FROM sdata_asset_allocation aa
      WHERE aa.status = 'Active'
   ORDER BY asset_allocation;
