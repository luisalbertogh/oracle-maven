--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:VIEW_VW_RUSSIAN_CONNECTION_LIST runOnChange:TRUE  failOnError:TRUE splitStatements:TRUE


  CREATE OR REPLACE FORCE VIEW "VW_RUSSIAN_CONNECTION_LIST" ("ID", "ASOFDATE", "CLIENT_TYPE", "CONTRACTING_PARTY_ID", "CONTRACTING_PARTY_NAME", "BUSINESS", "BUSINESS_RELATIONSHIP_ID", "BUSINESS_RELATIONSHIP_NAME", "BUSINESS_RELATIONSHIP_ROLETYPE", "UNDERLYING_PRINCIPAL_ID", "UNDERLYING_PRINCIPAL_NAME", "SUB_PRODUCT_NAME", "SYSTEM", "PRODUCT", "ACCOUNT_ID", "PARAGON_ID", "ASPEN_ID", "LOCATION") AS 
  SELECT rownum as ID, ASOFDATE, CLIENT_TYPE, CONTRACTING_PARTY_ID, CONTRACTING_PARTY_NAME, BUSINESS, 
    BUSINESS_RELATIONSHIP_ID, BUSINESS_RELATIONSHIP_NAME, BUSINESS_RELATIONSHIP_ROLETYPE,
        UNDERLYING_PRINCIPAL_ID, UNDERLYING_PRINCIPAL_NAME, SUB_PRODUCT_NAME, SYSTEM, PRODUCT,
            ACCOUNT_ID, PARAGON_ID, ASPEN_ID, LOCATION
  FROM PROHIBITED_COUNTERPARTIES
  WHERE ASOFDATE = (select MAX(ASOFDATE) from PROHIBITED_COUNTERPARTIES);
