--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:PACKAGE_PKG_HOUSEKEEPING runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE


  CREATE OR REPLACE PACKAGE "PKG_HOUSEKEEPING" AS

 /**************************************************************************************************************
  ||
  || Date: 18/12/2013
  || 
  || This package manage all the related functions or procedures to Housekeeping process (Archive, Purge, Compress)
  ||  P_ACTION_GROUP               --> Porcedure to get the tables from the group and run the process of the housekeeping automatically

  ***************************************************************************************************************/

        FUNCTION P_ACTION_GROUP (A_GROUP_NAME VARCHAR2) RETURN NUMBER;
    
END PKG_HOUSEKEEPING;
