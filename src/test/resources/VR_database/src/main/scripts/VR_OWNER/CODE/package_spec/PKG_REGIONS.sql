--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:PACKAGE_PKG_REGIONS runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE


  CREATE OR REPLACE PACKAGE "PKG_REGIONS" as

 /**************************************************************************************************************
  ||
  || Autor: BELLJOR
  || Date: 23/07/2013
  ||  
  || This package manage all the related functions or procedures directly related to regions table
  ||  F_SET_ROLL_DATE    --> Funtion to roll the date to next business date, update PREV_DATE,COB_DATE and NEXT_DATE columns of REGIONS table
  ||  F_GET_COB_DATE     --> Function to get the maximum COB_DATE from a specific REGION_ID into REGIONS table
  ||  F_GET_PREV_DATE     --> Function to get the maximum PREV_DATE from a specific REGION_ID into REGIONS table
 ||  F_GET_NEXT_DATE     --> Function to get the maximum NEXT_DATE from a specific REGION_ID into REGIONS table
 ***************************************************************************************************************/
  
  FUNCTION F_SET_ROLL_DATE (P_REGION source_system.region_id%TYPE) RETURN NUMBER;
  FUNCTION F_GET_COB_DATE (P_REGION IN VARCHAR2,P_COB_DATE OUT DATE) RETURN NUMBER;
  FUNCTION F_GET_PREV_DATE (P_REGION IN VARCHAR2,P_PREV_DATE OUT DATE) RETURN NUMBER;
  FUNCTION F_GET_NEXT_DATE (P_REGION IN VARCHAR2,P_NEXT_DATE OUT DATE) RETURN NUMBER;
  
  end PKG_REGIONS;
