--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:PACKAGE_PKG_BH_STANDALONE runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE


  CREATE OR REPLACE PACKAGE "PKG_BH_STANDALONE" AS  
  --start GBSVR-27206
  PROCEDURE P_BH_RPL_CLEANUP(P_ASOFDATE VARCHAR);
  --end GBSVR-27206
END PKG_BH_STANDALONE;
