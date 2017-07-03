--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:PACKAGE_BODY_PKG_BRDS_BH_RPL_DUPS runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE stripComments:FALSE


  CREATE OR REPLACE PACKAGE BODY "PKG_BRDS_BH_RPL_DUPS" AS

procedure p_brds_etl_remove_duplicates
as
begin  
  p_brds_etl_clear_duplicates;
  
  p_brds_etl_remove_dup_hierarch;
  p_brds_etl_remove_dup_cru;
  p_brds_etl_remove_dup_crp;
  -- GBSVR-33754 Start: CFBU decommissioning
  -- GBSVR-33754 End:   CFBU decommissioning
  p_brds_etl_remove_dup_vtd;
  p_brds_etl_remove_dup_book;
  p_brds_etl_remove_dup_portfol;
end p_brds_etl_remove_duplicates;


procedure p_brds_etl_remove_dup_hierarch
as
  type array is table of brds_vw_hierarchy%rowtype;
  l_data array;
begin
  delete from brds_vw_hierarchy s1
   where s1.nodeid in ( 
      select s.nodeId
        from  brds_vw_hierarchy s
       group by s.nodeId
      having count(*) > 1
    )
    and s1.validfrom NOT in  ( 
      select max(s2.validfrom) 
       from brds_vw_hierarchy s2
      where s2.nodeId = s1.nodeId
    )
    returning s1.VERSION,s1.VALIDFROM,s1.VALIDTO,s1.NODETYPE,s1.NODEID,s1.PARENTNODEID,s1.PARENTNODENAME,s1.NODENAME,s1.NODEDESC,s1.FINANCIALACCOUNTINGTREATMENT,s1.REGULATORYREPORTINGTREATMENT,
              s1.REPORTINGCATEGORY,s1.RPLCODE,s1.VOLCKERTRADINGDESK,s1.CHARGEREPORTINGPARENT,s1.CHARGEREPORTINGUNIT,s1.CHANGEINDICATOR,s1.LASTACTIONBY,s1.LASTACTIONBYEMAIL,s1.PARENTNODETYPE
              -- GBSVR-33754 Start: CFBU decommissioning
              -- GBSVR-33754 End:   CFBU decommissioning
         bulk collect into l_data; 
    
  forall i in 1 .. l_data.count
    insert into brds_dups_vw_hierarchy values l_data(i);
end p_brds_etl_remove_dup_hierarch;

procedure p_brds_etl_remove_dup_cru
as
  type array is table of brds_vw_cru%rowtype;
  l_data array;
begin
  delete from brds_vw_cru s1
   where s1.chargeReportingUnitCode in ( 
      select s.chargeReportingUnitCode
        from  brds_vw_cru s
       group by s.chargeReportingUnitCode
      having count(*) > 1
    )
    and s1.validfrom NOT in  ( 
      select max(s2.validfrom) 
       from brds_vw_cru s2
      where s2.chargeReportingUnitCode = s1.chargeReportingUnitCode
    )
    returning s1.VALIDFROM,s1.VALIDTO,s1.CHARGEREPORTINGPARENTCODE,s1.CHARGEREPORTINGUNITCODE,s1.CHARGEREPORTINGUNIT,s1.CHARGEREPORTINGUNITID,
              s1.CHANGEINDICATOR,s1.LASTACTIONBY,s1.LASTACTIONBYEMAIL
         bulk collect into l_data; 
    
  forall i in 1 .. l_data.count
    insert into brds_dups_vw_cru values l_data(i);
end p_brds_etl_remove_dup_cru;

procedure p_brds_etl_remove_dup_crp
as
  type array is table of brds_vw_crp%rowtype;
  l_data array;
begin
  delete from brds_vw_crp s1
   where s1.chargeReportingParentCode in ( 
      select s.chargeReportingParentCode
        from  brds_vw_crp s
       group by s.chargeReportingParentCode
      having count(*) > 1
    )
    and s1.validfrom NOT in  ( 
      select max(s2.validfrom) 
       from brds_vw_crp s2
      where s2.chargeReportingParentCode = s1.chargeReportingParentCode
    )
    returning s1.VALIDFROM,s1.VALIDTO,s1.CHARGEREPORTINGPARENTCODE,s1.CHARGEREPORTINGPARENT,s1.CHARGEREPORTINGPARENTID,s1.CHARGEHIERARCHYREGION,s1.CHARGESUBAREA,
              s1.CHANGEINDICATOR,s1.LASTACTIONBY,s1.LASTACTIONBYEMAIL
         bulk collect into l_data; 
    
  forall i in 1 .. l_data.count
    insert into brds_dups_vw_crp values l_data(i);
end p_brds_etl_remove_dup_crp;

-- GBSVR-33754 Start: CFBU decommissioning
-- GBSVR-33754 End:   CFBU decommissioning

procedure p_brds_etl_remove_dup_vtd
as
  type array is table of brds_vw_vtd%rowtype;
  l_data array;
begin
  delete from brds_vw_vtd s1
   where s1.volckerTradingDesk in ( 
      select s.volckerTradingDesk
        from  brds_vw_vtd s
       group by s.volckerTradingDesk
      having count(*) > 1
    )
    and s1.validfrom NOT in  ( 
      select max(s2.validfrom) 
       from brds_vw_vtd s2
      where s2.volckerTradingDesk = s1.volckerTradingDesk
    )
    returning s1.VALIDFROM,s1.VALIDTO,s1.VOLCKERTRADINGDESK,s1.VOLCKERTRADINGDESKFULL,s1.VOLCKERTRADINGDESKID,
              s1.CHANGEINDICATOR,s1.LASTACTIONBY,s1.LASTACTIONBYEMAIL
         bulk collect into l_data; 
    
  forall i in 1 .. l_data.count
    insert into brds_dups_vw_vtd values l_data(i);
end p_brds_etl_remove_dup_vtd;

procedure p_brds_etl_remove_dup_book
as
  type array is table of brds_vw_book%rowtype;
  l_data array;
begin
  delete from brds_vw_book s1
   where s1.bookName in ( 
      select s.bookName
        from  brds_vw_book s
       group by s.GLOBALTRADERBOOKID,
                --s.VALIDFROM,
                --s.VALIDTO,
                s.PORTFOLIOID,
                s.PORTFOLIONAME,
                s.PROFITCENTRECODE,
                s.PROFITCENTRECODEFULL,
                s.PROFITCENTRE,
                s.COSTCENTRECODE,
                s.COSTCENTRE,
                s.BOOKSTATUS,
                s.TRADECAPTURESYSTEMID,
                s.TRADECAPTURESYSTEMNAME,
                s.BOOKSOURCE,
                s.LASTUPDATEDDATETIME,
                s.BOOKNAME,
                s.LEGALENTITYCODE,
                s.LEGALENTITYNAME,
                s.LEGALENTITYCCDB,
                s.BOOKFUNCTIONCODE,
                s.BOOKSUBFUNCTIONCODE,
                s.ESMAINSTRUMENTAPPLICABILITY,
                s.ESMABOOKCATEGORY,
                s.MILOCATION,
                s.MIS3SIGNOFF,
                s.MCCCATEGORYCODE,
                s.MCCEXEMPTREASON,
                s.LASTRECERTIFICATIONDATE,
                s.LASTRECERTIFICATIONBY,
                s.LASTRECERTIFICATIONBYEMAIL,
                s.BOOKRECERTIFICATIONSTATUS,
                s.PRIMARYTRADER,
                s.PRIMARYTRADEREMAIL,
                s.PRIMARYBOOKRUNNER,
                s.PRIMARYBOOKRUNNEREMAIL,
                s.PRIMARYFINCON,
                s.PRIMARYFINCONEMAIL,
                s.PRIMARYMOESCALATION,
                s.PRIMARYMOESCALATIONEMAIL,
                s.BUSINESSORIGCODE,
                s.WASHBOOKFLAG,
                s.JOINTVENTUREFLAG,
                s.GLOBALBOOKFLAG,
                s.UTILITYBOOKFLAG,
                s.DESIGNATEDRNFLAG,
                s.DESKLEVEL1,
                s.LEVEL55BUSINESSUNIT,
                s.UBRLEVEL10,
                s.LOWESTLEVELUBR,
                s.UBRMACODE,
                s.BOOKSTRATEGY,
                s.ACCTREATCATEGORY,
                s.FINANCIALACCOUNTINGTREATMENT,
                s.REGULATORYREPORTINGTREATMENT,
                s.DATACOMPLETENESSSTATUS,
                s.MANAGEDBY,
                s.BUSINESSOWNER,
                --s.CHANGEINDICATOR,
                --s.LASTACTIONBY,
                --s.LASTACTIONBYEMAIL,
                s.FDRGROUP,
                s.MIDDLEOFFICEGROUP,
                s.MOTMQ,
                s.MOGROUPEMAILID,
                s.SHOULDFEEDCRES,
                s.ISSDOSABLE,
                s.INCLUDEINNMA,
                s.USEABSSDOS,
                s.INCLUDESLATETRADES,
                s.NVBOOKLOCATION,
                s.SPIDERBONDFILTER,
                s.TRADECAPTUREINSTANCE,
                s.RISKCLASS,
                s.PROMOTIONSTATUS,
                s.CDSGROUP,
                s.CLEARINGELIGIBLE,
                s.CLEARINGCOMMENT,
                s.COMPRESSIONELIGIBLE,
                s.COMPRESSIONCOMMENT,
                s.HUBBINGELIGIBLE,
                s.HUBBINGCOMMENT,
                s.BASEL3COVEREDPOSITION,
                s.VOLCKERTRADINGDESK,
                s.CHARGEREPORTINGPARENTCODE,
                s.CHARGEREPORTINGUNITCODE
                -- GBSVR-33754 Start: CFBU decommissioning
                -- GBSVR-33754 End:   CFBU decommissioning
      having count(*) > 1
    )
    and s1.validfrom NOT in  ( 
      select max(s2.validfrom) 
       from brds_vw_book s2
      where s2.bookName = s1.bookName
    )
    returning s1.GLOBALTRADERBOOKID,
              s1.VALIDFROM,
              s1.VALIDTO,
              s1.PORTFOLIOID,
              s1.PORTFOLIONAME,
              s1.PROFITCENTRECODE,
              s1.PROFITCENTRECODEFULL,
              s1.PROFITCENTRE,
              s1.COSTCENTRECODE,
              s1.COSTCENTRE,
              s1.BOOKSTATUS,
              s1.TRADECAPTURESYSTEMID,
              s1.TRADECAPTURESYSTEMNAME,
              s1.BOOKSOURCE,
              s1.LASTUPDATEDDATETIME,
              s1.BOOKNAME,
              s1.LEGALENTITYCODE,
              s1.LEGALENTITYNAME,
              s1.LEGALENTITYCCDB,
              s1.BOOKFUNCTIONCODE,
              s1.BOOKSUBFUNCTIONCODE,
              s1.ESMAINSTRUMENTAPPLICABILITY,
              s1.ESMABOOKCATEGORY,
              s1.MILOCATION,
              s1.MIS3SIGNOFF,
              s1.MCCCATEGORYCODE,
              s1.MCCEXEMPTREASON,
              s1.LASTRECERTIFICATIONDATE,
              s1.LASTRECERTIFICATIONBY,
              s1.LASTRECERTIFICATIONBYEMAIL,
              s1.BOOKRECERTIFICATIONSTATUS,
              s1.PRIMARYTRADER,
              s1.PRIMARYTRADEREMAIL,
              s1.PRIMARYBOOKRUNNER,
              s1.PRIMARYBOOKRUNNEREMAIL,
              s1.PRIMARYFINCON,
              s1.PRIMARYFINCONEMAIL,
              s1.PRIMARYMOESCALATION,
              s1.PRIMARYMOESCALATIONEMAIL,
              s1.BUSINESSORIGCODE,
              s1.WASHBOOKFLAG,
              s1.JOINTVENTUREFLAG,
              s1.GLOBALBOOKFLAG,
              s1.UTILITYBOOKFLAG,
              s1.DESIGNATEDRNFLAG,
              s1.DESKLEVEL1,
              s1.LEVEL55BUSINESSUNIT,
              s1.UBRLEVEL10,
              s1.LOWESTLEVELUBR,
              s1.UBRMACODE,
              s1.BOOKSTRATEGY,
              s1.ACCTREATCATEGORY,
              s1.FINANCIALACCOUNTINGTREATMENT,
              s1.REGULATORYREPORTINGTREATMENT,
              s1.DATACOMPLETENESSSTATUS,
              s1.MANAGEDBY,
              s1.BUSINESSOWNER,
              s1.CHANGEINDICATOR,
              s1.LASTACTIONBY,
              s1.LASTACTIONBYEMAIL,
              s1.FDRGROUP,
              s1.MIDDLEOFFICEGROUP,
              s1.MOTMQ,
              s1.MOGROUPEMAILID,
              s1.SHOULDFEEDCRES,
              s1.ISSDOSABLE,
              s1.INCLUDEINNMA,
              s1.USEABSSDOS,
              s1.INCLUDESLATETRADES,
              s1.NVBOOKLOCATION,
              s1.SPIDERBONDFILTER,
              s1.TRADECAPTUREINSTANCE,
              s1.RISKCLASS,
              s1.PROMOTIONSTATUS,
              s1.CDSGROUP,
              s1.CLEARINGELIGIBLE,
              s1.CLEARINGCOMMENT,
              s1.COMPRESSIONELIGIBLE,
              s1.COMPRESSIONCOMMENT,
              s1.HUBBINGELIGIBLE,
              s1.HUBBINGCOMMENT,
              s1.BASEL3COVEREDPOSITION,
              s1.VOLCKERTRADINGDESK,
              s1.CHARGEREPORTINGPARENTCODE,
              s1.CHARGEREPORTINGUNITCODE
              -- GBSVR-33754 Start: CFBU decommissioning
              -- GBSVR-33754 End:   CFBU decommissioning
         bulk collect into l_data;
    
  forall i in 1 .. l_data.count
    insert into brds_dups_vw_book values l_data(i);
end p_brds_etl_remove_dup_book;

procedure p_brds_etl_remove_dup_portfol
as
  type array is table of brds_vw_portfolio%rowtype;
  l_data array;
begin
  delete from brds_vw_portfolio s1
   where s1.portfolioId in ( 
      select s.portfolioId
        from  brds_vw_portfolio s
       group by s.portfolioId
      having count(*) > 1
    )
    and s1.validfrom NOT in  ( 
      select max(s2.validfrom) 
       from brds_vw_portfolio s2
      where s2.portfolioId = s1.portfolioId
    )
    returning s1.PORTFOLIOID,s1.VALIDFROM,s1.VALIDTO,s1.PORTFOLIONAME,s1.BOOKNAMEFULL,s1.FINANCIALACCOUNTINGTREATMENT,s1.REGULATORYREPORTINGTREATMENT,s1.REPORTINGCATEGORY,s1.BUSINESSORIGINATIONTYPE,
              s1.RPLCODE,s1.PORTFOLIOSTATUS,s1.CHANGEINDICATOR,s1.LASTACTIONBY,s1.LASTACTIONBYEMAIL,s1.XDBGIMMISKEY,s1.REPORTINGCURRENCY,s1.XDBGIMMISKEYREQUIRED,s1.VOLCKERTRADINGDESK,
              s1.CHARGEREPORTINGPARENT,s1.CHARGEREPORTINGUNIT
         bulk collect into l_data; 
    
  forall i in 1 .. l_data.count
    insert into brds_dups_vw_portfolio values l_data(i);
end p_brds_etl_remove_dup_portfol;

procedure p_brds_etl_add_duplicates
as
begin
  insert into BRDS_VW_BOOK select * from BRDS_DUPS_VW_BOOK;
  -- GBSVR-33754 Start: CFBU decommissioning
  -- GBSVR-33754 End:   CFBU decommissioning
  insert into BRDS_VW_CRP select * from BRDS_DUPS_VW_CRP;
  insert into BRDS_VW_CRU select * from BRDS_DUPS_VW_CRU;
  insert into BRDS_VW_HIERARCHY select * from BRDS_DUPS_VW_HIERARCHY;
  insert into BRDS_VW_PORTFOLIO select * from BRDS_DUPS_VW_PORTFOLIO;
  insert into BRDS_VW_VTD select * from BRDS_DUPS_VW_VTD;  
  
  p_brds_etl_clear_duplicates;
end p_brds_etl_add_duplicates;

procedure p_brds_etl_clear_duplicates
as
begin
  delete from BRDS_DUPS_VW_BOOK;
  -- GBSVR-33754 Start: CFBU decommissioning
  -- GBSVR-33754 End:   CFBU decommissioning
  delete from BRDS_DUPS_VW_CRP;
  delete from BRDS_DUPS_VW_CRU;
  delete from BRDS_DUPS_VW_HIERARCHY;
  delete from BRDS_DUPS_VW_PORTFOLIO;
  delete from BRDS_DUPS_VW_VTD;  
end p_brds_etl_clear_duplicates;

END PKG_BRDS_BH_RPL_DUPS;
