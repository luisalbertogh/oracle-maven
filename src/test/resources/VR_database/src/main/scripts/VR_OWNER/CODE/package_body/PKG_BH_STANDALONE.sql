--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:PACKAGE_BODY_PKG_BH_STANDALONE runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE stripComments:FALSE


  CREATE OR REPLACE PACKAGE BODY "PKG_BH_STANDALONE" 
AS

--start GBSVR-27206
PROCEDURE P_BH_RPL_CLEANUP(P_ASOFDATE VARCHAR)
IS
BEGIN      
  -- Set source_system = null to the duplicates          
  UPDATE book_hierarchy_rpl b2 SET b2.source_system = NULL
   WHERE b2.asofdate = P_ASOFDATE
     AND (b2.book_id, b2.volcker_trading_desk) IN (
      select b.book_id, b.volcker_trading_desk
        from book_hierarchy_rpl b,
        (
        select b1.book_id, b1.volcker_trading_desk 
        from book_hierarchy_rpl b1
        where b1.asofdate = P_ASOFDATE
        group by b1.book_id, b1.volcker_trading_desk
        having count(*)>1
        ) t
        where b.asofdate = P_ASOFDATE
          and b.book_id = t.book_id
          and b.volcker_trading_desk = t.volcker_trading_desk
          AND NOT EXISTS ( SELECT 1 
                          from book_hierarchy_rpl bh
                          WHERE bh.asofdate = b.asofdate
                          AND bh.book_id = b.book_id
                          AND bh.source_system IS NULL
                          AND bh.volcker_trading_desk <> b.volcker_trading_desk )
  );
  pkg_monitoring.pr_insert_log_jobs_qv('bRDS', null, 'bRDS', current_date, 'PKG_BH_STANDALONE.P_BH_RPL_CLEANUP', 'INFO', 'LOGGING', 'Duplicates updated: '||SQL%ROWCOUNT, '', 'bRDS');
     
  -- Delete all records with source_system = NULL
  -- except one with the latest last_modified_date
  DELETE FROM book_hierarchy_rpl s1
   WHERE s1.asofdate = P_ASOFDATE
     AND s1.source_system is NULL 
     AND s1.book_id IN ( 
         SELECT s.book_id
           FROM book_hierarchy_rpl s
          WHERE s.asofdate = P_ASOFDATE
            AND s.source_system is NULL 
          GROUP BY s.book_id  
         HAVING COUNT(*) > 1 AND COUNT(DISTINCT NVL(s.volcker_trading_desk, ' ')) = 1
         )
     AND s1.last_modified_date NOT in ( 
         SELECT MAX(s2.last_modified_date) 
           FROM book_hierarchy_rpl s2
          WHERE s2.asofdate = P_ASOFDATE
            AND s2.source_system IS NULL
            AND s2.book_id = s1.book_id
  );
  pkg_monitoring.pr_insert_log_jobs_qv('bRDS', null, 'bRDS', current_date, 'PKG_BH_STANDALONE.P_BH_RPL_CLEANUP', 'INFO', 'LOGGING', 'Data deleted: '||SQL%ROWCOUNT, '', 'bRDS');
  
  -- Single mappings with count(book_id) = 1 and source_system not null must be updated to source_system = NULL so that it can be expanded to all the source systems later
  UPDATE book_hierarchy_rpl b1 SET b1.source_system = NULL
   WHERE b1.asofdate = P_ASOFDATE
     and b1.source_system IS NOT NULL
     and 1 = (SELECT COUNT(*) 
                FROM book_hierarchy_rpl b2
               WHERE b2.asofdate = P_ASOFDATE
                 AND b2.book_id = b1.book_id);
  pkg_monitoring.pr_insert_log_jobs_qv('bRDS', null, 'bRDS', current_date, 'PKG_BH_STANDALONE.P_BH_RPL_CLEANUP', 'INFO', 'LOGGING', 'SS not null -> SS null updated: '||SQL%ROWCOUNT, '', 'bRDS');  
END P_BH_RPL_CLEANUP;
--end GBSVR-27206
  
END PKG_BH_STANDALONE;
