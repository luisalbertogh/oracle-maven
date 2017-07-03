--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:PACKAGE_BODY_PKG_BH_PROCESS_INTERMED runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE stripComments:FALSE


  CREATE OR REPLACE PACKAGE BODY "PKG_BH_PROCESS_INTERMED" AS

--Execute the intermediary data action in BH_STAGING with asofdate=(day 1, current month+1, current year)
--AND in BH_RPL with asofdate=(day 1, current month, current year) only if it's emergency. 
FUNCTION F_BH_INTERMEDIARY_TO_STAGING(P_ID NUMBER) RETURN BOOLEAN
IS
   v_intermediary_data BH_INTERMEDIARY%ROWTYPE;
   v_upload_data REF_DATA_UI_UPLOAD%ROWTYPE;
   v_bh_rpl BOOK_HIERARCHY_RPL%ROWTYPE;
   v_bh_staging_row BH_STAGING%ROWTYPE;
   v_bh_conflict_row BH_CONFLICTS%ROWTYPE;
   v_bh_intermed_id BH_STAGING.BH_INTERMEDIARY_ID%TYPE;
   v_bh_conflict_id BH_CONFLICTS.ID%TYPE := 0;
   v_bh_intermed_datasource BH_STAGING.DATA_SOURCE%TYPE;
   v_count NUMBER;
   
   v_expanded_data BH_STAGING%ROWTYPE;
   v_asofdate DATE;
   v_data_source BH_STAGING.DATA_SOURCE%TYPE;
   v_is_full_match_staging BOOLEAN;
   CURSOR c_delete IS 
	   SELECT b.bh_intermediary_id, data_source
   	     FROM BH_STAGING b
        WHERE b.asofdate = v_asofdate
          -- GBSVR-31214 Start 1: 
          AND PKG_BH_COMMONS.F_IS_MANUAL(data_source) = 0
          -- AND UPPER(active_flag) = 'Y'	-- Include for deletion even if inactive manual. 
          -- GBSVR-31214 End 1: 
          AND ((b.global_trader_book_id IS NOT NULL AND v_intermediary_data.global_trader_book_id = b.global_trader_book_id)
          OR (b.book_id = v_intermediary_data.book_id AND NVL(v_intermediary_data.source_system_id, ' ') = NVL(b.source_system, ' ')));
BEGIN
   SELECT * INTO v_intermediary_data
   FROM BH_INTERMEDIARY b
   WHERE b.id = P_ID;
   
   SELECT * INTO v_upload_data
   FROM REF_DATA_UI_UPLOAD u
   WHERE u.id=v_intermediary_data.upload_id;
   --First day of (current month +1)
   v_asofdate := to_date(last_day(current_date)+1, 'DD-MON-YY');
   
   
   v_is_full_match_staging:= PKG_BH_COMMONS.F_IS_FULL_MATCH_STAGING(v_intermediary_data, FALSE);
   --1=Insert
   IF v_intermediary_data.action_id = 1 THEN
  
      --Delete bh_conflicts record related to the previous manual record (if there was some)
      BEGIN
        SELECT bh_intermediary_id INTO v_bh_intermed_id
          FROM BH_STAGING b
         WHERE active_flag = 'Y'
           AND ((b.global_trader_book_id IS NOT NULL AND v_intermediary_data.global_trader_book_id = b.global_trader_book_id)
           OR (b.book_id = v_intermediary_data.book_id AND NVL(v_intermediary_data.source_system_id, ' ') = NVL(b.source_system, ' ')))
           --start GBSVR-30255
           AND PKG_BH_COMMONS.F_IS_MANUAL(b.data_source) = 0;
           --end GBSVR-30255
        
        --start GBSVR-30255
        DELETE FROM BH_CONFLICTS
         WHERE bh_intermediary_id = v_bh_intermed_id
           and status = 'PENDING';
        --end GBSVR-30255
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          NULL;
      END;
      --Delete any previous manual bh_staging record related to the record to insert (if there was some)
      DELETE FROM BH_STAGING b 
       WHERE ((b.global_trader_book_id IS NOT NULL AND v_intermediary_data.global_trader_book_id = b.global_trader_book_id)
             OR (b.book_id = v_intermediary_data.book_id AND NVL(v_intermediary_data.source_system_id, ' ') = NVL(b.source_system, ' ')))
             AND PKG_BH_COMMONS.F_IS_MANUAL(b.data_source) = 0;
      
      v_expanded_data := F_BH_INTERMEDIARY_EXPAND(v_intermediary_data, v_asofdate, v_is_full_match_staging);
      v_expanded_data.create_user := v_upload_data.uploaded_by;
      v_expanded_data.create_date := v_upload_data.uploaded_on;
      v_expanded_data.last_modification_user := v_upload_data.uploaded_by;
      v_expanded_data.last_modified_date := v_upload_data.uploaded_on;
      v_expanded_data.APPROVER_USER := v_upload_data.approved_by;
      v_expanded_data.APPROVAL_DATE := v_upload_data.approved_on;
      
      --Insert in BH_STAGING
      INSERT INTO BH_STAGING VALUES v_expanded_data;
      
      IF UPPER(v_intermediary_data.emergency_flag) = 'Y' THEN
        IF PKG_BH_COMMONS.F_IS_BRDS_INTEGRATION_ACTIVE = TRUE THEN
            --Insert in BH_RPL (asofdate = first day of current month)
            v_expanded_data.asofdate := ADD_MONTHS(v_expanded_data.asofdate, -1);
            P_BH_EMERGENCY_LOAD(v_expanded_data, v_intermediary_data.action_id);
        ELSE
            --Setting comments
            UPDATE REF_DATA_UI_UPLOAD
               SET COMMENTS = PKG_BH_COMMONS.F_GET_VALIDATION_MSG(2047)
             WHERE ID = v_intermediary_data.upload_id;
        END IF;
      END IF;  
      
      --If exists full-match between manual and BRDS in staging: deactivate manual + activate BRDS.
      --If exists partial-match, activate manual + deactivate BRDS + raise conflict
      P_BH_CHECK_RAISING_CONFLICT(v_expanded_data, v_is_full_match_staging);
   ELSIF v_intermediary_data.action_id = 2 THEN --2=Modify
      --Check there is an active row in bh_staging already inserted for this book/SS
      BEGIN
          SELECT bh_intermediary_id, data_source INTO v_bh_intermed_id, v_bh_intermed_datasource
           FROM BH_STAGING b
           WHERE b.asofdate = v_asofdate
             AND active_flag = 'Y'
             AND ((b.global_trader_book_id IS NOT NULL AND v_intermediary_data.global_trader_book_id = b.global_trader_book_id)
             OR (b.book_id = v_intermediary_data.book_id AND NVL(v_intermediary_data.source_system_id, ' ') = NVL(b.source_system, ' ')));
          
          --start GBSVR-30255   
          --Delete any existing conflict
          DELETE FROM BH_CONFLICTS
             WHERE bh_intermediary_id = v_bh_intermed_id
               AND status = 'PENDING';
          --end GBSVR-30255         
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
         --Error. Nothing found to update
         ROLLBACK;
         pkg_monitoring.pr_insert_log_jobs_qv('bRDS', null, 'bRDS', current_date, 'PKG_BH_PROCESS_INTERMED.F_BH_INTERMEDIARY_TO_STAGI', 'ERROR', 'LOGGING', 'Update error: not active record found in bh_staging.', '', 'bRDS');
         RETURN FALSE;
      END;
      --Check there is a row in bh_rpl (if it is an emergency load) already inserted for this book/SS
      IF UPPER(v_intermediary_data.emergency_flag) = 'Y' THEN
         SELECT COUNT(*) INTO v_count
           FROM BOOK_HIERARCHY_RPL b
          WHERE b.asofdate = ADD_MONTHS(v_asofdate, -1) --First day of current month
            AND ((b.global_trader_book_id IS NOT NULL AND v_intermediary_data.global_trader_book_id = b.global_trader_book_id)
            OR (b.book_id = v_intermediary_data.book_id AND NVL(v_intermediary_data.source_system_id, ' ') = NVL(b.source_system, ' ')));
         IF v_count = 0 THEN
           --Error. Nothing found to update
           ROLLBACK;
           pkg_monitoring.pr_insert_log_jobs_qv('bRDS', null, 'bRDS', current_date, 'PKG_BH_PROCESS_INTERMED.F_BH_INTERMEDIARY_TO_STAGI', 'ERROR', 'LOGGING', 'Update error: not active record found in book_hierarchy_rpl.', '', 'bRDS');
           RETURN FALSE;
         END IF;
      END IF;
      
      v_expanded_data := F_BH_INTERMEDIARY_EXPAND(v_intermediary_data, v_asofdate, v_is_full_match_staging);
      v_expanded_data.last_modification_user := v_upload_data.uploaded_by;
      v_expanded_data.last_modified_date := v_upload_data.uploaded_on;
      v_expanded_data.APPROVER_USER := v_upload_data.approved_by;
      v_expanded_data.APPROVAL_DATE := v_upload_data.approved_on;
      
      --If the active record was from bRDS, deactivate and create new manual record
      IF PKG_BH_COMMONS.F_IS_BRDS(v_bh_intermed_datasource) = 0 THEN
      
        IF not v_is_full_match_staging THEN
          --Update BH_STAGING
          UPDATE BH_STAGING b
             SET active_flag = 'N'
           WHERE ((b.global_trader_book_id IS NOT NULL AND v_intermediary_data.global_trader_book_id = b.global_trader_book_id)
               OR (b.book_id = v_intermediary_data.book_id AND NVL(v_intermediary_data.source_system_id, ' ') = NVL(b.source_system, ' ')))
             --start GBSVR-30255
             AND PKG_BH_COMMONS.F_IS_BRDS(b.data_source) = 0;
             --end GBSVR-30255
        END IF;

        --Delete from bh_staging when there is an old full match record
        --start GBSVR-30255
        Delete from BH_STAGING b
         where ((b.global_trader_book_id IS NOT NULL AND v_intermediary_data.global_trader_book_id = b.global_trader_book_id)
             OR (b.book_id = v_intermediary_data.book_id AND NVL(v_intermediary_data.source_system_id, ' ') = NVL(b.source_system, ' ')))
           AND PKG_BH_COMMONS.F_IS_MANUAL(b.data_source) = 0
           AND b.active_flag = 'N';
        --end GBSVR-30255

        --Insert in BH_STAGING
        INSERT INTO BH_STAGING VALUES v_expanded_data;
      --If Manual, overwrite existing record
      ELSE   
        --Update BH_STAGING
        UPDATE BH_STAGING b
           SET ASOFDATE                     = v_expanded_data.ASOFDATE,
               BOOK_ID                      = v_expanded_data.BOOK_ID,
               VOLCKER_TRADING_DESK         = v_expanded_data.VOLCKER_TRADING_DESK,
               VOLCKER_TRADING_DESK_FULL    = v_expanded_data.VOLCKER_TRADING_DESK_FULL,
               LOWEST_LEVEL_RPL_CODE        = v_expanded_data.LOWEST_LEVEL_RPL_CODE,
               LOWEST_LEVEL_RPL_FULL_NAME   = v_expanded_data.LOWEST_LEVEL_RPL_FULL_NAME,
               LOWEST_LEVEL_RPL             = v_expanded_data.LOWEST_LEVEL_RPL,
               SOURCE_SYSTEM                = v_expanded_data.SOURCE_SYSTEM,
               LEGAL_ENTITY                 = v_expanded_data.LEGAL_ENTITY,
               GLOBAL_TRADER_BOOK_ID        = v_expanded_data.GLOBAL_TRADER_BOOK_ID,
               PROFIT_CENTER_ID             = v_expanded_data.PROFIT_CENTER_ID,
               COMMENTS                     = v_expanded_data.COMMENTS,
               DATA_SOURCE                  = v_expanded_data.DATA_SOURCE,
               LAST_MODIFIED_DATE           = v_expanded_data.LAST_MODIFIED_DATE,
               CHARGE_REPORTING_UNIT_CODE   = v_expanded_data.CHARGE_REPORTING_UNIT_CODE,
               CHARGE_REPORTING_UNIT        = v_expanded_data.CHARGE_REPORTING_UNIT,
               CHARGE_REPORTING_PARENT_CODE = v_expanded_data.CHARGE_REPORTING_PARENT_CODE,
               CHARGE_REPORTING_PARENT      = v_expanded_data.CHARGE_REPORTING_PARENT,
               -- GBSVR-33754 Start: CFBU decommissioning
               -- GBSVR-33754 End:   CFBU decommissioning
               MI_LOCATION                  = v_expanded_data.MI_LOCATION,
               UBR_LEVEL_1_ID               = v_expanded_data.UBR_LEVEL_1_ID,
               UBR_LEVEL_1_NAME             = v_expanded_data.UBR_LEVEL_1_NAME,
               UBR_LEVEL_1_RPL_CODE         = v_expanded_data.UBR_LEVEL_1_RPL_CODE,
               UBR_LEVEL_2_ID               = v_expanded_data.UBR_LEVEL_2_ID,
               UBR_LEVEL_2_NAME             = v_expanded_data.UBR_LEVEL_2_NAME,
               UBR_LEVEL_2_RPL_CODE         = v_expanded_data.UBR_LEVEL_2_RPL_CODE,
               UBR_LEVEL_3_ID               = v_expanded_data.UBR_LEVEL_3_ID,
               UBR_LEVEL_3_NAME             = v_expanded_data.UBR_LEVEL_3_NAME,
               UBR_LEVEL_3_RPL_CODE         = v_expanded_data.UBR_LEVEL_3_RPL_CODE,
               UBR_LEVEL_4_ID               = v_expanded_data.UBR_LEVEL_4_ID,
               UBR_LEVEL_4_NAME             = v_expanded_data.UBR_LEVEL_4_NAME,
               UBR_LEVEL_4_RPL_CODE         = v_expanded_data.UBR_LEVEL_4_RPL_CODE,
               UBR_LEVEL_5_ID               = v_expanded_data.UBR_LEVEL_5_ID,
               UBR_LEVEL_5_NAME             = v_expanded_data.UBR_LEVEL_5_NAME,
               UBR_LEVEL_5_RPL_CODE         = v_expanded_data.UBR_LEVEL_5_RPL_CODE,
               UBR_LEVEL_6_ID               = v_expanded_data.UBR_LEVEL_6_ID,
               UBR_LEVEL_6_NAME             = v_expanded_data.UBR_LEVEL_6_NAME,
               UBR_LEVEL_6_RPL_CODE         = v_expanded_data.UBR_LEVEL_6_RPL_CODE,
               UBR_LEVEL_7_ID               = v_expanded_data.UBR_LEVEL_7_ID,
               UBR_LEVEL_7_NAME             = v_expanded_data.UBR_LEVEL_7_NAME,
               UBR_LEVEL_7_RPL_CODE         = v_expanded_data.UBR_LEVEL_7_RPL_CODE,
               UBR_LEVEL_8_ID               = v_expanded_data.UBR_LEVEL_8_ID,
               UBR_LEVEL_8_NAME             = v_expanded_data.UBR_LEVEL_8_NAME,
               UBR_LEVEL_8_RPL_CODE         = v_expanded_data.UBR_LEVEL_8_RPL_CODE,
               UBR_LEVEL_9_ID               = v_expanded_data.UBR_LEVEL_9_ID,
               UBR_LEVEL_9_NAME             = v_expanded_data.UBR_LEVEL_9_NAME,
               UBR_LEVEL_9_RPL_CODE         = v_expanded_data.UBR_LEVEL_9_RPL_CODE,
               UBR_LEVEL_10_ID              = v_expanded_data.UBR_LEVEL_10_ID,
               UBR_LEVEL_10_NAME            = v_expanded_data.UBR_LEVEL_10_NAME,
               UBR_LEVEL_10_RPL_CODE        = v_expanded_data.UBR_LEVEL_10_RPL_CODE,
               UBR_LEVEL_11_ID              = v_expanded_data.UBR_LEVEL_11_ID,
               UBR_LEVEL_11_NAME            = v_expanded_data.UBR_LEVEL_11_NAME,
               UBR_LEVEL_11_RPL_CODE        = v_expanded_data.UBR_LEVEL_11_RPL_CODE,
               UBR_LEVEL_12_ID              = v_expanded_data.UBR_LEVEL_12_ID,
               UBR_LEVEL_12_NAME            = v_expanded_data.UBR_LEVEL_12_NAME,
               UBR_LEVEL_12_RPL_CODE        = v_expanded_data.UBR_LEVEL_12_RPL_CODE,
               UBR_LEVEL_13_ID              = v_expanded_data.UBR_LEVEL_13_ID,
               UBR_LEVEL_13_NAME            = v_expanded_data.UBR_LEVEL_13_NAME,
               UBR_LEVEL_13_RPL_CODE        = v_expanded_data.UBR_LEVEL_13_RPL_CODE,
               UBR_LEVEL_14_ID              = v_expanded_data.UBR_LEVEL_14_ID,
               UBR_LEVEL_14_NAME            = v_expanded_data.UBR_LEVEL_14_NAME,
               UBR_LEVEL_14_RPL_CODE        = v_expanded_data.UBR_LEVEL_14_RPL_CODE,
               DESK_LEVEL_1_ID              = v_expanded_data.DESK_LEVEL_1_ID,
               DESK_LEVEL_1_NAME            = v_expanded_data.DESK_LEVEL_1_NAME,
               DESK_LEVEL_1_RPL_CODE        = v_expanded_data.DESK_LEVEL_1_RPL_CODE,
               DESK_LEVEL_2_ID              = v_expanded_data.DESK_LEVEL_2_ID,
               DESK_LEVEL_2_NAME            = v_expanded_data.DESK_LEVEL_2_NAME,
               DESK_LEVEL_2_RPL_CODE        = v_expanded_data.DESK_LEVEL_2_RPL_CODE,
               DESK_LEVEL_3_ID              = v_expanded_data.DESK_LEVEL_3_ID,
               DESK_LEVEL_3_NAME            = v_expanded_data.DESK_LEVEL_3_NAME,
               DESK_LEVEL_3_RPL_CODE        = v_expanded_data.DESK_LEVEL_3_RPL_CODE,
               DESK_LEVEL_4_ID              = v_expanded_data.DESK_LEVEL_4_ID,
               DESK_LEVEL_4_NAME            = v_expanded_data.DESK_LEVEL_4_NAME,
               DESK_LEVEL_4_RPL_CODE        = v_expanded_data.DESK_LEVEL_4_RPL_CODE,
               DESK_LEVEL_5_ID              = v_expanded_data.DESK_LEVEL_5_ID,
               DESK_LEVEL_5_NAME            = v_expanded_data.DESK_LEVEL_5_NAME,
               DESK_LEVEL_5_RPL_CODE        = v_expanded_data.DESK_LEVEL_5_RPL_CODE,
               PORTFOLIO_ID                 = v_expanded_data.PORTFOLIO_ID,
               PORTFOLIO_NAME               = v_expanded_data.PORTFOLIO_NAME,
               PORTFOLIO_RPL_CODE           = v_expanded_data.PORTFOLIO_RPL_CODE,
               BUSINESS                     = v_expanded_data.BUSINESS,
               SUB_BUSINESS                 = v_expanded_data.SUB_BUSINESS,
               LAST_MODIFICATION_USER       = v_expanded_data.LAST_MODIFICATION_USER,
               REGION                       = v_expanded_data.REGION,
               SUBREGION                    = v_expanded_data.SUBREGION,
               OVERRIDDEN_FLAG              = v_expanded_data.OVERRIDDEN_FLAG,
               ACTIVE_FLAG                  = v_expanded_data.ACTIVE_FLAG,
               EMERGENCY_FLAG               = v_expanded_data.EMERGENCY_FLAG,
               BH_INTERMEDIARY_ID           = v_expanded_data.BH_INTERMEDIARY_ID,
               APPROVER_USER                = v_expanded_data.APPROVER_USER,
               APPROVAL_DATE                = v_expanded_data.APPROVAL_DATE,
               -- start GBSVR-31380
               ACC_TREAT_CATEGORY           = v_expanded_data.ACC_TREAT_CATEGORY,
               PRIMARY_TRADER               = v_expanded_data.PRIMARY_TRADER,
               PRIMARY_BOOK_RUNNER          = v_expanded_data.PRIMARY_BOOK_RUNNER,
               PRIMARY_FINCON               = v_expanded_data.PRIMARY_FINCON,
               PRIMARY_MOESCALATION         = v_expanded_data.PRIMARY_MOESCALATION,
               LEGAL_ENTITY_CODE            = v_expanded_data.LEGAL_ENTITY_CODE,
               UBR_MA_CODE                  = v_expanded_data.UBR_MA_CODE,
               HIERARCHY_UBR_NODENAME       = v_expanded_data.HIERARCHY_UBR_NODENAME,
               PROFIT_CENTRE_NAME           = v_expanded_data.PROFIT_CENTRE_NAME,
               -- end GBSVR-31380
			   NON_VTD_CODE  				= v_expanded_data.NON_VTD_CODE,
			   NON_VTD_NAME                = v_expanded_data.NON_VTD_NAME,
			   NON_VTD_RPL_CODE            = v_expanded_data.NON_VTD_RPL_CODE,
			   NON_VTD_EXCLUSION_TYPE      = v_expanded_data.NON_VTD_EXCLUSION_TYPE,
			   REGULATORY_REPORTING_TREATMENT = v_expanded_data.REGULATORY_REPORTING_TREATMENT,
			   VOLCKER_REPORTABLE_FLAG 	   = v_expanded_data.VOLCKER_REPORTABLE_FLAG,
			   --start GBSVR-30224
			   NON_VTD_DIVISION 		   = v_expanded_data.NON_VTD_DIVISION,
			   NON_VTD_PVF 			       = v_expanded_data.NON_VTD_PVF,
			   NON_VTD_BUSINESS			   = v_expanded_data.NON_VTD_BUSINESS
			   --end GBSVR-30224
         WHERE b.asofdate = v_asofdate
           AND active_flag = 'Y'
           AND ((b.global_trader_book_id IS NOT NULL AND v_intermediary_data.global_trader_book_id = b.global_trader_book_id)
           OR (b.book_id = v_intermediary_data.book_id AND NVL(v_intermediary_data.source_system_id, ' ') = NVL(b.source_system, ' ')));
      END IF;
      
      --Update BH_RPL
      IF UPPER(v_intermediary_data.emergency_flag) = 'Y' THEN
        IF PKG_BH_COMMONS.F_IS_BRDS_INTEGRATION_ACTIVE = TRUE THEN
            --Update in BH_RPL (asofdate = first day of current month)
            v_expanded_data.asofdate := ADD_MONTHS(v_expanded_data.asofdate, -1);
            P_BH_EMERGENCY_LOAD(v_expanded_data, v_intermediary_data.action_id);
        ELSE
            --Setting comments
            UPDATE REF_DATA_UI_UPLOAD
               SET COMMENTS = PKG_BH_COMMONS.F_GET_VALIDATION_MSG(2047)
             WHERE ID = v_intermediary_data.upload_id;
        END IF;
      END IF;
      
      --If exists full-match between manual and BRDS in staging: deactivate manual + activate BRDS
      P_BH_CHECK_RAISING_CONFLICT(v_expanded_data, v_is_full_match_staging);
   ELSIF v_intermediary_data.action_id = 3 THEN --3=Delete
      
      --start GBSVR-29651
      --Delete From BH_STAGING
      --Looping to cover the situation of having more than one record in staging
      OPEN c_delete;
  
        LOOP
        FETCH c_delete INTO v_bh_intermed_id, v_data_source;
          EXIT WHEN c_delete%NOTFOUND;
          
          --Delete existing pending conflicts (if there is one)
          DELETE FROM BH_CONFLICTS
             WHERE bh_intermediary_id = v_bh_intermed_id;
            
          --Delete if manual
          IF PKG_BH_COMMONS.F_IS_MANUAL(v_data_source) = 0 THEN
            DELETE FROM BH_STAGING
             WHERE bh_intermediary_id = v_bh_intermed_id;
            pkg_monitoring.pr_insert_log_jobs_qv('bRDS', null, 'bRDS', current_date, 'PKG_BH_PROCESS_INTERMED.F_BH_INTERMEDIARY_TO_STAGI', 'DEBUG', 'LOGGING', 'BH_STAGING Deleted Progress rows found: '||SQL%ROWCOUNT, '', 'bRDS');
          --Activate the corresponding bRDS entry: 
            -- GBSVR-31214 Start 2: 
--          ELSE                      -- This should NOT be an IF-THEN-ELSE as we need to delete the Manual AND set the bRDS entry active (in the case of a partial match)
            -- GBSVR-31214 End 2: 
            UPDATE BH_STAGING b
               -- GBSVR-31214 Start 3: 
               SET b.active_flag = 'Y' 
               -- GBSVR-31214 End 3: 
             WHERE b.asofdate = v_asofdate
               -- GBSVR-31214 Start 4: 
               AND UPPER(b.active_flag) = 'N'
               -- GBSVR-31214 End 4: 
               AND PKG_BH_COMMONS.F_IS_BRDS(b.data_source) = 0
               AND ((b.global_trader_book_id IS NOT NULL AND v_intermediary_data.global_trader_book_id = b.global_trader_book_id)
               OR (b.book_id = v_intermediary_data.book_id AND NVL(v_intermediary_data.source_system_id, ' ') = NVL(b.source_system, ' ')));
            pkg_monitoring.pr_insert_log_jobs_qv('bRDS', null, 'bRDS', current_date, 'PKG_BH_PROCESS_INTERMED.F_BH_INTERMEDIARY_TO_STAGI', 'DEBUG', 'LOGGING', 'BH_STAGING Updated Progress rows found: '||SQL%ROWCOUNT, '', 'bRDS');
          END IF;
        END LOOP;

      --Delete From BOOK_HIERARCHY_RPL
      --Find in BH_RPL with current month AND delete (only for emergency records)
      IF UPPER(v_intermediary_data.emergency_flag) = 'Y' THEN
          IF PKG_BH_COMMONS.F_IS_BRDS_INTEGRATION_ACTIVE = TRUE THEN
              DELETE FROM BOOK_HIERARCHY_RPL b
             WHERE b.asofdate = ADD_MONTHS(v_asofdate, -1)
               -- GBSVR-31214 Start 5: 
               AND PKG_BH_COMMONS.F_IS_MANUAL(b.data_source) = 0
               -- GBSVR-31214 End 5: 
               AND ((b.global_trader_book_id IS NOT NULL AND v_intermediary_data.global_trader_book_id = b.global_trader_book_id)
               OR (b.book_id = v_intermediary_data.book_id AND NVL(v_intermediary_data.source_system_id, ' ') = NVL(b.source_system, ' ')));
            pkg_monitoring.pr_insert_log_jobs_qv('bRDS', null, 'bRDS', current_date, 'PKG_BH_PROCESS_INTERMED.F_BH_INTERMEDIARY_TO_STAGI', 'DEBUG', 'LOGGING', 'BOOK_HIERARCHY_RPL Deleted Progress rows found: '||SQL%ROWCOUNT, '', 'bRDS');
          END IF;
        END IF;
        --end GBSVR-29651
      
      --start GBSVR-29651
      --Double-check removed
    --end GBSVR-29651
   END IF;
   
   RETURN TRUE;
EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;
    --start GBSVR-30255
     IF v_intermediary_data.id IS NULL THEN --Standard error message
       pkg_monitoring.pr_insert_log_jobs_qv('bRDS', null, 'bRDS', current_date,'PKG_BH_PROCESS_INTERMED.F_BH_INTERMEDIARY_TO_STAGI','ERROR', 'FATAL', 'Error: '||TO_CHAR(SQLCODE), SUBSTR(SQLERRM, 1, 2500), 'bRDS');
     ELSE  --Detailed error message
       pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'PKG_BH_PROCESS_INTERMED.F_BH_INTERMEDIARY_TO_STAGI','ERROR', 'FATAL', 
        'Error: '||TO_CHAR(SQLCODE)||
        '. Book_id: '||v_intermediary_data.book_id||
        ', Source_system: '||v_intermediary_data.source_system_id||
        ', volcker_trading_desk: '||v_intermediary_data.volcker_trading_desk||
        '. v_asofdate: '||v_asofdate||
        '. v_is_full_match_staging: '||(CASE WHEN v_is_full_match_staging THEN 'TRUE' ELSE 'FALSE' END)||'.', SUBSTR(SQLERRM, 1, 2500), 'bRDS');
     END IF;
     --end GBSVR-30255
    RAISE;
END F_BH_INTERMEDIARY_TO_STAGING;

--start GBSVR-30255
PROCEDURE P_BH_CHECK_RAISING_CONFLICT(p_staging_row IN BH_STAGING%ROWTYPE, p_is_full_match_staging IN BOOLEAN)
IS
  v_bh_staging_brds BH_STAGING%ROWTYPE;
BEGIN
  --If exists full-match between manual and BRDS in staging: deactivate manual + activate BRDS.
  --If exists partial-match, activate manual + deactivate BRDS + raise conflict
  IF p_is_full_match_staging = TRUE THEN
    --Deactivate manual  
    UPDATE BH_STAGING b SET active_flag = 'N'
     WHERE ((b.global_trader_book_id IS NOT NULL AND p_staging_row.global_trader_book_id = b.global_trader_book_id)
       OR (b.book_id = p_staging_row.book_id AND NVL(p_staging_row.source_system, ' ') = NVL(b.source_system, ' ')))
       AND PKG_BH_COMMONS.F_IS_MANUAL(b.data_source) = 0;
    --Activate BRDS
    UPDATE BH_STAGING b SET active_flag = 'Y'
     WHERE ((b.global_trader_book_id IS NOT NULL AND p_staging_row.global_trader_book_id = b.global_trader_book_id)
       OR (b.book_id = p_staging_row.book_id AND NVL(p_staging_row.source_system, ' ') = NVL(b.source_system, ' ')))
       AND PKG_BH_COMMONS.F_IS_BRDS(b.data_source) = 0;
  ELSE
    BEGIN
      SELECT * INTO v_bh_staging_brds
        FROM bh_staging
       WHERE PKG_BH_COMMONS.F_IS_BRDS(data_source) = 0
         AND book_id = p_staging_row.book_id;
      
        --Deactivate bRDS
        UPDATE BH_STAGING b SET active_flag = 'N'
         WHERE b.asofdate = p_staging_row.asofdate
           AND ((b.global_trader_book_id IS NOT NULL AND p_staging_row.global_trader_book_id = b.global_trader_book_id)
           OR (b.book_id = p_staging_row.book_id AND NVL(p_staging_row.source_system, ' ') = NVL(b.source_system, ' ')))
           AND PKG_BH_COMMONS.F_IS_BRDS(b.data_source) = 0;
        --Create conflict if bRDS record exists in staging
        INSERT INTO bh_conflicts (
              id, status, asofdate, book_id, volcker_trading_desk, created_on, resolved_by, resolved_on, 
              global_trader_book_id, source_system, charge_reporting_unit_code, charge_reporting_parent_code, 
              -- GBSVR-33754 Start: CFBU decommissioning
              -- GBSVR-33754 End:   CFBU decommissioning
              business, sub_business, comments, bh_intermediary_id) 
        VALUES (
              seq_bh_conflicts.nextval, 'PENDING', v_bh_staging_brds.asofdate, v_bh_staging_brds.book_id, v_bh_staging_brds.volcker_trading_desk, SYSDATE, NULL, NULL,
              v_bh_staging_brds.global_trader_book_id, v_bh_staging_brds.source_system, v_bh_staging_brds.charge_reporting_unit_code, v_bh_staging_brds.charge_reporting_parent_code, 
              -- GBSVR-33754 Start: CFBU decommissioning
              -- GBSVR-33754 End:   CFBU decommissioning
              v_bh_staging_brds.business, v_bh_staging_brds.sub_business, 
              NULL, nvl(p_staging_row.bh_intermediary_id, 0));
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        NULL;
      WHEN TOO_MANY_ROWS THEN
        ROLLBACK;
        pkg_monitoring.pr_insert_log_jobs_qv('bRDS', null, 'bRDS', current_date, 'PKG_BH_PROCESS_INTERMED.P_BH_CHECK_RAISING_CONFLICT', 'ERROR', 'LOGGING', 'Error: More than one bRDS record found in bh_staging. book_id: '||p_staging_row.book_id, substr(SQLERRM,1,2500), 'bRDS');
        RAISE;
    END;
  END IF;
END P_BH_CHECK_RAISING_CONFLICT;
--end GBSVR_30255      

--Load data INTO BOOK_HIERARCHY_RPL
PROCEDURE P_BH_EMERGENCY_LOAD(p_bh_staging_row BH_STAGING%ROWTYPE, p_action_id INTEGER)
IS
BEGIN    
    
    IF p_action_id = 1 THEN --Insert
      INSERT INTO BOOK_HIERARCHY_RPL
        (
          ASOFDATE,
          BOOK_ID,
          VOLCKER_TRADING_DESK,
          VOLCKER_TRADING_DESK_FULL,
          LOWEST_LEVEL_RPL_CODE,
          LOWEST_LEVEL_RPL_FULL_NAME,
          LOWEST_LEVEL_RPL,
          SOURCE_SYSTEM,
          LEGAL_ENTITY,
          GLOBAL_TRADER_BOOK_ID,
          PROFIT_CENTER_ID,
          COMMENTS,
          DATA_SOURCE,
          CREATE_DATE,
          LAST_MODIFIED_DATE,
          CHARGE_REPORTING_UNIT_CODE,
          CHARGE_REPORTING_UNIT,
          CHARGE_REPORTING_PARENT_CODE,
          CHARGE_REPORTING_PARENT,
          -- GBSVR-33754 Start: CFBU decommissioning
          -- GBSVR-33754 End:   CFBU decommissioning
          MI_LOCATION,
          UBR_LEVEL_1_ID,
          UBR_LEVEL_1_NAME,
          UBR_LEVEL_1_RPL_CODE,
          UBR_LEVEL_2_ID,
          UBR_LEVEL_2_NAME,
          UBR_LEVEL_2_RPL_CODE,
          UBR_LEVEL_3_ID,
          UBR_LEVEL_3_NAME,
          UBR_LEVEL_3_RPL_CODE,
          UBR_LEVEL_4_ID,
          UBR_LEVEL_4_NAME,
          UBR_LEVEL_4_RPL_CODE,
          UBR_LEVEL_5_ID,
          UBR_LEVEL_5_NAME,
          UBR_LEVEL_5_RPL_CODE,
          UBR_LEVEL_6_ID,
          UBR_LEVEL_6_NAME,
          UBR_LEVEL_6_RPL_CODE,
          UBR_LEVEL_7_ID,
          UBR_LEVEL_7_NAME,
          UBR_LEVEL_7_RPL_CODE,
          UBR_LEVEL_8_ID,
          UBR_LEVEL_8_NAME,
          UBR_LEVEL_8_RPL_CODE,
          UBR_LEVEL_9_ID,
          UBR_LEVEL_9_NAME,
          UBR_LEVEL_9_RPL_CODE,
          UBR_LEVEL_10_ID,
          UBR_LEVEL_10_NAME,
          UBR_LEVEL_10_RPL_CODE,
          UBR_LEVEL_11_ID,
          UBR_LEVEL_11_NAME,
          UBR_LEVEL_11_RPL_CODE,
          UBR_LEVEL_12_ID,
          UBR_LEVEL_12_NAME,
          UBR_LEVEL_12_RPL_CODE,
          UBR_LEVEL_13_ID,
          UBR_LEVEL_13_NAME,
          UBR_LEVEL_13_RPL_CODE,
          UBR_LEVEL_14_ID,
          UBR_LEVEL_14_NAME,
          UBR_LEVEL_14_RPL_CODE,
          DESK_LEVEL_1_ID,
          DESK_LEVEL_1_NAME,
          DESK_LEVEL_1_RPL_CODE,
          DESK_LEVEL_2_ID,
          DESK_LEVEL_2_NAME,
          DESK_LEVEL_2_RPL_CODE,
          DESK_LEVEL_3_ID,
          DESK_LEVEL_3_NAME,
          DESK_LEVEL_3_RPL_CODE,
          DESK_LEVEL_4_ID,
          DESK_LEVEL_4_NAME,
          DESK_LEVEL_4_RPL_CODE,
          DESK_LEVEL_5_ID,
          DESK_LEVEL_5_NAME,
          DESK_LEVEL_5_RPL_CODE,
          PORTFOLIO_ID,
          PORTFOLIO_NAME,
          PORTFOLIO_RPL_CODE,
          BUSINESS,
          SUB_BUSINESS,
          CREATE_USER,
          LAST_MODIFICATION_USER,
          REGION,
          SUBREGION,
		  APPROVER_USER,
		  APPROVAL_DATE,
      -- start GBSVR-31380
      ACC_TREAT_CATEGORY,
      PRIMARY_TRADER,
      PRIMARY_BOOK_RUNNER,
      PRIMARY_FINCON,
      PRIMARY_MOESCALATION,
      LEGAL_ENTITY_CODE,
      UBR_MA_CODE,
      HIERARCHY_UBR_NODENAME,
      PROFIT_CENTRE_NAME,
      -- end GBSVR-31380
		  NON_VTD_CODE,
		  NON_VTD_NAME,
		  NON_VTD_RPL_CODE,
		  NON_VTD_EXCLUSION_TYPE,
		  REGULATORY_REPORTING_TREATMENT,
          VOLCKER_REPORTABLE_FLAG,
          --start GBSVR-30224
		  NON_VTD_DIVISION,  
		  NON_VTD_PVF, 
		  NON_VTD_BUSINESS
          --end GBSVR-30224  
        ) VALUES (
        p_bh_staging_row.ASOFDATE,
        p_bh_staging_row.BOOK_ID,
        p_bh_staging_row.VOLCKER_TRADING_DESK,
        p_bh_staging_row.VOLCKER_TRADING_DESK_FULL,
        p_bh_staging_row.LOWEST_LEVEL_RPL_CODE,
        p_bh_staging_row.LOWEST_LEVEL_RPL_FULL_NAME,
        p_bh_staging_row.LOWEST_LEVEL_RPL,
        p_bh_staging_row.SOURCE_SYSTEM,
        p_bh_staging_row.LEGAL_ENTITY,
        p_bh_staging_row.GLOBAL_TRADER_BOOK_ID,
        p_bh_staging_row.PROFIT_CENTER_ID,
        p_bh_staging_row.COMMENTS,
        p_bh_staging_row.DATA_SOURCE,
        p_bh_staging_row.CREATE_DATE,
        p_bh_staging_row.LAST_MODIFIED_DATE,
        p_bh_staging_row.CHARGE_REPORTING_UNIT_CODE,
        p_bh_staging_row.CHARGE_REPORTING_UNIT,
        p_bh_staging_row.CHARGE_REPORTING_PARENT_CODE,
        p_bh_staging_row.CHARGE_REPORTING_PARENT,
        -- GBSVR-33754 Start: CFBU decommissioning
        -- GBSVR-33754 End:   CFBU decommissioning
        p_bh_staging_row.MI_LOCATION,
        p_bh_staging_row.UBR_LEVEL_1_ID,
        p_bh_staging_row.UBR_LEVEL_1_NAME,
        p_bh_staging_row.UBR_LEVEL_1_RPL_CODE,
        p_bh_staging_row.UBR_LEVEL_2_ID,
        p_bh_staging_row.UBR_LEVEL_2_NAME,
        p_bh_staging_row.UBR_LEVEL_2_RPL_CODE,
        p_bh_staging_row.UBR_LEVEL_3_ID,
        p_bh_staging_row.UBR_LEVEL_3_NAME,
        p_bh_staging_row.UBR_LEVEL_3_RPL_CODE,
        p_bh_staging_row.UBR_LEVEL_4_ID,
        p_bh_staging_row.UBR_LEVEL_4_NAME,
        p_bh_staging_row.UBR_LEVEL_4_RPL_CODE,
        p_bh_staging_row.UBR_LEVEL_5_ID,
        p_bh_staging_row.UBR_LEVEL_5_NAME,
        p_bh_staging_row.UBR_LEVEL_5_RPL_CODE,
        p_bh_staging_row.UBR_LEVEL_6_ID,
        p_bh_staging_row.UBR_LEVEL_6_NAME,
        p_bh_staging_row.UBR_LEVEL_6_RPL_CODE,
        p_bh_staging_row.UBR_LEVEL_7_ID,
        p_bh_staging_row.UBR_LEVEL_7_NAME,
        p_bh_staging_row.UBR_LEVEL_7_RPL_CODE,
        p_bh_staging_row.UBR_LEVEL_8_ID,
        p_bh_staging_row.UBR_LEVEL_8_NAME,
        p_bh_staging_row.UBR_LEVEL_8_RPL_CODE,
        p_bh_staging_row.UBR_LEVEL_9_ID,
        p_bh_staging_row.UBR_LEVEL_9_NAME,
        p_bh_staging_row.UBR_LEVEL_9_RPL_CODE,
        p_bh_staging_row.UBR_LEVEL_10_ID,
        p_bh_staging_row.UBR_LEVEL_10_NAME,
        p_bh_staging_row.UBR_LEVEL_10_RPL_CODE,
        p_bh_staging_row.UBR_LEVEL_11_ID,
        p_bh_staging_row.UBR_LEVEL_11_NAME,
        p_bh_staging_row.UBR_LEVEL_11_RPL_CODE,
        p_bh_staging_row.UBR_LEVEL_12_ID,
        p_bh_staging_row.UBR_LEVEL_12_NAME,
        p_bh_staging_row.UBR_LEVEL_12_RPL_CODE,
        p_bh_staging_row.UBR_LEVEL_13_ID,
        p_bh_staging_row.UBR_LEVEL_13_NAME,
        p_bh_staging_row.UBR_LEVEL_13_RPL_CODE,
        p_bh_staging_row.UBR_LEVEL_14_ID,
        p_bh_staging_row.UBR_LEVEL_14_NAME,
        p_bh_staging_row.UBR_LEVEL_14_RPL_CODE,
        p_bh_staging_row.DESK_LEVEL_1_ID,
        p_bh_staging_row.DESK_LEVEL_1_NAME,
        p_bh_staging_row.DESK_LEVEL_1_RPL_CODE,
        p_bh_staging_row.DESK_LEVEL_2_ID,
        p_bh_staging_row.DESK_LEVEL_2_NAME,
        p_bh_staging_row.DESK_LEVEL_2_RPL_CODE,
        p_bh_staging_row.DESK_LEVEL_3_ID,
        p_bh_staging_row.DESK_LEVEL_3_NAME,
        p_bh_staging_row.DESK_LEVEL_3_RPL_CODE,
        p_bh_staging_row.DESK_LEVEL_4_ID,
        p_bh_staging_row.DESK_LEVEL_4_NAME,
        p_bh_staging_row.DESK_LEVEL_4_RPL_CODE,
        p_bh_staging_row.DESK_LEVEL_5_ID,
        p_bh_staging_row.DESK_LEVEL_5_NAME,
        p_bh_staging_row.DESK_LEVEL_5_RPL_CODE,
        p_bh_staging_row.PORTFOLIO_ID,
        p_bh_staging_row.PORTFOLIO_NAME,
        p_bh_staging_row.PORTFOLIO_RPL_CODE,
        p_bh_staging_row.BUSINESS,
        p_bh_staging_row.SUB_BUSINESS,
        p_bh_staging_row.CREATE_USER,
        p_bh_staging_row.LAST_MODIFICATION_USER,
        p_bh_staging_row.REGION,
        p_bh_staging_row.SUBREGION,
        p_bh_staging_row.APPROVER_USER,
        p_bh_staging_row.APPROVAL_DATE,
        -- start GBSVR-31380
        p_bh_staging_row.ACC_TREAT_CATEGORY,
        p_bh_staging_row.PRIMARY_TRADER,
        p_bh_staging_row.PRIMARY_BOOK_RUNNER,
        p_bh_staging_row.PRIMARY_FINCON,
        p_bh_staging_row.PRIMARY_MOESCALATION,
        p_bh_staging_row.LEGAL_ENTITY_CODE,
        p_bh_staging_row.UBR_MA_CODE,
        p_bh_staging_row.HIERARCHY_UBR_NODENAME,
        p_bh_staging_row.PROFIT_CENTRE_NAME,
        -- end GBSVR-31380
        p_bh_staging_row.NON_VTD_CODE,
        p_bh_staging_row.NON_VTD_NAME,
        p_bh_staging_row.NON_VTD_RPL_CODE,
        p_bh_staging_row.NON_VTD_EXCLUSION_TYPE,
        p_bh_staging_row.REGULATORY_REPORTING_TREATMENT,
        p_bh_staging_row.VOLCKER_REPORTABLE_FLAG,
        --start GBSVR-30224
		p_bh_staging_row.NON_VTD_DIVISION,
		p_bh_staging_row.NON_VTD_PVF,
		p_bh_staging_row.NON_VTD_BUSINESS);
        --end GBSVR-30224
    ELSIF p_action_id = 2 THEN --Update
       
        UPDATE BOOK_HIERARCHY_RPL b
            SET ASOFDATE                     = p_bh_staging_row.ASOFDATE,
                BOOK_ID                      = p_bh_staging_row.BOOK_ID,
                VOLCKER_TRADING_DESK         = p_bh_staging_row.VOLCKER_TRADING_DESK,
                VOLCKER_TRADING_DESK_FULL    = p_bh_staging_row.VOLCKER_TRADING_DESK_FULL,
                LOWEST_LEVEL_RPL_CODE        = p_bh_staging_row.LOWEST_LEVEL_RPL_CODE,
                LOWEST_LEVEL_RPL_FULL_NAME   = p_bh_staging_row.LOWEST_LEVEL_RPL_FULL_NAME,
                LOWEST_LEVEL_RPL             = p_bh_staging_row.LOWEST_LEVEL_RPL,
                SOURCE_SYSTEM                = p_bh_staging_row.SOURCE_SYSTEM,
                LEGAL_ENTITY                 = p_bh_staging_row.LEGAL_ENTITY,
                GLOBAL_TRADER_BOOK_ID        = p_bh_staging_row.GLOBAL_TRADER_BOOK_ID,
                PROFIT_CENTER_ID             = p_bh_staging_row.PROFIT_CENTER_ID,
                COMMENTS                     = p_bh_staging_row.COMMENTS,
                DATA_SOURCE                  = p_bh_staging_row.DATA_SOURCE,
                LAST_MODIFIED_DATE           = p_bh_staging_row.LAST_MODIFIED_DATE,
                CHARGE_REPORTING_UNIT_CODE   = p_bh_staging_row.CHARGE_REPORTING_UNIT_CODE,
                CHARGE_REPORTING_UNIT        = p_bh_staging_row.CHARGE_REPORTING_UNIT,
                CHARGE_REPORTING_PARENT_CODE = p_bh_staging_row.CHARGE_REPORTING_PARENT_CODE,
                CHARGE_REPORTING_PARENT      = p_bh_staging_row.CHARGE_REPORTING_PARENT,
                -- GBSVR-33754 Start: CFBU decommissioning
                -- GBSVR-33754 End:   CFBU decommissioning
                MI_LOCATION                  = p_bh_staging_row.MI_LOCATION,
                UBR_LEVEL_1_ID               = p_bh_staging_row.UBR_LEVEL_1_ID,
                UBR_LEVEL_1_NAME             = p_bh_staging_row.UBR_LEVEL_1_NAME,
                UBR_LEVEL_1_RPL_CODE         = p_bh_staging_row.UBR_LEVEL_1_RPL_CODE,
                UBR_LEVEL_2_ID               = p_bh_staging_row.UBR_LEVEL_2_ID,
                UBR_LEVEL_2_NAME             = p_bh_staging_row.UBR_LEVEL_2_NAME,
                UBR_LEVEL_2_RPL_CODE         = p_bh_staging_row.UBR_LEVEL_2_RPL_CODE,
                UBR_LEVEL_3_ID               = p_bh_staging_row.UBR_LEVEL_3_ID,
                UBR_LEVEL_3_NAME             = p_bh_staging_row.UBR_LEVEL_3_NAME,
                UBR_LEVEL_3_RPL_CODE         = p_bh_staging_row.UBR_LEVEL_3_RPL_CODE,
                UBR_LEVEL_4_ID               = p_bh_staging_row.UBR_LEVEL_4_ID,
                UBR_LEVEL_4_NAME             = p_bh_staging_row.UBR_LEVEL_4_NAME,
                UBR_LEVEL_4_RPL_CODE         = p_bh_staging_row.UBR_LEVEL_4_RPL_CODE,
                UBR_LEVEL_5_ID               = p_bh_staging_row.UBR_LEVEL_5_ID,
                UBR_LEVEL_5_NAME             = p_bh_staging_row.UBR_LEVEL_5_NAME,
                UBR_LEVEL_5_RPL_CODE         = p_bh_staging_row.UBR_LEVEL_5_RPL_CODE,
                UBR_LEVEL_6_ID               = p_bh_staging_row.UBR_LEVEL_6_ID,
                UBR_LEVEL_6_NAME             = p_bh_staging_row.UBR_LEVEL_6_NAME,
                UBR_LEVEL_6_RPL_CODE         = p_bh_staging_row.UBR_LEVEL_6_RPL_CODE,
                UBR_LEVEL_7_ID               = p_bh_staging_row.UBR_LEVEL_7_ID,
                UBR_LEVEL_7_NAME             = p_bh_staging_row.UBR_LEVEL_7_NAME,
                UBR_LEVEL_7_RPL_CODE         = p_bh_staging_row.UBR_LEVEL_7_RPL_CODE,
                UBR_LEVEL_8_ID               = p_bh_staging_row.UBR_LEVEL_8_ID,
                UBR_LEVEL_8_NAME             = p_bh_staging_row.UBR_LEVEL_8_NAME,
                UBR_LEVEL_8_RPL_CODE         = p_bh_staging_row.UBR_LEVEL_8_RPL_CODE,
                UBR_LEVEL_9_ID               = p_bh_staging_row.UBR_LEVEL_9_ID,
                UBR_LEVEL_9_NAME             = p_bh_staging_row.UBR_LEVEL_9_NAME,
                UBR_LEVEL_9_RPL_CODE         = p_bh_staging_row.UBR_LEVEL_9_RPL_CODE,
                UBR_LEVEL_10_ID              = p_bh_staging_row.UBR_LEVEL_10_ID,
                UBR_LEVEL_10_NAME            = p_bh_staging_row.UBR_LEVEL_10_NAME,
                UBR_LEVEL_10_RPL_CODE        = p_bh_staging_row.UBR_LEVEL_10_RPL_CODE,
                UBR_LEVEL_11_ID              = p_bh_staging_row.UBR_LEVEL_11_ID,
                UBR_LEVEL_11_NAME            = p_bh_staging_row.UBR_LEVEL_11_NAME,
                UBR_LEVEL_11_RPL_CODE        = p_bh_staging_row.UBR_LEVEL_11_RPL_CODE,
                UBR_LEVEL_12_ID              = p_bh_staging_row.UBR_LEVEL_12_ID,
                UBR_LEVEL_12_NAME            = p_bh_staging_row.UBR_LEVEL_12_NAME,
                UBR_LEVEL_12_RPL_CODE        = p_bh_staging_row.UBR_LEVEL_12_RPL_CODE,
                UBR_LEVEL_13_ID              = p_bh_staging_row.UBR_LEVEL_13_ID,
                UBR_LEVEL_13_NAME            = p_bh_staging_row.UBR_LEVEL_13_NAME,
                UBR_LEVEL_13_RPL_CODE        = p_bh_staging_row.UBR_LEVEL_13_RPL_CODE,
                UBR_LEVEL_14_ID              = p_bh_staging_row.UBR_LEVEL_14_ID,
                UBR_LEVEL_14_NAME            = p_bh_staging_row.UBR_LEVEL_14_NAME,
                UBR_LEVEL_14_RPL_CODE        = p_bh_staging_row.UBR_LEVEL_14_RPL_CODE,
                DESK_LEVEL_1_ID              = p_bh_staging_row.DESK_LEVEL_1_ID,
                DESK_LEVEL_1_NAME            = p_bh_staging_row.DESK_LEVEL_1_NAME,
                DESK_LEVEL_1_RPL_CODE        = p_bh_staging_row.DESK_LEVEL_1_RPL_CODE,
                DESK_LEVEL_2_ID              = p_bh_staging_row.DESK_LEVEL_2_ID,
                DESK_LEVEL_2_NAME            = p_bh_staging_row.DESK_LEVEL_2_NAME,
                DESK_LEVEL_2_RPL_CODE        = p_bh_staging_row.DESK_LEVEL_2_RPL_CODE,
                DESK_LEVEL_3_ID              = p_bh_staging_row.DESK_LEVEL_3_ID,
                DESK_LEVEL_3_NAME            = p_bh_staging_row.DESK_LEVEL_3_NAME,
                DESK_LEVEL_3_RPL_CODE        = p_bh_staging_row.DESK_LEVEL_3_RPL_CODE,
                DESK_LEVEL_4_ID              = p_bh_staging_row.DESK_LEVEL_4_ID,
                DESK_LEVEL_4_NAME            = p_bh_staging_row.DESK_LEVEL_4_NAME,
                DESK_LEVEL_4_RPL_CODE        = p_bh_staging_row.DESK_LEVEL_4_RPL_CODE,
                DESK_LEVEL_5_ID              = p_bh_staging_row.DESK_LEVEL_5_ID,
                DESK_LEVEL_5_NAME            = p_bh_staging_row.DESK_LEVEL_5_NAME,
                DESK_LEVEL_5_RPL_CODE        = p_bh_staging_row.DESK_LEVEL_5_RPL_CODE,
                PORTFOLIO_ID                 = p_bh_staging_row.PORTFOLIO_ID,
                PORTFOLIO_NAME               = p_bh_staging_row.PORTFOLIO_NAME,
                PORTFOLIO_RPL_CODE           = p_bh_staging_row.PORTFOLIO_RPL_CODE,
                BUSINESS                     = p_bh_staging_row.BUSINESS,
                SUB_BUSINESS                 = p_bh_staging_row.SUB_BUSINESS,
                LAST_MODIFICATION_USER       = p_bh_staging_row.LAST_MODIFICATION_USER,
                REGION                       = p_bh_staging_row.REGION,
                SUBREGION                    = p_bh_staging_row.SUBREGION,
			    APPROVER_USER                = p_bh_staging_row.APPROVER_USER,
		        APPROVAL_DATE                = p_bh_staging_row.APPROVAL_DATE,
            -- start GBSVR-31380
            ACC_TREAT_CATEGORY           = p_bh_staging_row.ACC_TREAT_CATEGORY,
               PRIMARY_TRADER               = p_bh_staging_row.PRIMARY_TRADER,
               PRIMARY_BOOK_RUNNER          = p_bh_staging_row.PRIMARY_BOOK_RUNNER,
               PRIMARY_FINCON               = p_bh_staging_row.PRIMARY_FINCON,
               PRIMARY_MOESCALATION         = p_bh_staging_row.PRIMARY_MOESCALATION,
               LEGAL_ENTITY_CODE            = p_bh_staging_row.LEGAL_ENTITY_CODE,
               UBR_MA_CODE                  = p_bh_staging_row.UBR_MA_CODE,
               HIERARCHY_UBR_NODENAME       = p_bh_staging_row.HIERARCHY_UBR_NODENAME,
               PROFIT_CENTRE_NAME           = p_bh_staging_row.PROFIT_CENTRE_NAME,
            -- end GBSVR-31380
			    NON_VTD_CODE                 = p_bh_staging_row.NON_VTD_CODE,
			    NON_VTD_NAME                 = p_bh_staging_row.NON_VTD_NAME,
			    NON_VTD_RPL_CODE             = p_bh_staging_row.NON_VTD_RPL_CODE,
			    NON_VTD_EXCLUSION_TYPE       = p_bh_staging_row.NON_VTD_EXCLUSION_TYPE,
			    REGULATORY_REPORTING_TREATMENT       = p_bh_staging_row.REGULATORY_REPORTING_TREATMENT,
                VOLCKER_REPORTABLE_FLAG = p_bh_staging_row.VOLCKER_REPORTABLE_FLAG,
                --start GBSVR-30224
				NON_VTD_DIVISION = p_bh_staging_row.NON_VTD_DIVISION,			
				NON_VTD_PVF = p_bh_staging_row.NON_VTD_PVF,	
				NON_VTD_BUSINESS = p_bh_staging_row.NON_VTD_BUSINESS			   
                --end GBSVR-30224
          WHERE b.asofdate = p_bh_staging_row.asofdate
            AND ((b.global_trader_book_id IS NOT NULL AND p_bh_staging_row.global_trader_book_id = b.global_trader_book_id)
            OR (b.book_id = p_bh_staging_row.book_id AND NVL(p_bh_staging_row.source_system, ' ') = NVL(b.source_system, ' ')));
    END IF;
END P_BH_EMERGENCY_LOAD;

FUNCTION F_BH_INTERMEDIARY_EXPAND(p_intermediary_data BH_INTERMEDIARY%ROWTYPE, v_asofdate DATE, p_is_full_match_staging BOOLEAN) RETURN BH_STAGING%ROWTYPE
AS
    v_expanded_data BH_STAGING%ROWTYPE;
    max_ubr_level NUMBER;
    v_num_manual_cru  INT;
    v_num_manual_crp  INT;
    -- GBSVR-33754 Start: CFBU decommissioning
    -- GBSVR-33754 End:   CFBU decommissioning
    v_num_gtb           INT;
    v_book_in_brds BOOLEAN;
    v_brds_cru_code VARCHAR2(100);
    v_brds_crp_code VARCHAR2(100);
    -- GBSVR-33754 Start: CFBU decommissioning
    -- GBSVR-33754 End:   CFBU decommissioning
    v_legal_entity VARCHAR2(200);
    v_legal_entity_code VARCHAR2(100);
    
    max_asodate_nonvtd DATE;
BEGIN

    --Step 1: Fill intermediary fields
    --Compulsory
    v_expanded_data.emergency_flag               := p_intermediary_data.emergency_flag;
    v_expanded_data.book_id                      := p_intermediary_data.book_id;
    v_expanded_data.volcker_trading_desk         := p_intermediary_data.volcker_trading_desk;  
    --Optional
    v_expanded_data.global_trader_book_id          := p_intermediary_data.global_trader_book_id; 
    v_expanded_data.source_system                  := p_intermediary_data.source_system_id;  
    v_expanded_data.charge_reporting_unit_code     := p_intermediary_data.charge_reporting_unit_code; 
    v_expanded_data.charge_reporting_parent_code   := p_intermediary_data.charge_reporting_parent_code; 
    -- GBSVR-33754 Start: CFBU decommissioning
    -- GBSVR-33754 End:   CFBU decommissioning
    
    v_expanded_data.business                     := p_intermediary_data.business; 
    v_expanded_data.sub_business                 := p_intermediary_data.sub_business;
    v_expanded_data.comments                     := p_intermediary_data.comments;
    
    v_expanded_data.non_vtd_rpl_code           := p_intermediary_data.non_vtd_rpl_code;
    
    -- GBSVR-31380
    v_expanded_data.legal_entity                := p_intermediary_data.legal_entity;
    v_expanded_data.legal_entity_code           := p_intermediary_data.legal_entity_code;
    -- end GBSVR-31380
    
    --Step 2: Fill constant data
    v_expanded_data.asofdate           := v_asofdate;
    v_expanded_data.data_source        := 'MANUAL';
    v_expanded_data.create_date        := systimestamp;
    v_expanded_data.last_modified_date := systimestamp;
    
    IF not p_is_full_match_staging THEN
        v_expanded_data.active_flag        := 'Y';
    ELSE 
        v_expanded_data.active_flag        := 'N';
    END IF;
    v_expanded_data.bh_intermediary_id := p_intermediary_data.id;
    
    --Step 3: Try to fill empty fields not set in intermediary from BRDS as the p_brds_etl logic does
    BEGIN   
      SELECT --Fields in intermediary (set only if they were null)
                b.chargeReportingUnitCode, 
                b.chargeReportingParentCode,
                -- GBSVR-33754 Start: CFBU decommissioning
                -- GBSVR-33754 End:   CFBU decommissioning
                --Other fields
                b.legalEntityName,
                b.legalEntityCode,
                b.profitCentre,
                b.miLocation,
                p.portfolioId,
                p.portfolioName,
                p.rplCode
        INTO
                v_brds_cru_code,
                v_brds_crp_code,
                -- GBSVR-33754 Start: CFBU decommissioning
                -- GBSVR-33754 End:   CFBU decommissioning
                -- start GBSVR-31380
                v_legal_entity,
                v_legal_entity_code,
                --end GBSVR-31380
                v_expanded_data.profit_center_id,
                v_expanded_data.mi_location,
                v_expanded_data.portfolio_id,
                v_expanded_data.portfolio_name,
                v_expanded_data.portfolio_rpl_code
        FROM brds_vw_book         b, 
                brds_vw_portfolio    p
       WHERE b.portfolioId = p.portfolioId(+)
	     --start GBSVR-30695
           -- and NOT exists ( select w.* from bh_workflow w where w.global_trader_book_id = b.globalTraderBookId and workflow_type_id in ( 1, 2, 3, 4, 5, 7 ) )
            AND (   (p_intermediary_data.global_trader_book_id IS NOT NULL AND p_intermediary_data.global_trader_book_id = b.globaltraderbookid)
            OR  (   (p_intermediary_data.global_trader_book_id IS NULL AND p_intermediary_data.book_id = b.bookname)));   
   
      v_book_in_brds := TRUE;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
       v_book_in_brds := FALSE;
       pkg_monitoring.pr_insert_log_jobs_qv('bRDS', null, 'bRDS', current_date, 'PKG_BH_PROCESS_INTERMED.F_BH_INTERMEDIARY_EXPAND', 'WARN', 'LOGGING', 'Step 3: No data found in brds_vw_book/brds_vw_portfolio join', substr(SQLERRM,1,2500), 'bRDS');
    WHEN TOO_MANY_ROWS THEN
       v_book_in_brds := FALSE;
       --start GBSVR-31098
       v_brds_cru_code := null;
       v_brds_crp_code := null;
       -- GBSVR-33754 Start: CFBU decommissioning
       -- GBSVR-33754 End:   CFBU decommissioning
	   -- start GBSVR-31380
       v_legal_entity := null;
	   v_legal_entity_code := null;
       --end GBSVR-31380
       v_expanded_data.profit_center_id := null;
       v_expanded_data.mi_location := null;
       v_expanded_data.portfolio_id := null;
       v_expanded_data.portfolio_name := null; 
       v_expanded_data.portfolio_rpl_code := null;
          --end GBSVR-31098
       pkg_monitoring.pr_insert_log_jobs_qv('bRDS', null, 'bRDS', current_date, 'PKG_BH_PROCESS_INTERMED.F_BH_INTERMEDIARY_EXPAND', 'WARN', 'LOGGING', 'Step 3: Too many rows in brds_vw_book/brds_vw_portfolio join', substr(SQLERRM,1,2500), 'bRDS');
   --end GBSVR-30695
	END;
    
    --Step 4: Rewrite mi_location with description
    IF v_expanded_data.mi_location IS NOT NULL THEN
      BEGIN
      select upper(c.description)
        into v_expanded_data.mi_location
        from brds_country c where c.country_code = v_expanded_data.mi_location;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          pkg_monitoring.pr_insert_log_jobs_qv('bRDS', null, 'bRDS', current_date, 'PKG_BH_PROCESS_INTERMED.F_BH_INTERMEDIARY_EXPAND', 'WARN', 'LOGGING', 'Step 4: No data found in brds_country', substr(SQLERRM,1,2500), 'bRDS');
      END;
    END IF;
    
    --Step 5: Fill CRU/CRP/CFBU fields
    --If not coming from intermediary
    IF p_intermediary_data.charge_reporting_unit_code IS NULL THEN
      IF v_brds_cru_code IS NOT NULL THEN
           v_expanded_data.charge_reporting_unit_code:= v_brds_cru_code;
      END IF;
    END IF;
    
    --GBSVR-31380
    --If legal_entity recovered from UI is null, then recovered from BRDS 
    IF p_intermediary_data.legal_entity IS NULL THEN
      IF v_legal_entity IS NOT NULL THEN
           v_expanded_data.legal_entity:= v_legal_entity;
      END IF;
    END IF;
    
    --If legal_entity_code recovered from UI is null, then recovered from BRDS 
    IF p_intermediary_data.legal_entity_code IS NULL THEN
      IF v_legal_entity_code IS NOT NULL THEN
           v_expanded_data.legal_entity_code:= v_legal_entity_code;
      END IF;
    END IF;   
    -- end GBSVR-31380
    
    IF v_expanded_data.charge_reporting_unit_code IS NOT NULL THEN
        --Recover CRU text
        
            select count(*)
           into v_num_manual_cru
           from BH_MANUAL_HIERARCHY_ELEMS mhe
          where mhe.rplCode = v_expanded_data.charge_reporting_unit_code
            and mhe.element_type = 'CRU';
          
        IF  v_num_manual_cru != 0 THEN
            select mhe.name
               into v_expanded_data.charge_reporting_unit
               from BH_MANUAL_HIERARCHY_ELEMS mhe
              where mhe.rplCode = v_expanded_data.charge_reporting_unit_code
                and mhe.element_type = 'CRU';
            ELSE
            BEGIN
                select cru.chargeReportingUnit
               into v_expanded_data.charge_reporting_unit
               from brds_vw_cru cru
              where cru.chargeReportingUnitCode = v_expanded_data.charge_reporting_unit_code;
            EXCEPTION
            WHEN NO_DATA_FOUND THEN
                pkg_monitoring.pr_insert_log_jobs_qv('bRDS', null, 'bRDS', current_date, 'PKG_BH_PROCESS_INTERMED.F_BH_INTERMEDIARY_EXPAND', 'WARN', 'LOGGING', 'Step 5: No data found in brds_vw_cru neither in Manual for charge_reporting_unit_code', substr(SQLERRM,1,2500), 'bRDS');
            END;
        END IF;
    END IF;
        
    IF p_intermediary_data.charge_reporting_parent_code IS NULL THEN
      IF v_brds_crp_code IS NOT NULL THEN
           v_expanded_data.charge_reporting_parent_code:= v_brds_crp_code;
      END IF;
    END IF;
        
    IF v_expanded_data.charge_reporting_parent_code IS NOT NULL THEN
       --Recover CRP text     
    
            select count(*)
           into v_num_manual_crp
           from BH_MANUAL_HIERARCHY_ELEMS mhe
          where mhe.rplCode = v_expanded_data.charge_reporting_parent_code
            and mhe.element_type = 'CRP';
          
        IF  v_num_manual_crp !=0 THEN
            select mhe.name
               into v_expanded_data.charge_reporting_parent
               from BH_MANUAL_HIERARCHY_ELEMS mhe
              where mhe.rplCode = v_expanded_data.charge_reporting_parent_code
                and mhe.element_type = 'CRP';
            ELSE
            BEGIN
              select crp.chargeReportingParent
                into v_expanded_data.charge_reporting_parent
                from brds_vw_crp crp
               where crp.chargeReportingParentCode = v_expanded_data.charge_reporting_parent_code;
            EXCEPTION
            WHEN NO_DATA_FOUND THEN
                pkg_monitoring.pr_insert_log_jobs_qv('bRDS', null, 'bRDS', current_date, 'PKG_BH_PROCESS_INTERMED.F_BH_INTERMEDIARY_EXPAND', 'WARN', 'LOGGING', 'Step 5: No data found in brds_vw_crp neither in Manual for charge_reporting_parent_code', substr(SQLERRM,1,2500), 'bRDS');
            END;
          END IF;
    END IF; 
    
    -- GBSVR-33754 Start: CFBU decommissioning
    -- GBSVR-33754 End:   CFBU decommissioning
    
    
    --Step 6: Fill Region/Subregion if not coming from intermediary
    --If charge_reporting_parent recovered from brds is not null, calculate the region/subregion
    IF v_expanded_data.charge_reporting_parent_code IS NOT NULL THEN
  
      v_num_manual_crp:=0;
        
      select count(*)
        into v_num_manual_crp
        from BH_MANUAL_HIERARCHY_ELEMS mhe
       where mhe.rplCode = v_expanded_data.charge_reporting_parent_code
            and mhe.element_type = 'CRP';
        
      IF  v_num_manual_crp !=0 THEN
           
            select mhe.HIERARCHYREGION,
                mhe.SUBAREA
           into v_expanded_data.region,
                v_expanded_data.subregion
           from BH_MANUAL_HIERARCHY_ELEMS mhe
          where mhe.rplCode = v_expanded_data.charge_reporting_parent_code
            and mhe.element_type = 'CRP';
      ELSE
        BEGIN
          select crp.chargeHierarchyRegion,
                    crp.chargeSubArea
            into v_expanded_data.region,
                    v_expanded_data.subregion
            from brds_vw_crp crp
              where crp.chargeReportingParentCode = v_expanded_data.charge_reporting_parent_code;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
           pkg_monitoring.pr_insert_log_jobs_qv('bRDS', null, 'bRDS', current_date, 'PKG_BH_PROCESS_INTERMED.F_BH_INTERMEDIARY_EXPAND', 'WARN', 'LOGGING', 'Step 6: No data found in brds_vw_crp for charge_reporting_parent', substr(SQLERRM,1,2500), 'bRDS');
      END;
        END IF;
    END IF;
  
    -- Step 6.1 valid region 
    v_expanded_data.region := pkg_bh_commons.F_CONVERT_REGION_AMER(v_expanded_data.region);
    v_expanded_data.region := pkg_bh_commons.F_CONVERT_TO_NULL(v_expanded_data.region);
    v_expanded_data.subregion := pkg_bh_commons.F_CONVERT_TO_NULL(v_expanded_data.subregion);
    --Step 7: Set volcker trading desk full name
    BEGIN
      SELECT v.volckerTradingDeskFull
        INTO v_expanded_data.volcker_trading_desk_full
        FROM brds_vw_vtd v
       WHERE v.volckerTradingDesk = p_intermediary_data.volcker_trading_desk;
    EXCEPTION
       WHEN NO_DATA_FOUND THEN
         pkg_monitoring.pr_insert_log_jobs_qv('bRDS', null, 'bRDS', current_date, 'PKG_BH_PROCESS_INTERMED.F_BH_INTERMEDIARY_EXPAND', 'WARN', 'LOGGING', 'Step 7: No data found in brds_vw_vtd', substr(SQLERRM,1,2500), 'bRDS');
       END;
    
    --Step 8: Preload bh_ubr_desk_hierarchy_ui
       --mimic p_brds_etl_build_hierarchy for a single book
       p_load_single_hierarchy(p_intermediary_data.id, v_book_in_brds, v_expanded_data);
       select count(distinct global_trader_book_id) into v_num_gtb
            from bh_ubr_desk_hierarchy_ui 
           where bh_intermediary_id = v_expanded_data.bh_intermediary_id;
      --start GBSVR-30408
      IF v_expanded_data.global_trader_book_id IS NULL and v_num_gtb = 1 THEN
      --end GBSVR-30408
          select distinct global_trader_book_id
            into v_expanded_data.global_trader_book_id
            from bh_ubr_desk_hierarchy_ui 
           where bh_intermediary_id = v_expanded_data.bh_intermediary_id;
       END IF;
       --mimic p_brds_etl_set_hierarchy for a single book
       p_update_single_hierarchy(p_intermediary_data.id, v_expanded_data);
       --updates
    
    --Step 9: lowest_level_rpl_code and lowest_level_rpl_full_name
    BEGIN
      --Hierarchy built by gtbid or book_id (either gtbid or book_id existing in bRDS reported)
      IF p_intermediary_data.global_trader_book_id IS NOT NULL OR v_book_in_brds = TRUE THEN
        --start GBSVR-29336
        select h.rpl_code, h.node_name
        --end GBSVR-29336
          into v_expanded_data.lowest_level_rpl_code,
               v_expanded_data.lowest_level_rpl_full_name
          from bh_ubr_desk_hierarchy_ui h
            where h.global_trader_book_id = v_expanded_data.global_trader_book_id
           and h.node_type = 'PORTFOLIO'
           -- GBSVR-34994: Start: 
           and rownum = 1
           -- GBSVR-34994: End: 
           and h.bh_intermediary_id = p_intermediary_data.id;
      --Hierarchy built by VTD
      ELSE
        --start GBSVR-29336
        select h.rpl_code, h.node_name
        --end GBSVR-29336
          into v_expanded_data.lowest_level_rpl_code,
               v_expanded_data.lowest_level_rpl_full_name
          from bh_ubr_desk_hierarchy_ui h
            where h.global_trader_book_id = v_expanded_data.global_trader_book_id
           and h.node_type = 'UBR'
           and h.bh_intermediary_id = p_intermediary_data.id
           and rownum = 1
            order by ubr_desk_level;
      END IF;
    EXCEPTION
       WHEN NO_DATA_FOUND THEN
         pkg_monitoring.pr_insert_log_jobs_qv('bRDS', null, 'bRDS', current_date, 'PKG_BH_PROCESS_INTERMED.F_BH_INTERMEDIARY_EXPAND', 'WARN', 'LOGGING', 'Step 9: No data found in bh_ubr_desk_hierarchy_ui for lowest_level_rpl_code/lowest_level_rpl_full_name', substr(SQLERRM,1,2500),'bRDS');
       END;
    
    --Step 10: lowest_level_rpl
    BEGIN
      SELECT ((select NVL(max(ubr_desk_level), 0)
                    from bh_ubr_desk_hierarchy_ui h
                where h.global_trader_book_id = v_expanded_data.global_trader_book_id
                  and h.node_type = 'UBR' ) + 
              (select NVL(max(ubr_desk_level), 0)
                    from bh_ubr_desk_hierarchy_ui h
                where h.global_trader_book_id = v_expanded_data.global_trader_book_id
                  and h.node_type = 'DESK' ) + 
              (select count(*) 
                    from bh_ubr_desk_hierarchy_ui h
                where h.global_trader_book_id = v_expanded_data.global_trader_book_id
                  and h.node_type = 'PORTFOLIO' ))
        INTO v_expanded_data.lowest_level_rpl
        FROM DUAL;
    EXCEPTION
       WHEN OTHERS THEN
         pkg_monitoring.pr_insert_log_jobs_qv('bRDS', null, 'bRDS', current_date, 'PKG_BH_PROCESS_INTERMED.F_BH_INTERMEDIARY_EXPAND', 'WARN', 'LOGGING', 'Step 10: Error when setting the lowest_level_rpl', substr(SQLERRM,1,2500),'bRDS');
       END;   
    
    --Step 11: Business and Sub_business if not coming from intermediary
    IF p_intermediary_data.business IS NULL THEN
      BEGIN
            select max(h.ubr_desk_level)
           into max_ubr_level
           from bh_ubr_desk_hierarchy_ui h
          where h.node_type = 'UBR';
          
          -- 8 or more Ubr levels: Just use levels 8 and 7 as sub-business and business      
          IF max_ubr_level >= 8 THEN
                v_expanded_data.business     := v_expanded_data.ubr_level_7_name;
                v_expanded_data.sub_business := v_expanded_data.ubr_level_8_name;
          -- Fewer than 8 Ubr levels: Use level 14 Ubr name for sub-business and the next different one beneath that as business: 
          ELSE
            --start GBSVR-29536
            --UBR/Desk levels not expanded yet (done in step 12)
            select h.node_name
              into v_expanded_data.sub_business
              from bh_ubr_desk_hierarchy_ui h
             where h.bh_intermediary_id = p_intermediary_data.id
               and h.ubr_desk_level in (
                 select max(h1.ubr_desk_level)
                   from bh_ubr_desk_hierarchy_ui h1
                  where h1.bh_intermediary_id = p_intermediary_data.id
                   and h1.node_type = 'UBR');
               
            select h.node_name
              into v_expanded_data.business
              from bh_ubr_desk_hierarchy_ui h
             where h.bh_intermediary_id = p_intermediary_data.id
               and h.ubr_desk_level in (
                 select max(h1.ubr_desk_level)-1
                   from bh_ubr_desk_hierarchy_ui h1
                  where h1.bh_intermediary_id = p_intermediary_data.id
                    and h1.node_type = 'UBR');
            --end GBSVR-29536
          END IF;
       EXCEPTION
           WHEN NO_DATA_FOUND THEN
             pkg_monitoring.pr_insert_log_jobs_qv('bRDS', null, 'bRDS', current_date, 'PKG_BH_PROCESS_INTERMED.F_BH_INTERMEDIARY_EXPAND', 'WARN', 'LOGGING', 'Step 11: No data found in bh_ubr_desk_hierarchy_ui for max_ubr_level', substr(SQLERRM,1,2500), 'bRDS');
           END;
    ELSE
      --Convert rplcode provided by user into nodename
      BEGIN
        SELECT nodename INTO v_expanded_data.business
          FROM brds_vw_hierarchy
            WHERE rplcode = p_intermediary_data.business
           AND NODETYPE='UBR' 
           --There should only exist one UBR record. By caution, we retrieve the first one
           AND ROWNUM = 1;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
           pkg_monitoring.pr_insert_log_jobs_qv('bRDS', null, 'bRDS', current_date, 'PKG_BH_PROCESS_INTERMED.F_BH_INTERMEDIARY_EXPAND', 'WARN', 'LOGGING', 'Step 11: No data found in brds_vw_hierarchy for business', substr(SQLERRM,1,2500), 'bRDS');
      END;
      BEGIN
        SELECT nodename INTO v_expanded_data.sub_business
          FROM brds_vw_hierarchy
            WHERE rplcode = p_intermediary_data.sub_business
           AND NODETYPE='UBR' 
           --There should only exist one UBR record. By caution, we retrieve the first one
           AND ROWNUM = 1;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
           pkg_monitoring.pr_insert_log_jobs_qv('bRDS', null, 'bRDS', current_date, 'PKG_BH_PROCESS_INTERMED.F_BH_INTERMEDIARY_EXPAND', 'WARN', 'LOGGING', 'Step 11: No data found in brds_vw_hierarchy for sub_business', substr(SQLERRM,1,2500),'bRDS');
      END;
    END IF;
    --Step 12
    -- ************************************************************************ 

-- Any ubr/desk levels that are not set should have the value of the lowest level that has been set: 
   
    -- ************************************************************************ 
    -- Desks: Set lowest level: 
    IF v_expanded_data.desk_level_2_id IS NULL AND v_expanded_data.desk_level_1_id IS NOT NULL THEN
       v_expanded_data.desk_level_2_id := v_expanded_data.desk_level_1_id;
       v_expanded_data.desk_level_2_name := v_expanded_data.desk_level_1_name;
       v_expanded_data.desk_level_2_rpl_code := v_expanded_data.desk_level_1_rpl_code;
    END IF;
    IF v_expanded_data.desk_level_3_id IS NULL AND v_expanded_data.desk_level_2_id IS NOT NULL THEN
       v_expanded_data.desk_level_3_id := v_expanded_data.desk_level_2_id;
       v_expanded_data.desk_level_3_name := v_expanded_data.desk_level_2_name;
       v_expanded_data.desk_level_3_rpl_code := v_expanded_data.desk_level_2_rpl_code;
    END IF;
    IF v_expanded_data.desk_level_4_id IS NULL AND v_expanded_data.desk_level_3_id IS NOT NULL THEN
       v_expanded_data.desk_level_4_id := v_expanded_data.desk_level_3_id;
       v_expanded_data.desk_level_4_name := v_expanded_data.desk_level_3_name;
       v_expanded_data.desk_level_4_rpl_code := v_expanded_data.desk_level_3_rpl_code;
    END IF;
    IF v_expanded_data.desk_level_5_id IS NULL AND v_expanded_data.desk_level_4_id IS NOT NULL THEN
       v_expanded_data.desk_level_5_id := v_expanded_data.desk_level_4_id;
       v_expanded_data.desk_level_5_name := v_expanded_data.desk_level_4_name;
       v_expanded_data.desk_level_5_rpl_code := v_expanded_data.desk_level_4_rpl_code;
    END IF;
   
   -- UBRs: Set lowest level:
    IF v_expanded_data.ubr_level_2_id IS NULL AND v_expanded_data.ubr_level_1_id IS NOT NULL THEN
       v_expanded_data.ubr_level_2_id := v_expanded_data.ubr_level_1_id;
       v_expanded_data.ubr_level_2_name := v_expanded_data.ubr_level_1_name;
       v_expanded_data.ubr_level_2_rpl_code := v_expanded_data.ubr_level_1_rpl_code;
    END IF;
    IF v_expanded_data.ubr_level_3_id IS NULL AND v_expanded_data.ubr_level_2_id IS NOT NULL THEN
       v_expanded_data.ubr_level_3_id := v_expanded_data.ubr_level_2_id;
       v_expanded_data.ubr_level_3_name := v_expanded_data.ubr_level_2_name;
       v_expanded_data.ubr_level_3_rpl_code := v_expanded_data.ubr_level_2_rpl_code;
    END IF;
    IF v_expanded_data.ubr_level_4_id IS NULL AND v_expanded_data.ubr_level_3_id IS NOT NULL THEN
       v_expanded_data.ubr_level_4_id := v_expanded_data.ubr_level_3_id;
       v_expanded_data.ubr_level_4_name := v_expanded_data.ubr_level_3_name;
       v_expanded_data.ubr_level_4_rpl_code := v_expanded_data.ubr_level_3_rpl_code;
    END IF;
    IF v_expanded_data.ubr_level_5_id IS NULL AND v_expanded_data.ubr_level_4_id IS NOT NULL THEN
       v_expanded_data.ubr_level_5_id := v_expanded_data.ubr_level_4_id;
       v_expanded_data.ubr_level_5_name := v_expanded_data.ubr_level_4_name;
       v_expanded_data.ubr_level_5_rpl_code := v_expanded_data.ubr_level_4_rpl_code;
    END IF;
    IF v_expanded_data.ubr_level_6_id IS NULL AND v_expanded_data.ubr_level_5_id IS NOT NULL THEN
       v_expanded_data.ubr_level_6_id := v_expanded_data.ubr_level_5_id;
       v_expanded_data.ubr_level_6_name := v_expanded_data.ubr_level_5_name;
       v_expanded_data.ubr_level_6_rpl_code := v_expanded_data.ubr_level_5_rpl_code;
    END IF;
    IF v_expanded_data.ubr_level_7_id IS NULL AND v_expanded_data.ubr_level_6_id IS NOT NULL THEN
       v_expanded_data.ubr_level_7_id := v_expanded_data.ubr_level_6_id;
       v_expanded_data.ubr_level_7_name := v_expanded_data.ubr_level_6_name;
       v_expanded_data.ubr_level_7_rpl_code := v_expanded_data.ubr_level_6_rpl_code;
    END IF;
    IF v_expanded_data.ubr_level_8_id IS NULL AND v_expanded_data.ubr_level_7_id IS NOT NULL THEN
       v_expanded_data.ubr_level_8_id := v_expanded_data.ubr_level_7_id;
       v_expanded_data.ubr_level_8_name := v_expanded_data.ubr_level_7_name;
       v_expanded_data.ubr_level_8_rpl_code := v_expanded_data.ubr_level_7_rpl_code;
    END IF;
    IF v_expanded_data.ubr_level_9_id IS NULL AND v_expanded_data.ubr_level_8_id IS NOT NULL THEN
       v_expanded_data.ubr_level_9_id := v_expanded_data.ubr_level_8_id;
       v_expanded_data.ubr_level_9_name := v_expanded_data.ubr_level_8_name;
       v_expanded_data.ubr_level_9_rpl_code := v_expanded_data.ubr_level_8_rpl_code;
    END IF;
    IF v_expanded_data.ubr_level_10_id IS NULL AND v_expanded_data.ubr_level_9_id IS NOT NULL THEN
       v_expanded_data.ubr_level_10_id := v_expanded_data.ubr_level_9_id;
       v_expanded_data.ubr_level_10_name := v_expanded_data.ubr_level_9_name;
       v_expanded_data.ubr_level_10_rpl_code := v_expanded_data.ubr_level_9_rpl_code;
    END IF;
    IF v_expanded_data.ubr_level_11_id IS NULL AND v_expanded_data.ubr_level_10_id IS NOT NULL THEN
       v_expanded_data.ubr_level_11_id := v_expanded_data.ubr_level_10_id;
       v_expanded_data.ubr_level_11_name := v_expanded_data.ubr_level_10_name;
       v_expanded_data.ubr_level_11_rpl_code := v_expanded_data.ubr_level_10_rpl_code;
    END IF;
    IF v_expanded_data.ubr_level_12_id IS NULL AND v_expanded_data.ubr_level_11_id IS NOT NULL THEN
       v_expanded_data.ubr_level_12_id := v_expanded_data.ubr_level_11_id;
       v_expanded_data.ubr_level_12_name := v_expanded_data.ubr_level_11_name;
       v_expanded_data.ubr_level_12_rpl_code := v_expanded_data.ubr_level_11_rpl_code;
    END IF;
    IF v_expanded_data.ubr_level_13_id IS NULL AND v_expanded_data.ubr_level_12_id IS NOT NULL THEN
       v_expanded_data.ubr_level_13_id := v_expanded_data.ubr_level_12_id;
       v_expanded_data.ubr_level_13_name := v_expanded_data.ubr_level_12_name;
       v_expanded_data.ubr_level_13_rpl_code := v_expanded_data.ubr_level_12_rpl_code;
    END IF;
    IF v_expanded_data.ubr_level_14_id IS NULL AND v_expanded_data.ubr_level_13_id IS NOT NULL THEN
       v_expanded_data.ubr_level_14_id := v_expanded_data.ubr_level_13_id;
       v_expanded_data.ubr_level_14_name := v_expanded_data.ubr_level_13_name;
       v_expanded_data.ubr_level_14_rpl_code := v_expanded_data.ubr_level_13_rpl_code;
    END IF;
    
    
    --start GBSVR-32749    
    select  max(asofdate) into max_asodate_nonvtd
		from    bh_non_vtd
		where   asofdate <= ( last_day( current_date ) + 1 );
    --end GBSVR-32749   

    BEGIN
       select 	nonvtd.NON_VTD_CODE,
       			nonvtd.NON_VTD_RPL_NAME,
       			nonvtd.NON_VTD_EXCLUSION_TYPE,
       			nonvtd.NON_VTD_DIVISION ,
       			nonvtd.NON_VTD_PVF ,
       			nonvtd.NON_VTD_BUSINESS 
	   into 	
	   			v_expanded_data.NON_VTD_CODE,
	   			v_expanded_data.NON_VTD_NAME,
	   			v_expanded_data.NON_VTD_EXCLUSION_TYPE,
	  			v_expanded_data.NON_VTD_DIVISION,
	   			v_expanded_data.NON_VTD_PVF,
	   			v_expanded_data.NON_VTD_BUSINESS
	  from BH_NON_VTD nonvtd
	  where nonvtd.NON_VTD_RPL_CODE = p_intermediary_data.non_vtd_rpl_code and nonvtd.ASOFDATE =  max_asodate_nonvtd;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
	       pkg_monitoring.pr_insert_log_jobs_qv('bRDS', null, 'bRDS', current_date, 'PKG_BH_PROCESS_INTERMED.F_BH_INTERMEDIARY_EXPAND', 'WARN', 'LOGGING', 'NON VTD : NO data match on BH_NON_VTD', substr(SQLERRM,1,2500),'bRDS');
	        --start GBSVR-31012
        WHEN TOO_MANY_ROWS THEN
        --start GBSVR-31098
        v_expanded_data.NON_VTD_CODE := null;
	   	v_expanded_data.NON_VTD_NAME := null;
	   	v_expanded_data.NON_VTD_EXCLUSION_TYPE := null;
	  	v_expanded_data.NON_VTD_DIVISION := null;
	   	v_expanded_data.NON_VTD_PVF := null;
	   	v_expanded_data.NON_VTD_BUSINESS := null;
        --end GBSVR-31098
	       pkg_monitoring.pr_insert_log_jobs_qv('bRDS', null, 'bRDS', current_date, 'PKG_BH_PROCESS_INTERMED.F_BH_INTERMEDIARY_EXPAND', 'WARN', 'LOGGING', 'NON VTD : Too Many rows BH_NON_VTD', substr(SQLERRM,1,2500),'bRDS');
	 --end GBSVR-31012 
	END;
	
	--start GBSVR-30220 NON_VTD_BOOK_EXCEPTION	   
	IF (p_intermediary_data.GLOBAL_TRADER_BOOK_ID) IS NULL THEN
	  --BOOK_ID + GTB
	  BEGIN
	       select nonvtde.VALUE
	       	 into v_expanded_data.NON_VTD_EXCLUSION_TYPE
		     from BH_NON_VTD_EXCEPTIONS nonvtde
		    where nonvtde.BOOK_NAME = p_intermediary_data.book_id and nonvtde.EXCEPTION_TYPE like 'ET' ;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
	       pkg_monitoring.pr_insert_log_jobs_qv('bRDS', null, 'bRDS', current_date, 'PKG_BH_PROCESS_INTERMED.F_BH_INTERMEDIARY_EXPAND', 'WARN', 'LOGGING', 'NON VTD : NO data match on BH_NON_VTD_EXCEPTIONS', substr(SQLERRM,1,2500),'bRDS');	  
	  END;
	ELSE
	  --BOOK_ID + GTB
	  BEGIN
		  select nonvtde.VALUE
		 	into v_expanded_data.NON_VTD_EXCLUSION_TYPE
		    from BH_NON_VTD_EXCEPTIONS nonvtde
		   where nonvtde.BOOK_NAME = p_intermediary_data.book_id and nonvtde.global_trader_book_id = p_intermediary_data.global_trader_book_id and  nonvtde.EXCEPTION_TYPE like 'ET';
	  EXCEPTION
        WHEN NO_DATA_FOUND THEN
	       pkg_monitoring.pr_insert_log_jobs_qv('bRDS', null, 'bRDS', current_date, 'PKG_BH_PROCESS_INTERMED.F_BH_INTERMEDIARY_EXPAND', 'WARN', 'LOGGING', 'NON VTD : NO data match on BH_NON_VTD_EXCEPTIONS', substr(SQLERRM,1,2500), 'bRDS');	  
	  END;
	  --BOOK_ID + null
	  BEGIN
		  select nonvtde.VALUE
		    into v_expanded_data.NON_VTD_EXCLUSION_TYPE
		    from BH_NON_VTD_EXCEPTIONS nonvtde
		   where nonvtde.BOOK_NAME = p_intermediary_data.book_id and nonvtde.global_trader_book_id is null and  nonvtde.EXCEPTION_TYPE like 'ET' ;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
	       pkg_monitoring.pr_insert_log_jobs_qv('bRDS', null, 'bRDS', current_date, 'PKG_BH_PROCESS_INTERMED.F_BH_INTERMEDIARY_EXPAND', 'WARN', 'LOGGING',  'NON VTD : NO data match on BH_NON_VTD_EXCEPTIONS', substr(SQLERRM,1,2500),'bRDS');	  
 		  END;
	END IF;    
	--end GBSVR-30220 NON_VTD_BOOK_EXCEPTION	  
	  
    v_expanded_data.REGULATORY_REPORTING_TREATMENT := p_intermediary_data.REGULATORY_REPORTING_TREATMENT;
    --end GBSVR-28882 NONVTD
    
    --start GBSVR-30040 VOLCKER TRADING FLAG
    v_expanded_data.VOLCKER_REPORTABLE_FLAG := 'N';
    IF p_intermediary_data.VOLCKER_TRADING_DESK IS NOT NULL THEN
       IF UPPER(p_intermediary_data.VOLCKER_TRADING_DESK) NOT IN ('00_EXCLUDE','00_NO_MATCH') THEN
        v_expanded_data.VOLCKER_REPORTABLE_FLAG := 'Y';
        END IF;
    END IF;
     --end GBSVR-30040 VOLCKER TRADING FLAG
    
    --Step 13: Clear temporary data 
    --and delete global_trader_book_id from v_expanded_data 
    --when Hierarchy built by VTD and it doesn't exist in bh_intermediary
    p_unload_single_hierarchy(p_intermediary_data.id);
    
    IF p_intermediary_data.global_trader_book_id IS NULL THEN
       v_expanded_data.global_trader_book_id := '';
    END IF;
          
    RETURN v_expanded_data;
    
--start GBSVR-30408
EXCEPTION
  WHEN OTHERS THEN
     ROLLBACK;
     pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'F_BH_INTERMEDIARY_EXPAND','ERROR', 'FATAL', 
     	'Error: '||TO_CHAR(SQLCODE)||
     	'. Book_id: '||p_intermediary_data.book_id||
     	', Source_system: '||p_intermediary_data.source_system_id||
     	', volcker_trading_desk: '||p_intermediary_data.volcker_trading_desk||
     	'. v_asofdate: '||v_asofdate||
     	'. p_is_full_match_staging: '||(CASE WHEN p_is_full_match_staging THEN 'TRUE' ELSE 'FALSE' END)||'.', SUBSTR(SQLERRM, 1, 2500), 'bRDS');
     RAISE;
--end GBSVR-30408
END F_BH_INTERMEDIARY_EXPAND;

PROCEDURE p_load_single_hierarchy(p_bh_intermediary_id IN NUMBER, p_book_in_brds IN BOOLEAN, p_bh_staging_row IN BH_STAGING%ROWTYPE)
IS
   v_level_correction NUMBER := 3;
   v_search_wf33_gtb NUMBER;
   v_search_wf33_book NUMBER;
   --start GBSVR-30408
   v_search_wf4_book NUMBER;
   --end GBSVR-30408
   --start GBSVR-30255
   v_num_dup_hierarchy_vtd NUMBER;
   --end GBSVR-30255
BEGIN  
   SELECT COUNT(*) INTO v_search_wf33_gtb
        FROM bh_workflow bw
    WHERE bw.workflow_type_id = 33
      AND bw.global_trader_book_id = p_bh_staging_row.global_trader_book_id;
      
   SELECT COUNT(*) INTO v_search_wf33_book
        FROM bh_workflow bw
    WHERE bw.workflow_type_id = 33
      AND bw.book_id = p_bh_staging_row.book_id;
   --start GBSVR-30408
   SELECT COUNT(*) INTO v_search_wf4_book
        FROM bh_workflow bw
    WHERE bw.workflow_type_id = 4
      AND bw.book_id = p_bh_staging_row.book_id;
   --end GBSVR-30408
   --By caution, delete previous unfinished executions with this same id
   DELETE FROM bh_ubr_desk_hierarchy_ui WHERE bh_intermediary_id = p_bh_staging_row.bh_intermediary_id;
   
   IF p_bh_staging_row.global_trader_book_id IS NOT NULL AND v_search_wf33_gtb = 0 THEN
        insert into bh_ubr_desk_hierarchy_ui
        select h.nodeType, 
            connect_by_root h.nodeId book, 
            h.nodeId, 
            h.nodeName, 
            h.rplCode, 
            level   ubr_desk_level, 
            0 num_desks, 
            0 num_ubrs,
            p_bh_staging_row.bh_intermediary_id
       from brds_vw_hierarchy h
       start with (h.nodeId = p_bh_staging_row.global_trader_book_id
            and h.nodetype = 'BOOK')
       connect by prior parentNodeId = nodeId;
   --start GBSVR-30408
   ELSIF p_book_in_brds = TRUE AND v_search_wf33_book = 0 AND v_search_wf4_book = 0 THEN
   --end GBSVR-30408
        insert into bh_ubr_desk_hierarchy_ui
        select h.nodeType, 
            connect_by_root h.nodeId book, 
            h.nodeId, 
            h.nodeName, 
            h.rplCode, 
            level   ubr_desk_level, 
            0 num_desks, 
            0 num_ubrs,
            p_bh_staging_row.bh_intermediary_id
       from brds_vw_hierarchy h
       start with (h.nodeName = p_bh_staging_row.book_id
            and h.nodetype = 'BOOK')
       connect by prior parentNodeId = nodeId;
   ELSE
        insert into bh_ubr_desk_hierarchy_ui
        select h.nodeType, 
            connect_by_root h.nodeId book, 
            h.nodeId, 
            h.nodeName, 
            h.rplCode, 
            level   ubr_desk_level, 
            0 num_desks, 
            0 num_ubrs,
            p_bh_staging_row.bh_intermediary_id
       from brds_vw_hierarchy h
       start with h.rplCode = p_bh_staging_row.volcker_trading_desk
       connect by prior parentNodeId = nodeId;
       
        --start GBSVR-30255
        --If there is a duplicated node, we won't use any hierarchy
        with aux as (
         select h.nodeType, 
	           connect_by_root h.nodeId book, 
	           h.nodeId, 
	           h.nodeName, 
	           h.rplCode, 
	           level   ubr_desk_level
	      from brds_vw_hierarchy h
	      start with h.rplCode = p_bh_staging_row.volcker_trading_desk
	      connect by prior parentNodeId = nodeId)
		select count(*) into v_num_dup_hierarchy_vtd from (select ubr_desk_level aux from aux
		 group by ubr_desk_level
		having count(*) > 1);
		
		if v_num_dup_hierarchy_vtd > 0 then
			delete from bh_ubr_desk_hierarchy_ui
			 where bh_intermediary_id = p_bh_staging_row.bh_intermediary_id;
		end if;
        --end GBSVR-30255       
        v_level_correction := 1;
   END IF;
   
   -- ************************************************************************ 
   -- Set Number of desk levels
   update  bh_ubr_desk_hierarchy_ui h1
      set  num_desks = ( select  count(*) 
                            from    bh_ubr_desk_hierarchy_ui h2 
                            where   h2.global_trader_book_id = h1.global_trader_book_id 
                            and     h2.node_type = 'DESK'
                            and     bh_intermediary_id = p_bh_intermediary_id)
    where bh_intermediary_id = p_bh_intermediary_id;
   
   -- ************************************************************************ 
   -- Set Number of ubr levels
   update  bh_ubr_desk_hierarchy_ui h1
      set  num_ubrs = ( select  count(*) 
                            from    bh_ubr_desk_hierarchy_ui h2 
                            where   h2.global_trader_book_id = h1.global_trader_book_id 
                            --start GBSVR-30032
                            --and     h2.node_name != 'Group (aggregated)'
                            --end GBSVR-30032
                            and     h2.node_type = 'UBR'
                            and     bh_intermediary_id = p_bh_intermediary_id)
    where  bh_intermediary_id = p_bh_intermediary_id;
   
   -- ************************************************************************ 
   -- Set adjusted ubr and desk levels: 
   update  bh_ubr_desk_hierarchy_ui
   set     ubr_desk_level = ( v_level_correction + num_ubrs + num_desks - ubr_desk_level )
   where   node_type = 'UBR'
   --start GBSVR-30032
   --and     node_name != 'Group (aggregated)'
   --end GBSVR-30032
    and     bh_intermediary_id = p_bh_intermediary_id;
  
   --start GBSVR-30032
   /*update  bh_ubr_desk_hierarchy_ui
   set     ubr_desk_level = 0
   where   node_type = 'UBR'
   and     node_name = 'Group (aggregated)'
   and     bh_intermediary_id = p_bh_intermediary_id;*/
   --end GBSVR-30032
   
   update  bh_ubr_desk_hierarchy_ui
   set     ubr_desk_level = ( v_level_correction + num_desks - ubr_desk_level )
   where   node_type = 'DESK'
   and     bh_intermediary_id = p_bh_intermediary_id;  
   
   update  bh_ubr_desk_hierarchy_ui 
   set     ubr_desk_level = NULL
   where   node_type NOT in ( 'UBR', 'DESK' )
   and     bh_intermediary_id = p_bh_intermediary_id;
END p_load_single_hierarchy;

PROCEDURE p_update_single_hierarchy(p_bh_intermediary_id IN NUMBER, p_bh_staging_row IN OUT BH_STAGING%ROWTYPE)
IS
   v_num_ctr int;
   v_str_sql varchar2(1000);
BEGIN

   -- ************************************************************************ 
   -- Set ubr level data
   --start GBSVR-30032
   p_set_level_fields(p_bh_intermediary_id, 1,  p_bh_staging_row.global_trader_book_id, 'UBR', p_bh_staging_row.ubr_level_1_id, p_bh_staging_row.ubr_level_1_name,   p_bh_staging_row.ubr_level_1_rpl_code);
   p_set_level_fields(p_bh_intermediary_id, 2,  p_bh_staging_row.global_trader_book_id, 'UBR', p_bh_staging_row.ubr_level_2_id, p_bh_staging_row.ubr_level_2_name,   p_bh_staging_row.ubr_level_2_rpl_code);
   p_set_level_fields(p_bh_intermediary_id, 3,  p_bh_staging_row.global_trader_book_id, 'UBR', p_bh_staging_row.ubr_level_3_id, p_bh_staging_row.ubr_level_3_name,   p_bh_staging_row.ubr_level_3_rpl_code);
   p_set_level_fields(p_bh_intermediary_id, 4,  p_bh_staging_row.global_trader_book_id, 'UBR', p_bh_staging_row.ubr_level_4_id, p_bh_staging_row.ubr_level_4_name,   p_bh_staging_row.ubr_level_4_rpl_code);
   p_set_level_fields(p_bh_intermediary_id, 5,  p_bh_staging_row.global_trader_book_id, 'UBR', p_bh_staging_row.ubr_level_5_id, p_bh_staging_row.ubr_level_5_name,   p_bh_staging_row.ubr_level_5_rpl_code);
   p_set_level_fields(p_bh_intermediary_id, 6,  p_bh_staging_row.global_trader_book_id, 'UBR', p_bh_staging_row.ubr_level_6_id, p_bh_staging_row.ubr_level_6_name,   p_bh_staging_row.ubr_level_6_rpl_code);
   p_set_level_fields(p_bh_intermediary_id, 7,  p_bh_staging_row.global_trader_book_id, 'UBR', p_bh_staging_row.ubr_level_7_id, p_bh_staging_row.ubr_level_7_name,   p_bh_staging_row.ubr_level_7_rpl_code);
   p_set_level_fields(p_bh_intermediary_id, 8,  p_bh_staging_row.global_trader_book_id, 'UBR', p_bh_staging_row.ubr_level_8_id, p_bh_staging_row.ubr_level_8_name,   p_bh_staging_row.ubr_level_8_rpl_code);
   p_set_level_fields(p_bh_intermediary_id, 9,  p_bh_staging_row.global_trader_book_id, 'UBR', p_bh_staging_row.ubr_level_9_id, p_bh_staging_row.ubr_level_9_name,   p_bh_staging_row.ubr_level_9_rpl_code);
   p_set_level_fields(p_bh_intermediary_id, 10, p_bh_staging_row.global_trader_book_id, 'UBR', p_bh_staging_row.ubr_level_10_id, p_bh_staging_row.ubr_level_10_name, p_bh_staging_row.ubr_level_10_rpl_code);
   p_set_level_fields(p_bh_intermediary_id, 11, p_bh_staging_row.global_trader_book_id, 'UBR', p_bh_staging_row.ubr_level_11_id, p_bh_staging_row.ubr_level_11_name, p_bh_staging_row.ubr_level_11_rpl_code);
   p_set_level_fields(p_bh_intermediary_id, 12, p_bh_staging_row.global_trader_book_id, 'UBR', p_bh_staging_row.ubr_level_12_id, p_bh_staging_row.ubr_level_12_name, p_bh_staging_row.ubr_level_12_rpl_code);
   p_set_level_fields(p_bh_intermediary_id, 13, p_bh_staging_row.global_trader_book_id, 'UBR', p_bh_staging_row.ubr_level_13_id, p_bh_staging_row.ubr_level_13_name, p_bh_staging_row.ubr_level_13_rpl_code);
   p_set_level_fields(p_bh_intermediary_id, 14, p_bh_staging_row.global_trader_book_id, 'UBR', p_bh_staging_row.ubr_level_14_id, p_bh_staging_row.ubr_level_14_name, p_bh_staging_row.ubr_level_14_rpl_code);
        
   -- ************************************************************************ 
   -- Set desk level data
   
   p_set_level_fields(p_bh_intermediary_id, 1, p_bh_staging_row.global_trader_book_id, 'DESK', p_bh_staging_row.desk_level_1_id, p_bh_staging_row.desk_level_1_name, p_bh_staging_row.desk_level_1_rpl_code);
   p_set_level_fields(p_bh_intermediary_id, 2, p_bh_staging_row.global_trader_book_id, 'DESK', p_bh_staging_row.desk_level_2_id, p_bh_staging_row.desk_level_2_name, p_bh_staging_row.desk_level_2_rpl_code);
   p_set_level_fields(p_bh_intermediary_id, 3, p_bh_staging_row.global_trader_book_id, 'DESK', p_bh_staging_row.desk_level_3_id, p_bh_staging_row.desk_level_3_name, p_bh_staging_row.desk_level_3_rpl_code);
   p_set_level_fields(p_bh_intermediary_id, 4, p_bh_staging_row.global_trader_book_id, 'DESK', p_bh_staging_row.desk_level_4_id, p_bh_staging_row.desk_level_4_name, p_bh_staging_row.desk_level_4_rpl_code);
   p_set_level_fields(p_bh_intermediary_id, 5, p_bh_staging_row.global_trader_book_id, 'DESK', p_bh_staging_row.desk_level_5_id, p_bh_staging_row.desk_level_5_name, p_bh_staging_row.desk_level_5_rpl_code);
   --end GBSVR-30032
END p_update_single_hierarchy;

--start GBSVR-30032
PROCEDURE P_SET_LEVEL_FIELDS(P_BH_INTERMEDIARY_ID IN NUMBER, P_LEVEL IN NUMBER, P_GLOBAL_TRADER_BOOK_ID IN NUMBER, P_NODE_TYPE IN VARCHAR2, P_ID OUT VARCHAR2, P_NAME OUT VARCHAR2, P_RPL_CODE OUT VARCHAR2)
IS
BEGIN
   SELECT t.node_id, 
          t.node_name, 
          t.rpl_code
        INTO P_ID,
          P_NAME,
          P_RPL_CODE
        FROM bh_ubr_desk_hierarchy_ui t
    WHERE t.global_trader_book_id = p_global_trader_book_id
      AND t.bh_intermediary_id = p_bh_intermediary_id
      AND t.node_type = p_node_type
      AND t.ubr_desk_level = p_level;
    EXCEPTION
   WHEN NO_DATA_FOUND THEN
        pkg_monitoring.pr_insert_log_jobs_qv('bRDS', null, 'bRDS', current_date, 'PKG_BH_PROCESS_INTERMED.P_SET_LEVEL_FIELDS', 'WARN', 'LOGGING', 'Step 8: Level/Nodetype '||p_level||'/'||p_node_type||' - No data found in bh_ubr_desk_hierarchy_ui', substr(SQLERRM,1,2500), 'bRDS');
   WHEN TOO_MANY_ROWS THEN
   		--start GBSVR-31098
   		  P_ID := null;
          P_NAME := null;
          P_RPL_CODE := null;
   		--end GBSVR-31098
        pkg_monitoring.pr_insert_log_jobs_qv('bRDS', null, 'bRDS', current_date, 'PKG_BH_PROCESS_INTERMED.P_SET_LEVEL_FIELDS', 'WARN', 'LOGGING', 'Step 8: Level/Nodetype '||p_level||'/'||p_node_type||' - More than one hierarchy was found. No one was chosen', substr(SQLERRM,1,2500), 'bRDS');
END P_SET_LEVEL_FIELDS;
--end GBSVR-30032

PROCEDURE p_unload_single_hierarchy(p_bh_intermediary_id IN NUMBER)
IS
BEGIN
   DELETE FROM bh_ubr_desk_hierarchy_ui
    WHERE bh_intermediary_id = p_bh_intermediary_id;
END p_unload_single_hierarchy;


END PKG_BH_PROCESS_INTERMED;

