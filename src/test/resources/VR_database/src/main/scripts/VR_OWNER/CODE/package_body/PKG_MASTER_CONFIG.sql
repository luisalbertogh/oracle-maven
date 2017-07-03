--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:PACKAGE_BODY_PKG_MASTER_CONFIG runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE stripComments:FALSE


CREATE OR REPLACE PACKAGE BODY "PKG_MASTER_CONFIG" 
AS
  FUNCTION F_GET_PARAM_VALUE(
      A_PARAM_KEY IN MASTER_PARAM.PARAM_KEY%TYPE,
      A_PARAM_VALUE OUT MASTER_PARAM.PARAM_VALUE%TYPE,
      A_METRIC_ID IN VARCHAR2 DEFAULT 'IA')
    RETURN NUMBER
  IS
  BEGIN
    SELECT PARAM_VALUE
    INTO A_PARAM_VALUE
    FROM MASTER_PARAM MP
    WHERE MP.PARAM_KEY=A_PARAM_KEY
    AND MP.METRIC_ID  =A_METRIC_ID;
    RETURN 0;
  EXCEPTION
  WHEN OTHERS THEN
    RETURN 1;
  END F_GET_PARAM_VALUE;
  FUNCTION F_GET_OOZIE_CONSTANTS(
      p_source_system_id SOURCE_SYSTEM.SOURCE_SYSTEM_ID%TYPE,
      A_METRIC_ID IN VARCHAR2 DEFAULT 'IA')
    RETURN TYPE_RESULSET_OOZIE
  IS
    R_OOZIE TYPE_RESULSET_OOZIE;
    updateResult NUMBER;
    v_query VARCHAR2 (32767);
  BEGIN
    v_query := 'SELECT ';
    FOR r IN (SELECT CASE WHEN METRIC_ID != 'CFTR' AND PARAM_KEY = 'C_PIG_SCRIPT_NAME' THEN
                          '''' || PARAM_VALUE || ''' AS PIG_SCRIPT_NAME,' || CHR (10) || '       '
                     END ||
                     '''' || PARAM_VALUE || ''' AS "' || PARAM_KEY || '",' || CHR (10) || '       ' AS TEXT
              FROM OOZIE_PARAM
              WHERE METRIC_ID = A_METRIC_ID
              UNION ALL
              SELECT '''' || PARAM_VALUE || ''' AS "' || PARAM_KEY ||'",' || CHR (10) || '       ' AS TEXT
              FROM MASTER_PARAM
              WHERE METRIC_ID = A_METRIC_ID
                AND PARAM_GROUP = 'ALGORITHM_PERFORMANCE') LOOP
        v_query := v_query || r.TEXT;
    END LOOP;
    v_query := v_query || 'NVL (VW_SOURCE_SYSTEM.BH_BUSINESS, ''N/A'') AS PIG_BUSINESS,' || CHR (10);
    v_query := v_query || '       NVL (VW_SOURCE_SYSTEM.BH_TRADING_UNIT, ''N/A'') AS PIG_TRADING_UNIT,' || CHR (10);
    v_query := v_query || '       NVL (VW_SOURCE_SYSTEM.BH_SUB_BUSINESS, ''N/A'') AS PIG_SUB_BUSINESS,' || CHR (10);
    v_query := v_query || '       JOBS.JOB_ID AS "Job_ID",' || CHR (10);
    v_query := v_query || '       VW_SOURCE_SYSTEM.SOURCE_SYSTEM_ID AS "Source_System_ID",' || CHR (10);
    v_query := v_query || '       TO_CHAR (VW_SOURCE_SYSTEM.LAST_LOAD, ''YYYYMMDD'') AS "lastLoad",' || CHR (10);
    v_query := v_query || '       TO_CHAR (VW_SOURCE_SYSTEM.NEXT_LOAD, ''YYYYMMDD'') AS "nextLoad",' || CHR (10);
    v_query := v_query || '       TO_CHAR (VW_SOURCE_SYSTEM.AFTER_NEXT_LOAD, ''YYYYMMDD'') AS "afterNextLoad",' || CHR (10);
    v_query := v_query || '       VW_SOURCE_SYSTEM.SYSTEM_FOLDER AS "system",' || CHR (10);
    v_query := v_query || '       NVL (VW_SOURCE_SYSTEM.SYSTEM_FOLDER_AUX, ''N/A'') AS "systemEus",' || CHR (10);
    v_query := v_query || '       VW_SOURCE_SYSTEM.REGION_ID AS "region",' || CHR (10);
    v_query := v_query || '       VW_SOURCE_SYSTEM.REGION_FOLDER AS "region_folder",' || CHR (10);
    v_query := v_query || '       VW_SOURCE_SYSTEM.NUM_REDUCE_TASK AS "numReduceTasks",' || CHR (10);
    v_query := v_query || '       VW_SOURCE_SYSTEM.MAPRED_CHILD_JAVA_OPTS AS "mapred_child_java_opts",' || CHR (10);
    v_query := v_query || '       VW_SOURCE_SYSTEM.IO_SORT_MB AS "io_sort_mb",' || CHR (10);
    v_query := v_query || '       MASTER_PARAM.PARAM_VALUE AS "threshold",' || CHR (10);
    v_query := v_query || '       VW_SOURCE_SYSTEM.CFTRFILE AS "cftrFile"';
    IF A_METRIC_ID = 'ITR' THEN
        v_query := v_query || ',' || CHR (10) ||
            '       CASE (1 + TRUNC (VW_SOURCE_SYSTEM.NEXT_LOAD - 90) - TRUNC (VW_SOURCE_SYSTEM.NEXT_LOAD - 90, ''IW''))' || CHR (10) ||
            '            WHEN 6 THEN TO_CHAR (VW_SOURCE_SYSTEM.NEXT_LOAD - 91, ''YYYYMMDD'')' || CHR (10) ||
            '            WHEN 7 THEN TO_CHAR (VW_SOURCE_SYSTEM.NEXT_LOAD - 92, ''YYYYMMDD'')' || CHR (10) ||
            '            ELSE TO_CHAR (VW_SOURCE_SYSTEM.NEXT_LOAD - 90, ''YYYYMMDD'')' || CHR (10) ||
            '       END AS "itrPosDate90",' || CHR (10) ||
            '       CASE (1 + TRUNC (VW_SOURCE_SYSTEM.NEXT_LOAD - 60) - TRUNC (VW_SOURCE_SYSTEM.NEXT_LOAD - 60, ''IW''))' || CHR (10) ||
            '            WHEN 6 THEN TO_CHAR (VW_SOURCE_SYSTEM.NEXT_LOAD - 61, ''YYYYMMDD'')' || CHR (10) ||
            '            WHEN 7 THEN TO_CHAR (VW_SOURCE_SYSTEM.NEXT_LOAD - 62, ''YYYYMMDD'')' || CHR (10) ||
            '            ELSE TO_CHAR (VW_SOURCE_SYSTEM.NEXT_LOAD - 60, ''YYYYMMDD'')' || CHR (10) ||
            '       END AS "itrPosDate60",' || CHR (10) ||
            '       CASE (1 + TRUNC (VW_SOURCE_SYSTEM.NEXT_LOAD - 30) - TRUNC (VW_SOURCE_SYSTEM.NEXT_LOAD - 30, ''IW''))' || CHR (10) ||
            '            WHEN 6 THEN TO_CHAR (VW_SOURCE_SYSTEM.NEXT_LOAD - 31, ''YYYYMMDD'')' || CHR (10) ||
            '            WHEN 7 THEN TO_CHAR (VW_SOURCE_SYSTEM.NEXT_LOAD - 32, ''YYYYMMDD'')' || CHR (10) ||
            '            ELSE TO_CHAR (VW_SOURCE_SYSTEM.NEXT_LOAD - 30, ''YYYYMMDD'')' || CHR (10) ||
            '       END AS "itrPosDate30"' || CHR (10);
    END IF;
    IF A_METRIC_ID = 'CFTR' THEN
        v_query := v_query || ',' || CHR (10) || '       VW_SOURCE_SYSTEM.SOURCE_SYSTEM_CRDS_NAME AS "source_system_crds_name"';
    END IF;
    v_query := v_query || CHR (10) || 'FROM MASTER_PARAM' || CHR (10);
    v_query := v_query || '     JOIN VW_SOURCE_SYSTEM' || CHR (10);
    v_query := v_query || '       ON VW_SOURCE_SYSTEM.METRIC_ID = MASTER_PARAM.METRIC_ID' || CHR (10);
    v_query := v_query || '      AND VW_SOURCE_SYSTEM.SOURCE_SYSTEM_ID = ''' || p_source_system_id || '''' || CHR (10);
    v_query := v_query || '     JOIN JOBS' || CHR (10);
    v_query := v_query || '       ON JOBS.SOURCE_SYSTEM_ID = VW_SOURCE_SYSTEM.SOURCE_SYSTEM_ID' || CHR (10);
    v_query := v_query || 'WHERE MASTER_PARAM.METRIC_ID = ''' || A_METRIC_ID || '''' || CHR (10);
    v_query := v_query || '  AND MASTER_PARAM.PARAM_KEY = ''THRESHOLD_FAIL''';
    dbms_output.put_line (v_query);
    OPEN r_oozie FOR v_query;
    
    if ((A_METRIC_ID = 'CFTR') OR (A_METRIC_ID = 'IA') OR (A_METRIC_ID = 'ITR') OR (A_METRIC_ID = 'COVERED_FUNDS')) then
      updateResult := PKG_AUTO_EXEC.F_FREE_BOOTSTRAP_FLAG_METRIC(p_source_system_id, A_METRIC_ID);
    else
      updateResult := PKG_AUTO_EXEC.F_FREE_BOOTSTRAP_FLAG_COMMON(p_source_system_id);
    END IF;
    
    RETURN r_oozie;
  END F_GET_OOZIE_CONSTANTS;
END PKG_MASTER_CONFIG;