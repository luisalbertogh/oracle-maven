--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:PACKAGE_BODY_PKG_REFERENCE_DATA runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE stripComments:FALSE


  CREATE OR REPLACE PACKAGE BODY "PKG_REFERENCE_DATA" 
AS
  FUNCTION F_GET_COB_DATE(
      A_REGION_ID IN REGIONS.REGION_ID%TYPE,
      A_COB_DATE OUT REGIONS.COB_DATE%TYPE)
    RETURN NUMBER
  IS
  BEGIN
    SELECT MAX(COB_DATE)
    INTO A_COB_DATE
    FROM REGIONS RG
    WHERE RG.REGION_ID=A_REGION_ID;
    RETURN 0;
  EXCEPTION
  WHEN OTHERS THEN
    RETURN 1;
  END F_GET_COB_DATE;
  FUNCTION F_GET_PREV_DATE(
      A_REGION_ID IN REGIONS.REGION_ID%TYPE,
      A_PREV_DATE OUT REGIONS.PREV_DATE%TYPE)
    RETURN NUMBER
  IS
  BEGIN
    SELECT MAX(PREV_DATE)
    INTO A_PREV_DATE
    FROM REGIONS RG
    WHERE RG.REGION_ID=A_REGION_ID;
    RETURN 0;
  EXCEPTION
  WHEN OTHERS THEN
    RETURN 1;
  END F_GET_PREV_DATE;
  FUNCTION F_GET_PREPREV_DATE(
      A_REGION_ID IN REGIONS.REGION_ID%TYPE,
      A_PREPREV_DATE OUT REGIONS.PREPREV_DATE%TYPE)
    RETURN NUMBER
  IS
  BEGIN
    SELECT MAX(PREPREV_DATE)
    INTO A_PREPREV_DATE
    FROM REGIONS RG
    WHERE RG.REGION_ID=A_REGION_ID;
    RETURN 0;
  EXCEPTION
  WHEN OTHERS THEN
    RETURN 1;
  END F_GET_PREPREV_DATE;
  FUNCTION F_GET_FX_RATES(
      A_region_id REGIONS.REGION_ID%TYPE,
      A_RATES_RESULTSET OUT TYPE_RESULTSET)
    RETURN NUMBER
  IS
    V_PREV_DATE REGIONS.PREV_DATE%TYPE;
    V_RESULT NUMBER;
  BEGIN
    v_result := pkg_reference_data.f_get_prev_date(a_region_id,v_prev_date);
    OPEN A_RATES_RESULTSET FOR SELECT FR.ASOFDATE,
    FR.REGION_ID,
    FR.CURRENCY_ID,
    FR.SPOT FROM FX_RATES FR WHERE ASOFDATE = V_PREV_DATE AND REGION_ID= A_REGION_ID;
    RETURN 0;
  EXCEPTION
  WHEN OTHERS THEN
    RETURN 1;
    RAISE;
  END F_GET_FX_RATES;
  FUNCTION F_GET_LAST_FX_RATES(
      A_region_id REGIONS.REGION_ID%TYPE,
      A_cobdate FX_RATES.ASOFDATE%TYPE,
      A_RATES_RESULTSET OUT TYPE_RESULTSET)
    RETURN NUMBER
  IS
    V_PREV_DATE REGIONS.PREV_DATE%TYPE;
    V_RESULT NUMBER;
  BEGIN
    SELECT MAX(asofdate)
    INTO V_PREV_DATE
    FROM fx_rates
    WHERE asofdate<=A_cobdate
    AND region_id  =A_REGION_ID;
    OPEN A_RATES_RESULTSET FOR SELECT FR.ASOFDATE,
    FR.CURRENCY_ID,
    FR.SPOT,
    FR.FX_DATE FROM FX_RATES FR WHERE ASOFDATE = V_PREV_DATE AND REGION_ID= A_REGION_ID;
    RETURN 0;
  EXCEPTION
  WHEN OTHERS THEN
    RETURN 1;
    RAISE;
  END F_GET_LAST_FX_RATES;
  FUNCTION F_GET_ASOFDATE_FX_RATES(
      A_source_system_id SOURCE_SYSTEM.SOURCE_SYSTEM_ID%TYPE,
      A_cobdate FX_RATES.ASOFDATE%TYPE,
      A_RATES_RESULTSET OUT TYPE_RESULTSET)
    RETURN NUMBER
  IS
  BEGIN
    OPEN A_RATES_RESULTSET FOR SELECT FR.REGION_ID,
    FR.CURRENCY_ID,
    FR.SPOT FROM FX_RATES FR WHERE ASOFDATE = A_cobdate AND REGION_ID=
    (SELECT REGION_ID
    FROM SOURCE_SYSTEM
    WHERE SOURCE_SYSTEM_ID=A_source_system_id
    );
    RETURN 0;
  EXCEPTION
  WHEN OTHERS THEN
    RETURN 1;
    RAISE;
  END F_GET_ASOFDATE_FX_RATES;
  FUNCTION F_GET_FX_CURRENCIES(
      A_REGION_ID REGIONS.REGION_ID%TYPE,
      A_ASOFDATE AGE.ASOFDATE%TYPE,
      A_CURRENCIES_RESULTSET OUT TYPE_RESULTSET)
    RETURN NUMBER
  IS
    V_PREV_DATE REGIONS.PREV_DATE%TYPE;
    V_RESULT NUMBER;
  BEGIN
    IF A_ASOFDATE IS NULL THEN
      V_RESULT    := PKG_REFERENCE_DATA.F_GET_COB_DATE(A_REGION_ID,V_PREV_DATE);
      IF V_RESULT != 0 THEN
        RETURN V_RESULT;
        RAISE_APPLICATION_ERROR (-20001, 'Unable to get prev date. Region ' || A_region_id || ' not found');
      END IF;
    ELSE
      V_PREV_DATE:= A_ASOFDATE;
    END IF;
    OPEN A_CURRENCIES_RESULTSET FOR
  WITH SOURCES AS
    (SELECT SOURCE_SYSTEM_ID FROM SOURCE_SYSTEM WHERE REGION_ID =A_REGION_ID
    )
  SELECT DISTINCT A.NOTIONAL_CURRENCY_ID
  FROM AGE A,
    SOURCES S
  WHERE S.SOURCE_SYSTEM_ID = A.SOURCE_SYSTEM_ID
  AND ASOFDATE             = V_PREV_DATE
  UNION
  SELECT DISTINCT CURRENCY_ID FROM FX_RATES WHERE ASOFDATE < V_PREV_DATE;
  RETURN 0;
EXCEPTION
WHEN OTHERS THEN
  RETURN 1;
  RAISE;
END F_GET_FX_CURRENCIES;
  FUNCTION F_INSERT_FX_RATES(
      A_asofdate FX_RATES.ASOFDATE%TYPE,
      A_region_id REGIONS.REGION_ID%TYPE,
      A_CURRENCY_ID FX_RATES.CURRENCY_ID%TYPE,
      A_SPOT FX_RATES.SPOT%TYPE,
      A_fx_date FX_RATES.FX_DATE%TYPE)
    RETURN NUMBER
  IS
    V_RESULT VARCHAR2(1);
  BEGIN
    SELECT
      CASE
        WHEN EXISTS
          (SELECT 1
          FROM FX_RATES
          WHERE ASOFDATE =A_ASOFDATE
          AND REGION_ID  =A_REGION_ID
          AND CURRENCY_ID=A_CURRENCY_ID
          )
        THEN '1'
        ELSE '0'
      END
    INTO V_RESULT
    FROM DUAL;
    IF V_RESULT<>'1' THEN
      INSERT
      INTO FX_RATES
        (
          ASOFDATE,
          REGION_ID,
          CURRENCY_ID,
          SPOT,
          FX_DATE
        )
        VALUES
        (
          A_ASOFDATE,
          A_REGION_ID,
          A_CURRENCY_ID,
          A_SPOT,
          A_FX_DATE
        );
    END IF;
    RETURN 0;
  EXCEPTION
  WHEN OTHERS THEN
    RETURN 1;
    raise;
  END F_INSERT_FX_RATES;
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Functionality: Checks the last load and next load dates by the region/source system given and return 1 if the data load is allowed and 0 if not.
-- Used: QlickView
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
  FUNCTION F_GET_ALLOW_QV_LOAD
    (
      p_source_system_id IN SOURCE_SYSTEM.SOURCE_SYSTEM_ID%TYPE,
      p_region_id        IN SOURCE_SYSTEM.REGION_ID%TYPE,
      p_metric_id        IN VARCHAR2 DEFAULT 'IA'
    )
    RETURN NUMBER
  IS
    v_allowed NUMBER;
  BEGIN
    SELECT
      CASE
        WHEN b.AGE_QV_NEXTLOAD > b.AGE_QV_LASTLOAD
        THEN 1
        ELSE 0
      END
    INTO v_allowed
    FROM SOURCE_SYSTEM a,
      SOURCE_SYSTEM_METRIC b
    WHERE a.source_system_id=b.source_system_id
    AND b.metric_id         =p_metric_id
    AND a.SOURCE_SYSTEM_ID  = p_source_system_id
    AND a.REGION_ID         = p_region_id;
    RETURN v_allowed;
  EXCEPTION
  WHEN OTHERS THEN
    RETURN 0;
    RAISE;
  END F_GET_ALLOW_QV_LOAD;
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Functionality: Updated the load dates for QV by region/source_system with the given dates.
-- Used: QlickView
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
  PROCEDURE PR_SET_QV_LOAD_DATES(
      p_source_system_id IN SOURCE_SYSTEM.SOURCE_SYSTEM_ID%TYPE,
      p_region_id        IN SOURCE_SYSTEM.REGION_ID%TYPE,
      p_last_load        IN SOURCE_SYSTEM_METRIC.AGE_QV_LASTLOAD%TYPE,
      p_next_load        IN SOURCE_SYSTEM_METRIC.AGE_QV_NEXTLOAD%TYPE,
      p_metric           IN VARCHAR2 DEFAULT 'IA' )
  IS
  BEGIN
    UPDATE SOURCE_SYSTEM_METRIC a
    SET a.AGE_QV_LASTLOAD    = p_last_load,
      a.AGE_QV_NEXTLOAD      = p_next_load
    WHERE a.SOURCE_SYSTEM_ID = p_source_system_id
    AND a.metric_id          = p_metric
    AND EXISTS
      (SELECT 1
      FROM source_system b
      WHERE a.source_system_id=b.source_system_id
      AND b.REGION_ID         = p_region_id
      );
    COMMIT;
  END PR_SET_QV_LOAD_DATES;
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Functionality: Returns the next load date for the given region/source system
-- Used: IA Hadoop processes
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
  FUNCTION F_GET_HADOOP_LOAD_DATE(
      p_source_system_id IN SOURCE_SYSTEM.SOURCE_SYSTEM_ID%TYPE,
      p_region_id        IN SOURCE_SYSTEM.REGION_ID%TYPE,
      p_metric           IN SOURCE_SYSTEM_METRIC.METRIC_ID%TYPE DEFAULT 'IA' )
    RETURN TYPE_RESULSET_UNIX
  IS
    p_bh SOURCE_SYSTEM.BH_BUSINESS%TYPE;
    p_bh_rpl SOURCE_SYSTEM_BH.BH_UBR_LEVEL_1_ID%TYPE;
    r_unix TYPE_RESULSET_UNIX;
    v_query VARCHAR2(20000);
  BEGIN
    v_query := 'select ';
    IF p_metric IN ('IA') THEN
      SELECT bh_ubr_level_1_id
      INTO p_bh_rpl
      FROM VW_SOURCE_SYSTEM
      WHERE source_system_id = p_source_system_id
      AND metric_id          = p_metric;
      IF p_bh_rpl           IS NOT NULL THEN
        v_query             := v_query||
        ' ''"''||bh_volcker_trading_desk||''"'' as "bh_trading_unit",
''"''||bh_volcker_trading_desk_full||''"'' as "bh_volcker_trading_desk_full",
''"''||bh_lowest_level_rpl_code||''"'' as "bh_lowest_level_rpl_code",
''"''||bh_lowest_level_rpl_full_name||''"'' as "bh_lowest_level_rpl_full_name",
''"''||bh_lowest_level_rpl||''"'' as "bh_lowest_level_rpl",
''"''||bh_charge_report_unit_code||''"'' as "bh_charge_report_unit_code",
''"''||bh_charge_report_unit||''"'' as "bh_charge_report_unit",
''"''||bh_charge_report_parent_code||''"'' as "bh_charge_report_parent_code",
''"''||bh_charge_report_parent||''"'' as "bh_charge_report_parent",
''"''||bh_ubr_level_1_id||''"'' as "bh_ubr_level_1_id",
''"''||bh_ubr_level_1_name||''"'' as "bh_ubr_level_1_name",
''"''||bh_ubr_level_1_rpl_code||''"'' as "bh_ubr_level_1_rpl_code",
''"''||bh_ubr_level_2_id||''"'' as "bh_ubr_level_2_id",
''"''||bh_ubr_level_2_name||''"'' as "bh_ubr_level_2_name",
''"''||bh_ubr_level_2_rpl_code||''"'' as "bh_ubr_level_2_rpl_code",
''"''||bh_ubr_level_3_id||''"'' as "bh_ubr_level_3_id",
''"''||bh_ubr_level_3_name||''"'' as "bh_ubr_level_3_name",
''"''||bh_ubr_level_3_rpl_code||''"'' as "bh_ubr_level_3_rpl_code",
''"''||bh_ubr_level_4_id||''"'' as "bh_ubr_level_4_id",
''"''||bh_ubr_level_4_name||''"'' as "bh_ubr_level_4_name",
''"''||bh_ubr_level_4_rpl_code||''"'' as "bh_ubr_level_4_rpl_code",
''"''||bh_ubr_level_5_id||''"'' as "bh_ubr_level_5_id",
''"''||bh_ubr_level_5_name||''"'' as "bh_ubr_level_5_name",
''"''||bh_ubr_level_5_rpl_code||''"'' as "bh_ubr_level_5_rpl_code",
''"''||bh_ubr_level_6_id||''"'' as "bh_ubr_level_6_id",
''"''||bh_ubr_level_6_name||''"'' as "bh_ubr_level_6_name",
''"''||bh_ubr_level_6_rpl_code||''"'' as "bh_ubr_level_6_rpl_code",
''"''||bh_ubr_level_7_id||''"'' as "bh_ubr_level_7_id",
''"''||bh_ubr_level_7_name||''"'' as "bh_ubr_level_7_name",
''"''||bh_ubr_level_7_rpl_code||''"'' as "bh_ubr_level_7_rpl_code",
''"''||bh_ubr_level_8_id||''"'' as "bh_ubr_level_8_id",
''"''||bh_ubr_level_8_name||''"'' as "bh_ubr_level_8_name",
''"''||bh_ubr_level_8_rpl_code||''"'' as "bh_ubr_level_8_rpl_code",
''"''||bh_ubr_level_9_id||''"'' as "bh_ubr_level_9_id",
''"''||bh_ubr_level_9_name||''"'' as "bh_ubr_level_9_name",
''"''||bh_ubr_level_9_rpl_code||''"'' as "bh_ubr_level_9_rpl_code",
''"''||bh_ubr_level_10_id||''"'' as "bh_ubr_level_10_id",
''"''||bh_ubr_level_10_name||''"'' as "bh_ubr_level_10_name",
''"''||bh_ubr_level_10_rpl_code||''"'' as "bh_ubr_level_10_rpl_code",
''"''||bh_ubr_level_11_id||''"'' as "bh_ubr_level_11_id",
''"''||bh_ubr_level_11_name||''"'' as "bh_ubr_level_11_name",
''"''||bh_ubr_level_11_rpl_code||''"'' as "bh_ubr_level_11_rpl_code",
''"''||bh_ubr_level_12_id||''"'' as "bh_ubr_level_12_id",
''"''||bh_ubr_level_12_name||''"'' as "bh_ubr_level_12_name",
''"''||bh_ubr_level_12_rpl_code||''"'' as "bh_ubr_level_12_rpl_code",
''"''||bh_ubr_level_13_id||''"'' as "bh_ubr_level_13_id",
''"''||bh_ubr_level_13_name||''"'' as "bh_ubr_level_13_name",
''"''||bh_ubr_level_13_rpl_code||''"'' as "bh_ubr_level_13_rpl_code",
''"''||bh_ubr_level_14_id||''"'' as "bh_ubr_level_14_id",
''"''||bh_ubr_level_14_name||''"'' as "bh_ubr_level_14_name",
''"''||bh_ubr_level_14_rpl_code||''"'' as "bh_ubr_level_14_rpl_code",
''"''||bh_desk_level_1_id||''"'' as "bh_desk_level_1_id",
''"''||bh_desk_level_1_name||''"'' as "bh_desk_level_1_name",
''"''||bh_desk_level_1_rpl_code||''"'' as "bh_desk_level_1_rpl_code",
''"''||bh_desk_level_2_id||''"'' as "bh_desk_level_2_id",
''"''||bh_desk_level_2_name||''"'' as "bh_desk_level_2_name",
''"''||bh_desk_level_2_rpl_code||''"'' as "bh_desk_level_2_rpl_code",
''"''||bh_desk_level_3_id||''"'' as "bh_desk_level_3_id",
''"''||bh_desk_level_3_name||''"'' as "bh_desk_level_3_name",
''"''||bh_desk_level_3_rpl_code||''"'' as "bh_desk_level_3_rpl_code",
''"''||bh_desk_level_4_id||''"'' as "bh_desk_level_4_id",
''"''||bh_desk_level_4_name||''"'' as "bh_desk_level_4_name",
''"''||bh_desk_level_4_rpl_code||''"'' as "bh_desk_level_4_rpl_code",
''"''||bh_desk_level_5_id||''"'' as "bh_desk_level_5_id",
''"''||bh_desk_level_5_name||''"'' as "bh_desk_level_5_name",
''"''||bh_desk_level_5_rpl_code||''"'' as "bh_desk_level_5_rpl_code",
''"''||bh_portfolio_id||''"'' as "bh_portfolio_id",
''"''||bh_portfolio_name||''"'' as "bh_portfolio_name",
''"''||bh_portfolio_rpl_code||''"'' as "bh_portfolio_rpl_code",'
        ;
      END IF;
    END IF;
    IF p_metric IN ('ITR','CFTR') THEN
      SELECT bh_business
      INTO p_bh
      FROM VW_SOURCE_SYSTEM
      WHERE source_system_id = p_source_system_id
      AND metric_id          = p_metric;
      IF p_bh               IS NOT NULL THEN
        v_query             := v_query||' ''"''||bh_business||''"'' as "bh_business",
''"''||bh_sub_business||''"'' as "bh_sub_business",
''"''||bh_trading_unit||''"'' as "bh_trading_unit",';
      END IF;
    END IF;
    v_query    :=v_query||' to_char(last_load,''yyyymmdd'') as "prevDate",
to_char(next_load,''yyyymmdd'') as "cobDate",
to_char(after_next_load,''yyyymmdd'') as "nextDate",
num_reduce_task as "numReduceTasks",
mapred_child_java_opts as "mapredChildJavaOpts",
io_sort_mb as "ioSortMb",
source_system_id as "source_system_id",
metric_id as "metric_id",
cftrFile as "cftrFile",
to_char(next_load,''D'') as "cobDayWeek",
to_char(next_load - 1,''yyyymmdd'') as "cobDateMinus1",
to_char(next_load - 2,''yyyymmdd'') as "cobDateMinus2"';
    IF p_metric ='CFTR' THEN
      v_query  := v_query||', ''"''||SOURCE_SYSTEM_CRDS_NAME||''"'' as "source_system_crds_name"';
    END IF;
    IF p_metric NOT IN ('IA','SOURCE_SYSTEM') THEN
      v_query:=v_query||',to_char(next_load, ''YYYY-MM-DD'') as "ReportDate"';
    END IF;
    v_query:=v_query||' FROM VW_SOURCE_SYSTEM
WHERE   source_system_id = '''||p_source_system_id||'''
AND metric_id = '''||p_metric||'''';
    dbms_output.put_line(v_query);
    OPEN r_unix FOR v_query;
    RETURN r_unix;
  EXCEPTION
  WHEN OTHERS THEN
    RAISE;
  END F_GET_HADOOP_LOAD_DATE;
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Functionality: Returns the next load date for the given region/source system
-- Used: IA Hadoop processes
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
  FUNCTION F_SET_HADOOP_LOAD_DATES(
      p_source_system_id IN SOURCE_SYSTEM.SOURCE_SYSTEM_ID%TYPE,
      p_region_id        IN SOURCE_SYSTEM.REGION_ID%TYPE,
      p_metric_id        IN VARCHAR2 DEFAULT 'IA'
      --p_last_load                IN SOURCE_SYSTEM.AGE_HADOOP_LASTLOAD%TYPE
    )
    RETURN NUMBER
  IS
    v_saturday SOURCE_SYSTEM.SATURDAY_ACTIVITY%TYPE;
    v_sunday SOURCE_SYSTEM.SATURDAY_ACTIVITY%TYPE;
  BEGIN
    IF p_metric_id = 'SOURCE_SYSTEM' THEN
      UPDATE SOURCE_SYSTEM
      SET AGE_HADOOP_LASTLOAD = NVL(AGE_HADOOP_NEXTLOAD, sysdate - 1),
        AGE_HADOOP_NEXTLOAD   = DECODE(AGE_HADOOP_NEXTLOAD, NULL, sysdate-1,AGE_HADOOP_NEXTLOAD + DECODE(SATURDAY_ACTIVITY||SUNDAY_ACTIVITY, '10', DECODE(1 + TRUNC (AGE_HADOOP_NEXTLOAD+1) - TRUNC (AGE_HADOOP_NEXTLOAD+1, 'IW'),7,2,1),'01',DECODE(1 + TRUNC (AGE_HADOOP_NEXTLOAD+1) - TRUNC (AGE_HADOOP_NEXTLOAD+1, 'IW'),6,2,1),'11',1, DECODE(1 + TRUNC (AGE_HADOOP_NEXTLOAD+1) - TRUNC (AGE_HADOOP_NEXTLOAD+1, 'IW'), 6, 3, 7, 2, 1)) )
      WHERE SOURCE_SYSTEM_ID  = p_source_system_id
      AND REGION_ID           = p_region_id;
    ELSE
      SELECT SATURDAY_ACTIVITY
      INTO v_saturday
      FROM source_system
      WHERE source_system_id=p_source_system_id
      AND region_id         =p_region_id;
      SELECT SUNDAY_ACTIVITY
      INTO v_sunday
      FROM source_system
      WHERE source_system_id=p_source_system_id
      AND region_id         =p_region_id;
      UPDATE SOURCE_SYSTEM_METRIC a
      SET a.AGE_HADOOP_LASTLOAD = NVL(a.AGE_HADOOP_NEXTLOAD, sysdate - 1),
        a.AGE_HADOOP_NEXTLOAD   = DECODE(a.AGE_HADOOP_NEXTLOAD, NULL, sysdate-1,a.AGE_HADOOP_NEXTLOAD + DECODE(v_saturday||v_sunday, '10', DECODE(1 + TRUNC (a.AGE_HADOOP_NEXTLOAD+1) - TRUNC (a.AGE_HADOOP_NEXTLOAD+1, 'IW'),7,2,1),'01', DECODE(1 + TRUNC (a.AGE_HADOOP_NEXTLOAD+1) - TRUNC (a.AGE_HADOOP_NEXTLOAD+1, 'IW'),6,2,1),'11', 1, DECODE(1 + TRUNC (a.AGE_HADOOP_NEXTLOAD+1) - TRUNC (a.AGE_HADOOP_NEXTLOAD+1, 'IW'), 6, 3, 7, 2, 1)) )
      WHERE a.SOURCE_SYSTEM_ID  = p_source_system_id
      AND a.metric_id           =upper(p_metric_id)
      AND EXISTS
        (SELECT 1
        FROM source_system b
        WHERE a.source_system_id=b.source_system_id
        AND b.REGION_ID         = p_region_id
        );
    END IF;
    IF SQL%ROWCOUNT = 1 THEN
      RETURN 0;
    ELSE
      RETURN 1;
    END IF;
  EXCEPTION
  WHEN OTHERS THEN
    RETURN 1;
    RAISE;
  END F_SET_HADOOP_LOAD_DATES;
  FUNCTION F_CALC_CHARGE_THRESHOLD(
      p_source_system_id SOURCE_SYSTEM.SOURCE_SYSTEM_ID%TYPE,
      p_asofdate AGE.ASOFDATE%TYPE,
      p_region_id FX_RATES.REGION_ID%TYPE)
    RETURN NUMBER
  IS
    v_return             NUMBER(1);
    v_spot_eur           NUMBER(15,6);
    v_default_threshold  NUMBER(15,2);
    v_bh_asofdate        DATE;
    v_ct_historical_date DATE;
    v_number             NUMBER(15);
  BEGIN
    --Get eur rates
    SELECT spot
    INTO v_spot_eur
    FROM
      (SELECT eur.spot,
        DENSE_RANK() OVER (PARTITION BY eur.region_id, eur.currency_id ORDER BY eur.asofdate DESC) ranking
      FROM fx_rates eur
      WHERE eur.asofdate<= p_asofdate
      AND region_id      = p_region_id
      AND currency_id    = 'EUR'
      )
    WHERE ranking = '1';
    --Get default threshold
    SELECT ct.threshold
    INTO v_default_threshold
    FROM charge_threshold ct
    WHERE trading_unit  = 'DEFAULT'
    AND historical_date =
      (SELECT MAX(historical_date)
      FROM charge_threshold
      WHERE historical_date <= p_asofdate
      );
    --Get nearest bh_asofdate
    SELECT MAX(b.asofdate)
    INTO v_bh_asofdate
    FROM vw_book_hierarchy b
    WHERE asofdate<= p_asofdate;
    BEGIN
      --Get nearest charge_threshold historical date
      SELECT MAX(historical_date)
      INTO v_ct_historical_date
      FROM
        (SELECT aux.historical_date,
          DENSE_RANK () OVER (PARTITION BY historical_date ORDER BY historical_date DESC) ranking
        FROM charge_threshold aux
        WHERE aux.historical_date <= p_asofdate
        )
      WHERE ranking=1;
    EXCEPTION
    WHEN NO_DATA_FOUND THEN
      v_ct_historical_date := NULL;
    END;
    --Orphan and duplicated positions haven't charges.
    UPDATE age
    SET charge_threshold_flg='0'
    WHERE source_system_id  = p_source_system_id
    AND asofdate            = p_asofdate
    AND (orphan_flg         ='1'
    OR duplicated_flg       ='1');
    --It updates positions with not present trading units in charge_threshold table
    UPDATE age
    SET charge_threshold_flg = '0'
    WHERE source_system_id   = p_source_system_id
    AND asofdate             = p_asofdate
    AND book_id             IN
      ( SELECT DISTINCT ag2.book_id
      FROM vw_book_hierarchy bh2,
        age ag2,
        (SELECT ce.trading_unit
        FROM
          (
          --WITH RATES AS
          -- (SELECT currency_id,
          -- spot
          -- FROM
          --(SELECT aux.currency_id,
          --  aux.spot,
          --   DENSE_RANK () OVER (PARTITION BY currency_id ORDER BY ASOFDATE DESC) ranking
          --  FROM fx_rates aux
          --  WHERE aux.asofdate <= p_asofdate
          -- AND region_id       = p_region_id
          -- )
          -- WHERE ranking=1
          -- )
          SELECT bh.trading_unit,
            --(((SUM(ag.final_B0TO30) + SUM(ag.final_B31TO60) + SUM(ag.final_B61TO90) + SUM(ag.final_B91TO180) + SUM(ag.final_B181TO360) +   SUM(ag.final_B361)) * r.spot) / v_spot_eur) CHARGES_EURO
            -- SUM((ag.FINAL_B0TO30+ag.FINAL_B31TO60+ag.FINAL_B61TO90+ag.FINAL_B91TO180+ag.FINAL_B181TO360+ag.FINAL_B361) * r.spot / v_spot_eur) CHARGES_EURO
            SUM((ag.FINAL_B0TO30+ag.FINAL_B31TO60+ag.FINAL_B61TO90+ag.FINAL_B91TO180+ag.FINAL_B181TO360+ag.FINAL_B361) / v_spot_eur) CHARGES_EURO
          FROM vw_book_hierarchy bh ,
            age Ag
            -- rates r
          WHERE bh.book_id         = ag.book_id
          AND ag.source_system_id  = p_source_system_id
          AND bh.trading_unit NOT IN
            (SELECT trading_unit
            FROM charge_threshold
            WHERE historical_date= v_ct_historical_date
            )
          AND ag.asofdate = p_asofdate
          AND bh.asofdate = v_bh_asofdate
            -- AND ag.NOTIONAL_CURRENCY_ID = r.currency_id
          GROUP BY bh.trading_unit
            --  r.spot
          ) CE
        HAVING SUM(ce.CHARGES_EURO) < v_default_threshold
        GROUP BY ce.trading_unit
        ) TRADING_UNIT
      WHERE bh2.trading_unit   = TRADING_UNIT.trading_unit
      AND bh2.asofdate         = v_bh_asofdate
      AND ag2.book_id          = bh2.book_id
      AND ag2.source_system_id = p_source_system_id
      AND ag2.asofdate         = p_asofdate
      );
    --By default we apply charges, but if the sum of charges buckets is less than threshold, we update charge_threshold_flg to 0, then we will not apply any charges.
    --It Updates positions with present trading units in charge_threshold table.
    IF v_ct_historical_date IS NOT NULL THEN
      UPDATE age
      SET charge_threshold_flg = '0'
      WHERE source_system_id   = p_source_system_id
      AND asofdate             = p_asofdate
      AND book_id             IN
        ( SELECT DISTINCT ag2.book_id
        FROM vw_book_hierarchy bh2,
          age ag2,
          (SELECT ce.trading_unit
          FROM
            (
            --WITH RATES AS
            -- (SELECT currency_id,
            --  spot
            --  FROM
            --  (SELECT aux.currency_id,
            --   aux.spot,
            --   DENSE_RANK () OVER (PARTITION BY currency_id ORDER BY ASOFDATE DESC) ranking
            --   FROM fx_rates aux
            --   WHERE aux.asofdate <= p_asofdate
            --  AND region_id       = p_region_id
            --  )
            --  WHERE ranking=1
            --   )
            SELECT bh.trading_unit,
              -- (SUM(ag.final_B0TO30) + SUM(ag.final_B31TO60) + SUM(ag.final_B61TO90) + SUM(ag.final_B91TO180) + SUM(ag.final_B181TO360) +   SUM(ag.final_B361)* rates.spot / v_spot_eur) CHARGES_EURO
              -- SUM((ag.FINAL_B0TO30+ag.FINAL_B31TO60+ag.FINAL_B61TO90+ag.FINAL_B91TO180+ag.FINAL_B181TO360+ag.FINAL_B361) * rates.spot / v_spot_eur) CHARGES_EURO
              SUM((ag.FINAL_B0TO30+ag.FINAL_B31TO60+ag.FINAL_B61TO90+ag.FINAL_B91TO180+ag.FINAL_B181TO360+ag.FINAL_B361) / v_spot_eur) CHARGES_EURO
            FROM age ag,
              --  RATES,
              vw_book_hierarchy bh
            LEFT OUTER JOIN charge_threshold ct
            ON bh.trading_unit        =ct.trading_unit
            WHERE ag.source_system_id = p_source_system_id
            AND ag.asofdate           = p_asofdate
            AND bh.book_id            = ag.book_id
            AND bh.asofdate           = v_bh_asofdate -- BH nearest ASOFDATE
              --  AND ag.NOTIONAL_CURRENCY_ID = rates.currency_id
            AND ct.historical_date = v_ct_historical_date --Charge threshold nearest historical_date
            GROUP BY bh.trading_unit
              --  rates.spot,
              -- rates.currency_id
            ) CE
          HAVING SUM(ce.CHARGES_EURO) <
            (SELECT aux.threshold
            FROM charge_threshold aux
            WHERE aux.trading_unit =ce.trading_unit
            AND aux.historical_date=v_ct_historical_date
            )
          GROUP BY ce.trading_unit
          ) TRADING_UNIT
        WHERE bh2.trading_unit   = TRADING_UNIT.trading_unit
        AND bh2.asofdate         = v_bh_asofdate
        AND ag2.book_id          = bh2.book_id
        AND ag2.source_system_id = p_source_system_id
        AND ag2.asofdate         = p_asofdate
        );
    END IF;
    RETURN 0;
  EXCEPTION
  WHEN OTHERS THEN
    v_number:=PKG_MONITORING.F_INSERT_LOG_JOBS ('CHARGE_THRESHOLD',NULL,NULL,sysdate,'CHARGE_THRESHOLD','ERROR', 'FATAL', 'CHARGE THRESHOLD ERROR','IA', DBMS_UTILITY.FORMAT_ERROR_STACK);
    RETURN 1;
  END;
  
    FUNCTION F_GET_REPORT_TRADING_DESK(p_metric_id  IN SOURCE_SYSTEM_METRIC.METRIC_ID%TYPE DEFAULT 'IA', p_asofdateIni IN AGE.ASOFDATE%TYPE, p_asofdateEnd IN AGE.ASOFDATE%TYPE) 
RETURN TYPE_RESULSET_UNIX 
IS
    v_number NUMBER;
    r_tradingDesk   TYPE_RESULSET_UNIX;
BEGIN


     IF p_metric_id='IA' THEN
     
            OPEN r_tradingDesk FOR            
            select b.source_system_id,
                   b.book_id,
                   bh.business,
                   bh.sub_business,
                   bh.volcker_trading_desk_full,
                   to_char(bh.asofdate, 'YYYYMMDD') asofdate, 
            case when exists (select 1 from age ag where b.book_id=ag.book_id and b.source_system_id = ag.source_system_id) then 'Y' else 'N' end as reported
            from (select * from book_reporting where book_reporting.asofdate between p_asofdateIni and p_asofdateEnd) b,
            book_hierarchy_rpl bh,
            source_system_metric c
            where bh.book_id(+) = b.book_id and ( bh.asofdate = (select max(asofdate) from book_hierarchy_rpl bh2 where bh2.asofdate<=b.asofdate) or bh.asofdate is null) and c.metric_id='IA' and c.source_system_id=b.source_system_id 
            group by b.book_id,
                     b.source_system_id,
                     bh.business,
                     bh.sub_business,
                     bh.volcker_trading_desk_full,
                     bh.asofdate; 
                          
    ELSE IF  p_metric_id='ITR' THEN
    
            OPEN r_tradingDesk FOR
            select b.source_system_id,
                   b.book_id,
                   bh.business,
                   bh.sub_business,
                   bh.volcker_trading_desk_full,
                   to_char(bh.asofdate, 'YYYYMMDD') asofdate           
            from (select * from book_reporting where book_reporting.asofdate between p_asofdateIni and p_asofdateEnd) b,
            book_hierarchy_rpl bh,
            source_system_metric c
            where bh.book_id(+) = b.book_id and ( bh.asofdate = (select max(asofdate) from book_hierarchy_rpl bh2 where bh2.asofdate<=b.asofdate) or bh.asofdate is null) and c.metric_id='ITR' and c.source_system_id=b.source_system_id 
            group by b.book_id,
                     b.source_system_id,
                     bh.business,
                     bh.sub_business,
                     bh.volcker_trading_desk_full,
                     bh.asofdate; 
    
    ELSE
            OPEN r_tradingDesk FOR
            select b.source_system_id,
                   b.book_id,
                   bh.business,
                   bh.sub_business,
                   bh.volcker_trading_desk_full,
                   to_char(bh.asofdate, 'YYYYMMDD') asofdate            
            from (select * from book_reporting where book_reporting.asofdate between p_asofdateIni and p_asofdateEnd) b,
            book_hierarchy_rpl bh,
            source_system_metric c
            where bh.book_id(+) = b.book_id and ( bh.asofdate = (select max(asofdate) from book_hierarchy_rpl bh2 where bh2.asofdate<=b.asofdate) or bh.asofdate is null) and c.metric_id='CFTR' and c.source_system_id=b.source_system_id 
            group by b.book_id,
                     b.source_system_id,
                     bh.business,
                     bh.sub_business,
                     bh.volcker_trading_desk_full,
                     bh.asofdate;
    

    END IF;
    END IF;
    
    RETURN r_tradingDesk;
            
    EXCEPTION 
    WHEN OTHERS THEN 
        v_number:=PKG_MONITORING.F_INSERT_LOG_JOBS ('F_GET_REPORT_TRADING_DESK',null,null,sysdate,'F_GET_REPORT_TRADING_DESK','ERROR', 'FATAL', 'F_GET_REPORT_TRADING_DESK ERROR', p_metric_id ,  DBMS_UTILITY.FORMAT_ERROR_STACK);

    END F_GET_REPORT_TRADING_DESK;
  
END PKG_REFERENCE_DATA;
