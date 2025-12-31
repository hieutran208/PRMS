create or replace PROCEDURE            "P1_M27_DDLY_NTWK_KEY_IDC_1718"
                 ( p_wkdate    IN  CHAR DEFAULT NULL
                 , p_stdate_01 IN  CHAR DEFAULT NULL
                 , p_eddate_01 IN  CHAR DEFAULT NULL
                 )
IS
    /*--------------------------------------------------------------------------
     * PROGRAM ID    : P1_M27_DDLY_NTWK_KEY_IDC_1718
     * PROGRAM NAME  : A program for insert data to TM27_DDLY_NTWK_KEY_IDC_A
     * SOURCE TABLE  : TM27_DDLY_PCF_KEY_IDC_A
     * TARGET TABLE  : TM27_DDLY_NTWK_KEY_IDC_A
     * PROGRAMER     : HIEU
     * LAST MODIFICATION DATE : 2025-12-26
     * UNIQUENESS    : N/A
     * COMMENTS      : N/A
    ----------------------------------------------------------------------------
     * Revision History : 2025-12-26 : Create
     * Revision History :
    --------------------------------------------------------------------------*/
    ----------------------------------------------------------------------------
    --  Declare Local Variables
    ----------------------------------------------------------------------------
    v_wk_date                 CHAR(8) DEFAULT NULL ;
    v_st_date_01              CHAR(8) DEFAULT NULL ;
    v_end_date_01             CHAR(8) DEFAULT NULL ;

    v_program_id              VARCHAR2(100) DEFAULT 'P1_M27_DDLY_NTWK_KEY_IDC_1718' ;
    v_program_type_name       VARCHAR2(100) DEFAULT 'Stored Procedure' ;
    v_step_code               VARCHAR2(3) ;
    v_step_desc               VARCHAR2(100) ;
    v_time                    DATE ;
    --v_cnt                     NUMBER DEFAULT 0 ;
    v_seq                     CHAR(14) DEFAULT ' '||to_char(SEQ_UID.NEXTVAL,'000000000000');
    ----------------------------------------------------------------------------
    --  Declare Cursor
    ----------------------------------------------------------------------------
/*
    CURSOR v_sbmt_day(v_dt1 VARCHAR2, v_dt2 VARCHAR2, v_dt3 VARCHAR2) IS
    SELECT BAS_DAY
    FROM   TM00_DDLY_CAL_D
    WHERE  BAS_DAY IN ('20230129','20230722')
    ORDER BY 1;
*/
    CURSOR v_sbmt_day(v_dt1 VARCHAR2, v_dt2 VARCHAR2, v_dt3 VARCHAR2) IS
    SELECT W2.BAS_DAY
    FROM   (SELECT NVL(MIN(BAS_DAY), TO_CHAR(TO_DATE(v_dt2, 'YYYYMMDD') -2, 'YYYYMMDD')) AS MIN_DAY, TO_CHAR(TO_DATE(v_dt2, 'YYYYMMDD') -2, 'YYYYMMDD') AS MAX_DAY
            FROM   (

--                  SELECT T1.BAS_DAY
--                  FROM   TM00_DDLY_CAL_D T1
--                  WHERE  T1.BAS_DAY BETWEEN '20200522' AND '20201024'

                    SELECT MAX(BAS_DAY) AS BAS_DAY
                    FROM   TM23_DDLY_BAL_SHET_A

                    UNION ALL

                    SELECT T1.BAS_DAY
                    FROM   TM00_DDLY_CAL_D T1 INNER JOIN (SELECT MIN(DATA_BAS_DAY) AS MIN_DAY, TO_CHAR(TO_DATE(v_dt2, 'YYYYMMDD') -2, 'YYYYMMDD') AS MAX_DAY
                                                          FROM   TBSM_INPT_RPT_SUBMIT_L
                                                          WHERE  BTCH_BAS_DAY = v_dt2
                                                          AND    INPT_RPT_ID  = 'G32_012_TTGS'
                                                         ) T2
                                                      ON T1.BAS_DAY BETWEEN T2.MIN_DAY AND T2.MAX_DAY
                    UNION ALL

                    SELECT T1.BAS_DAY
                    FROM   TM00_DDLY_CAL_D T1 INNER JOIN (SELECT MIN(DATA_BAS_DAY) AS MIN_DAY, TO_CHAR(TO_DATE(v_dt2, 'YYYYMMDD') -2, 'YYYYMMDD') AS MAX_DAY
                                                          FROM   TBSM_INPT_RPT_SUBMIT_L
                                                          WHERE  BTCH_BAS_DAY = v_dt2
                                                          AND    INPT_RPT_ID  = 'G32_003_TTGS'
                                                         ) T2
                                                      ON T1.BAS_DAY BETWEEN T2.MIN_DAY AND T2.MAX_DAY

                    UNION ALL

                    SELECT T1.BAS_DAY
                    FROM   TM00_DDLY_CAL_D T1 INNER JOIN (SELECT MIN(DATA_BAS_DAY) AS MIN_DAY, MAX(DATA_BAS_DAY) AS MAX_DAY
                                                          FROM   TBSM_INPT_RPT_SUBMIT_L
                                                          WHERE  BTCH_BAS_DAY = v_dt2
                                                          AND    INPT_RPT_ID  = 'G32_020_TTGS_01'
                                                         ) T2
                                                      ON T1.BAS_DAY BETWEEN T2.MIN_DAY AND T2.MAX_DAY

                    UNION ALL

                    SELECT T1.BAS_DAY
                    FROM   TM00_DDLY_CAL_D T1 INNER JOIN (SELECT SUBSTR(NVL(MIN(DATA_BAS_DAY), '999912'), 1, 6)||'01' AS MIN_DAY, TO_CHAR(SYSDATE - 10, 'YYYYMMDD') AS MAX_DAY
                                                          FROM   TBSM_INPT_RPT_SUBMIT_L
                                                          WHERE  BTCH_BAS_DAY = v_dt2
                                                          AND    INPT_RPT_ID  = 'G32_016_TTGS'
                                                         ) T2
                                                      ON T1.BAS_DAY BETWEEN T2.MIN_DAY AND T2.MAX_DAY

                    UNION ALL

                    SELECT T1.BAS_DAY
                    FROM   TM00_DDLY_CAL_D T1 INNER JOIN (SELECT MIN(DATA_BAS_DAY) AS MIN_DAY, MAX(DATA_BAS_DAY) AS MAX_DAY
                                                          FROM   TBSM_INPT_RPT_SUBMIT_L
                                                          WHERE  BTCH_BAS_DAY = v_dt2
                                                          AND    INPT_RPT_ID  = 'G32_005_TTGS'
                                                         ) T2
                                                      ON T1.BAS_DAY BETWEEN T2.MIN_DAY AND T2.MAX_DAY

                   )
           ) W1 INNER JOIN TM00_DDLY_CAL_D W2
                        ON W2.BAS_DAY BETWEEN W1.MIN_DAY AND W1.MAX_DAY
    WHERE W2.BAS_DAY >= '20210101'
    GROUP BY W2.BAS_DAY
    ORDER BY 1;

    ----------------------------------------------------------------------------
BEGIN
    ----------------------------------------------------------------------------
    --  Set Local Variables
    ----------------------------------------------------------------------------
    v_wk_date         := p_wkdate;
    v_st_date_01      := p_stdate_01;
    v_end_date_01     := p_eddate_01;
    ----------------------------------------------------------------------------
    --  Write Start Log
    ----------------------------------------------------------------------------
    v_step_code    := '000' ;
    v_step_desc    := v_wk_date || v_seq || ' : Start Procedure(' || TRIM(v_st_date_01) || ',' || TRIM(v_end_date_01) || ')';
    v_time         := SYSDATE ;

    P1_BSM_PROG_EXEC_LOG(v_program_id, v_program_type_name, v_step_code, v_step_desc, v_time, NULL, NULL, NULL) ;
    ----------------------------------------------------------------------------
    --  1.1 Delete & Insert
    ----------------------------------------------------------------------------

    FOR loop_bas_day in v_sbmt_day(v_wk_date,v_st_date_01,v_end_date_01)
    LOOP
    BEGIN

          DELETE
            FROM TM27_DDLY_NTWK_KEY_IDC_A T1
           WHERE T1.BAS_DAY = loop_bas_day.BAS_DAY;

          ----------------------------------------------------------------------------
          --  Transaction Log
          ----------------------------------------------------------------------------
          v_step_code    := '010' ;
          v_step_desc    := v_wk_date || v_seq || ' ' || loop_bas_day.BAS_DAY ||' : Deleting Data Result';
          v_time         := SYSDATE ;

          P1_BSM_PROG_EXEC_LOG(v_program_id, v_program_type_name, v_step_code, v_step_desc, v_time, sql%rowcount, NULL, NULL) ;

          ----------------------------------------------------------------------------

          INSERT INTO TM27_DDLY_NTWK_KEY_IDC_A
          (   BAS_DAY
             ,KPI_TYP_CD
             ,DATA_RNG_CD
             ,AGT_METH_CD
             ,TOP_BTOM_TYP_CD
             ,RNK_NUM
             ,PCF_ID
             ,AGT_VAL
             ,TRGT_DATA_LST_MOD_TM
          )
          WITH
          FULL_SET AS
          (SELECT  T1.BAS_DAY, T1.PCF_ID
           FROM   (SELECT BAS_DAY, PCF_ID FROM TM27_DDLY_PCF_KEY_IDC_A WHERE BAS_DAY = loop_bas_day.BAS_DAY GROUP BY BAS_DAY, PCF_ID
                  ) T1
          ),
          FROM_G32_016_TTGS AS
          (SELECT B.BAS_YM,
                  A.PCF_ID,
                  A.TOTAL_CURRENT_MEMBERS AS TOT_PCF_MBR_NUM_CNT
           FROM   TB01_G32_016_TTGS_A A INNER JOIN TM00_MMLY_CAL_D B
                                           ON B.BAS_YM = A.BAS_YM

           UNION ALL

           SELECT TO_CHAR(ADD_MONTHS(TO_DATE(B.BAS_YM, 'YYYYMM'), 1), 'YYYYMM') AS BAS_YM,
                  A.PCF_ID,
                  A.TOTAL_CURRENT_MEMBERS AS TOT_PCF_MBR_NUM_CNT
           FROM   TB01_G32_016_TTGS_A A INNER JOIN TM00_MMLY_CAL_D B
                                           ON B.BAS_YM = A.BAS_YM

           UNION ALL

           SELECT TO_CHAR(ADD_MONTHS(TO_DATE(B.BAS_YM, 'YYYYMM'), 2), 'YYYYMM') AS BAS_YM,
                  A.PCF_ID,
                  A.TOTAL_CURRENT_MEMBERS AS TOT_PCF_MBR_NUM_CNT
           FROM   TB01_G32_016_TTGS_A A INNER JOIN TM00_MMLY_CAL_D B
                                           ON B.BAS_YM = A.BAS_YM
          ),
          FROM_TM27_DDLY_PCF_KEY_IDC_A AS
          (SELECT BAS_DAY
                 ,PCF_ID
                 ,EQT_AMT
                 ,CNSLDT_EQT_AMT
                 ,CCAP_AMT
                 ,CCAP_RSRV_FUND_SUPL_CCAP_AMT
                 ,TOT_AST_AMT
                 ,TOT_LN_BAL
                 ,SHRT_TRM_LN_BAL
                 ,MED_LT_LN_BAL
                 ,WO_COLL_LN_BAL
                 ,WIT_COLL_LN_BAL
                 ,TOT_LN_CNTR_NUM_CNT
                 ,TOT_BRWR_NUM_CNT
                 ,NEW_ARISN_LN_AMT
                 ,NEW_ARISN_LN_CNTR_NUM_CNT
                 ,NEW_BRWR_NUM_CNT
                 ,BAD_DBT_AMT
                 ,GENL_PRVS_AMT
                 ,SPEC_PRVS_AMT
                 ,TDP_BAL
                 ,MBR_DPST_BAL
                 ,NON_MBR_DPST_BAL
                 ,SHRT_TRM_DPST_BAL
                 ,MED_LT_DPST_BAL
                 ,TOT_DPSTR_NUM_CNT
                 ,MBR_DPSTR_NUM_CNT
                 ,NON_MBR_DPSTR_NUM_CNT
                 ,MBR_DPST_VS_TDP_RTO
                 ,CBV_BOR_EXCL_SFTY_FUND_AMT
                 ,CBV_OVD_BOR_AMT
                 ,SFTY_FUND_BOR_AMT
                 ,OTHR_INST_BOR_AMT
                 ,CBV_SVG_AMT
                 ,OTHR_INST_SVG_AMT
                 ,IN_HAND_CSH_AMT
                 ,CUR_TOT_INCM_AMT
                 ,CUR_TOT_EXP_AMT
                 ,CUR_NET_PNL_AMT
                 ,INCL_ALOSS_INCM_MNS_EXP_AMT
                 ,NXT_WRK_DAY_AST_AMT
                 ,NXT_WRK_DAY_PAY_LBLTY_AMT
                 ,NXT_WRK_DAY_LQDTY_RTO
                 ,NXT_7_WRK_DAY_AST_AMT
                 ,NXT_7_WRK_DAY_PAY_LBLTY_AMT
                 ,NXT_7_WRK_DAY_LQDTY_RTO
                 ,L3M_NXT_WRK_DAY_LQDTY_RTO
                 ,L3M_NXT_7_WRK_DAY_LQDTY_RTO
                 ,BAL_SHET_BAS_SHRT_TRM_LN_BAL
                 ,BAL_SHET_BAS_MED_LT_LN_BAL
                 ,RISK_PRVS_FUND_AMT
           FROM   TM27_DDLY_PCF_KEY_IDC_A
           WHERE  BAS_DAY = loop_bas_day.BAS_DAY
          ),
          PCF_CNT_WEIGHTED_AVG AS
          (SELECT T2.BAS_DAY,
                  T1.CBV_BR_CD,
                  T1.PCF_CNT_WEIGHTED_AVG
           FROM   (SELECT A.BAS_DAY,
                          A.CBV_BR_CD,
                          A.TOT_PCF_MBR_AT_BR / B.TOT_PCF_MBR_AT_NETWK AS PCF_CNT_WEIGHTED_AVG
                   FROM   (SELECT T1.BAS_DAY,
                                  T2.CBV_BR_CD,
                                  COUNT(DISTINCT T1.PCF_ID) AS TOT_PCF_MBR_AT_BR
                           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_DAY, T2.CBV_BR_CD
                          ) A INNER JOIN (SELECT BAS_DAY, COUNT(DISTINCT PCF_ID) AS TOT_PCF_MBR_AT_NETWK
                                          FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A
                                          GROUP BY BAS_DAY
                                         ) B
                                      ON B.BAS_DAY = A.BAS_DAY
                  ) T1 INNER JOIN (SELECT A.BAS_DAY,
                                          B.CBV_BR_CD
                                   FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A A INNER JOIN TM00_PCF_D B
                                                                                 ON A.PCF_ID = B.PCF_ID
                                   GROUP BY A.BAS_DAY, B.CBV_BR_CD) T2
                                ON T2.BAS_DAY    = T1.BAS_DAY
                               AND T2.CBV_BR_CD  = T1.CBV_BR_CD
          ),
          FROM_KPI_TYP_001 AS
          (SELECT A.BAS_DAY,
                  '001' AS KPI_TYP_CD,
                  '1'   AS DATA_RNG_CD,
                  '1'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  B.TOT_PCF_MBR_NUM_CNT AS AGT_VAL
           FROM   (SELECT BAS_DAY
                   FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A
                   GROUP BY BAS_DAY) A INNER JOIN (SELECT BAS_YM, SUM(TOT_PCF_MBR_NUM_CNT) AS TOT_PCF_MBR_NUM_CNT
                                                   FROM   FROM_G32_016_TTGS
                                                   GROUP BY BAS_YM
                                                  ) B
                                               ON B.BAS_YM = SUBSTR(A.BAS_DAY,1,6)

           UNION ALL

           SELECT A.BAS_DAY,
                  '001' AS KPI_TYP_CD,
                  '1'   AS DATA_RNG_CD,
                  '2'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  B.TOT_PCF_MBR_NUM_CNT AS AGT_VAL
           FROM   (SELECT BAS_DAY
                   FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A
                   GROUP BY BAS_DAY) A INNER JOIN (SELECT BAS_YM, ROUND(AVG(TOT_PCF_MBR_NUM_CNT), 0) AS TOT_PCF_MBR_NUM_CNT
                                                   FROM   FROM_G32_016_TTGS
                                                   GROUP BY BAS_YM
                                                  ) B
                                               ON B.BAS_YM = SUBSTR(A.BAS_DAY,1,6)

           UNION ALL

           SELECT A.BAS_DAY,
                  '001' AS KPI_TYP_CD,
                  '2'   AS DATA_RNG_CD,
                  '2'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  B.TOT_PCF_MBR_NUM_CNT AS AGT_VAL
           FROM   (SELECT BAS_DAY
                   FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A
                   GROUP BY BAS_DAY) A INNER JOIN (SELECT BAS_YM
                                                   FROM   FROM_G32_016_TTGS T1 
                                                   GROUP BY BAS_YM
                                                  ) B
                                               ON B.BAS_YM = SUBSTR(A.BAS_DAY,1,6)

           UNION ALL

           SELECT XX.BAS_DAY,
                  '001' AS KPI_TYP_CD,
                  '1'   AS DATA_RNG_CD,
                  '0'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.TOT_PCF_MBR_NUM_CNT AS AGT_VAL
           FROM   (
                   SELECT BAS_DAY, PCF_ID, TOT_PCF_MBR_NUM_CNT, RANK() OVER(PARTITION BY BAS_DAY ORDER BY TOT_PCF_MBR_NUM_CNT DESC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_DAY,
                                  T1.PCF_ID,
                                  T2.TOT_PCF_MBR_NUM_CNT
                           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A T1 INNER JOIN (SELECT BAS_YM, PCF_ID, TOT_PCF_MBR_NUM_CNT
                                                                              FROM   FROM_G32_016_TTGS
                                                                             ) T2
                                                                          ON T2.BAS_YM = SUBSTR(T1.BAS_DAY,1,6)
                                                                         AND T2.PCF_ID = T1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 100

           UNION ALL

           SELECT XX.BAS_DAY,
                  '001' AS KPI_TYP_CD,
                  '1'   AS DATA_RNG_CD,
                  '0'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.TOT_PCF_MBR_NUM_CNT AS AGT_VAL
           FROM   (
                   SELECT BAS_DAY, PCF_ID, TOT_PCF_MBR_NUM_CNT, RANK() OVER(PARTITION BY BAS_DAY ORDER BY TOT_PCF_MBR_NUM_CNT ASC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_DAY,
                                  T1.PCF_ID,
                                  T2.TOT_PCF_MBR_NUM_CNT
                           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A T1 INNER JOIN (SELECT BAS_YM, PCF_ID, TOT_PCF_MBR_NUM_CNT
                                                                              FROM   FROM_G32_016_TTGS
                                                                             ) T2
                                                                          ON T2.BAS_YM = SUBSTR(T1.BAS_DAY,1,6)
                                                                         AND T2.PCF_ID = T1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 100

           UNION ALL

           SELECT BAS_DAY,
                  '001' AS KPI_TYP_CD,
                  '2'   AS DATA_RNG_CD,
                  '3'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(TOT_PCF_MBR_NUM_CNT) AS AGT_VAL
           FROM   (
                   SELECT XX.BAS_DAY,
                          XX.CBV_BR_CD,
                          ROUND(XX.TOT_PCF_MBR_NUM_CNT * YY.PCF_CNT_WEIGHTED_AVG, 0) AS TOT_PCF_MBR_NUM_CNT
                   FROM   (SELECT B.BAS_DAY,
                                  A.CBV_BR_CD,
                                  A.TOT_PCF_MBR_NUM_CNT
                           FROM   (SELECT T1.BAS_YM,
                                          T2.CBV_BR_CD,
                                          SUM(T1.TOT_PCF_MBR_NUM_CNT) AS TOT_PCF_MBR_NUM_CNT
                                   FROM   FROM_G32_016_TTGS T1 INNER JOIN TM00_PCF_D T2
                                                                  ON T1.PCF_ID = T2.PCF_ID
                                   GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                                  ) A INNER JOIN (SELECT A.BAS_DAY,
                                                         B.CBV_BR_CD
                                                  FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A A INNER JOIN TM00_PCF_D B
                                                                                                ON A.PCF_ID = B.PCF_ID
                                                  GROUP BY A.BAS_DAY, B.CBV_BR_CD) B
                                               ON SUBSTR(B.BAS_DAY,1,6) = A.BAS_YM
                                              AND B.CBV_BR_CD           = A.CBV_BR_CD
                          ) XX INNER JOIN PCF_CNT_WEIGHTED_AVG YY
                                       ON YY.BAS_DAY    = XX.BAS_DAY
                                      AND YY.CBV_BR_CD = XX.CBV_BR_CD
                  )
           GROUP BY BAS_DAY
          ),
          FROM_KPI_TYP_002 AS
          (SELECT BAS_DAY,
                  '002' AS KPI_TYP_CD,
                  '1'   AS DATA_RNG_CD,
                  '1'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(TDP_BAL) AS AGT_VAL
           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A
           GROUP BY BAS_DAY

           UNION ALL

           SELECT BAS_DAY,
                  '002' AS KPI_TYP_CD,
                  '1'   AS DATA_RNG_CD,
                  '2'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(TDP_BAL), 0) AS AGT_VAL
           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A
           GROUP BY BAS_DAY

           UNION ALL

           SELECT BAS_DAY,
                  '002' AS KPI_TYP_CD,
                  '2'   AS DATA_RNG_CD,
                  '2'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(TDP_BAL), 0) AS AGT_VAL
           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A
           GROUP BY BAS_DAY

           UNION ALL

           SELECT XX.BAS_DAY,
                  '002' AS KPI_TYP_CD,
                  '1'   AS DATA_RNG_CD,
                  '0'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.TDP_BAL AS AGT_VAL
           FROM   (
                   SELECT BAS_DAY, PCF_ID, TDP_BAL, RANK() OVER(PARTITION BY BAS_DAY ORDER BY TDP_BAL DESC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_DAY,
                                  T1.PCF_ID,
                                  T1.TDP_BAL
                           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A T1
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 100

           UNION ALL

           SELECT XX.BAS_DAY,
                  '002' AS KPI_TYP_CD,
                  '1'   AS DATA_RNG_CD,
                  '0'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.TDP_BAL AS AGT_VAL
           FROM   (
                   SELECT BAS_DAY, PCF_ID, TDP_BAL, RANK() OVER(PARTITION BY BAS_DAY ORDER BY TDP_BAL ASC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_DAY,
                                  T1.PCF_ID,
                                  T1.TDP_BAL
                           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A T1
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 100

           UNION ALL

           SELECT BAS_DAY,
                  '002' AS KPI_TYP_CD,
                  '2'   AS DATA_RNG_CD,
                  '3'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(TDP_BAL) AS AGT_VAL
           FROM   (
                   SELECT XX.BAS_DAY,
                          XX.CBV_BR_CD,
                          ROUND(XX.TDP_BAL * YY.PCF_CNT_WEIGHTED_AVG, 0) AS TDP_BAL
                   FROM   (SELECT T1.BAS_DAY,
                                  T2.CBV_BR_CD,
                                  SUM(T1.TDP_BAL) AS TDP_BAL
                           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_DAY, T2.CBV_BR_CD
                          ) XX INNER JOIN PCF_CNT_WEIGHTED_AVG YY
                                       ON YY.BAS_DAY    = XX.BAS_DAY
                                      AND YY.CBV_BR_CD = XX.CBV_BR_CD
                  )
           GROUP BY BAS_DAY
          ),
          FROM_KPI_TYP_003 AS
          (SELECT BAS_DAY,
                  '003' AS KPI_TYP_CD,
                  '1'   AS DATA_RNG_CD,
                  '1'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(TOT_LN_BAL) AS AGT_VAL
           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A
           GROUP BY BAS_DAY

           UNION ALL

           SELECT BAS_DAY,
                  '003' AS KPI_TYP_CD,
                  '1'   AS DATA_RNG_CD,
                  '2'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(TOT_LN_BAL), 0) AS AGT_VAL
           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A
           GROUP BY BAS_DAY

           UNION ALL

           SELECT BAS_DAY,
                  '003' AS KPI_TYP_CD,
                  '2'   AS DATA_RNG_CD,
                  '2'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(TOT_LN_BAL), 0) AS AGT_VAL
           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A
           GROUP BY BAS_DAY

           UNION ALL

           SELECT XX.BAS_DAY,
                  '003' AS KPI_TYP_CD,
                  '1'   AS DATA_RNG_CD,
                  '0'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.TOT_LN_BAL AS AGT_VAL
           FROM   (
                   SELECT BAS_DAY, PCF_ID, TOT_LN_BAL, RANK() OVER(PARTITION BY BAS_DAY ORDER BY TOT_LN_BAL DESC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_DAY,
                                  T1.PCF_ID,
                                  T1.TOT_LN_BAL
                           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A T1
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 100

           UNION ALL

           SELECT XX.BAS_DAY,
                  '003' AS KPI_TYP_CD,
                  '1'   AS DATA_RNG_CD,
                  '0'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.TOT_LN_BAL AS AGT_VAL
           FROM   (
                   SELECT BAS_DAY, PCF_ID, TOT_LN_BAL, RANK() OVER(PARTITION BY BAS_DAY ORDER BY TOT_LN_BAL ASC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_DAY,
                                  T1.PCF_ID,
                                  T1.TOT_LN_BAL
                           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A T1
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 100

           UNION ALL

           SELECT BAS_DAY,
                  '003' AS KPI_TYP_CD,
                  '2'   AS DATA_RNG_CD,
                  '3'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(TOT_LN_BAL) AS AGT_VAL
           FROM   (
                   SELECT XX.BAS_DAY,
                          XX.CBV_BR_CD,
                          ROUND(XX.TOT_LN_BAL * YY.PCF_CNT_WEIGHTED_AVG, 0) AS TOT_LN_BAL
                   FROM   (SELECT T1.BAS_DAY,
                                  T2.CBV_BR_CD,
                                  SUM(T1.TOT_LN_BAL) AS TOT_LN_BAL
                           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_DAY, T2.CBV_BR_CD
                          ) XX INNER JOIN PCF_CNT_WEIGHTED_AVG YY
                                       ON YY.BAS_DAY    = XX.BAS_DAY
                                      AND YY.CBV_BR_CD = XX.CBV_BR_CD
                  )
           GROUP BY BAS_DAY
          ),
          FROM_KPI_TYP_004 AS
          (SELECT BAS_DAY,
                  '004' AS KPI_TYP_CD,
                  '1'   AS DATA_RNG_CD,
                  '1'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(WO_COLL_LN_BAL) AS AGT_VAL
           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A
           GROUP BY BAS_DAY

           UNION ALL

           SELECT BAS_DAY,
                  '004' AS KPI_TYP_CD,
                  '1'   AS DATA_RNG_CD,
                  '2'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(WO_COLL_LN_BAL), 0) AS AGT_VAL
           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A
           GROUP BY BAS_DAY

           UNION ALL

           SELECT BAS_DAY,
                  '004' AS KPI_TYP_CD,
                  '2'   AS DATA_RNG_CD,
                  '2'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(WO_COLL_LN_BAL), 0) AS AGT_VAL
           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A
           GROUP BY BAS_DAY

           UNION ALL

           SELECT XX.BAS_DAY,
                  '004' AS KPI_TYP_CD,
                  '1'   AS DATA_RNG_CD,
                  '0'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.WO_COLL_LN_BAL AS AGT_VAL
           FROM   (
                   SELECT BAS_DAY, PCF_ID, WO_COLL_LN_BAL, RANK() OVER(PARTITION BY BAS_DAY ORDER BY WO_COLL_LN_BAL DESC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_DAY,
                                  T1.PCF_ID,
                                  T1.WO_COLL_LN_BAL
                           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A T1
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 100

           UNION ALL

           SELECT XX.BAS_DAY,
                  '004' AS KPI_TYP_CD,
                  '1'   AS DATA_RNG_CD,
                  '0'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.WO_COLL_LN_BAL AS AGT_VAL
           FROM   (
                   SELECT BAS_DAY, PCF_ID, WO_COLL_LN_BAL, RANK() OVER(PARTITION BY BAS_DAY ORDER BY WO_COLL_LN_BAL ASC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_DAY,
                                  T1.PCF_ID,
                                  T1.WO_COLL_LN_BAL
                           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A T1
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 100

           UNION ALL

           SELECT BAS_DAY,
                  '004' AS KPI_TYP_CD,
                  '2'   AS DATA_RNG_CD,
                  '3'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(WO_COLL_LN_BAL) AS AGT_VAL
           FROM   (
                   SELECT XX.BAS_DAY,
                          XX.CBV_BR_CD,
                          ROUND(XX.WO_COLL_LN_BAL * YY.PCF_CNT_WEIGHTED_AVG, 0) AS WO_COLL_LN_BAL
                   FROM   (SELECT T1.BAS_DAY,
                                  T2.CBV_BR_CD,
                                  SUM(T1.WO_COLL_LN_BAL) AS WO_COLL_LN_BAL
                           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_DAY, T2.CBV_BR_CD
                          ) XX INNER JOIN PCF_CNT_WEIGHTED_AVG YY
                                       ON YY.BAS_DAY    = XX.BAS_DAY
                                      AND YY.CBV_BR_CD = XX.CBV_BR_CD
                  )
           GROUP BY BAS_DAY
          ),
          FROM_KPI_TYP_005 AS
          (SELECT BAS_DAY,
                  '005' AS KPI_TYP_CD,
                  '1'   AS DATA_RNG_CD,
                  '1'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(BAD_DBT_AMT) AS AGT_VAL
           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A
           GROUP BY BAS_DAY

           UNION ALL

           SELECT BAS_DAY,
                  '005' AS KPI_TYP_CD,
                  '1'   AS DATA_RNG_CD,
                  '2'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(BAD_DBT_AMT), 0) AS AGT_VAL
           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A
           GROUP BY BAS_DAY

           UNION ALL

           SELECT BAS_DAY,
                  '005' AS KPI_TYP_CD,
                  '2'   AS DATA_RNG_CD,
                  '2'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(BAD_DBT_AMT), 0) AS AGT_VAL
           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A
           GROUP BY BAS_DAY

           UNION ALL

           SELECT XX.BAS_DAY,
                  '005' AS KPI_TYP_CD,
                  '1'   AS DATA_RNG_CD,
                  '0'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.BAD_DBT_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_DAY, PCF_ID, BAD_DBT_AMT, RANK() OVER(PARTITION BY BAS_DAY ORDER BY BAD_DBT_AMT DESC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_DAY,
                                  T1.PCF_ID,
                                  T1.BAD_DBT_AMT
                           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A T1
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 100

           UNION ALL

           SELECT XX.BAS_DAY,
                  '005' AS KPI_TYP_CD,
                  '1'   AS DATA_RNG_CD,
                  '0'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.BAD_DBT_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_DAY, PCF_ID, BAD_DBT_AMT, RANK() OVER(PARTITION BY BAS_DAY ORDER BY BAD_DBT_AMT ASC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_DAY,
                                  T1.PCF_ID,
                                  T1.BAD_DBT_AMT
                           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A T1
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 100

           UNION ALL

           SELECT BAS_DAY,
                  '005' AS KPI_TYP_CD,
                  '2'   AS DATA_RNG_CD,
                  '3'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(BAD_DBT_AMT) AS AGT_VAL
           FROM   (
                   SELECT XX.BAS_DAY,
                          XX.CBV_BR_CD,
                          ROUND(XX.BAD_DBT_AMT * YY.PCF_CNT_WEIGHTED_AVG, 0) AS BAD_DBT_AMT
                   FROM   (SELECT T1.BAS_DAY,
                                  T2.CBV_BR_CD,
                                  SUM(T1.BAD_DBT_AMT) AS BAD_DBT_AMT
                           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_DAY, T2.CBV_BR_CD
                          ) XX INNER JOIN PCF_CNT_WEIGHTED_AVG YY
                                       ON YY.BAS_DAY    = XX.BAS_DAY
                                      AND YY.CBV_BR_CD = XX.CBV_BR_CD
                  )
           GROUP BY BAS_DAY
          ),
          FROM_KPI_TYP_006 AS
          (SELECT BAS_DAY,
                  '006' AS KPI_TYP_CD,
                  '1'   AS DATA_RNG_CD,
                  '1'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(EQT_AMT) AS AGT_VAL
           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A
           GROUP BY BAS_DAY

           UNION ALL

           SELECT BAS_DAY,
                  '006' AS KPI_TYP_CD,
                  '1'   AS DATA_RNG_CD,
                  '2'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(EQT_AMT), 0) AS AGT_VAL
           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A
           GROUP BY BAS_DAY

           UNION ALL

           SELECT BAS_DAY,
                  '006' AS KPI_TYP_CD,
                  '2'   AS DATA_RNG_CD,
                  '2'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(EQT_AMT), 0) AS AGT_VAL
           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A
           GROUP BY BAS_DAY

           UNION ALL

           SELECT XX.BAS_DAY,
                  '006' AS KPI_TYP_CD,
                  '1'   AS DATA_RNG_CD,
                  '0'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.EQT_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_DAY, PCF_ID, EQT_AMT, RANK() OVER(PARTITION BY BAS_DAY ORDER BY EQT_AMT DESC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_DAY,
                                  T1.PCF_ID,
                                  T1.EQT_AMT
                           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A T1
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 100

           UNION ALL

           SELECT XX.BAS_DAY,
                  '006' AS KPI_TYP_CD,
                  '1'   AS DATA_RNG_CD,
                  '0'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.EQT_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_DAY, PCF_ID, EQT_AMT, RANK() OVER(PARTITION BY BAS_DAY ORDER BY EQT_AMT ASC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_DAY,
                                  T1.PCF_ID,
                                  T1.EQT_AMT
                           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A T1
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 100

           UNION ALL

           SELECT BAS_DAY,
                  '006' AS KPI_TYP_CD,
                  '2'   AS DATA_RNG_CD,
                  '3'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(EQT_AMT) AS AGT_VAL
           FROM   (
                   SELECT XX.BAS_DAY,
                          XX.CBV_BR_CD,
                          ROUND(XX.EQT_AMT * YY.PCF_CNT_WEIGHTED_AVG, 0) AS EQT_AMT
                   FROM   (SELECT T1.BAS_DAY,
                                  T2.CBV_BR_CD,
                                  SUM(T1.EQT_AMT) AS EQT_AMT
                           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_DAY, T2.CBV_BR_CD
                          ) XX INNER JOIN PCF_CNT_WEIGHTED_AVG YY
                                       ON YY.BAS_DAY    = XX.BAS_DAY
                                      AND YY.CBV_BR_CD = XX.CBV_BR_CD
                  )
           GROUP BY BAS_DAY
          ),
          FROM_KPI_TYP_007 AS
          (SELECT BAS_DAY,
                  '007' AS KPI_TYP_CD,
                  '1'   AS DATA_RNG_CD,
                  '1'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(CCAP_AMT) AS AGT_VAL
           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A
           GROUP BY BAS_DAY

           UNION ALL

           SELECT BAS_DAY,
                  '007' AS KPI_TYP_CD,
                  '1'   AS DATA_RNG_CD,
                  '2'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(CCAP_AMT), 0) AS AGT_VAL
           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A
           GROUP BY BAS_DAY

           UNION ALL

           SELECT BAS_DAY,
                  '007' AS KPI_TYP_CD,
                  '2'   AS DATA_RNG_CD,
                  '2'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(CCAP_AMT), 0) AS AGT_VAL
           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A
           GROUP BY BAS_DAY

           UNION ALL

           SELECT XX.BAS_DAY,
                  '007' AS KPI_TYP_CD,
                  '1'   AS DATA_RNG_CD,
                  '0'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.CCAP_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_DAY, PCF_ID, CCAP_AMT, RANK() OVER(PARTITION BY BAS_DAY ORDER BY CCAP_AMT DESC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_DAY,
                                  T1.PCF_ID,
                                  T1.CCAP_AMT
                           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A T1
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 100

           UNION ALL

           SELECT XX.BAS_DAY,
                  '007' AS KPI_TYP_CD,
                  '1'   AS DATA_RNG_CD,
                  '0'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.CCAP_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_DAY, PCF_ID, CCAP_AMT, RANK() OVER(PARTITION BY BAS_DAY ORDER BY CCAP_AMT ASC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_DAY,
                                  T1.PCF_ID,
                                  T1.CCAP_AMT
                           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A T1
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 100

           UNION ALL

           SELECT BAS_DAY,
                  '007' AS KPI_TYP_CD,
                  '2'   AS DATA_RNG_CD,
                  '3'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(CCAP_AMT) AS AGT_VAL
           FROM   (
                   SELECT XX.BAS_DAY,
                          XX.CBV_BR_CD,
                          ROUND(XX.CCAP_AMT * YY.PCF_CNT_WEIGHTED_AVG, 0) AS CCAP_AMT
                   FROM   (SELECT T1.BAS_DAY,
                                  T2.CBV_BR_CD,
                                  SUM(T1.CCAP_AMT) AS CCAP_AMT
                           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_DAY, T2.CBV_BR_CD
                          ) XX INNER JOIN PCF_CNT_WEIGHTED_AVG YY
                                       ON YY.BAS_DAY    = XX.BAS_DAY
                                      AND YY.CBV_BR_CD = XX.CBV_BR_CD
                  )
           GROUP BY BAS_DAY
          ),
          FROM_KPI_TYP_008 AS
          (SELECT BAS_DAY,
                  '008' AS KPI_TYP_CD,
                  '1'   AS DATA_RNG_CD,
                  '1'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(CBV_BOR_EXCL_SFTY_FUND_AMT) AS AGT_VAL
           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A
           GROUP BY BAS_DAY

           UNION ALL

           SELECT BAS_DAY,
                  '008' AS KPI_TYP_CD,
                  '1'   AS DATA_RNG_CD,
                  '2'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(CBV_BOR_EXCL_SFTY_FUND_AMT), 0) AS AGT_VAL
           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A
           GROUP BY BAS_DAY

           UNION ALL

           SELECT BAS_DAY,
                  '008' AS KPI_TYP_CD,
                  '2'   AS DATA_RNG_CD,
                  '2'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(CBV_BOR_EXCL_SFTY_FUND_AMT), 0) AS AGT_VAL
           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A
           GROUP BY BAS_DAY

           UNION ALL

           SELECT XX.BAS_DAY,
                  '008' AS KPI_TYP_CD,
                  '1'   AS DATA_RNG_CD,
                  '0'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.CBV_BOR_EXCL_SFTY_FUND_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_DAY, PCF_ID, CBV_BOR_EXCL_SFTY_FUND_AMT, RANK() OVER(PARTITION BY BAS_DAY ORDER BY CBV_BOR_EXCL_SFTY_FUND_AMT DESC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_DAY,
                                  T1.PCF_ID,
                                  T1.CBV_BOR_EXCL_SFTY_FUND_AMT
                           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A T1
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 100

           UNION ALL

           SELECT XX.BAS_DAY,
                  '008' AS KPI_TYP_CD,
                  '1'   AS DATA_RNG_CD,
                  '0'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.CBV_BOR_EXCL_SFTY_FUND_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_DAY, PCF_ID, CBV_BOR_EXCL_SFTY_FUND_AMT, RANK() OVER(PARTITION BY BAS_DAY ORDER BY CBV_BOR_EXCL_SFTY_FUND_AMT ASC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_DAY,
                                  T1.PCF_ID,
                                  T1.CBV_BOR_EXCL_SFTY_FUND_AMT
                           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A T1
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 100

           UNION ALL

           SELECT BAS_DAY,
                  '008' AS KPI_TYP_CD,
                  '2'   AS DATA_RNG_CD,
                  '3'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(CBV_BOR_EXCL_SFTY_FUND_AMT) AS AGT_VAL
           FROM   (
                   SELECT XX.BAS_DAY,
                          XX.CBV_BR_CD,
                          ROUND(XX.CBV_BOR_EXCL_SFTY_FUND_AMT * YY.PCF_CNT_WEIGHTED_AVG, 0) AS CBV_BOR_EXCL_SFTY_FUND_AMT
                   FROM   (SELECT T1.BAS_DAY,
                                  T2.CBV_BR_CD,
                                  SUM(T1.CBV_BOR_EXCL_SFTY_FUND_AMT) AS CBV_BOR_EXCL_SFTY_FUND_AMT
                           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_DAY, T2.CBV_BR_CD
                          ) XX INNER JOIN PCF_CNT_WEIGHTED_AVG YY
                                       ON YY.BAS_DAY    = XX.BAS_DAY
                                      AND YY.CBV_BR_CD = XX.CBV_BR_CD
                  )
           GROUP BY BAS_DAY
          ),
          FROM_KPI_TYP_009 AS
          (SELECT BAS_DAY,
                  '009' AS KPI_TYP_CD,
                  '1'   AS DATA_RNG_CD,
                  '1'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(CBV_SVG_AMT) AS AGT_VAL
           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A
           GROUP BY BAS_DAY

           UNION ALL

           SELECT BAS_DAY,
                  '009' AS KPI_TYP_CD,
                  '1'   AS DATA_RNG_CD,
                  '2'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(CBV_SVG_AMT), 0) AS AGT_VAL
           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A
           GROUP BY BAS_DAY

           UNION ALL

           SELECT BAS_DAY,
                  '009' AS KPI_TYP_CD,
                  '2'   AS DATA_RNG_CD,
                  '2'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(CBV_SVG_AMT), 0) AS AGT_VAL
           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A
           GROUP BY BAS_DAY

           UNION ALL

           SELECT XX.BAS_DAY,
                  '009' AS KPI_TYP_CD,
                  '1'   AS DATA_RNG_CD,
                  '0'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.CBV_SVG_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_DAY, PCF_ID, CBV_SVG_AMT, RANK() OVER(PARTITION BY BAS_DAY ORDER BY CBV_SVG_AMT DESC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_DAY,
                                  T1.PCF_ID,
                                  T1.CBV_SVG_AMT
                           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A T1
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 100

           UNION ALL

           SELECT XX.BAS_DAY,
                  '009' AS KPI_TYP_CD,
                  '1'   AS DATA_RNG_CD,
                  '0'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.CBV_SVG_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_DAY, PCF_ID, CBV_SVG_AMT, RANK() OVER(PARTITION BY BAS_DAY ORDER BY CBV_SVG_AMT ASC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_DAY,
                                  T1.PCF_ID,
                                  T1.CBV_SVG_AMT
                           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A T1
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 100

           UNION ALL

           SELECT BAS_DAY,
                  '009' AS KPI_TYP_CD,
                  '2'   AS DATA_RNG_CD,
                  '3'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(CBV_SVG_AMT) AS AGT_VAL
           FROM   (
                   SELECT XX.BAS_DAY,
                          XX.CBV_BR_CD,
                          ROUND(XX.CBV_SVG_AMT * YY.PCF_CNT_WEIGHTED_AVG, 0) AS CBV_SVG_AMT
                   FROM   (SELECT T1.BAS_DAY,
                                  T2.CBV_BR_CD,
                                  SUM(T1.CBV_SVG_AMT) AS CBV_SVG_AMT
                           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_DAY, T2.CBV_BR_CD
                          ) XX INNER JOIN PCF_CNT_WEIGHTED_AVG YY
                                       ON YY.BAS_DAY    = XX.BAS_DAY
                                      AND YY.CBV_BR_CD = XX.CBV_BR_CD
                  )
           GROUP BY BAS_DAY
          ),
          FROM_KPI_TYP_010 AS
          (SELECT BAS_DAY,
                  '010' AS KPI_TYP_CD,
                  '1'   AS DATA_RNG_CD,
                  '1'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(OTHR_INST_SVG_AMT) AS AGT_VAL
           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A
           GROUP BY BAS_DAY

           UNION ALL

           SELECT BAS_DAY,
                  '010' AS KPI_TYP_CD,
                  '1'   AS DATA_RNG_CD,
                  '2'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(OTHR_INST_SVG_AMT), 0) AS AGT_VAL
           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A
           GROUP BY BAS_DAY

           UNION ALL

           SELECT BAS_DAY,
                  '010' AS KPI_TYP_CD,
                  '2'   AS DATA_RNG_CD,
                  '2'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(OTHR_INST_SVG_AMT), 0) AS AGT_VAL
           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A
           GROUP BY BAS_DAY

           UNION ALL

           SELECT XX.BAS_DAY,
                  '010' AS KPI_TYP_CD,
                  '1'   AS DATA_RNG_CD,
                  '0'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.OTHR_INST_SVG_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_DAY, PCF_ID, OTHR_INST_SVG_AMT, RANK() OVER(PARTITION BY BAS_DAY ORDER BY OTHR_INST_SVG_AMT DESC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_DAY,
                                  T1.PCF_ID,
                                  T1.OTHR_INST_SVG_AMT
                           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A T1
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 100

           UNION ALL

           SELECT XX.BAS_DAY,
                  '010' AS KPI_TYP_CD,
                  '1'   AS DATA_RNG_CD,
                  '0'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.OTHR_INST_SVG_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_DAY, PCF_ID, OTHR_INST_SVG_AMT, RANK() OVER(PARTITION BY BAS_DAY ORDER BY OTHR_INST_SVG_AMT ASC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_DAY,
                                  T1.PCF_ID,
                                  T1.OTHR_INST_SVG_AMT
                           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A T1
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 100

           UNION ALL

           SELECT BAS_DAY,
                  '010' AS KPI_TYP_CD,
                  '2'   AS DATA_RNG_CD,
                  '3'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(OTHR_INST_SVG_AMT) AS AGT_VAL
           FROM   (
                   SELECT XX.BAS_DAY,
                          XX.CBV_BR_CD,
                          ROUND(XX.OTHR_INST_SVG_AMT * YY.PCF_CNT_WEIGHTED_AVG, 0) AS OTHR_INST_SVG_AMT
                   FROM   (SELECT T1.BAS_DAY,
                                  T2.CBV_BR_CD,
                                  SUM(T1.OTHR_INST_SVG_AMT) AS OTHR_INST_SVG_AMT
                           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_DAY, T2.CBV_BR_CD
                          ) XX INNER JOIN PCF_CNT_WEIGHTED_AVG YY
                                       ON YY.BAS_DAY    = XX.BAS_DAY
                                      AND YY.CBV_BR_CD = XX.CBV_BR_CD
                  )
           GROUP BY BAS_DAY
          ),
          FROM_KPI_TYP_067 AS
          (SELECT BAS_DAY,
                  '067' AS KPI_TYP_CD,
                  '1'   AS DATA_RNG_CD,
                  '2'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(NXT_WRK_DAY_LQDTY_RTO), 3) AS AGT_VAL
           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A
           GROUP BY BAS_DAY

           UNION ALL

           SELECT XX.BAS_DAY,
                  '067' AS KPI_TYP_CD,
                  '1'   AS DATA_RNG_CD,
                  '0'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.NXT_WRK_DAY_LQDTY_RTO AS AGT_VAL
           FROM   (
                   SELECT BAS_DAY, PCF_ID, NXT_WRK_DAY_LQDTY_RTO, RANK() OVER(PARTITION BY BAS_DAY ORDER BY NXT_WRK_DAY_LQDTY_RTO DESC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_DAY,
                                  T1.PCF_ID,
                                  T1.NXT_WRK_DAY_LQDTY_RTO
                           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A T1
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 100

           UNION ALL

           SELECT XX.BAS_DAY,
                  '067' AS KPI_TYP_CD,
                  '1'   AS DATA_RNG_CD,
                  '0'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.NXT_WRK_DAY_LQDTY_RTO AS AGT_VAL
           FROM   (
                   SELECT BAS_DAY, PCF_ID, NXT_WRK_DAY_LQDTY_RTO, RANK() OVER(PARTITION BY BAS_DAY ORDER BY NXT_WRK_DAY_LQDTY_RTO ASC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_DAY,
                                  T1.PCF_ID,
                                  T1.NXT_WRK_DAY_LQDTY_RTO
                           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A T1
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 100
          ),
          FROM_KPI_TYP_068 AS
          (SELECT BAS_DAY,
                  '068' AS KPI_TYP_CD,
                  '1'   AS DATA_RNG_CD,
                  '2'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(NXT_7_WRK_DAY_LQDTY_RTO), 3) AS AGT_VAL
           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A
           GROUP BY BAS_DAY

           UNION ALL

           SELECT XX.BAS_DAY,
                  '068' AS KPI_TYP_CD,
                  '1'   AS DATA_RNG_CD,
                  '0'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.NXT_7_WRK_DAY_LQDTY_RTO AS AGT_VAL
           FROM   (
                   SELECT BAS_DAY, PCF_ID, NXT_7_WRK_DAY_LQDTY_RTO, RANK() OVER(PARTITION BY BAS_DAY ORDER BY NXT_7_WRK_DAY_LQDTY_RTO DESC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_DAY,
                                  T1.PCF_ID,
                                  T1.NXT_7_WRK_DAY_LQDTY_RTO
                           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A T1
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 100

           UNION ALL

           SELECT XX.BAS_DAY,
                  '068' AS KPI_TYP_CD,
                  '1'   AS DATA_RNG_CD,
                  '0'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.NXT_7_WRK_DAY_LQDTY_RTO AS AGT_VAL
           FROM   (
                   SELECT BAS_DAY, PCF_ID, NXT_7_WRK_DAY_LQDTY_RTO, RANK() OVER(PARTITION BY BAS_DAY ORDER BY NXT_7_WRK_DAY_LQDTY_RTO ASC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_DAY,
                                  T1.PCF_ID,
                                  T1.NXT_7_WRK_DAY_LQDTY_RTO
                           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A T1
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 100
          ),
          FROM_KPI_TYP_143 AS
          (SELECT BAS_DAY,
                  '143' AS KPI_TYP_CD,
                  '1'   AS DATA_RNG_CD,
                  '1'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(CUR_TOT_INCM_AMT - CUR_TOT_EXP_AMT) AS AGT_VAL
           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A
           GROUP BY BAS_DAY

           UNION ALL

           SELECT BAS_DAY,
                  '143' AS KPI_TYP_CD,
                  '1'   AS DATA_RNG_CD,
                  '2'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(CUR_TOT_INCM_AMT - CUR_TOT_EXP_AMT), 0) AS AGT_VAL
           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A
           GROUP BY BAS_DAY

           UNION ALL

           SELECT BAS_DAY,
                  '143' AS KPI_TYP_CD,
                  '2'   AS DATA_RNG_CD,
                  '2'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(CUR_TOT_INCM_AMT - CUR_TOT_EXP_AMT), 0) AS AGT_VAL
           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A
           GROUP BY BAS_DAY

           UNION ALL

           SELECT BAS_DAY,
                  '143' AS KPI_TYP_CD,
                  '2'   AS DATA_RNG_CD,
                  '3'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(NET_PROFIT) AS AGT_VAL
           FROM   (
                   SELECT XX.BAS_DAY,
                          XX.CBV_BR_CD,
                          ROUND(XX.NET_PROFIT * YY.PCF_CNT_WEIGHTED_AVG, 0) AS NET_PROFIT
                   FROM   (SELECT T1.BAS_DAY,
                                  T2.CBV_BR_CD,
                                  SUM(T1.CUR_TOT_INCM_AMT - T1.CUR_TOT_EXP_AMT) AS NET_PROFIT
                           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_DAY, T2.CBV_BR_CD
                          ) XX INNER JOIN PCF_CNT_WEIGHTED_AVG YY
                                       ON YY.BAS_DAY    = XX.BAS_DAY
                                      AND YY.CBV_BR_CD = XX.CBV_BR_CD
                  )
           GROUP BY BAS_DAY

           UNION ALL

           SELECT XX.BAS_DAY,
                  '143' AS KPI_TYP_CD,
                  '1'   AS DATA_RNG_CD,
                  '0'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.NET_PROFIT AS AGT_VAL
           FROM   (
                   SELECT BAS_DAY, PCF_ID, NET_PROFIT, RANK() OVER(PARTITION BY BAS_DAY ORDER BY NET_PROFIT DESC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT BAS_DAY,
                                  PCF_ID,
                                  CUR_TOT_INCM_AMT - CUR_TOT_EXP_AMT AS NET_PROFIT
                           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 100

           UNION ALL

           SELECT XX.BAS_DAY,
                  '143' AS KPI_TYP_CD,
                  '1'   AS DATA_RNG_CD,
                  '0'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.NET_PROFIT AS AGT_VAL
           FROM   (
                   SELECT BAS_DAY, PCF_ID, NET_PROFIT, RANK() OVER(PARTITION BY BAS_DAY ORDER BY NET_PROFIT ASC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT BAS_DAY,
                                  PCF_ID,
                                  CUR_TOT_INCM_AMT - CUR_TOT_EXP_AMT AS NET_PROFIT
                           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 100
          ),
          FROM_KPI_TYP_133 AS
          (SELECT BAS_DAY,
                  '133' AS KPI_TYP_CD,
                  '1'   AS DATA_RNG_CD,
                  '1'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(WIT_COLL_LN_BAL) AS AGT_VAL
           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A
           GROUP BY BAS_DAY

           UNION ALL

           SELECT BAS_DAY,
                  '133' AS KPI_TYP_CD,
                  '2'   AS DATA_RNG_CD,
                  '2'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(WIT_COLL_LN_BAL), 0) AS AGT_VAL
           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A
           GROUP BY BAS_DAY

           UNION ALL

           SELECT BAS_DAY,
                  '133' AS KPI_TYP_CD,
                  '2'   AS DATA_RNG_CD,
                  '3'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(WIT_COLL_LN_BAL) AS AGT_VAL
           FROM   (
                   SELECT XX.BAS_DAY,
                          XX.CBV_BR_CD,
                          ROUND(XX.WIT_COLL_LN_BAL * YY.PCF_CNT_WEIGHTED_AVG, 0) AS WIT_COLL_LN_BAL
                   FROM   (SELECT T1.BAS_DAY,
                                  T2.CBV_BR_CD,
                                  SUM(T1.WIT_COLL_LN_BAL) AS WIT_COLL_LN_BAL
                           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_DAY, T2.CBV_BR_CD
                          ) XX INNER JOIN PCF_CNT_WEIGHTED_AVG YY
                                       ON YY.BAS_DAY    = XX.BAS_DAY
                                      AND YY.CBV_BR_CD = XX.CBV_BR_CD
                  )
           GROUP BY BAS_DAY

           UNION ALL

           SELECT XX.BAS_DAY,
                  '133' AS KPI_TYP_CD,
                  '1'   AS DATA_RNG_CD,
                  '0'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  WIT_COLL_LN_BAL AS AGT_VAL
           FROM   (
                   SELECT BAS_DAY, PCF_ID, WIT_COLL_LN_BAL, RANK() OVER(PARTITION BY BAS_DAY ORDER BY WIT_COLL_LN_BAL DESC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT A.BAS_DAY,
                                  A.PCF_ID,
                                  A.WIT_COLL_LN_BAL
                           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A A
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 100

           UNION ALL

           SELECT XX.BAS_DAY,
                  '133' AS KPI_TYP_CD,
                  '1'   AS DATA_RNG_CD,
                  '0'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  WIT_COLL_LN_BAL AS AGT_VAL
           FROM   (
                   SELECT BAS_DAY, PCF_ID, WIT_COLL_LN_BAL, RANK() OVER(PARTITION BY BAS_DAY ORDER BY WIT_COLL_LN_BAL ASC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT A.BAS_DAY,
                                  A.PCF_ID,
                                  A.WIT_COLL_LN_BAL
                           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A A
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 100
          ),
          FROM_TB07_G32_005_TTGS_A_3M_SET1 AS
          (SELECT XX.BAS_DAY,
                  '1' AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.NXT_WRK_DAY_LQDTY_RTO
           FROM   (
                   SELECT BAS_DAY, PCF_ID, NXT_WRK_DAY_LQDTY_RTO, RANK() OVER(PARTITION BY BAS_DAY ORDER BY NXT_WRK_DAY_LQDTY_RTO DESC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (SELECT X1.BAS_DAY,
                                  X1.PCF_ID,
                                  X2.NXT_WRK_DAY_LQDTY_RTO
                           FROM  (
                                  SELECT T1.BAS_DAY,
                                         T1.PCF_ID,
                                         MAX(T2.BAS_DAY) AS BAS_DAY1
                                  FROM   TM27_DDLY_PCF_KEY_IDC_A T1 INNER JOIN TM27_DDLY_PCF_KEY_IDC_A T2
                                                                            ON T2.BAS_DAY <= T1.BAS_DAY
                                                                           AND T2.PCF_ID   = T1.PCF_ID
                                                                           AND T2.NXT_WRK_DAY_LQDTY_RTO IS NOT NULL
                                  WHERE  T1.BAS_DAY BETWEEN TO_CHAR(TO_DATE(loop_bas_day.BAS_DAY, 'YYYYMMDD') - 120, 'YYYYMMDD') AND loop_bas_day.BAS_DAY
                                  GROUP BY T1.BAS_DAY, T1.PCF_ID
                                 ) X1 INNER JOIN  TM27_DDLY_PCF_KEY_IDC_A X2
                                              ON  X2.BAS_DAY = X1.BAS_DAY1
                                             AND  X2.PCF_ID  = X1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 100

           UNION ALL

           SELECT XX.BAS_DAY,
                  '2' AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.NXT_WRK_DAY_LQDTY_RTO
           FROM   (
                   SELECT BAS_DAY, PCF_ID, NXT_WRK_DAY_LQDTY_RTO, RANK() OVER(PARTITION BY BAS_DAY ORDER BY NXT_WRK_DAY_LQDTY_RTO ASC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (SELECT X1.BAS_DAY,
                                  X1.PCF_ID,
                                  X2.NXT_WRK_DAY_LQDTY_RTO
                           FROM  (
                                  SELECT T1.BAS_DAY,
                                         T1.PCF_ID,
                                         MAX(T2.BAS_DAY) AS BAS_DAY1
                                  FROM   TM27_DDLY_PCF_KEY_IDC_A T1 INNER JOIN TM27_DDLY_PCF_KEY_IDC_A T2
                                                                            ON T2.BAS_DAY <= T1.BAS_DAY
                                                                           AND T2.PCF_ID   = T1.PCF_ID
                                                                           AND T2.NXT_WRK_DAY_LQDTY_RTO IS NOT NULL
                                  WHERE  T1.BAS_DAY BETWEEN TO_CHAR(TO_DATE(loop_bas_day.BAS_DAY, 'YYYYMMDD') - 120, 'YYYYMMDD') AND loop_bas_day.BAS_DAY
                                  GROUP BY T1.BAS_DAY, T1.PCF_ID
                                 ) X1 INNER JOIN  TM27_DDLY_PCF_KEY_IDC_A X2
                                              ON  X2.BAS_DAY = X1.BAS_DAY1
                                             AND  X2.PCF_ID  = X1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 100
          ),
          FROM_TB07_G32_005_TTGS_A_3M_SET2 AS
          (SELECT T1.BAS_DAY AS BAS_DAY1,
                  T1.TOP_BTOM_TYP_CD,
                  T1.RNK_NUM,
                  MAX(T2.BAS_DAY) AS BAS_DAY2,
                  MAX(T3.BAS_DAY) AS BAS_DAY3
           FROM   FROM_TB07_G32_005_TTGS_A_3M_SET1 T1 LEFT OUTER JOIN FROM_TB07_G32_005_TTGS_A_3M_SET1 T2
                                                              ON T2.BAS_DAY <= TO_CHAR(ADD_MONTHS(TO_DATE(T1.BAS_DAY, 'YYYYMMDD'), -1), 'YYYYMMDD')
                                                             AND T2.TOP_BTOM_TYP_CD = T1.TOP_BTOM_TYP_CD
                                                             AND T2.RNK_NUM = T1.RNK_NUM
                                                 LEFT OUTER JOIN FROM_TB07_G32_005_TTGS_A_3M_SET1 T3
                                                              ON T3.BAS_DAY <= TO_CHAR(ADD_MONTHS(TO_DATE(T1.BAS_DAY, 'YYYYMMDD'), -2), 'YYYYMMDD')
                                                             AND T3.TOP_BTOM_TYP_CD = T1.TOP_BTOM_TYP_CD
                                                             AND T3.RNK_NUM = T1.RNK_NUM
           WHERE  T1.BAS_DAY = loop_bas_day.BAS_DAY
           GROUP BY T1.BAS_DAY, T1.TOP_BTOM_TYP_CD, T1.RNK_NUM
          ),
          FROM_KPI_TYP_139 AS
          (SELECT BAS_DAY,
                  '139' AS KPI_TYP_CD,
                  '1'   AS DATA_RNG_CD,
                  '5'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(L3M_NXT_WRK_DAY_LQDTY_RTO), 3) AS AGT_VAL
           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A
           GROUP BY BAS_DAY

           UNION ALL

           SELECT XX.BAS_DAY,
                  '139' AS KPI_TYP_CD,
                  '1'   AS DATA_RNG_CD,
                  '5'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PCF_ID,
                  XX.L3M_NXT_WRK_DAY_LQDTY_RTO AS AGT_VAL
           FROM  (
                  SELECT BAS_DAY, L3M_NXT_WRK_DAY_LQDTY_RTO, RANK() OVER(PARTITION BY BAS_DAY ORDER BY L3M_NXT_WRK_DAY_LQDTY_RTO DESC NULLS LAST, RNK_NUM DESC) AS RNK_NUM
                  FROM  (
                         SELECT T1.BAS_DAY1 AS BAS_DAY,
                                T1.RNK_NUM,
                                CASE WHEN NVL(T2.NXT_WRK_DAY_LQDTY_RTO, 0) + NVL(T3.NXT_WRK_DAY_LQDTY_RTO, 0) + NVL(T4.NXT_WRK_DAY_LQDTY_RTO, 0) = 0 THEN 0
                                     WHEN CASE WHEN T2.NXT_WRK_DAY_LQDTY_RTO IS NOT NULL THEN 1 ELSE 0 END +
                                          CASE WHEN T3.NXT_WRK_DAY_LQDTY_RTO IS NOT NULL THEN 1 ELSE 0 END +
                                          CASE WHEN T4.NXT_WRK_DAY_LQDTY_RTO IS NOT NULL THEN 1 ELSE 0 END = 0 THEN 99999.99
                                     ELSE
                                          ROUND((NVL(T2.NXT_WRK_DAY_LQDTY_RTO, 0) +
                                                 NVL(T3.NXT_WRK_DAY_LQDTY_RTO, 0) +
                                                 NVL(T4.NXT_WRK_DAY_LQDTY_RTO, 0)
                                                ) / (CASE WHEN T2.NXT_WRK_DAY_LQDTY_RTO IS NOT NULL THEN 1 ELSE 0 END +
                                                     CASE WHEN T3.NXT_WRK_DAY_LQDTY_RTO IS NOT NULL THEN 1 ELSE 0 END +
                                                     CASE WHEN T4.NXT_WRK_DAY_LQDTY_RTO IS NOT NULL THEN 1 ELSE 0 END
                                                    ), 3)
                                     END AS L3M_NXT_WRK_DAY_LQDTY_RTO
                         FROM   FROM_TB07_G32_005_TTGS_A_3M_SET2 T1 INNER JOIN FROM_TB07_G32_005_TTGS_A_3M_SET1 T2
                                                                       ON T2.BAS_DAY = T1.BAS_DAY1
                                                                      AND T2.TOP_BTOM_TYP_CD = T1.TOP_BTOM_TYP_CD
                                                                      AND T2.RNK_NUM = T1.RNK_NUM
                                                               INNER JOIN FROM_TB07_G32_005_TTGS_A_3M_SET1 T3
                                                                       ON T3.BAS_DAY = T1.BAS_DAY2
                                                                      AND T3.TOP_BTOM_TYP_CD = T1.TOP_BTOM_TYP_CD
                                                                      AND T3.RNK_NUM = T1.RNK_NUM
                                                               INNER JOIN FROM_TB07_G32_005_TTGS_A_3M_SET1 T4
                                                                       ON T4.BAS_DAY = T1.BAS_DAY3
                                                                      AND T4.TOP_BTOM_TYP_CD = T1.TOP_BTOM_TYP_CD
                                                                      AND T4.RNK_NUM = T1.RNK_NUM
                         WHERE T1.TOP_BTOM_TYP_CD = '1'
                        )
                 ) XX

           UNION ALL

           SELECT XX.BAS_DAY,
                  '139' AS KPI_TYP_CD,
                  '1'   AS DATA_RNG_CD,
                  '5'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PCF_ID,
                  XX.L3M_NXT_WRK_DAY_LQDTY_RTO AS AGT_VAL
           FROM  (
                  SELECT BAS_DAY, L3M_NXT_WRK_DAY_LQDTY_RTO, RANK() OVER(PARTITION BY BAS_DAY ORDER BY L3M_NXT_WRK_DAY_LQDTY_RTO ASC NULLS LAST, RNK_NUM DESC) AS RNK_NUM
                  FROM  (
                         SELECT T1.BAS_DAY1 AS BAS_DAY,
                                T1.RNK_NUM,
                                CASE WHEN NVL(T2.NXT_WRK_DAY_LQDTY_RTO, 0) + NVL(T3.NXT_WRK_DAY_LQDTY_RTO, 0) + NVL(T4.NXT_WRK_DAY_LQDTY_RTO, 0) = 0 THEN 0
                                     WHEN CASE WHEN T2.NXT_WRK_DAY_LQDTY_RTO IS NOT NULL THEN 1 ELSE 0 END +
                                          CASE WHEN T3.NXT_WRK_DAY_LQDTY_RTO IS NOT NULL THEN 1 ELSE 0 END +
                                          CASE WHEN T4.NXT_WRK_DAY_LQDTY_RTO IS NOT NULL THEN 1 ELSE 0 END = 0 THEN 99999.99
                                     ELSE
                                          ROUND((NVL(T2.NXT_WRK_DAY_LQDTY_RTO, 0) +
                                                 NVL(T3.NXT_WRK_DAY_LQDTY_RTO, 0) +
                                                 NVL(T4.NXT_WRK_DAY_LQDTY_RTO, 0)
                                                ) / (CASE WHEN T2.NXT_WRK_DAY_LQDTY_RTO IS NOT NULL THEN 1 ELSE 0 END +
                                                     CASE WHEN T3.NXT_WRK_DAY_LQDTY_RTO IS NOT NULL THEN 1 ELSE 0 END +
                                                     CASE WHEN T4.NXT_WRK_DAY_LQDTY_RTO IS NOT NULL THEN 1 ELSE 0 END
                                                    ), 3)
                                     END AS L3M_NXT_WRK_DAY_LQDTY_RTO
                         FROM   FROM_TB07_G32_005_TTGS_A_3M_SET2 T1 INNER JOIN FROM_TB07_G32_005_TTGS_A_3M_SET1 T2
                                                                       ON T2.BAS_DAY = T1.BAS_DAY1
                                                                      AND T2.TOP_BTOM_TYP_CD = T1.TOP_BTOM_TYP_CD
                                                                      AND T2.RNK_NUM = T1.RNK_NUM
                                                               INNER JOIN FROM_TB07_G32_005_TTGS_A_3M_SET1 T3
                                                                       ON T3.BAS_DAY = T1.BAS_DAY2
                                                                      AND T3.TOP_BTOM_TYP_CD = T1.TOP_BTOM_TYP_CD
                                                                      AND T3.RNK_NUM = T1.RNK_NUM
                                                               INNER JOIN FROM_TB07_G32_005_TTGS_A_3M_SET1 T4
                                                                       ON T4.BAS_DAY = T1.BAS_DAY3
                                                                      AND T4.TOP_BTOM_TYP_CD = T1.TOP_BTOM_TYP_CD
                                                                      AND T4.RNK_NUM = T1.RNK_NUM
                         WHERE T1.TOP_BTOM_TYP_CD = '2'
                        )
                 ) XX
          ),
          FROM_TB07_G32_005_TTGS_A_3M_SET3 AS
          (SELECT XX.BAS_DAY,
                  '1' AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.NXT_7_WRK_DAY_LQDTY_RTO
           FROM   (
                   SELECT BAS_DAY, PCF_ID, NXT_7_WRK_DAY_LQDTY_RTO, RANK() OVER(PARTITION BY BAS_DAY ORDER BY NXT_7_WRK_DAY_LQDTY_RTO DESC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (SELECT X1.BAS_DAY,
                                  X1.PCF_ID,
                                  X2.NXT_7_WRK_DAY_LQDTY_RTO
                           FROM  (
                                  SELECT T1.BAS_DAY,
                                         T1.PCF_ID,
                                         MAX(T2.BAS_DAY) AS BAS_DAY1
                                  FROM   TM27_DDLY_PCF_KEY_IDC_A T1 INNER JOIN TM27_DDLY_PCF_KEY_IDC_A T2
                                                                            ON T2.BAS_DAY <= T1.BAS_DAY
                                                                           AND T2.PCF_ID   = T1.PCF_ID
                                                                           AND T2.NXT_7_WRK_DAY_LQDTY_RTO IS NOT NULL
                                  WHERE  T1.BAS_DAY BETWEEN TO_CHAR(TO_DATE(loop_bas_day.BAS_DAY, 'YYYYMMDD') - 120, 'YYYYMMDD') AND loop_bas_day.BAS_DAY
                                  GROUP BY T1.BAS_DAY, T1.PCF_ID
                                 ) X1 INNER JOIN  TM27_DDLY_PCF_KEY_IDC_A X2
                                              ON  X2.BAS_DAY = X1.BAS_DAY1
                                             AND  X2.PCF_ID  = X1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 100

           UNION ALL

           SELECT XX.BAS_DAY,
                  '2' AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.NXT_7_WRK_DAY_LQDTY_RTO
           FROM   (
                   SELECT BAS_DAY, PCF_ID, NXT_7_WRK_DAY_LQDTY_RTO, RANK() OVER(PARTITION BY BAS_DAY ORDER BY NXT_7_WRK_DAY_LQDTY_RTO ASC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (SELECT X1.BAS_DAY,
                                  X1.PCF_ID,
                                  X2.NXT_7_WRK_DAY_LQDTY_RTO
                           FROM  (
                                  SELECT T1.BAS_DAY,
                                         T1.PCF_ID,
                                         MAX(T2.BAS_DAY) AS BAS_DAY1
                                  FROM   TM27_DDLY_PCF_KEY_IDC_A T1 INNER JOIN TM27_DDLY_PCF_KEY_IDC_A T2
                                                                            ON T2.BAS_DAY <= T1.BAS_DAY
                                                                           AND T2.PCF_ID   = T1.PCF_ID
                                                                           AND T2.NXT_7_WRK_DAY_LQDTY_RTO IS NOT NULL
                                  WHERE  T1.BAS_DAY BETWEEN TO_CHAR(TO_DATE(loop_bas_day.BAS_DAY, 'YYYYMMDD') - 120, 'YYYYMMDD') AND loop_bas_day.BAS_DAY
                                  GROUP BY T1.BAS_DAY, T1.PCF_ID
                                 ) X1 INNER JOIN  TM27_DDLY_PCF_KEY_IDC_A X2
                                              ON  X2.BAS_DAY = X1.BAS_DAY1
                                             AND  X2.PCF_ID  = X1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 100
          ),
          FROM_KPI_TYP_140 AS
          (SELECT BAS_DAY,
                  '140' AS KPI_TYP_CD,
                  '1'   AS DATA_RNG_CD,
                  '5'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(L3M_NXT_7_WRK_DAY_LQDTY_RTO), 3) AS AGT_VAL
           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A
           GROUP BY BAS_DAY

           UNION ALL

           SELECT XX.BAS_DAY,
                  '140' AS KPI_TYP_CD,
                  '1'   AS DATA_RNG_CD,
                  '5'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PCF_ID,
                  XX.L3M_NXT_7_WRK_DAY_LQDTY_RTO AS AGT_VAL
           FROM  (
                  SELECT BAS_DAY, L3M_NXT_7_WRK_DAY_LQDTY_RTO, RANK() OVER(PARTITION BY BAS_DAY ORDER BY L3M_NXT_7_WRK_DAY_LQDTY_RTO DESC NULLS LAST, RNK_NUM DESC) AS RNK_NUM
                  FROM  (
                         SELECT T1.BAS_DAY1 AS BAS_DAY,
                                T1.RNK_NUM,
                                CASE WHEN NVL(T2.NXT_7_WRK_DAY_LQDTY_RTO, 0) + NVL(T3.NXT_7_WRK_DAY_LQDTY_RTO, 0) + NVL(T4.NXT_7_WRK_DAY_LQDTY_RTO, 0) = 0 THEN 0
                                     WHEN CASE WHEN T2.NXT_7_WRK_DAY_LQDTY_RTO IS NOT NULL THEN 1 ELSE 0 END +
                                          CASE WHEN T3.NXT_7_WRK_DAY_LQDTY_RTO IS NOT NULL THEN 1 ELSE 0 END +
                                          CASE WHEN T4.NXT_7_WRK_DAY_LQDTY_RTO IS NOT NULL THEN 1 ELSE 0 END = 0 THEN 99999.99
                                     ELSE
                                          ROUND((NVL(T2.NXT_7_WRK_DAY_LQDTY_RTO, 0) +
                                                 NVL(T3.NXT_7_WRK_DAY_LQDTY_RTO, 0) +
                                                 NVL(T4.NXT_7_WRK_DAY_LQDTY_RTO, 0)
                                                ) / (CASE WHEN T2.NXT_7_WRK_DAY_LQDTY_RTO IS NOT NULL THEN 1 ELSE 0 END +
                                                     CASE WHEN T3.NXT_7_WRK_DAY_LQDTY_RTO IS NOT NULL THEN 1 ELSE 0 END +
                                                     CASE WHEN T4.NXT_7_WRK_DAY_LQDTY_RTO IS NOT NULL THEN 1 ELSE 0 END
                                                    ), 3)
                                     END AS L3M_NXT_7_WRK_DAY_LQDTY_RTO
                         FROM   FROM_TB07_G32_005_TTGS_A_3M_SET2 T1 INNER JOIN FROM_TB07_G32_005_TTGS_A_3M_SET3 T2
                                                                       ON T2.BAS_DAY = T1.BAS_DAY1
                                                                      AND T2.TOP_BTOM_TYP_CD = T1.TOP_BTOM_TYP_CD
                                                                      AND T2.RNK_NUM = T1.RNK_NUM
                                                               INNER JOIN FROM_TB07_G32_005_TTGS_A_3M_SET3 T3
                                                                       ON T3.BAS_DAY = T1.BAS_DAY2
                                                                      AND T3.TOP_BTOM_TYP_CD = T1.TOP_BTOM_TYP_CD
                                                                      AND T3.RNK_NUM = T1.RNK_NUM
                                                               INNER JOIN FROM_TB07_G32_005_TTGS_A_3M_SET3 T4
                                                                       ON T4.BAS_DAY = T1.BAS_DAY3
                                                                      AND T4.TOP_BTOM_TYP_CD = T1.TOP_BTOM_TYP_CD
                                                                      AND T4.RNK_NUM = T1.RNK_NUM
                         WHERE T1.TOP_BTOM_TYP_CD = '1'
                        )
                 ) XX

           UNION ALL

           SELECT XX.BAS_DAY,
                  '140' AS KPI_TYP_CD,
                  '1'   AS DATA_RNG_CD,
                  '5'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PCF_ID,
                  XX.L3M_NXT_7_WRK_DAY_LQDTY_RTO AS AGT_VAL
           FROM  (
                  SELECT BAS_DAY, L3M_NXT_7_WRK_DAY_LQDTY_RTO, RANK() OVER(PARTITION BY BAS_DAY ORDER BY L3M_NXT_7_WRK_DAY_LQDTY_RTO ASC NULLS LAST, RNK_NUM DESC) AS RNK_NUM
                  FROM  (
                         SELECT T1.BAS_DAY1 AS BAS_DAY,
                                T1.RNK_NUM,
                                CASE WHEN NVL(T2.NXT_7_WRK_DAY_LQDTY_RTO, 0) + NVL(T3.NXT_7_WRK_DAY_LQDTY_RTO, 0) + NVL(T4.NXT_7_WRK_DAY_LQDTY_RTO, 0) = 0 THEN 0
                                     WHEN CASE WHEN T2.NXT_7_WRK_DAY_LQDTY_RTO IS NOT NULL THEN 1 ELSE 0 END +
                                          CASE WHEN T3.NXT_7_WRK_DAY_LQDTY_RTO IS NOT NULL THEN 1 ELSE 0 END +
                                          CASE WHEN T4.NXT_7_WRK_DAY_LQDTY_RTO IS NOT NULL THEN 1 ELSE 0 END = 0 THEN 99999.99
                                     ELSE
                                          ROUND((NVL(T2.NXT_7_WRK_DAY_LQDTY_RTO, 0) +
                                                 NVL(T3.NXT_7_WRK_DAY_LQDTY_RTO, 0) +
                                                 NVL(T4.NXT_7_WRK_DAY_LQDTY_RTO, 0)
                                                ) / (CASE WHEN T2.NXT_7_WRK_DAY_LQDTY_RTO IS NOT NULL THEN 1 ELSE 0 END +
                                                     CASE WHEN T3.NXT_7_WRK_DAY_LQDTY_RTO IS NOT NULL THEN 1 ELSE 0 END +
                                                     CASE WHEN T4.NXT_7_WRK_DAY_LQDTY_RTO IS NOT NULL THEN 1 ELSE 0 END
                                                    ), 3)
                                     END AS L3M_NXT_7_WRK_DAY_LQDTY_RTO
                         FROM   FROM_TB07_G32_005_TTGS_A_3M_SET2 T1 INNER JOIN FROM_TB07_G32_005_TTGS_A_3M_SET3 T2
                                                                       ON T2.BAS_DAY = T1.BAS_DAY1
                                                                      AND T2.TOP_BTOM_TYP_CD = T1.TOP_BTOM_TYP_CD
                                                                      AND T2.RNK_NUM = T1.RNK_NUM
                                                               INNER JOIN FROM_TB07_G32_005_TTGS_A_3M_SET3 T3
                                                                       ON T3.BAS_DAY = T1.BAS_DAY2
                                                                      AND T3.TOP_BTOM_TYP_CD = T1.TOP_BTOM_TYP_CD
                                                                      AND T3.RNK_NUM = T1.RNK_NUM
                                                               INNER JOIN FROM_TB07_G32_005_TTGS_A_3M_SET3 T4
                                                                       ON T4.BAS_DAY = T1.BAS_DAY3
                                                                      AND T4.TOP_BTOM_TYP_CD = T1.TOP_BTOM_TYP_CD
                                                                      AND T4.RNK_NUM = T1.RNK_NUM
                         WHERE T1.TOP_BTOM_TYP_CD = '2'
                        )
                 ) XX
          ),
          FROM_KPI_TYP_144 AS
          (SELECT BAS_DAY,
                  '144' AS KPI_TYP_CD,
                  '1'   AS DATA_RNG_CD,
                  '1'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(INCL_ALOSS_INCM_MNS_EXP_AMT) AS AGT_VAL
           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A
           GROUP BY BAS_DAY

           UNION ALL

           SELECT BAS_DAY,
                  '144' AS KPI_TYP_CD,
                  '1'   AS DATA_RNG_CD,
                  '2'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(INCL_ALOSS_INCM_MNS_EXP_AMT), 0) AS AGT_VAL
           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A
           GROUP BY BAS_DAY

           UNION ALL

           SELECT BAS_DAY,
                  '144' AS KPI_TYP_CD,
                  '2'   AS DATA_RNG_CD,
                  '2'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(INCL_ALOSS_INCM_MNS_EXP_AMT), 0) AS AGT_VAL
           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A
           GROUP BY BAS_DAY

           UNION ALL

           SELECT XX.BAS_DAY,
                  '144' AS KPI_TYP_CD,
                  '1'   AS DATA_RNG_CD,
                  '0'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.INCL_ALOSS_INCM_MNS_EXP_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_DAY, PCF_ID, INCL_ALOSS_INCM_MNS_EXP_AMT, RANK() OVER(PARTITION BY BAS_DAY ORDER BY INCL_ALOSS_INCM_MNS_EXP_AMT DESC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_DAY,
                                  T1.PCF_ID,
                                  T1.INCL_ALOSS_INCM_MNS_EXP_AMT
                           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A T1
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 100

           UNION ALL

           SELECT XX.BAS_DAY,
                  '144' AS KPI_TYP_CD,
                  '1'   AS DATA_RNG_CD,
                  '0'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.INCL_ALOSS_INCM_MNS_EXP_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_DAY, PCF_ID, INCL_ALOSS_INCM_MNS_EXP_AMT, RANK() OVER(PARTITION BY BAS_DAY ORDER BY INCL_ALOSS_INCM_MNS_EXP_AMT ASC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_DAY,
                                  T1.PCF_ID,
                                  T1.INCL_ALOSS_INCM_MNS_EXP_AMT
                           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A T1
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 100

           UNION ALL

           SELECT BAS_DAY,
                  '144' AS KPI_TYP_CD,
                  '2'   AS DATA_RNG_CD,
                  '3'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(INCL_ALOSS_INCM_MNS_EXP_AMT) AS AGT_VAL
           FROM   (
                   SELECT XX.BAS_DAY,
                          XX.CBV_BR_CD,
                          ROUND(XX.INCL_ALOSS_INCM_MNS_EXP_AMT * YY.PCF_CNT_WEIGHTED_AVG, 0) AS INCL_ALOSS_INCM_MNS_EXP_AMT
                   FROM   (SELECT T1.BAS_DAY,
                                  T2.CBV_BR_CD,
                                  SUM(T1.INCL_ALOSS_INCM_MNS_EXP_AMT) AS INCL_ALOSS_INCM_MNS_EXP_AMT
                           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_DAY, T2.CBV_BR_CD
                          ) XX INNER JOIN PCF_CNT_WEIGHTED_AVG YY
                                       ON YY.BAS_DAY    = XX.BAS_DAY
                                      AND YY.CBV_BR_CD = XX.CBV_BR_CD
                  )
           GROUP BY BAS_DAY
          )
          SELECT BAS_DAY, KPI_TYP_CD, DATA_RNG_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_001
          UNION ALL
          SELECT BAS_DAY, KPI_TYP_CD, DATA_RNG_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_002
          UNION ALL
          SELECT BAS_DAY, KPI_TYP_CD, DATA_RNG_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_003
          UNION ALL
          SELECT BAS_DAY, KPI_TYP_CD, DATA_RNG_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_004
          UNION ALL
          SELECT BAS_DAY, KPI_TYP_CD, DATA_RNG_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_005
          UNION ALL
          SELECT BAS_DAY, KPI_TYP_CD, DATA_RNG_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_006
          UNION ALL
          SELECT BAS_DAY, KPI_TYP_CD, DATA_RNG_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_007
          UNION ALL
          SELECT BAS_DAY, KPI_TYP_CD, DATA_RNG_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_008
          UNION ALL
          SELECT BAS_DAY, KPI_TYP_CD, DATA_RNG_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_009
          UNION ALL
          SELECT BAS_DAY, KPI_TYP_CD, DATA_RNG_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_010
          UNION ALL
          SELECT BAS_DAY, KPI_TYP_CD, DATA_RNG_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_067
          UNION ALL
          SELECT BAS_DAY, KPI_TYP_CD, DATA_RNG_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_068
          UNION ALL
          SELECT BAS_DAY, KPI_TYP_CD, DATA_RNG_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_143
          UNION ALL
          SELECT BAS_DAY, KPI_TYP_CD, DATA_RNG_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_133
          UNION ALL
          SELECT BAS_DAY, KPI_TYP_CD, DATA_RNG_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_139
          UNION ALL
          SELECT BAS_DAY, KPI_TYP_CD, DATA_RNG_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_140
          UNION ALL
          SELECT BAS_DAY, KPI_TYP_CD, DATA_RNG_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_144
          ;

          ----------------------------------------------------------------------------
          --  Transaction Log
          ----------------------------------------------------------------------------
          v_step_code    := '020' ;
          v_step_desc    := v_wk_date || v_seq || ' ' || loop_bas_day.BAS_DAY ||' : Inserting Data Result';
          v_time         := SYSDATE ;

          P1_BSM_PROG_EXEC_LOG(v_program_id, v_program_type_name, v_step_code, v_step_desc, v_time, sql%rowcount, NULL, NULL) ;

          COMMIT ;
          ----------------------------------------------------------------------------

          EXCEPTION
          WHEN OTHERS THEN
               begin
                     v_step_code    := '098' ;
                     v_step_desc    := v_wk_date || v_seq || ' : Errors occurred when data was deleted or inserted on BAS_DAY = ' ;
                     v_time         := SYSDATE ;
                     P1_BSM_PROG_EXEC_LOG(v_program_id, v_program_type_name, v_step_code, v_step_desc||loop_bas_day.BAS_DAY, v_time, NULL, TO_CHAR(SQLCODE), SQLERRM) ;
                     CONTINUE;
               end;
    End;
    END LOOP;
    ----------------------------------------------------------------------------
    --  Write Program End Log
    ----------------------------------------------------------------------------
    v_step_code    := '999' ;
    v_step_desc    := v_wk_date || v_seq || ' : End Procedure' ;
    v_time         := SYSDATE ;

    P1_BSM_PROG_EXEC_LOG(v_program_id, v_program_type_name, v_step_code, v_step_desc, v_time, NULL, NULL, NULL) ;

    ----------------------------------------------------------------------------
    --  EXCEPTION
    ----------------------------------------------------------------------------
    EXCEPTION
    WHEN OTHERS THEN ROLLBACK ;

    P1_BSM_PROG_EXEC_LOG(v_program_id, v_program_type_name, v_step_code, v_step_desc, v_time, NULL, TO_CHAR(SQLCODE), SQLERRM) ;

END ;
/