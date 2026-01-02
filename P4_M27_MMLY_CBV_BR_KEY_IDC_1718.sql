create or replace PROCEDURE "P4_M27_MMLY_CBV_BR_KEY_IDC_1718"
                 ( p_wkdate    IN  CHAR DEFAULT NULL
                 , p_stdate_01 IN  CHAR DEFAULT NULL
                 , p_eddate_01 IN  CHAR DEFAULT NULL
                 )
IS
    /*--------------------------------------------------------------------------
     * PROGRAM ID    : P4_M27_MMLY_CBV_BR_KEY_IDC_1718
     * PROGRAM NAME  : A program for insert data to TM27_MMLY_CBV_BR_KEY_IDC_A
     * SOURCE TABLE  : TB03_G32_006_TTGS_A
                       TM27_MMLY_PCF_KEY_IDC_A
                       TM26_MMLY_AST_QAL_A
                       TM23_MMLY_INCM_EXP_A
                       TM24_MMLY_FUND_SRC_USG_A
     * TARGET TABLE  : TM27_MMLY_CBV_BR_KEY_IDC_A
     * PROGRAMER     : HIEU
     * LAST MODIFICATION DATE : 2025-12-29
     * UNIQUENESS    : N/A
     * COMMENTS      : N/A
    ----------------------------------------------------------------------------
     * Revision History : 2025-12-29 : Create
     * Revision History :
    --------------------------------------------------------------------------*/
    ----------------------------------------------------------------------------
    --  Declare Local Variables
    ----------------------------------------------------------------------------
    v_wk_date                 CHAR(8) DEFAULT NULL ;
    v_st_date_01              CHAR(8) DEFAULT NULL ;
    v_end_date_01             CHAR(8) DEFAULT NULL ;

    v_program_id              VARCHAR2(100) DEFAULT 'P4_M27_MMLY_CBV_BR_KEY_IDC_1718' ;
    v_program_type_name       VARCHAR2(100) DEFAULT 'Stored Procedure' ;
    v_step_code               VARCHAR2(3) ;
    v_step_desc               VARCHAR2(100) ;
    v_time                    DATE ;
    v_cnt                     NUMBER DEFAULT 0 ;
    v_seq                     CHAR(14) DEFAULT ' '||to_char(SEQ_UID.NEXTVAL,'000000000000');
    ----------------------------------------------------------------------------
    --  Declare Cursor
    ----------------------------------------------------------------------------
/*
    CURSOR v_sbmt_day(v_dt1 VARCHAR2, v_dt2 VARCHAR2, v_dt3 VARCHAR2) IS
    SELECT BAS_YM
    FROM   TM00_MMLY_CAL_D
    WHERE  BAS_YM BETWEEN '202303' AND '202306'
    ORDER BY 1;
*/
    CURSOR v_sbmt_day(v_dt1 VARCHAR2, v_dt2 VARCHAR2, v_dt3 VARCHAR2) IS
    SELECT BAS_YM
    FROM   (SELECT T1.BAS_YM
            FROM   TM00_MMLY_CAL_D T1 INNER JOIN (SELECT MIN(TRIM(DATA_BAS_DAY)) AS MIN_YM, MAX(TRIM(DATA_BAS_DAY)) AS MAX_YM
                                                  FROM   TBSM_INPT_RPT_SUBMIT_L
                                                  WHERE  BTCH_BAS_DAY >= v_st_date_01
                                                  AND    INPT_RPT_ID IN ('G32_020_TTGS_02','G32_011_TTGS','G32_010_TTGS','G32_001_TTGS_01','G32_001_TTGS_02','G32_002_TTGS')
                                                 ) T2
                                              ON T1.BAS_YM BETWEEN T2.MIN_YM AND T2.MAX_YM

            UNION

            SELECT T1.BAS_YM
            FROM   TM00_MMLY_CAL_D T1 INNER JOIN (SELECT SUBSTR(MIN(DATA_BAS_DAY), 1, 6) AS MIN_YM, SUBSTR(MAX(DATA_BAS_DAY), 1, 6) AS MAX_YM
                                                  FROM   TBSM_INPT_RPT_SUBMIT_L
                                                  WHERE  BTCH_BAS_DAY >= v_st_date_01
                                                  AND    INPT_RPT_ID  = 'G32_012_TTGS'
                                                 ) T2
                                              ON T1.BAS_YM BETWEEN T2.MIN_YM AND T2.MAX_YM

            UNION

            SELECT T1.BAS_YM
            FROM   TM00_MMLY_CAL_D T1 INNER JOIN (SELECT SUBSTR(MIN(DATA_BAS_DAY), 1, 6) AS MIN_YM, SUBSTR(MAX(DATA_BAS_DAY), 1, 6) AS MAX_YM
                                                  FROM   TBSM_INPT_RPT_SUBMIT_L
                                                  WHERE  BTCH_BAS_DAY >= v_st_date_01
                                                  AND    INPT_RPT_ID  = 'G32_003_TTGS'
                                                 ) T2
                                              ON T1.BAS_YM BETWEEN T2.MIN_YM AND T2.MAX_YM

            UNION

            SELECT T1.BAS_YM
            FROM   TM00_MMLY_CAL_D T1 INNER JOIN (SELECT SUBSTR(MIN(DATA_BAS_DAY), 1, 6) AS MIN_YM, TO_CHAR(SYSDATE - 35, 'YYYYMM') AS MAX_YM
                                                  FROM   TBSM_INPT_RPT_SUBMIT_L
                                                  WHERE  BTCH_BAS_DAY >= v_st_date_01
                                                  AND    INPT_RPT_ID  = 'G32_016_TTGS'
                                                 ) T2
                                              ON T1.BAS_YM BETWEEN T2.MIN_YM AND T2.MAX_YM
           )
    WHERE  BAS_YM <= TO_CHAR(SYSDATE - 35, 'YYYYMM')
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
    --  1.1 Delete Historical Data
    ----------------------------------------------------------------------------

    FOR loop_bas_day in v_sbmt_day(v_wk_date,v_st_date_01,v_end_date_01)
    LOOP
    BEGIN

          DELETE
            FROM TM27_MMLY_CBV_BR_KEY_IDC_A T1
           WHERE T1.BAS_YM = loop_bas_day.BAS_YM;

          ----------------------------------------------------------------------------
          --  Transaction Log
          ----------------------------------------------------------------------------
          v_step_code    := '010' ;
          v_step_desc    := v_wk_date || v_seq || ' ' || loop_bas_day.BAS_YM ||' : Deleting Data Result';
          v_time         := SYSDATE ;

          P1_BSM_PROG_EXEC_LOG(v_program_id, v_program_type_name, v_step_code, v_step_desc, v_time, sql%rowcount, NULL, NULL) ;

          COMMIT ;
          ----------------------------------------------------------------------------

          v_cnt := v_cnt+sql%rowcount;

          EXCEPTION
          WHEN OTHERS THEN
               begin
                     v_step_code    := '011' ;
                     v_step_desc    := v_wk_date || v_seq || ' : Deleting Data Error with BAS_YM = ' ;
                     v_time         := SYSDATE ;
                     P1_BSM_PROG_EXEC_LOG(v_program_id, v_program_type_name, v_step_code, v_step_desc||loop_bas_day.BAS_YM, v_time, v_cnt, TO_CHAR(SQLCODE), SQLERRM) ;
                     CONTINUE;
               end;
    End;
    END LOOP;

    ----------------------------------------------------------------------------
    --  1.2 Inserting Data verification results
    ----------------------------------------------------------------------------
    FOR loop_bas_day in v_sbmt_day(v_wk_date,v_st_date_01,v_end_date_01)
    LOOP
    BEGIN

          INSERT INTO TM27_MMLY_CBV_BR_KEY_IDC_A
          (   BAS_YM
             ,CBV_BR_CD
             ,KPI_TYP_CD
             ,AGT_METH_CD
             ,TOP_BTOM_TYP_CD
             ,RNK_NUM
             ,PCF_ID
             ,AGT_VAL
             ,TRGT_DATA_LST_MOD_TM
          )
          WITH
          FULL_SET AS
          (SELECT  T1.BAS_YM, T1.PCF_ID
           FROM   (SELECT BAS_YM, PCF_ID FROM TM23_MMLY_BAL_SHET_A WHERE BAS_YM = loop_bas_day.BAS_YM GROUP BY BAS_YM, PCF_ID
                  ) T1
          ),
          MED_AND_LONG_TERM_LENDING AS
          (SELECT BAS_YM,
                  PCF_ID,
                  SUM(CASE WHEN INDC_CODE = '1' THEN INDC_VALUE ELSE 0 END) AS SHORT_TERM_CAPITAL,
                  SUM(CASE WHEN INDC_CODE = '2' THEN INDC_VALUE ELSE 0 END) AS MED_AND_LONG_TERM_CAPITAL,
                  SUM(CASE WHEN INDC_CODE = '3' THEN INDC_VALUE ELSE 0 END) AS MTLTLD_AMT
           FROM   TB03_G32_006_TTGS_A
           GROUP BY BAS_YM, PCF_ID
          ),
          FROM_TM27_MMLY_PCF_KEY_IDC_A AS
          (SELECT BAS_YM
                 ,PCF_ID
                 ,TOT_PCF_MBR_NUM_CNT
                 ,NEW_JON_PCF_MBR_NUM_CNT
                 ,LV_PCF_MBR_NUM_CNT
                 ,PCF_CR_OFCR_NUM_CNT
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
                 ,COLL_VAL
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
                 ,CBV_BOR_AMT
                 ,CBV_STBOR_AMT
                 ,CBV_MED_LT_BOR_AMT
                 ,CBV_OVD_BOR_AMT
                 ,SFTY_FUND_BOR_AMT
                 ,OTHR_INST_BOR_AMT
                 ,CBV_SVG_AMT
                 ,CBV_SHRT_TRM_SVG_AMT
                 ,CBV_MED_LT_SVG_AMT
                 ,OTHR_INST_SVG_AMT
                 ,IN_HAND_CSH_AMT
                 ,CAR
                 ,CNTRBT_CBV_LT_CAP_AMT
                 ,TOT_INCM_AMT
                 ,CUR_TOT_INCM_AMT
                 ,TOT_EXP_AMT
                 ,CUR_TOT_EXP_AMT
                 ,NET_PNL_AMT
                 ,CUR_NET_PNL_AMT
                 ,INCL_ALOSS_INCM_MNS_EXP_AMT
                 ,PRFBL_AST_AMT
                 ,PRFBL_AST_VS_TOT_AST_RTO
                 ,L3M_TOT_PCF_MBR_CNT
                 ,L3M_TDP_BAL
                 ,L3M_TOT_LN_BAL
                 ,L3M_WO_COLL_LN_BAL
                 ,L3M_WIT_COLL_LN_BAL
                 ,L3M_BAD_DBT_AMT
                 ,L3M_EQT_AMT
                 ,L3M_CCAP_AMT
                 ,L3M_NET_PNL_AMT
                 ,L3M_CUR_NET_PNL_AMT
                 ,L3M_CBV_BOR_EXCL_SFTY_FUND_AMT
                 ,L3M_CBV_SVG_AMT
                 ,L3M_OTHR_INST_SVG_AMT
                 ,L3M_INCL_ALOS_INCM_MNS_EXP_AMT
                 ,L12M_AST_AMT
                 ,L12M_EQT_AMT
                 ,L12M_LN_AMT
                 ,L12M_DPST_AMT
                 ,BAL_SHET_BAS_SHRT_TRM_LN_BAL
                 ,BAL_SHET_BAS_MED_LT_LN_BAL
                 ,RISK_PRVS_FUND_AMT
           FROM   TM27_MMLY_PCF_KEY_IDC_A
           WHERE  BAS_YM = loop_bas_day.BAS_YM
          ),
          FROM_TM24_MMLY_FUND_SRC_USG_A AS
          (SELECT BAS_YM
                 ,PCF_ID
                 ,CUST_DPST_BAL
                 ,CBV_BOR_EXCL_SFTY_FUND_AMT
                 ,FR_SFTY_FUND_BOR_AMT
                 ,FR_OTHR_INST_BOR_AMT
                 ,EQT_AMT
                 ,CCAP_AMT
                 ,OTHR_SRC_FUND_AMT
                 ,CUST_LN_BAL
                 ,IN_HAND_CSH_AMT
                 ,AT_CBV_SVG_AMT
                 ,OTHR_USE_FUND_AMT
                 ,AFT_DED_CCAP_RSRV_FUND_AMT
                 ,OVR_1_YR_TRM_DPST_AMT
                 ,OVR_1_YR_OTHR_CI_BOR_AMT
                 ,NON_TRM_DPST_AMT
                 ,LSTHN_1_YR_TRM_DPST_AMT
                 ,LSTHN_1_YR_OTHR_CI_BOR_AMT
                 ,MTLTLD_AMT
                 ,STFND_USE_MTLTLD_RTO
           FROM   TM24_MMLY_FUND_SRC_USG_A
           WHERE  BAS_YM = loop_bas_day.BAS_YM
          ),
          FROM_TM26_MMLY_AST_QAL_A AS
          (SELECT BAS_YM
                 ,PCF_ID
                 ,DBT_GRP_1_BAL
                 ,DBT_GRP_2_BAL
                 ,DBT_GRP_3_BAL
                 ,DBT_GRP_4_BAL
                 ,DBT_GRP_5_BAL
                 ,CASE WHEN BAD_DBT_AMT > 0 AND TOT_LN_BAL = 0 THEN NULL ELSE TOT_LN_BAL END AS TOT_LN_BAL
                 ,BAD_DBT_AMT
                 ,TOT_COLL_VAL
                 ,RL_EST_COLL_VAL
                 ,DPST_PPL_CR_FUND_COLL_VAL
                 ,PROD_COLL_VAL
                 ,VP_COLL_VAL
                 ,OTHR_PRPTS_COLL_VAL
                 ,GENL_PRVS_AMT
                 ,DBT_GRP_1_SPEC_PRVS_AMT
                 ,DBT_GRP_2_SPEC_PRVS_AMT
                 ,DBT_GRP_3_SPEC_PRVS_AMT
                 ,DBT_GRP_4_SPEC_PRVS_AMT
                 ,DBT_GRP_5_SPEC_PRVS_AMT
                 ,TOT_PRVS_AMT
           FROM   TM26_MMLY_AST_QAL_A
           WHERE  BAS_YM = loop_bas_day.BAS_YM
          ),
          FROM_TM23_MMLY_INCM_EXP_A AS
          (SELECT BAS_YM
                 ,PCF_ID
                 ,CR_ACT_INCM_AMT
                 ,DPST_INT_INCM_AMT
                 ,LN_INT_INCM_AMT
                 ,OTHR_CR_ACT_INCM_AMT
                 ,SERV_ACT_INCM_AMT
                 ,OTHR_INCM_AMT
                 ,TOT_INCM_AMT
                 ,CR_ACT_EXP_AMT
                 ,DPST_INT_EXP_AMT
                 ,BOR_INT_EXP_AMT
                 ,OTHR_CR_ACT_EXP_AMT
                 ,SERV_ACT_EXP_AMT
                 ,STF_EXP_AMT
                 ,TX_FEE_PAY_EXP_AMT
                 ,OTHR_EXP_AMT
                 ,TOT_EXP_AMT
                 ,NET_PNL_AMT
                 ,CUR_TOT_INCM_AMT
                 ,CUR_TOT_EXP_AMT
                 ,CUR_NET_PNL_AMT
           FROM   TM23_MMLY_INCM_EXP_A
           WHERE  BAS_YM = loop_bas_day.BAS_YM
          ),
          ASSET_WEIGHTED_AVG AS
          (SELECT A.BAS_YM,
                  B.CBV_BR_CD,
                  A.PCF_ID,
                  CASE WHEN B.CBV_BR_AST_AMT = 0 THEN 0
                       ELSE A.PCF_AST_AMT / B.CBV_BR_AST_AMT
                       END  AS ASSET_WEIGHTED_AVG
           FROM   (SELECT T1.BAS_YM,
                          T2.CBV_BR_CD,
                          T1.PCF_ID,
                          T1.TOT_AST_AMT AS PCF_AST_AMT
                   FROM   TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                             ON T1.PCF_ID = T2.PCF_ID
                   WHERE  T1.TOT_AST_AMT IS NOT NULL
                  ) A INNER JOIN (SELECT T1.BAS_YM, T2.CBV_BR_CD, SUM(TOT_AST_AMT) AS CBV_BR_AST_AMT
                                  FROM   TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                            ON T1.PCF_ID = T2.PCF_ID
                                  WHERE  T1.TOT_AST_AMT IS NOT NULL
                                  GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                                 ) B
                              ON B.BAS_YM    = A.BAS_YM
                             AND B.CBV_BR_CD = A.CBV_BR_CD
          ),
          ASSET_WEIGHTED_AVG_066 AS
          (SELECT A.BAS_YM,
                  B.CBV_BR_CD,
                  A.PCF_ID,
                  CASE WHEN B.CBV_BR_AST_AMT = 0 THEN 0
                       ELSE A.PCF_AST_AMT / B.CBV_BR_AST_AMT
                       END  AS ASSET_WEIGHTED_AVG
           FROM   (SELECT T1.BAS_YM,
                          T2.CBV_BR_CD,
                          T1.PCF_ID,
                          T1.TOT_AST_AMT AS PCF_AST_AMT
                   FROM   TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                             ON T1.PCF_ID = T2.PCF_ID
                                                     INNER JOIN MED_AND_LONG_TERM_LENDING T3
                                                             ON T3.BAS_YM = T1.BAS_YM
                                                            AND T3.PCF_ID = T1.PCF_ID
                   WHERE  T1.TOT_AST_AMT IS NOT NULL
                  ) A INNER JOIN (SELECT T1.BAS_YM, T2.CBV_BR_CD, SUM(T1.TOT_AST_AMT) AS CBV_BR_AST_AMT
                                  FROM   TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                            ON T1.PCF_ID = T2.PCF_ID
                                                                    INNER JOIN MED_AND_LONG_TERM_LENDING T3
                                                                            ON T3.BAS_YM = T1.BAS_YM
                                                                           AND T3.PCF_ID = T1.PCF_ID
                                  WHERE  T1.TOT_AST_AMT IS NOT NULL
                                  GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                                 ) B
                              ON B.BAS_YM    = A.BAS_YM
                             AND B.CBV_BR_CD = A.CBV_BR_CD
          ),
          ASSET_WEIGHTED_AVG_070 AS
          (SELECT A.BAS_YM,
                  B.CBV_BR_CD,
                  A.PCF_ID,
                  CASE WHEN B.CBV_BR_AST_AMT = 0 THEN 0
                       ELSE A.PCF_AST_AMT / B.CBV_BR_AST_AMT
                       END  AS ASSET_WEIGHTED_AVG
           FROM   (SELECT T1.BAS_YM,
                          T2.CBV_BR_CD,
                          T1.PCF_ID,
                          T1.TOT_AST_AMT AS PCF_AST_AMT
                   FROM   TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                             ON T1.PCF_ID = T2.PCF_ID
                                                     INNER JOIN TM26_MMLY_AST_QAL_A T3
                                                             ON T3.BAS_YM = T1.BAS_YM
                                                            AND T3.PCF_ID = T1.PCF_ID
                                                            AND T3.TOT_COLL_VAL IS NOT NULL
                   WHERE  T1.TOT_AST_AMT IS NOT NULL
                  ) A INNER JOIN (SELECT T1.BAS_YM, T2.CBV_BR_CD, SUM(T1.TOT_AST_AMT) AS CBV_BR_AST_AMT
                                  FROM   TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                            ON T1.PCF_ID = T2.PCF_ID
                                                                    INNER JOIN TM26_MMLY_AST_QAL_A T3
                                                                            ON T3.BAS_YM = T1.BAS_YM
                                                                           AND T3.PCF_ID = T1.PCF_ID
                                                                           AND T3.TOT_COLL_VAL IS NOT NULL
                                  WHERE  T1.TOT_AST_AMT IS NOT NULL
                                  GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                                 ) B
                              ON B.BAS_YM    = A.BAS_YM
                             AND B.CBV_BR_CD = A.CBV_BR_CD
          ),
          ASSET_WEIGHTED_AVG_071 AS
          (SELECT A.BAS_YM,
                  B.CBV_BR_CD,
                  A.PCF_ID,
                  CASE WHEN B.CBV_BR_AST_AMT = 0 THEN 0
                       ELSE A.PCF_AST_AMT / B.CBV_BR_AST_AMT
                       END  AS ASSET_WEIGHTED_AVG
           FROM   (SELECT T1.BAS_YM,
                          T2.CBV_BR_CD,
                          T1.PCF_ID,
                          T1.TOT_AST_AMT AS PCF_AST_AMT
                   FROM   TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                             ON T1.PCF_ID = T2.PCF_ID
                                                     INNER JOIN TM26_MMLY_AST_QAL_A T3
                                                             ON T3.BAS_YM = T1.BAS_YM
                                                            AND T3.PCF_ID = T1.PCF_ID
                                                            AND T3.TOT_PRVS_AMT IS NOT NULL
                   WHERE  T1.TOT_AST_AMT IS NOT NULL
                  ) A INNER JOIN (SELECT T1.BAS_YM, T2.CBV_BR_CD, SUM(T1.TOT_AST_AMT) AS CBV_BR_AST_AMT
                                  FROM   TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                            ON T1.PCF_ID = T2.PCF_ID
                                                                    INNER JOIN TM26_MMLY_AST_QAL_A T3
                                                                            ON T3.BAS_YM = T1.BAS_YM
                                                                           AND T3.PCF_ID = T1.PCF_ID
                                                                           AND T3.TOT_PRVS_AMT IS NOT NULL
                                  WHERE  T1.TOT_AST_AMT IS NOT NULL
                                  GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                                 ) B
                              ON B.BAS_YM    = A.BAS_YM
                             AND B.CBV_BR_CD = A.CBV_BR_CD
          ),
          ASSET_WEIGHTED_AVG_094 AS
          (SELECT A.BAS_YM,
                  B.CBV_BR_CD,
                  A.PCF_ID,
                  CASE WHEN B.CBV_BR_AST_AMT = 0 THEN 0
                       ELSE A.PCF_AST_AMT / B.CBV_BR_AST_AMT
                       END  AS ASSET_WEIGHTED_AVG
           FROM   (SELECT T1.BAS_YM,
                          T2.CBV_BR_CD,
                          T1.PCF_ID,
                          T1.TOT_AST_AMT AS PCF_AST_AMT
                   FROM   TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                             ON T1.PCF_ID = T2.PCF_ID
                   WHERE  T1.TOT_AST_AMT IS NOT NULL
                   AND    T1.EQT_AMT IS NOT NULL
                  ) A INNER JOIN (SELECT T1.BAS_YM, T2.CBV_BR_CD, SUM(T1.TOT_AST_AMT) AS CBV_BR_AST_AMT
                                  FROM   TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                            ON T1.PCF_ID = T2.PCF_ID
                                  WHERE  T1.TOT_AST_AMT IS NOT NULL
                                  AND    T1.EQT_AMT IS NOT NULL
                                  GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                                 ) B
                              ON B.BAS_YM    = A.BAS_YM
                             AND B.CBV_BR_CD = A.CBV_BR_CD
          ),
          FROM_KPI_TYP_001 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '001' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.TOT_PCF_MBR_NUM_CNT) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '001' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '001' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '0' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '1' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.TOT_PCF_MBR_NUM_CNT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PCF_ID, TOT_PCF_MBR_NUM_CNT, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY TOT_PCF_MBR_NUM_CNT DESC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  T1.TOT_PCF_MBR_NUM_CNT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T2.PCF_ID = T1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '001' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '0' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '2' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.TOT_PCF_MBR_NUM_CNT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PCF_ID, TOT_PCF_MBR_NUM_CNT, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY TOT_PCF_MBR_NUM_CNT ASC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  T1.TOT_PCF_MBR_NUM_CNT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T2.PCF_ID = T1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3
          ),
          FROM_KPI_TYP_002 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '002' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.TDP_BAL) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '002' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.TDP_BAL),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '002' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '0' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '1' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.TDP_BAL AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PCF_ID, TDP_BAL, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY TDP_BAL DESC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  T1.TDP_BAL
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T2.PCF_ID = T1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '002' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '0' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '2' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.TDP_BAL AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PCF_ID, TDP_BAL, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY TDP_BAL ASC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  T1.TDP_BAL
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T2.PCF_ID = T1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3
          ),
          FROM_KPI_TYP_003 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '003' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.TOT_LN_BAL) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '003' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.TOT_LN_BAL),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '003' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '0' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '1' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.TOT_LN_BAL AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PCF_ID, TOT_LN_BAL, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY TOT_LN_BAL DESC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  T1.TOT_LN_BAL
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T2.PCF_ID = T1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '003' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '0' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '2' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.TOT_LN_BAL AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PCF_ID, TOT_LN_BAL, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY TOT_LN_BAL ASC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  T1.TOT_LN_BAL
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T2.PCF_ID = T1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3
          ),
          FROM_KPI_TYP_004 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '004' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.WO_COLL_LN_BAL) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '004' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.WO_COLL_LN_BAL),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '004' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '0' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '1' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.WO_COLL_LN_BAL AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PCF_ID, WO_COLL_LN_BAL, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY WO_COLL_LN_BAL DESC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  T1.WO_COLL_LN_BAL
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T2.PCF_ID = T1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '004' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '0' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '2' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.WO_COLL_LN_BAL AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PCF_ID, WO_COLL_LN_BAL, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY WO_COLL_LN_BAL ASC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  T1.WO_COLL_LN_BAL
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T2.PCF_ID = T1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3
          ),
          FROM_KPI_TYP_005 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '005' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.BAD_DBT_AMT) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '005' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.BAD_DBT_AMT),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '005' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '0' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '1' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.BAD_DBT_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PCF_ID, BAD_DBT_AMT, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY BAD_DBT_AMT DESC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  T1.BAD_DBT_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T2.PCF_ID = T1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '005' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '0' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '2' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.BAD_DBT_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PCF_ID, BAD_DBT_AMT, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY BAD_DBT_AMT ASC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  T1.BAD_DBT_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T2.PCF_ID = T1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3
          ),
          FROM_KPI_TYP_006 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '006' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.EQT_AMT) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '006' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.EQT_AMT),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '006' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '0' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '1' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.EQT_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PCF_ID, EQT_AMT, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY EQT_AMT DESC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  T1.EQT_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T2.PCF_ID = T1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '006' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '0' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '2' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.EQT_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PCF_ID, EQT_AMT, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY EQT_AMT ASC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  T1.EQT_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T2.PCF_ID = T1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3
          ),
          FROM_KPI_TYP_007 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '007' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.CCAP_AMT) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '007' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.CCAP_AMT),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '007' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '0' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '1' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.CCAP_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PCF_ID, CCAP_AMT, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY CCAP_AMT DESC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  T1.CCAP_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T2.PCF_ID = T1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '007' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '0' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '2' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.CCAP_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PCF_ID, CCAP_AMT, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY CCAP_AMT ASC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  T1.CCAP_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T2.PCF_ID = T1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3
          ),
          FROM_KPI_TYP_008 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '008' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.CBV_BOR_EXCL_SFTY_FUND_AMT) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '008' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.CBV_BOR_EXCL_SFTY_FUND_AMT),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '008' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '0' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '1' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.CBV_BOR_EXCL_SFTY_FUND_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PCF_ID, CBV_BOR_EXCL_SFTY_FUND_AMT, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY CBV_BOR_EXCL_SFTY_FUND_AMT DESC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  T1.CBV_BOR_EXCL_SFTY_FUND_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T2.PCF_ID = T1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '008' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '0' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '2' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.CBV_BOR_EXCL_SFTY_FUND_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PCF_ID, CBV_BOR_EXCL_SFTY_FUND_AMT, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY CBV_BOR_EXCL_SFTY_FUND_AMT ASC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  T1.CBV_BOR_EXCL_SFTY_FUND_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T2.PCF_ID = T1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3
          ),
          FROM_KPI_TYP_009 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '009' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.CBV_SVG_AMT) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '009' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.CBV_SVG_AMT),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '009' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '0' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '1' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.CBV_SVG_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PCF_ID, CBV_SVG_AMT, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY CBV_SVG_AMT DESC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  T1.CBV_SVG_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T2.PCF_ID = T1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '009' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '0' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '2' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.CBV_SVG_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PCF_ID, CBV_SVG_AMT, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY CBV_SVG_AMT ASC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  T1.CBV_SVG_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T2.PCF_ID = T1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3
          ),
          FROM_KPI_TYP_010 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '010' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.OTHR_INST_SVG_AMT) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '010' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.OTHR_INST_SVG_AMT),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '010' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '0' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '1' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.OTHR_INST_SVG_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PCF_ID, OTHR_INST_SVG_AMT, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY OTHR_INST_SVG_AMT DESC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  T1.OTHR_INST_SVG_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T2.PCF_ID = T1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '010' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '0' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '2' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.OTHR_INST_SVG_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PCF_ID, OTHR_INST_SVG_AMT, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY OTHR_INST_SVG_AMT ASC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  T1.OTHR_INST_SVG_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T2.PCF_ID = T1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3
          ),
          FROM_KPI_TYP_011 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '011' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.TOT_AST_AMT) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '011' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.TOT_AST_AMT),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
          ),
          FROM_KPI_TYP_058 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '058' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.CAR),2) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '058' AS KPI_TYP_CD,
                  '5'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(YY.CAR),2) AS AGT_VAL
           FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD
                   FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                  ON T1.PCF_ID = T2.PCF_ID
                   GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                  ) XX INNER JOIN (SELECT A.BAS_YM,
                                          B.CBV_BR_CD,
                                          ROUND(AVG(A.CAR), 2) AS CAR
                                   FROM   TM27_MMLY_PCF_KEY_IDC_A A INNER JOIN TM00_PCF_D B
                                                                            ON B.PCF_ID = A.PCF_ID
                                   GROUP BY A.BAS_YM, B.CBV_BR_CD
                                  ) YY
                               ON YY.BAS_YM    >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                              AND YY.BAS_YM    <= XX.BAS_YM
                              AND YY.CBV_BR_CD  = XX.CBV_BR_CD
           GROUP BY XX.BAS_YM, XX.CBV_BR_CD

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '058' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '0' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '1' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  ROUND(XX.CAR,2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PCF_ID, CAR, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY CAR DESC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  T1.CAR
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T2.PCF_ID = T1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '058' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '0' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '2' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  ROUND(XX.CAR,2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PCF_ID, CAR, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY CAR ASC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  T1.CAR
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T2.PCF_ID = T1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT BAS_YM,
                  CBV_BR_CD,
                  '058' AS KPI_TYP_CD,
                  '5' AS AGT_METH_CD,
                  '1' AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL AS PCF_ID,
                  ROUND(CAR,2) AS AGT_VAL
           FROM  (SELECT BAS_YM, CBV_BR_CD, CAR, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY CAR DESC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                CBV_BR_CD,
                                INI_RNK_NUM,
                                AVG(CAR) AS CAR
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, PCF_ID, CAR, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1, CBV_BR_CD ORDER BY CAR DESC NULLS LAST, PCF_ID DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               XX.PCF_ID,
                                               YY.CAR
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD, T1.PCF_ID
                                                FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                                               ON T1.PCF_ID = T2.PCF_ID
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       A.PCF_ID,
                                                                       A.CAR
                                                                FROM   TM27_MMLY_PCF_KEY_IDC_A A
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PCF_ID   = XX.PCF_ID
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, CBV_BR_CD, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  CBV_BR_CD,
                  '058' AS KPI_TYP_CD,
                  '5' AS AGT_METH_CD,
                  '2' AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL AS PCF_ID,
                  ROUND(CAR,2) AS AGT_VAL
           FROM  (SELECT BAS_YM, CBV_BR_CD, CAR, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY CAR ASC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                CBV_BR_CD,
                                INI_RNK_NUM,
                                AVG(CAR) AS CAR
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, PCF_ID, CAR, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1, CBV_BR_CD ORDER BY CAR ASC NULLS LAST, PCF_ID DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               XX.PCF_ID,
                                               YY.CAR
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD, T1.PCF_ID
                                                FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                                               ON T1.PCF_ID = T2.PCF_ID
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       A.PCF_ID,
                                                                       A.CAR
                                                                FROM   TM27_MMLY_PCF_KEY_IDC_A A
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PCF_ID   = XX.PCF_ID
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, CBV_BR_CD, INI_RNK_NUM
                        )
                 )
          ),
          FROM_KPI_TYP_059 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '059' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.AFT_DED_CCAP_RSRV_FUND_AMT) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM24_MMLY_FUND_SRC_USG_A T1 INNER JOIN TM00_PCF_D T2
                                                           ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '059' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.AFT_DED_CCAP_RSRV_FUND_AMT),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM24_MMLY_FUND_SRC_USG_A T1 INNER JOIN TM00_PCF_D T2
                                                           ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
          ),
          FROM_KPI_TYP_060 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '060' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.OVR_1_YR_TRM_DPST_AMT) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM24_MMLY_FUND_SRC_USG_A T1 INNER JOIN TM00_PCF_D T2
                                                           ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '060' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.OVR_1_YR_TRM_DPST_AMT),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM24_MMLY_FUND_SRC_USG_A T1 INNER JOIN TM00_PCF_D T2
                                                           ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
          ),
          FROM_KPI_TYP_061 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '061' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.OVR_1_YR_OTHR_CI_BOR_AMT) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM24_MMLY_FUND_SRC_USG_A T1 INNER JOIN TM00_PCF_D T2
                                                           ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '061' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.OVR_1_YR_OTHR_CI_BOR_AMT),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM24_MMLY_FUND_SRC_USG_A T1 INNER JOIN TM00_PCF_D T2
                                                           ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
          ),
          FROM_KPI_TYP_062 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '062' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.NON_TRM_DPST_AMT) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM24_MMLY_FUND_SRC_USG_A T1 INNER JOIN TM00_PCF_D T2
                                                           ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '062' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.NON_TRM_DPST_AMT),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM24_MMLY_FUND_SRC_USG_A T1 INNER JOIN TM00_PCF_D T2
                                                           ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
          ),
          FROM_KPI_TYP_063 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '063' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.LSTHN_1_YR_TRM_DPST_AMT) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM24_MMLY_FUND_SRC_USG_A T1 INNER JOIN TM00_PCF_D T2
                                                           ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '063' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.LSTHN_1_YR_TRM_DPST_AMT),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM24_MMLY_FUND_SRC_USG_A T1 INNER JOIN TM00_PCF_D T2
                                                           ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
          ),
          FROM_KPI_TYP_064 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '064' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.LSTHN_1_YR_OTHR_CI_BOR_AMT) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM24_MMLY_FUND_SRC_USG_A T1 INNER JOIN TM00_PCF_D T2
                                                           ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '064' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.LSTHN_1_YR_OTHR_CI_BOR_AMT),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM24_MMLY_FUND_SRC_USG_A T1 INNER JOIN TM00_PCF_D T2
                                                           ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
          ),
          FROM_KPI_TYP_065 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '065' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.MTLTLD_AMT) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM24_MMLY_FUND_SRC_USG_A T1 INNER JOIN TM00_PCF_D T2
                                                           ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '065' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.MTLTLD_AMT),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM24_MMLY_FUND_SRC_USG_A T1 INNER JOIN TM00_PCF_D T2
                                                           ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
          ),
          FROM_KPI_TYP_066 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '066' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.STFND_USE_MTLTLD_RTO), 2) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM24_MMLY_FUND_SRC_USG_A T1 INNER JOIN TM00_PCF_D T2
                                                           ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '066' AS KPI_TYP_CD,
                  '5'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(YY.STFND_USE_MTLTLD_RTO),2) AS AGT_VAL
           FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD
                   FROM   FROM_TM24_MMLY_FUND_SRC_USG_A T1 INNER JOIN TM00_PCF_D T2
                                                                   ON T1.PCF_ID = T2.PCF_ID
                   GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                  ) XX INNER JOIN (SELECT A.BAS_YM,
                                          B.CBV_BR_CD,
                                          ROUND(AVG(A.STFND_USE_MTLTLD_RTO), 2) AS STFND_USE_MTLTLD_RTO
                                   FROM   TM24_MMLY_FUND_SRC_USG_A A INNER JOIN TM00_PCF_D B
                                                                             ON B.PCF_ID = A.PCF_ID
                                   GROUP BY A.BAS_YM, B.CBV_BR_CD
                                  ) YY
                               ON YY.BAS_YM    >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                              AND YY.BAS_YM    <= XX.BAS_YM
                              AND YY.CBV_BR_CD  = XX.CBV_BR_CD
           GROUP BY XX.BAS_YM, XX.CBV_BR_CD

           UNION ALL

           SELECT BAS_YM,
                  CBV_BR_CD,
                  '066' AS KPI_TYP_CD,
                  '4'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(STFND_USE_MTLTLD_RTO, 2) AS AGT_VAL
           FROM   (SELECT T1.BAS_YM,
                          T2.CBV_BR_CD,
                          SUM(CASE WHEN T1.MTLTLD_AMT - T1.MED_AND_LONG_TERM_CAPITAL =  0 THEN 0
                                   WHEN T1.MTLTLD_AMT - T1.MED_AND_LONG_TERM_CAPITAL <> 0 AND T1.SHORT_TERM_CAPITAL = 0 THEN NULL
                                   ELSE CASE WHEN T1.SHORT_TERM_CAPITAL = 0 THEN 0
                                             ELSE (T1.MTLTLD_AMT - T1.MED_AND_LONG_TERM_CAPITAL) / (T1.SHORT_TERM_CAPITAL) * 100
                                             END
                                   END * T2.ASSET_WEIGHTED_AVG) AS STFND_USE_MTLTLD_RTO
                   FROM   MED_AND_LONG_TERM_LENDING T1 INNER JOIN ASSET_WEIGHTED_AVG_066 T2
                                                               ON T1.BAS_YM = T2.BAS_YM
                                                              AND T1.PCF_ID = T2.PCF_ID
                   WHERE  T1.BAS_YM = loop_bas_day.BAS_YM
                   GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                  )

           UNION ALL

           SELECT BAS_YM,
                  CBV_BR_CD,
                  '066' AS KPI_TYP_CD,
                  '7'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  STFND_USE_MTLTLD_RTO AS AGT_VAL
           FROM   (SELECT XX.BAS_YM,
                          XX.CBV_BR_CD,
                          ROUND(AVG(YY.STFND_USE_MTLTLD_RTO), 2) AS STFND_USE_MTLTLD_RTO
                   FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD
                           FROM   MED_AND_LONG_TERM_LENDING T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                           WHERE  T1.BAS_YM = loop_bas_day.BAS_YM
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          ) XX INNER JOIN (SELECT T1.BAS_YM,
                                                  T2.CBV_BR_CD,
                                                  SUM(CASE WHEN T1.MTLTLD_AMT - T1.MED_AND_LONG_TERM_CAPITAL =  0 THEN 0
                                                           WHEN T1.MTLTLD_AMT - T1.MED_AND_LONG_TERM_CAPITAL <> 0 AND T1.SHORT_TERM_CAPITAL = 0 THEN NULL
                                                           ELSE CASE WHEN T1.SHORT_TERM_CAPITAL = 0 THEN 0
                                                                     ELSE (T1.MTLTLD_AMT - T1.MED_AND_LONG_TERM_CAPITAL) / (T1.SHORT_TERM_CAPITAL) * 100
                                                                     END
                                                           END * T2.ASSET_WEIGHTED_AVG) AS STFND_USE_MTLTLD_RTO
                                           FROM   MED_AND_LONG_TERM_LENDING T1 INNER JOIN ASSET_WEIGHTED_AVG_066 T2
                                                                                       ON T1.BAS_YM = T2.BAS_YM
                                                                                      AND T1.PCF_ID = T2.PCF_ID
                                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                                          ) YY
                                       ON YY.BAS_YM    >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                      AND YY.BAS_YM    <= XX.BAS_YM
                                      AND YY.CBV_BR_CD  = XX.CBV_BR_CD
                   GROUP BY XX.BAS_YM, XX.CBV_BR_CD
                  )

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '066' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '0' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '1' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.STFND_USE_MTLTLD_RTO AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PCF_ID, STFND_USE_MTLTLD_RTO, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY STFND_USE_MTLTLD_RTO DESC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  T1.STFND_USE_MTLTLD_RTO
                           FROM   FROM_TM24_MMLY_FUND_SRC_USG_A T1 INNER JOIN TM00_PCF_D T2
                                                                           ON T2.PCF_ID = T1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '066' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '0' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '2' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.STFND_USE_MTLTLD_RTO AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PCF_ID, STFND_USE_MTLTLD_RTO, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY STFND_USE_MTLTLD_RTO ASC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  T1.STFND_USE_MTLTLD_RTO
                           FROM   FROM_TM24_MMLY_FUND_SRC_USG_A T1 INNER JOIN TM00_PCF_D T2
                                                                           ON T2.PCF_ID = T1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '066' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '4' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '1' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  ROUND(XX.STFND_USE_MTLTLD_RTO, 2) AS AGT_VAL
           FROM   (SELECT BAS_YM, CBV_BR_CD, PCF_ID, STFND_USE_MTLTLD_RTO, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY STFND_USE_MTLTLD_RTO DESC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  CASE WHEN T1.MTLTLD_AMT - T1.MED_AND_LONG_TERM_CAPITAL =  0 THEN 0
                                       WHEN T1.MTLTLD_AMT - T1.MED_AND_LONG_TERM_CAPITAL <> 0 AND T1.SHORT_TERM_CAPITAL = 0 THEN NULL
                                       ELSE CASE WHEN T1.SHORT_TERM_CAPITAL = 0 THEN 0
                                                 ELSE (T1.MTLTLD_AMT - T1.MED_AND_LONG_TERM_CAPITAL) / (T1.SHORT_TERM_CAPITAL) * 100
                                                 END
                                       END * T2.ASSET_WEIGHTED_AVG AS STFND_USE_MTLTLD_RTO
                           FROM   MED_AND_LONG_TERM_LENDING T1 INNER JOIN ASSET_WEIGHTED_AVG_066 T2
                                                                       ON T1.BAS_YM = T2.BAS_YM
                                                                      AND T1.PCF_ID = T2.PCF_ID
                           WHERE  T1.BAS_YM = loop_bas_day.BAS_YM
                          )
                  ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '066' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '4' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '2' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  ROUND(XX.STFND_USE_MTLTLD_RTO, 2) AS AGT_VAL
           FROM   (SELECT BAS_YM, CBV_BR_CD, PCF_ID, STFND_USE_MTLTLD_RTO, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY STFND_USE_MTLTLD_RTO ASC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  CASE WHEN T1.MTLTLD_AMT - T1.MED_AND_LONG_TERM_CAPITAL  = 0 THEN 0
                                       WHEN T1.MTLTLD_AMT - T1.MED_AND_LONG_TERM_CAPITAL <> 0 AND T1.SHORT_TERM_CAPITAL = 0 THEN NULL
                                       ELSE CASE WHEN T1.SHORT_TERM_CAPITAL = 0 THEN 0
                                                 ELSE (T1.MTLTLD_AMT - T1.MED_AND_LONG_TERM_CAPITAL) / (T1.SHORT_TERM_CAPITAL) * 100
                                                 END
                                       END * T2.ASSET_WEIGHTED_AVG AS STFND_USE_MTLTLD_RTO
                           FROM   MED_AND_LONG_TERM_LENDING T1 INNER JOIN ASSET_WEIGHTED_AVG_066 T2
                                                                       ON T1.BAS_YM = T2.BAS_YM
                                                                      AND T1.PCF_ID = T2.PCF_ID
                           WHERE  T1.BAS_YM = loop_bas_day.BAS_YM
                          )
                  ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT BAS_YM,
                  CBV_BR_CD,
                  '066' AS KPI_TYP_CD,
                  '5' AS AGT_METH_CD,
                  '1' AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL AS PCF_ID,
                  ROUND(STFND_USE_MTLTLD_RTO,2) AS AGT_VAL
           FROM  (SELECT BAS_YM, CBV_BR_CD, STFND_USE_MTLTLD_RTO, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY STFND_USE_MTLTLD_RTO DESC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                CBV_BR_CD,
                                INI_RNK_NUM,
                                AVG(STFND_USE_MTLTLD_RTO) AS STFND_USE_MTLTLD_RTO
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, PCF_ID, STFND_USE_MTLTLD_RTO, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1, CBV_BR_CD ORDER BY STFND_USE_MTLTLD_RTO DESC NULLS LAST, PCF_ID DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               XX.PCF_ID,
                                               YY.STFND_USE_MTLTLD_RTO
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD, T1.PCF_ID
                                                FROM   FROM_TM24_MMLY_FUND_SRC_USG_A T1 INNER JOIN TM00_PCF_D T2
                                                                                                ON T1.PCF_ID = T2.PCF_ID
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       A.PCF_ID,
                                                                       A.STFND_USE_MTLTLD_RTO
                                                                FROM   TM24_MMLY_FUND_SRC_USG_A A
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PCF_ID   = XX.PCF_ID
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, CBV_BR_CD, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  CBV_BR_CD,
                  '066' AS KPI_TYP_CD,
                  '5' AS AGT_METH_CD,
                  '2' AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL AS PCF_ID,
                  ROUND(STFND_USE_MTLTLD_RTO,2) AS AGT_VAL
           FROM  (SELECT BAS_YM, CBV_BR_CD, STFND_USE_MTLTLD_RTO, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY STFND_USE_MTLTLD_RTO ASC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                CBV_BR_CD,
                                INI_RNK_NUM,
                                AVG(STFND_USE_MTLTLD_RTO) AS STFND_USE_MTLTLD_RTO
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, PCF_ID, STFND_USE_MTLTLD_RTO, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1, CBV_BR_CD ORDER BY STFND_USE_MTLTLD_RTO ASC NULLS LAST, PCF_ID DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               XX.PCF_ID,
                                               YY.STFND_USE_MTLTLD_RTO
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD, T1.PCF_ID
                                                FROM   FROM_TM24_MMLY_FUND_SRC_USG_A T1 INNER JOIN TM00_PCF_D T2
                                                                                                ON T1.PCF_ID = T2.PCF_ID
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       A.PCF_ID,
                                                                       A.STFND_USE_MTLTLD_RTO
                                                                FROM   TM24_MMLY_FUND_SRC_USG_A A
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PCF_ID   = XX.PCF_ID
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, CBV_BR_CD, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  CBV_BR_CD,
                  '066' AS KPI_TYP_CD,
                  '7' AS AGT_METH_CD,
                  '1' AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL AS PCF_ID,
                  ROUND(STFND_USE_MTLTLD_RTO, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, CBV_BR_CD, STFND_USE_MTLTLD_RTO, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY STFND_USE_MTLTLD_RTO DESC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                CBV_BR_CD,
                                INI_RNK_NUM,
                                AVG(STFND_USE_MTLTLD_RTO) AS STFND_USE_MTLTLD_RTO
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, PCF_ID, STFND_USE_MTLTLD_RTO, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1, CBV_BR_CD ORDER BY STFND_USE_MTLTLD_RTO DESC NULLS LAST, PCF_ID DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               XX.PCF_ID,
                                               YY.STFND_USE_MTLTLD_RTO
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD, T1.PCF_ID
                                                FROM   MED_AND_LONG_TERM_LENDING T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                                WHERE  T1.BAS_YM = loop_bas_day.BAS_YM
                                               ) XX INNER JOIN (SELECT T1.BAS_YM,
                                                                       T2.CBV_BR_CD,
                                                                       T1.PCF_ID,
                                                                       CASE WHEN T1.MTLTLD_AMT - T1.MED_AND_LONG_TERM_CAPITAL  = 0 THEN 0
                                                                            WHEN T1.MTLTLD_AMT - T1.MED_AND_LONG_TERM_CAPITAL <> 0 AND T1.SHORT_TERM_CAPITAL = 0 THEN NULL
                                                                            ELSE CASE WHEN T1.SHORT_TERM_CAPITAL = 0 THEN 0
                                                                                      ELSE (T1.MTLTLD_AMT - T1.MED_AND_LONG_TERM_CAPITAL) / (T1.SHORT_TERM_CAPITAL) * 100
                                                                                      END
                                                                            END * T2.ASSET_WEIGHTED_AVG AS STFND_USE_MTLTLD_RTO
                                                                FROM   MED_AND_LONG_TERM_LENDING T1 INNER JOIN ASSET_WEIGHTED_AVG_066 T2
                                                                                                            ON T1.BAS_YM = T2.BAS_YM
                                                                                                           AND T1.PCF_ID = T2.PCF_ID
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PCF_ID   = XX.PCF_ID
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, CBV_BR_CD, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  CBV_BR_CD,
                  '066' AS KPI_TYP_CD,
                  '7' AS AGT_METH_CD,
                  '2' AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL AS PCF_ID,
                  ROUND(STFND_USE_MTLTLD_RTO, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, CBV_BR_CD, STFND_USE_MTLTLD_RTO, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY STFND_USE_MTLTLD_RTO ASC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                CBV_BR_CD,
                                INI_RNK_NUM,
                                AVG(STFND_USE_MTLTLD_RTO) AS STFND_USE_MTLTLD_RTO
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, PCF_ID, STFND_USE_MTLTLD_RTO, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1, CBV_BR_CD ORDER BY STFND_USE_MTLTLD_RTO ASC NULLS LAST, PCF_ID DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               XX.PCF_ID,
                                               YY.STFND_USE_MTLTLD_RTO
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD, T1.PCF_ID
                                                FROM   MED_AND_LONG_TERM_LENDING T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                                WHERE  T1.BAS_YM = loop_bas_day.BAS_YM
                                               ) XX INNER JOIN (SELECT T1.BAS_YM,
                                                                       T2.CBV_BR_CD,
                                                                       T1.PCF_ID,
                                                                       CASE WHEN T1.MTLTLD_AMT - T1.MED_AND_LONG_TERM_CAPITAL =  0 THEN 0
                                                                            WHEN T1.MTLTLD_AMT - T1.MED_AND_LONG_TERM_CAPITAL <> 0 AND T1.SHORT_TERM_CAPITAL = 0 THEN NULL
                                                                            ELSE CASE WHEN T1.SHORT_TERM_CAPITAL = 0 THEN 0
                                                                                      ELSE (T1.MTLTLD_AMT - T1.MED_AND_LONG_TERM_CAPITAL) / (T1.SHORT_TERM_CAPITAL) * 100
                                                                                      END
                                                                            END * T2.ASSET_WEIGHTED_AVG AS STFND_USE_MTLTLD_RTO
                                                                FROM   MED_AND_LONG_TERM_LENDING T1 INNER JOIN ASSET_WEIGHTED_AVG_066 T2
                                                                                                            ON T1.BAS_YM = T2.BAS_YM
                                                                                                           AND T1.PCF_ID = T2.PCF_ID
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PCF_ID   = XX.PCF_ID
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, CBV_BR_CD, INI_RNK_NUM
                        )
                 )
          ),
          FROM_KPI_TYP_069 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '069' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(CASE WHEN T1.TOT_LN_BAL = 0 AND T1.BAD_DBT_AMT = 0 THEN 0
                                 ELSE T1.BAD_DBT_AMT / T1.TOT_LN_BAL * 100
                                 END),2) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                      ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT BAS_YM,
                  CBV_BR_CD,
                  '069' AS KPI_TYP_CD,
                  '4'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(BAD_DBT_RTO, 2) AS AGT_VAL
           FROM   (SELECT T1.BAS_YM,
                          T2.CBV_BR_CD,
                          SUM(CASE WHEN T1.TOT_LN_BAL = 0 AND T1.BAD_DBT_AMT = 0 THEN 0
                                   ELSE T1.BAD_DBT_AMT / T1.TOT_LN_BAL * 100 * T2.ASSET_WEIGHTED_AVG
                                   END) AS BAD_DBT_RTO
                   FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN ASSET_WEIGHTED_AVG T2
                                                              ON T1.BAS_YM = T2.BAS_YM
                                                             AND T1.PCF_ID = T2.PCF_ID
                   GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                  )

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '069' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '0' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '1' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  ROUND(XX.BAD_DBT_RTO,2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PCF_ID, BAD_DBT_RTO, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY BAD_DBT_RTO DESC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  CASE WHEN T1.TOT_LN_BAL = 0 AND T1.BAD_DBT_AMT = 0 THEN 0
                                       ELSE T1.BAD_DBT_AMT / T1.TOT_LN_BAL * 100
                                       END AS BAD_DBT_RTO
                           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                                      ON T2.PCF_ID = T1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '069' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '0' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '2' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  ROUND(XX.BAD_DBT_RTO,2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PCF_ID, BAD_DBT_RTO, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY BAD_DBT_RTO ASC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  CASE WHEN T1.TOT_LN_BAL = 0 AND T1.BAD_DBT_AMT = 0 THEN 0
                                       ELSE T1.BAD_DBT_AMT / T1.TOT_LN_BAL * 100
                                       END AS BAD_DBT_RTO
                           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                                      ON T2.PCF_ID = T1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '069' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '4' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '1' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  ROUND(XX.BAD_DBT_RTO,2) AS AGT_VAL
           FROM   (SELECT BAS_YM, CBV_BR_CD, PCF_ID, BAD_DBT_RTO, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY BAD_DBT_RTO DESC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  CASE WHEN T1.TOT_LN_BAL = 0 AND T1.BAD_DBT_AMT = 0 THEN 0
                                       ELSE T1.BAD_DBT_AMT / T1.TOT_LN_BAL * 100 * T2.ASSET_WEIGHTED_AVG
                                       END AS BAD_DBT_RTO
                           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN ASSET_WEIGHTED_AVG T2
                                                                      ON T1.BAS_YM = T2.BAS_YM
                                                                     AND T1.PCF_ID = T2.PCF_ID
                          )
                  ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '069' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '4' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '2' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  ROUND(XX.BAD_DBT_RTO,2) AS AGT_VAL
           FROM   (SELECT BAS_YM, CBV_BR_CD, PCF_ID, BAD_DBT_RTO, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY BAD_DBT_RTO aSC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  CASE WHEN T1.TOT_LN_BAL = 0 AND T1.BAD_DBT_AMT = 0 THEN 0
                                       ELSE T1.BAD_DBT_AMT / T1.TOT_LN_BAL * 100 * T2.ASSET_WEIGHTED_AVG
                                       END AS BAD_DBT_RTO
                           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN ASSET_WEIGHTED_AVG T2
                                                                      ON T1.BAS_YM = T2.BAS_YM
                                                                     AND T1.PCF_ID = T2.PCF_ID
                          )
                  ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3
          ),
          FROM_KPI_TYP_070 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '070' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(CASE WHEN T1.TOT_LN_BAL = 0 THEN 0
                                 ELSE T1.TOT_COLL_VAL / T1.TOT_LN_BAL * 100
                                 END),2) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                      ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT BAS_YM,
                  CBV_BR_CD,
                  '070' AS KPI_TYP_CD,
                  '4'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(COLL_RTO, 2) AS AGT_VAL
           FROM   (SELECT T1.BAS_YM,
                          T2.CBV_BR_CD,
                          SUM(CASE WHEN T1.TOT_LN_BAL = 0 THEN 0
                                   ELSE T1.TOT_COLL_VAL / T1.TOT_LN_BAL * 100 * T2.ASSET_WEIGHTED_AVG
                                   END) AS COLL_RTO
                   FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN ASSET_WEIGHTED_AVG_070 T2
                                                              ON T1.BAS_YM = T2.BAS_YM
                                                             AND T1.PCF_ID = T2.PCF_ID
                   GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                  )

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '070' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '0' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '1' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  ROUND(XX.COLL_RTO,2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PCF_ID, COLL_RTO, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY COLL_RTO DESC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  CASE WHEN T1.TOT_LN_BAL = 0 THEN 0
                                       ELSE T1.TOT_COLL_VAL / T1.TOT_LN_BAL * 100
                                       END  AS COLL_RTO
                           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                                      ON T2.PCF_ID = T1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '070' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '0' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '2' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  ROUND(XX.COLL_RTO,2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PCF_ID, COLL_RTO, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY COLL_RTO ASC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  CASE WHEN T1.TOT_LN_BAL = 0 THEN 0
                                       ELSE T1.TOT_COLL_VAL / T1.TOT_LN_BAL * 100
                                       END  AS COLL_RTO
                           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                                      ON T2.PCF_ID = T1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '070' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '4' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '1' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  ROUND(XX.COLL_RTO,2) AS AGT_VAL
           FROM   (SELECT BAS_YM, CBV_BR_CD, PCF_ID, COLL_RTO, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY COLL_RTO DESC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  CASE WHEN T1.TOT_LN_BAL = 0 THEN 0
                                       ELSE T1.TOT_COLL_VAL / T1.TOT_LN_BAL * 100 * T2.ASSET_WEIGHTED_AVG
                                       END  AS COLL_RTO
                           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN ASSET_WEIGHTED_AVG_070 T2
                                                                      ON T1.BAS_YM = T2.BAS_YM
                                                                     AND T1.PCF_ID = T2.PCF_ID
                          )
                  ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '070' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '4' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '2' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  ROUND(XX.COLL_RTO,2) AS AGT_VAL
           FROM   (SELECT BAS_YM, CBV_BR_CD, PCF_ID, COLL_RTO, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY COLL_RTO ASC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  CASE WHEN T1.TOT_LN_BAL = 0 THEN 0
                                       ELSE T1.TOT_COLL_VAL / T1.TOT_LN_BAL * 100 * T2.ASSET_WEIGHTED_AVG
                                       END  AS COLL_RTO
                           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN ASSET_WEIGHTED_AVG_070 T2
                                                                      ON T1.BAS_YM = T2.BAS_YM
                                                                     AND T1.PCF_ID = T2.PCF_ID
                          )
                  ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

          ),
          FROM_KPI_TYP_071 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '071' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(CASE WHEN T1.TOT_LN_BAL = 0 THEN 0
                                 ELSE T1.TOT_PRVS_AMT / T1.TOT_LN_BAL * 100
                                 END), 2) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                      ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT BAS_YM,
                  CBV_BR_CD,
                  '071' AS KPI_TYP_CD,
                  '4'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(PRVS_RTO, 2) AS AGT_VAL
           FROM   (SELECT T1.BAS_YM,
                          T2.CBV_BR_CD,
                          SUM(CASE WHEN T1.TOT_LN_BAL = 0 THEN 0
                                   ELSE T1.TOT_PRVS_AMT / T1.TOT_LN_BAL * 100 * T2.ASSET_WEIGHTED_AVG
                                   END) AS PRVS_RTO
                   FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN ASSET_WEIGHTED_AVG_071 T2
                                                              ON T1.BAS_YM = T2.BAS_YM
                                                             AND T1.PCF_ID = T2.PCF_ID
                   GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                  )

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '071' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '0' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '1' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  ROUND(XX.PRVS_RTO, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PCF_ID, PRVS_RTO, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY PRVS_RTO DESC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  CASE WHEN T1.TOT_LN_BAL = 0 THEN 0
                                       ELSE T1.TOT_PRVS_AMT / T1.TOT_LN_BAL * 100
                                       END  AS PRVS_RTO
                           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                                      ON T2.PCF_ID = T1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '071' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '0' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '2' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  ROUND(XX.PRVS_RTO, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PCF_ID, PRVS_RTO, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY PRVS_RTO ASC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  CASE WHEN T1.TOT_LN_BAL = 0 THEN 0
                                       ELSE T1.TOT_PRVS_AMT / T1.TOT_LN_BAL * 100
                                       END  AS PRVS_RTO
                           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                                      ON T2.PCF_ID = T1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '071' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '4' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '1' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  ROUND(XX.PRVS_RTO, 2) AS AGT_VAL
           FROM   (SELECT BAS_YM, CBV_BR_CD, PCF_ID, PRVS_RTO, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY PRVS_RTO DESC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  CASE WHEN T1.TOT_LN_BAL = 0 THEN 0
                                       ELSE T1.TOT_PRVS_AMT / T1.TOT_LN_BAL * 100 * T2.ASSET_WEIGHTED_AVG
                                       END  AS PRVS_RTO
                           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN ASSET_WEIGHTED_AVG_071 T2
                                                                      ON T1.BAS_YM = T2.BAS_YM
                                                                     AND T1.PCF_ID = T2.PCF_ID
                          )
                  ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '071' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '4' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '2' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  ROUND(XX.PRVS_RTO, 2) AS AGT_VAL
           FROM   (SELECT BAS_YM, CBV_BR_CD, PCF_ID, PRVS_RTO, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY PRVS_RTO ASC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  CASE WHEN T1.TOT_LN_BAL = 0 THEN 0
                                       ELSE T1.TOT_PRVS_AMT / T1.TOT_LN_BAL * 100 * T2.ASSET_WEIGHTED_AVG
                                       END  AS PRVS_RTO
                           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN ASSET_WEIGHTED_AVG_071 T2
                                                                      ON T1.BAS_YM = T2.BAS_YM
                                                                     AND T1.PCF_ID = T2.PCF_ID
                          )
                  ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

          ),
          FROM_KPI_TYP_072 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '072' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.DBT_GRP_1_BAL) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                      ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '072' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.DBT_GRP_1_BAL),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                      ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
          ),
          FROM_KPI_TYP_073 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '073' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.DBT_GRP_2_BAL) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                      ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '073' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.DBT_GRP_2_BAL),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                      ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
          ),
          FROM_KPI_TYP_074 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '074' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.DBT_GRP_3_BAL) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                      ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '074' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.DBT_GRP_3_BAL),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                      ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
          ),
          FROM_KPI_TYP_075 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '075' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.DBT_GRP_4_BAL) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                      ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '075' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.DBT_GRP_4_BAL),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                      ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
          ),
          FROM_KPI_TYP_076 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '076' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.DBT_GRP_5_BAL) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                      ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '076' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.DBT_GRP_5_BAL),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                      ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
          ),
          FROM_KPI_TYP_077 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '077' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.RL_EST_COLL_VAL) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                      ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '077' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.RL_EST_COLL_VAL),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                      ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
          ),
          FROM_KPI_TYP_078 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '078' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.DPST_PPL_CR_FUND_COLL_VAL) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                      ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '078' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.DPST_PPL_CR_FUND_COLL_VAL),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                      ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
          ),
          FROM_KPI_TYP_079 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '079' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.PROD_COLL_VAL) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                      ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '079' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.PROD_COLL_VAL),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                      ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
          ),
          FROM_KPI_TYP_080 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '080' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.VP_COLL_VAL) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                      ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '080' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.VP_COLL_VAL),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                      ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
          ),
          FROM_KPI_TYP_081 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '081' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.OTHR_PRPTS_COLL_VAL) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                      ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '081' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.OTHR_PRPTS_COLL_VAL),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                      ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
          ),
          FROM_KPI_TYP_082 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '082' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.GENL_PRVS_AMT) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                      ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '082' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.GENL_PRVS_AMT),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                      ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
          ),
          FROM_KPI_TYP_083 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '083' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.DBT_GRP_1_SPEC_PRVS_AMT) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                      ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '083' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.DBT_GRP_1_SPEC_PRVS_AMT),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                      ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
          ),
          FROM_KPI_TYP_084 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '084' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.DBT_GRP_2_SPEC_PRVS_AMT) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                      ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '084' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.DBT_GRP_2_SPEC_PRVS_AMT),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                      ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
          ),
          FROM_KPI_TYP_085 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '085' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.DBT_GRP_3_SPEC_PRVS_AMT) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                      ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '085' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.DBT_GRP_3_SPEC_PRVS_AMT),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                      ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
          ),
          FROM_KPI_TYP_086 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '086' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.DBT_GRP_4_SPEC_PRVS_AMT) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                      ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '086' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.DBT_GRP_4_SPEC_PRVS_AMT),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                      ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
          ),
          FROM_KPI_TYP_087 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '087' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.DBT_GRP_5_SPEC_PRVS_AMT) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                      ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '087' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.DBT_GRP_5_SPEC_PRVS_AMT),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                      ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
          ),
          FROM_KPI_TYP_088 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '088' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.PRFBL_AST_VS_TOT_AST_RTO), 2) AS AGT_VAL
           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '088' AS KPI_TYP_CD,
                  '5'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(YY.PRFBL_AST_VS_TOT_AST_RTO), 2) AS AGT_VAL
           FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD
                   FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                  ON T1.PCF_ID = T2.PCF_ID
                   GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                  ) XX INNER JOIN (SELECT A.BAS_YM,
                                          B.CBV_BR_CD,
                                          ROUND(AVG(A.PRFBL_AST_VS_TOT_AST_RTO), 2) AS PRFBL_AST_VS_TOT_AST_RTO
                                   FROM   TM27_MMLY_PCF_KEY_IDC_A A INNER JOIN TM00_PCF_D B
                                                                           ON B.PCF_ID = A.PCF_ID
                                   GROUP BY A.BAS_YM, B.CBV_BR_CD
                                  ) YY
                               ON YY.BAS_YM    >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                              AND YY.BAS_YM    <= XX.BAS_YM
                              AND YY.CBV_BR_CD  = XX.CBV_BR_CD
           GROUP BY XX.BAS_YM, XX.CBV_BR_CD

           UNION ALL

           SELECT BAS_YM,
                  CBV_BR_CD,
                  '088' AS KPI_TYP_CD,
                  '4'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(PRFBL_AST_VS_TOT_AST_RTO, 2) AS AGT_VAL
           FROM   (SELECT T1.BAS_YM,
                          T2.CBV_BR_CD,
                          SUM(CASE WHEN T1.TOT_AST_AMT = 0 THEN 0
                                   ELSE T1.PRFBL_AST_AMT / T1.TOT_AST_AMT * 100 * T2.ASSET_WEIGHTED_AVG
                                   END) AS PRFBL_AST_VS_TOT_AST_RTO
                   FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN ASSET_WEIGHTED_AVG T2
                                                                  ON T1.BAS_YM = T2.BAS_YM
                                                                 AND T1.PCF_ID = T2.PCF_ID
                   GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                  )

           UNION ALL

           SELECT BAS_YM,
                  CBV_BR_CD,
                  '088' AS KPI_TYP_CD,
                  '7'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  PRFBL_AST_VS_TOT_AST_RTO AS AGT_VAL
           FROM   (SELECT XX.BAS_YM,
                          XX.CBV_BR_CD,
                          ROUND(AVG(YY.PRFBL_AST_VS_TOT_AST_RTO), 2) AS PRFBL_AST_VS_TOT_AST_RTO
                   FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          ) XX INNER JOIN (SELECT T1.BAS_YM,
                                                  T2.CBV_BR_CD,
                                                  SUM(CASE WHEN T1.TOT_AST_AMT = 0 THEN 0
                                                           ELSE T1.PRFBL_AST_AMT / T1.TOT_AST_AMT * 100 * T2.ASSET_WEIGHTED_AVG
                                                           END) AS PRFBL_AST_VS_TOT_AST_RTO
                                           FROM   TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN ASSET_WEIGHTED_AVG T2
                                                                                     ON T1.BAS_YM = T2.BAS_YM
                                                                                    AND T1.PCF_ID = T2.PCF_ID
                                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                                          ) YY
                                       ON YY.BAS_YM    >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                      AND YY.BAS_YM    <= XX.BAS_YM
                                      AND YY.CBV_BR_CD  = XX.CBV_BR_CD
                   GROUP BY XX.BAS_YM, XX.CBV_BR_CD
                  )

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '088' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '0' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '1' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.PRFBL_AST_VS_TOT_AST_RTO AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PCF_ID, PRFBL_AST_VS_TOT_AST_RTO, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY PRFBL_AST_VS_TOT_AST_RTO DESC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  T1.PRFBL_AST_VS_TOT_AST_RTO AS PRFBL_AST_VS_TOT_AST_RTO
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T2.PCF_ID = T1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '088' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '0' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '2' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.PRFBL_AST_VS_TOT_AST_RTO AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PCF_ID, PRFBL_AST_VS_TOT_AST_RTO, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY PRFBL_AST_VS_TOT_AST_RTO ASC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  T1.PRFBL_AST_VS_TOT_AST_RTO AS PRFBL_AST_VS_TOT_AST_RTO
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T2.PCF_ID = T1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT BAS_YM,
                  CBV_BR_CD,
                  '088' AS KPI_TYP_CD,
                  '5' AS AGT_METH_CD,
                  '1' AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL AS PCF_ID,
                  ROUND(PRFBL_AST_VS_TOT_AST_RTO,2) AS AGT_VAL
           FROM  (SELECT BAS_YM, CBV_BR_CD, PRFBL_AST_VS_TOT_AST_RTO, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY PRFBL_AST_VS_TOT_AST_RTO DESC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                CBV_BR_CD,
                                INI_RNK_NUM,
                                AVG(PRFBL_AST_VS_TOT_AST_RTO) AS PRFBL_AST_VS_TOT_AST_RTO
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, PCF_ID, PRFBL_AST_VS_TOT_AST_RTO, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1, CBV_BR_CD ORDER BY PRFBL_AST_VS_TOT_AST_RTO DESC NULLS LAST, PCF_ID DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               XX.PCF_ID,
                                               YY.PRFBL_AST_VS_TOT_AST_RTO
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD, T1.PCF_ID
                                                FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                                               ON T1.PCF_ID = T2.PCF_ID
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       A.PCF_ID,
                                                                       A.PRFBL_AST_VS_TOT_AST_RTO AS PRFBL_AST_VS_TOT_AST_RTO
                                                                FROM   TM27_MMLY_PCF_KEY_IDC_A A
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PCF_ID   = XX.PCF_ID
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, CBV_BR_CD, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  CBV_BR_CD,
                  '088' AS KPI_TYP_CD,
                  '5' AS AGT_METH_CD,
                  '2' AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL AS PCF_ID,
                  ROUND(PRFBL_AST_VS_TOT_AST_RTO,2) AS AGT_VAL
           FROM  (SELECT BAS_YM, CBV_BR_CD, PRFBL_AST_VS_TOT_AST_RTO, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY PRFBL_AST_VS_TOT_AST_RTO ASC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                CBV_BR_CD,
                                INI_RNK_NUM,
                                AVG(PRFBL_AST_VS_TOT_AST_RTO) AS PRFBL_AST_VS_TOT_AST_RTO
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, PCF_ID, PRFBL_AST_VS_TOT_AST_RTO, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1, CBV_BR_CD ORDER BY PRFBL_AST_VS_TOT_AST_RTO ASC NULLS LAST, PCF_ID DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               XX.PCF_ID,
                                               YY.PRFBL_AST_VS_TOT_AST_RTO
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD, T1.PCF_ID
                                                FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                                               ON T1.PCF_ID = T2.PCF_ID
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       A.PCF_ID,
                                                                       A.PRFBL_AST_VS_TOT_AST_RTO AS PRFBL_AST_VS_TOT_AST_RTO
                                                                FROM   TM27_MMLY_PCF_KEY_IDC_A A
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PCF_ID   = XX.PCF_ID
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, CBV_BR_CD, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '088' AS KPI_TYP_CD,  /* Code of variable corresponding to each KPI item */
                  '4' AS AGT_METH_CD, /* Code of variable corresponding to each aggregation method */
                  '1' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  ROUND(XX.PRFBL_AST_VS_TOT_AST_RTO, 2) AS AGT_VAL
           FROM   (SELECT BAS_YM, CBV_BR_CD, PCF_ID, PRFBL_AST_VS_TOT_AST_RTO, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY PRFBL_AST_VS_TOT_AST_RTO DESC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  CASE WHEN T1.TOT_AST_AMT = 0 THEN 0
                                       ELSE T1.PRFBL_AST_AMT / T1.TOT_AST_AMT * 100 * T2.ASSET_WEIGHTED_AVG
                                       END  AS PRFBL_AST_VS_TOT_AST_RTO
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A  T1 INNER JOIN ASSET_WEIGHTED_AVG T2
                                                                           ON T1.BAS_YM = T2.BAS_YM
                                                                          AND T1.PCF_ID = T2.PCF_ID
                          )
                  ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '088' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '4' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '2' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  ROUND(XX.PRFBL_AST_VS_TOT_AST_RTO, 2) AS AGT_VAL
           FROM   (SELECT BAS_YM, CBV_BR_CD, PCF_ID, PRFBL_AST_VS_TOT_AST_RTO, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY PRFBL_AST_VS_TOT_AST_RTO ASC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  CASE WHEN T1.TOT_AST_AMT = 0 THEN 0
                                       ELSE T1.PRFBL_AST_AMT / T1.TOT_AST_AMT * 100 * T2.ASSET_WEIGHTED_AVG
                                       END  AS PRFBL_AST_VS_TOT_AST_RTO
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A  T1 INNER JOIN ASSET_WEIGHTED_AVG T2
                                                                           ON T1.BAS_YM = T2.BAS_YM
                                                                          AND T1.PCF_ID = T2.PCF_ID
                          )
                  ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT BAS_YM,
                  CBV_BR_CD,
                  '088' AS KPI_TYP_CD,
                  '7' AS AGT_METH_CD,
                  '1' AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL AS PCF_ID,
                  ROUND(PRFBL_AST_VS_TOT_AST_RTO, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, CBV_BR_CD, PRFBL_AST_VS_TOT_AST_RTO, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY PRFBL_AST_VS_TOT_AST_RTO DESC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                CBV_BR_CD,
                                INI_RNK_NUM,
                                AVG(PRFBL_AST_VS_TOT_AST_RTO) AS PRFBL_AST_VS_TOT_AST_RTO
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, PCF_ID, PRFBL_AST_VS_TOT_AST_RTO, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1, CBV_BR_CD ORDER BY PRFBL_AST_VS_TOT_AST_RTO DESC NULLS LAST, PCF_ID DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               XX.PCF_ID,
                                               YY.PRFBL_AST_VS_TOT_AST_RTO
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD, T1.PCF_ID
                                                FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                                               ON T1.PCF_ID = T2.PCF_ID
                                               ) XX INNER JOIN (SELECT T1.BAS_YM,
                                                                       T2.CBV_BR_CD,
                                                                       T1.PCF_ID,
                                                                       CASE WHEN T1.TOT_AST_AMT = 0 THEN 0
                                                                            ELSE T1.PRFBL_AST_AMT / T1.TOT_AST_AMT * 100 * T2.ASSET_WEIGHTED_AVG
                                                                            END  AS PRFBL_AST_VS_TOT_AST_RTO
                                                                FROM   TM27_MMLY_PCF_KEY_IDC_A  T1 INNER JOIN ASSET_WEIGHTED_AVG T2
                                                                                                           ON T1.BAS_YM = T2.BAS_YM
                                                                                                          AND T1.PCF_ID = T2.PCF_ID
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PCF_ID   = XX.PCF_ID
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, CBV_BR_CD, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  CBV_BR_CD,
                  '088' AS KPI_TYP_CD,
                  '7' AS AGT_METH_CD,
                  '2' AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL AS PCF_ID,
                  ROUND(PRFBL_AST_VS_TOT_AST_RTO, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, CBV_BR_CD, PRFBL_AST_VS_TOT_AST_RTO, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY PRFBL_AST_VS_TOT_AST_RTO ASC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                CBV_BR_CD,
                                INI_RNK_NUM,
                                AVG(PRFBL_AST_VS_TOT_AST_RTO) AS PRFBL_AST_VS_TOT_AST_RTO
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, PCF_ID, PRFBL_AST_VS_TOT_AST_RTO, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1, CBV_BR_CD ORDER BY PRFBL_AST_VS_TOT_AST_RTO ASC NULLS LAST, PCF_ID DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               XX.PCF_ID,
                                               YY.PRFBL_AST_VS_TOT_AST_RTO
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD, T1.PCF_ID
                                                FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                                               ON T1.PCF_ID = T2.PCF_ID
                                               ) XX INNER JOIN (SELECT T1.BAS_YM,
                                                                       T2.CBV_BR_CD,
                                                                       T1.PCF_ID,
                                                                       CASE WHEN T1.TOT_AST_AMT = 0 THEN 0
                                                                            ELSE T1.PRFBL_AST_AMT / T1.TOT_AST_AMT * 100 * T2.ASSET_WEIGHTED_AVG
                                                                            END  AS PRFBL_AST_VS_TOT_AST_RTO
                                                                FROM   TM27_MMLY_PCF_KEY_IDC_A  T1 INNER JOIN ASSET_WEIGHTED_AVG T2
                                                                                                           ON T1.BAS_YM = T2.BAS_YM
                                                                                                          AND T1.PCF_ID = T2.PCF_ID
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PCF_ID   = XX.PCF_ID
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, CBV_BR_CD, INI_RNK_NUM
                        )
                 )
          ),
          FROM_KPI_TYP_089 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '089' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.CNTRBT_CBV_LT_CAP_AMT) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '089' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.CNTRBT_CBV_LT_CAP_AMT),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
          ),
          FROM_KPI_TYP_090 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '090' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.TOT_INCM_AMT - T1.TOT_EXP_AMT) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                       ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '090' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.TOT_INCM_AMT - T1.TOT_EXP_AMT),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                       ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '090' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '0' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '1' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.NET_PROFIT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PCF_ID, NET_PROFIT, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY NET_PROFIT DESC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  T1.TOT_INCM_AMT - T1.TOT_EXP_AMT AS NET_PROFIT
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T2.PCF_ID = T1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '090' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '0' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '2' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.NET_PROFIT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PCF_ID, NET_PROFIT, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY NET_PROFIT ASC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  T1.TOT_INCM_AMT - T1.TOT_EXP_AMT AS NET_PROFIT
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T2.PCF_ID = T1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '090' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '4' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '1' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  ROUND(XX.NET_PROFIT, 0) AS AGT_VAL
           FROM   (SELECT BAS_YM, CBV_BR_CD, PCF_ID, NET_PROFIT, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY NET_PROFIT DESC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  (T1.TOT_INCM_AMT - T1.TOT_EXP_AMT) * T2.ASSET_WEIGHTED_AVG AS NET_PROFIT
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN ASSET_WEIGHTED_AVG T2
                                                                       ON T1.BAS_YM = T2.BAS_YM
                                                                      AND T1.PCF_ID = T2.PCF_ID
                          )
                  ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '090' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '4' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '2' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  ROUND(XX.NET_PROFIT, 0) AS AGT_VAL
           FROM   (SELECT BAS_YM, CBV_BR_CD, PCF_ID, NET_PROFIT, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY NET_PROFIT ASC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  (T1.TOT_INCM_AMT - T1.TOT_EXP_AMT) * T2.ASSET_WEIGHTED_AVG AS NET_PROFIT
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN ASSET_WEIGHTED_AVG T2
                                                                       ON T1.BAS_YM = T2.BAS_YM
                                                                      AND T1.PCF_ID = T2.PCF_ID
                          )
                  ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT BAS_YM,
                  CBV_BR_CD,
                  '090' AS KPI_TYP_CD,
                  '5' AS AGT_METH_CD,
                  '1' AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL AS PCF_ID,
                  ROUND(NET_PROFIT,0) AS AGT_VAL
           FROM  (SELECT BAS_YM, CBV_BR_CD, NET_PROFIT, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY NET_PROFIT DESC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                CBV_BR_CD,
                                INI_RNK_NUM,
                                AVG(NET_PROFIT) AS NET_PROFIT
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, PCF_ID, NET_PROFIT, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1, CBV_BR_CD ORDER BY NET_PROFIT DESC NULLS LAST, PCF_ID DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               XX.PCF_ID,
                                               YY.NET_PROFIT
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD, T1.PCF_ID
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       A.PCF_ID,
                                                                       A.TOT_INCM_AMT - A.TOT_EXP_AMT AS NET_PROFIT
                                                                FROM   TM23_MMLY_INCM_EXP_A A
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PCF_ID   = XX.PCF_ID
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, CBV_BR_CD, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  CBV_BR_CD,
                  '090' AS KPI_TYP_CD,
                  '5' AS AGT_METH_CD,
                  '2' AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL AS PCF_ID,
                  ROUND(NET_PROFIT,0) AS AGT_VAL
           FROM  (SELECT BAS_YM, CBV_BR_CD, NET_PROFIT, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY NET_PROFIT ASC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                CBV_BR_CD,
                                INI_RNK_NUM,
                                AVG(NET_PROFIT) AS NET_PROFIT
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, PCF_ID, NET_PROFIT, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1, CBV_BR_CD ORDER BY NET_PROFIT ASC NULLS LAST, PCF_ID DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               XX.PCF_ID,
                                               YY.NET_PROFIT
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD, T1.PCF_ID
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       A.PCF_ID,
                                                                       A.TOT_INCM_AMT - A.TOT_EXP_AMT AS NET_PROFIT
                                                                FROM   TM23_MMLY_INCM_EXP_A A
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PCF_ID   = XX.PCF_ID
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, CBV_BR_CD, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  CBV_BR_CD,
                  '090' AS KPI_TYP_CD,
                  '7' AS AGT_METH_CD,
                  '1' AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL AS PCF_ID,
                  ROUND(NET_PROFIT, 0) AS AGT_VAL
           FROM  (SELECT BAS_YM, CBV_BR_CD, NET_PROFIT, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY NET_PROFIT DESC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                CBV_BR_CD,
                                INI_RNK_NUM,
                                AVG(NET_PROFIT) AS NET_PROFIT
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, PCF_ID, NET_PROFIT, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1, CBV_BR_CD ORDER BY NET_PROFIT DESC NULLS LAST, PCF_ID DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               XX.PCF_ID,
                                               YY.NET_PROFIT
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD, T1.PCF_ID
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                               ) XX INNER JOIN (SELECT T1.BAS_YM,
                                                                       T2.CBV_BR_CD,
                                                                       T1.PCF_ID,
                                                                       (T1.TOT_INCM_AMT - T1.TOT_EXP_AMT) * T2.ASSET_WEIGHTED_AVG AS NET_PROFIT
                                                                FROM   TM23_MMLY_INCM_EXP_A T1 INNER JOIN ASSET_WEIGHTED_AVG T2
                                                                                                       ON T1.BAS_YM = T2.BAS_YM
                                                                                                      AND T1.PCF_ID = T2.PCF_ID
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PCF_ID   = XX.PCF_ID
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, CBV_BR_CD, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  CBV_BR_CD,
                  '090' AS KPI_TYP_CD,
                  '7' AS AGT_METH_CD,
                  '2' AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL AS PCF_ID,
                  ROUND(NET_PROFIT, 0) AS AGT_VAL
           FROM  (SELECT BAS_YM, CBV_BR_CD, NET_PROFIT, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY NET_PROFIT ASC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                CBV_BR_CD,
                                INI_RNK_NUM,
                                AVG(NET_PROFIT) AS NET_PROFIT
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, PCF_ID, NET_PROFIT, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1, CBV_BR_CD ORDER BY NET_PROFIT ASC NULLS LAST, PCF_ID DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               XX.PCF_ID,
                                               YY.NET_PROFIT
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD, T1.PCF_ID
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                               ) XX INNER JOIN (SELECT T1.BAS_YM,
                                                                       T2.CBV_BR_CD,
                                                                       T1.PCF_ID,
                                                                       (T1.TOT_INCM_AMT - T1.TOT_EXP_AMT) * T2.ASSET_WEIGHTED_AVG AS NET_PROFIT
                                                                FROM   TM23_MMLY_INCM_EXP_A T1 INNER JOIN ASSET_WEIGHTED_AVG T2
                                                                                                       ON T1.BAS_YM = T2.BAS_YM
                                                                                                      AND T1.PCF_ID = T2.PCF_ID
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PCF_ID   = XX.PCF_ID
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, CBV_BR_CD, INI_RNK_NUM
                        )
                 )
          ),
          FROM_KPI_TYP_091 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '091' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(CASE WHEN T1.TOT_INCM_AMT - T1.TOT_EXP_AMT = 0 THEN 0
                                 WHEN T1.TOT_INCM_AMT - T1.TOT_EXP_AMT <> 0 AND T1.TOT_INCM_AMT = 0 THEN NULL
                                 ELSE CASE WHEN T1.TOT_INCM_AMT = 0 THEN 0
                                           ELSE ( T1.TOT_INCM_AMT - T1.TOT_EXP_AMT ) / ( T1.TOT_INCM_AMT ) * 100
                                           END
                                 END)
                        , 2) AS AGT_VAL
           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                       ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '091' AS KPI_TYP_CD,
                  '5'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(YY.PROF_VS_TOT_INCM_RTO), 2) AS AGT_VAL
           FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD
                   FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                               ON T1.PCF_ID = T2.PCF_ID
                   GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                  ) XX INNER JOIN (SELECT A.BAS_YM,
                                          B.CBV_BR_CD,
                                          AVG(CASE WHEN A.TOT_INCM_AMT - A.TOT_EXP_AMT = 0 THEN 0
                                                   WHEN A.TOT_INCM_AMT - A.TOT_EXP_AMT <> 0 AND A.TOT_INCM_AMT = 0 THEN  NULL
                                                   ELSE CASE WHEN A.TOT_INCM_AMT = 0 THEN 0
                                                             ELSE ( A.TOT_INCM_AMT - A.TOT_EXP_AMT ) / ( A.TOT_INCM_AMT ) * 100
                                                             END
                                                   END
                                              ) AS PROF_VS_TOT_INCM_RTO
                                   FROM   TM23_MMLY_INCM_EXP_A A INNER JOIN TM00_PCF_D B
                                                                         ON B.PCF_ID = A.PCF_ID
                                   GROUP BY A.BAS_YM, B.CBV_BR_CD
                                  ) YY
                               ON YY.BAS_YM    >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                              AND YY.BAS_YM    <= XX.BAS_YM
                              AND YY.CBV_BR_CD  = XX.CBV_BR_CD
           GROUP BY XX.BAS_YM, XX.CBV_BR_CD

           UNION ALL

           SELECT BAS_YM,
                  CBV_BR_CD,
                  '091' AS KPI_TYP_CD,
                  '4'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(PROF_VS_TOT_INCM_RTO, 2) AS AGT_VAL
           FROM   (SELECT T1.BAS_YM,
                          T2.CBV_BR_CD,
                          SUM(CASE WHEN T1.TOT_INCM_AMT - T1.TOT_EXP_AMT = 0 THEN 0
                                   WHEN T1.TOT_INCM_AMT - T1.TOT_EXP_AMT <> 0 AND T1.TOT_INCM_AMT = 0 THEN  NULL
                                   ELSE CASE WHEN T1.TOT_INCM_AMT = 0 THEN 0
                                             ELSE ( T1.TOT_INCM_AMT - T1.TOT_EXP_AMT ) / ( T1.TOT_INCM_AMT ) * 100
                                             END
                                   END * T2.ASSET_WEIGHTED_AVG) AS PROF_VS_TOT_INCM_RTO
                   FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN ASSET_WEIGHTED_AVG T2
                                                               ON T1.BAS_YM = T2.BAS_YM
                                                              AND T1.PCF_ID = T2.PCF_ID
                   GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                  )

           UNION ALL

           SELECT BAS_YM,
                  CBV_BR_CD,
                  '091' AS KPI_TYP_CD,
                  '7'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  PROF_VS_TOT_INCM_RTO AS AGT_VAL
           FROM   (SELECT XX.BAS_YM,
                          XX.CBV_BR_CD,
                          ROUND(AVG(YY.PROF_VS_TOT_INCM_RTO), 2) AS PROF_VS_TOT_INCM_RTO
                   FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          ) XX INNER JOIN (SELECT T1.BAS_YM,
                                                  T2.CBV_BR_CD,
                                                  SUM(CASE WHEN T1.TOT_INCM_AMT - T1.TOT_EXP_AMT = 0 THEN 0
                                                           WHEN T1.TOT_INCM_AMT - T1.TOT_EXP_AMT <> 0 AND T1.TOT_INCM_AMT = 0 THEN  NULL
                                                           ELSE CASE WHEN T1.TOT_INCM_AMT = 0 THEN 0
                                                                     ELSE ( T1.TOT_INCM_AMT - T1.TOT_EXP_AMT ) / ( T1.TOT_INCM_AMT ) * 100
                                                                     END
                                                           END * T2.ASSET_WEIGHTED_AVG) AS PROF_VS_TOT_INCM_RTO
                                           FROM   TM23_MMLY_INCM_EXP_A T1 INNER JOIN ASSET_WEIGHTED_AVG T2
                                                                                  ON T1.BAS_YM = T2.BAS_YM
                                                                                 AND T1.PCF_ID = T2.PCF_ID
                                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                                          ) YY
                                       ON YY.BAS_YM    >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                      AND YY.BAS_YM    <= XX.BAS_YM
                                      AND YY.CBV_BR_CD  = XX.CBV_BR_CD
                   GROUP BY XX.BAS_YM, XX.CBV_BR_CD
                  )

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '091' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '0' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '1' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  ROUND(XX.NET_PROF_VS_INCM_RTO, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PCF_ID, NET_PROF_VS_INCM_RTO, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY NET_PROF_VS_INCM_RTO DESC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  CASE WHEN T1.TOT_INCM_AMT - T1.TOT_EXP_AMT = 0 THEN 0
                                       WHEN T1.TOT_INCM_AMT - T1.TOT_EXP_AMT <> 0 AND T1.TOT_INCM_AMT = 0 THEN  NULL
                                       ELSE CASE WHEN T1.TOT_INCM_AMT = 0 THEN 0
                                                 ELSE ( T1.TOT_INCM_AMT - T1.TOT_EXP_AMT ) / ( T1.TOT_INCM_AMT ) * 100
                                                 END
                                       END AS NET_PROF_VS_INCM_RTO
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T2.PCF_ID = T1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '091' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '0' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '2' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  ROUND(XX.NET_PROF_VS_INCM_RTO, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PCF_ID, NET_PROF_VS_INCM_RTO, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY NET_PROF_VS_INCM_RTO ASC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  CASE WHEN T1.TOT_INCM_AMT - T1.TOT_EXP_AMT = 0 THEN 0
                                       WHEN T1.TOT_INCM_AMT - T1.TOT_EXP_AMT <> 0 AND T1.TOT_INCM_AMT = 0 THEN  NULL
                                       ELSE CASE WHEN T1.TOT_INCM_AMT = 0 THEN 0
                                                 ELSE ( T1.TOT_INCM_AMT - T1.TOT_EXP_AMT ) / ( T1.TOT_INCM_AMT ) * 100
                                                 END
                                       END AS NET_PROF_VS_INCM_RTO
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T2.PCF_ID = T1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '091' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '4' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '1' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  ROUND(XX.NET_PROF_VS_INCM_RTO, 2) AS AGT_VAL
           FROM   (SELECT BAS_YM, CBV_BR_CD, PCF_ID, NET_PROF_VS_INCM_RTO, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY NET_PROF_VS_INCM_RTO DESC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  CASE WHEN T1.TOT_INCM_AMT - T1.TOT_EXP_AMT = 0 THEN 0
                                       WHEN T1.TOT_INCM_AMT - T1.TOT_EXP_AMT <> 0 AND T1.TOT_INCM_AMT = 0 THEN NULL
                                       ELSE CASE WHEN T1.TOT_INCM_AMT = 0 THEN 0
                                                 ELSE ( T1.TOT_INCM_AMT - T1.TOT_EXP_AMT ) / ( T1.TOT_INCM_AMT ) * 100
                                                 END
                                       END * T2.ASSET_WEIGHTED_AVG AS NET_PROF_VS_INCM_RTO
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN ASSET_WEIGHTED_AVG T2
                                                                       ON T1.BAS_YM = T2.BAS_YM
                                                                      AND T1.PCF_ID = T2.PCF_ID
                          )
                  ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '091' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '4' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '2' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  ROUND(XX.NET_PROF_VS_INCM_RTO, 2) AS AGT_VAL
           FROM   (SELECT BAS_YM, CBV_BR_CD, PCF_ID, NET_PROF_VS_INCM_RTO, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY NET_PROF_VS_INCM_RTO ASC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  CASE WHEN T1.TOT_INCM_AMT - T1.TOT_EXP_AMT = 0 THEN 0
                                       WHEN T1.TOT_INCM_AMT - T1.TOT_EXP_AMT <> 0 AND T1.TOT_INCM_AMT = 0 THEN NULL
                                       ELSE CASE WHEN T1.TOT_INCM_AMT = 0 THEN 0
                                                 ELSE ( T1.TOT_INCM_AMT - T1.TOT_EXP_AMT ) / ( T1.TOT_INCM_AMT ) * 100
                                                 END
                                       END * T2.ASSET_WEIGHTED_AVG AS NET_PROF_VS_INCM_RTO
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN ASSET_WEIGHTED_AVG T2
                                                                       ON T1.BAS_YM = T2.BAS_YM
                                                                      AND T1.PCF_ID = T2.PCF_ID
                          )
                  ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT BAS_YM,
                  CBV_BR_CD,
                  '091' AS KPI_TYP_CD,
                  '5' AS AGT_METH_CD,
                  '1' AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL AS PCF_ID,
                  ROUND(NET_PROF_VS_INCM_RTO,2) AS AGT_VAL
           FROM  (SELECT BAS_YM, CBV_BR_CD, NET_PROF_VS_INCM_RTO, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY NET_PROF_VS_INCM_RTO DESC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                CBV_BR_CD,
                                INI_RNK_NUM,
                                AVG(NET_PROF_VS_INCM_RTO) AS NET_PROF_VS_INCM_RTO
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, PCF_ID, NET_PROF_VS_INCM_RTO, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1, CBV_BR_CD ORDER BY NET_PROF_VS_INCM_RTO DESC NULLS LAST, PCF_ID DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               XX.PCF_ID,
                                               YY.NET_PROF_VS_INCM_RTO
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD, T1.PCF_ID
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       A.PCF_ID,
                                                                       CASE WHEN A.TOT_INCM_AMT - A.TOT_EXP_AMT = 0 THEN 0
                                                                            WHEN A.TOT_INCM_AMT - A.TOT_EXP_AMT <> 0 AND A.TOT_INCM_AMT = 0 THEN NULL
                                                                            ELSE CASE WHEN A.TOT_INCM_AMT = 0 THEN 0
                                                                                      ELSE ( A.TOT_INCM_AMT - A.TOT_EXP_AMT ) / ( A.TOT_INCM_AMT ) * 100
                                                                                      END
                                                                            END AS NET_PROF_VS_INCM_RTO
                                                                FROM   TM23_MMLY_INCM_EXP_A A
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PCF_ID   = XX.PCF_ID
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, CBV_BR_CD, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  CBV_BR_CD,
                  '091' AS KPI_TYP_CD,
                  '5' AS AGT_METH_CD,
                  '2' AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL AS PCF_ID,
                  ROUND(NET_PROF_VS_INCM_RTO,2) AS AGT_VAL
           FROM  (SELECT BAS_YM, CBV_BR_CD, NET_PROF_VS_INCM_RTO, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY NET_PROF_VS_INCM_RTO ASC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                CBV_BR_CD,
                                INI_RNK_NUM,
                                AVG(NET_PROF_VS_INCM_RTO) AS NET_PROF_VS_INCM_RTO
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, PCF_ID, NET_PROF_VS_INCM_RTO, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1, CBV_BR_CD ORDER BY NET_PROF_VS_INCM_RTO ASC NULLS LAST, PCF_ID DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               XX.PCF_ID,
                                               YY.NET_PROF_VS_INCM_RTO
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD, T1.PCF_ID
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       A.PCF_ID,
                                                                       CASE WHEN A.TOT_INCM_AMT - A.TOT_EXP_AMT = 0 THEN 0
                                                                            WHEN A.TOT_INCM_AMT - A.TOT_EXP_AMT <> 0 AND A.TOT_INCM_AMT = 0 THEN NULL
                                                                            ELSE CASE WHEN A.TOT_INCM_AMT = 0 THEN 0
                                                                                      ELSE ( A.TOT_INCM_AMT - A.TOT_EXP_AMT ) / ( A.TOT_INCM_AMT ) * 100
                                                                                      END
                                                                            END AS NET_PROF_VS_INCM_RTO
                                                                FROM   TM23_MMLY_INCM_EXP_A A
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PCF_ID   = XX.PCF_ID
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, CBV_BR_CD, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  CBV_BR_CD,
                  '091' AS KPI_TYP_CD,
                  '7' AS AGT_METH_CD,
                  '1' AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL AS PCF_ID,
                  ROUND(NET_PROF_VS_INCM_RTO, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, CBV_BR_CD, NET_PROF_VS_INCM_RTO, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY NET_PROF_VS_INCM_RTO DESC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                CBV_BR_CD,
                                INI_RNK_NUM,
                                AVG(NET_PROF_VS_INCM_RTO) AS NET_PROF_VS_INCM_RTO
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, PCF_ID, NET_PROF_VS_INCM_RTO, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1, CBV_BR_CD ORDER BY NET_PROF_VS_INCM_RTO DESC NULLS LAST, PCF_ID DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               XX.PCF_ID,
                                               YY.NET_PROF_VS_INCM_RTO
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD, T1.PCF_ID
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                               ) XX INNER JOIN (SELECT T1.BAS_YM,
                                                                       T2.CBV_BR_CD,
                                                                       T1.PCF_ID,
                                                                       CASE WHEN T1.TOT_INCM_AMT - T1.TOT_EXP_AMT = 0 THEN 0
                                                                            WHEN T1.TOT_INCM_AMT - T1.TOT_EXP_AMT <> 0 AND T1.TOT_INCM_AMT = 0 THEN NULL
                                                                            ELSE CASE WHEN T1.TOT_INCM_AMT = 0 THEN 0
                                                                                      ELSE ( T1.TOT_INCM_AMT - T1.TOT_EXP_AMT ) / ( T1.TOT_INCM_AMT ) * 100
                                                                                      END
                                                                            END * T2.ASSET_WEIGHTED_AVG AS NET_PROF_VS_INCM_RTO
                                                                FROM   TM23_MMLY_INCM_EXP_A T1 INNER JOIN ASSET_WEIGHTED_AVG T2
                                                                                                       ON T1.BAS_YM = T2.BAS_YM
                                                                                                      AND T1.PCF_ID = T2.PCF_ID
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PCF_ID   = XX.PCF_ID
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, CBV_BR_CD, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  CBV_BR_CD,
                  '091' AS KPI_TYP_CD,
                  '7' AS AGT_METH_CD,
                  '2' AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL AS PCF_ID,
                  ROUND(NET_PROF_VS_INCM_RTO, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, CBV_BR_CD, NET_PROF_VS_INCM_RTO, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY NET_PROF_VS_INCM_RTO ASC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                CBV_BR_CD,
                                INI_RNK_NUM,
                                AVG(NET_PROF_VS_INCM_RTO) AS NET_PROF_VS_INCM_RTO
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, PCF_ID, NET_PROF_VS_INCM_RTO, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1, CBV_BR_CD ORDER BY NET_PROF_VS_INCM_RTO ASC NULLS LAST, PCF_ID DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               XX.PCF_ID,
                                               YY.NET_PROF_VS_INCM_RTO
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD, T1.PCF_ID
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                               ) XX INNER JOIN (SELECT T1.BAS_YM,
                                                                       T2.CBV_BR_CD,
                                                                       T1.PCF_ID,
                                                                       CASE WHEN T1.TOT_INCM_AMT - T1.TOT_EXP_AMT = 0 THEN 0
                                                                            WHEN T1.TOT_INCM_AMT - T1.TOT_EXP_AMT <> 0 AND T1.TOT_INCM_AMT = 0 THEN NULL
                                                                            ELSE CASE WHEN T1.TOT_INCM_AMT = 0 THEN 0
                                                                                      ELSE ( T1.TOT_INCM_AMT - T1.TOT_EXP_AMT ) / ( T1.TOT_INCM_AMT ) * 100
                                                                                      END
                                                                            END * T2.ASSET_WEIGHTED_AVG AS NET_PROF_VS_INCM_RTO
                                                                FROM   TM23_MMLY_INCM_EXP_A T1 INNER JOIN ASSET_WEIGHTED_AVG T2
                                                                                                       ON T1.BAS_YM = T2.BAS_YM
                                                                                                      AND T1.PCF_ID = T2.PCF_ID
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PCF_ID   = XX.PCF_ID
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, CBV_BR_CD, INI_RNK_NUM
                        )
                 )
          ),
          FROM_KPI_TYP_092 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '092' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(CASE WHEN T4.L12M_AST_AMT = 0 THEN 0
                                 ELSE (T1.TOT_INCM_AMT - T1.TOT_EXP_AMT ) * 12 / ( T4.L12M_AST_AMT ) * 100
                                 END),2) AS AGT_VAL
           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                       ON T1.PCF_ID = T2.PCF_ID
                                               INNER JOIN FROM_TM27_MMLY_PCF_KEY_IDC_A T4
                                                       ON T4.BAS_YM  = T1.BAS_YM
                                                      AND T4.PCF_ID  = T1.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '092' AS KPI_TYP_CD,
                  '5'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(YY.PROF_VS_LST_12_MM_AVG_AST),2) AS AGT_VAL
           FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD
                   FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                               ON T1.PCF_ID = T2.PCF_ID
                   GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                  ) XX INNER JOIN (SELECT A.BAS_YM,
                                          B.CBV_BR_CD,
                                          AVG(CASE WHEN C.L12M_AST_AMT = 0 THEN 0
                                                   ELSE (A.TOT_INCM_AMT - A.TOT_EXP_AMT ) * 12 / ( C.L12M_AST_AMT ) * 100
                                                   END) AS PROF_VS_LST_12_MM_AVG_AST
                                   FROM   TM23_MMLY_INCM_EXP_A A INNER JOIN TM00_PCF_D B
                                                                         ON B.PCF_ID = A.PCF_ID
                                                                 INNER JOIN TM27_MMLY_PCF_KEY_IDC_A C
                                                                         ON C.BAS_YM  = A.BAS_YM
                                                                        AND C.PCF_ID  = A.PCF_ID
                                   GROUP BY A.BAS_YM, B.CBV_BR_CD
                                  ) YY
                               ON YY.BAS_YM    >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                              AND YY.BAS_YM    <= XX.BAS_YM
                              AND YY.CBV_BR_CD  = XX.CBV_BR_CD
           GROUP BY XX.BAS_YM, XX.CBV_BR_CD

           UNION ALL

           SELECT BAS_YM,
                  CBV_BR_CD,
                  '092' AS KPI_TYP_CD,
                  '4'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(PROF_VS_LST_12_MM_AVG_AST, 2) AS AGT_VAL
           FROM   (SELECT T1.BAS_YM,
                          T2.CBV_BR_CD,
                          SUM(CASE WHEN T3.L12M_AST_AMT = 0 THEN 0
                                   ELSE (T1.TOT_INCM_AMT - T1.TOT_EXP_AMT ) * 12 / ( T3.L12M_AST_AMT ) * 100 * T2.ASSET_WEIGHTED_AVG
                                   END) AS PROF_VS_LST_12_MM_AVG_AST
                   FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN ASSET_WEIGHTED_AVG T2
                                                               ON T2.BAS_YM = T1.BAS_YM
                                                              AND T2.PCF_ID = T1.PCF_ID
                                                       INNER JOIN FROM_TM27_MMLY_PCF_KEY_IDC_A T3
                                                               ON T3.BAS_YM  = T1.BAS_YM
                                                              AND T3.PCF_ID  = T1.PCF_ID
                   GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                  )

           UNION ALL

           SELECT BAS_YM,
                  CBV_BR_CD,
                  '092' AS KPI_TYP_CD,
                  '7'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  PROF_VS_LST_12_MM_AVG_AST AS AGT_VAL
           FROM   (SELECT XX.BAS_YM,
                          XX.CBV_BR_CD,
                          ROUND(AVG(YY.PROF_VS_LST_12_MM_AVG_AST), 2) AS PROF_VS_LST_12_MM_AVG_AST
                   FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          ) XX INNER JOIN (SELECT T1.BAS_YM,
                                                  T2.CBV_BR_CD,
                                                  SUM(CASE WHEN T3.L12M_AST_AMT = 0 THEN 0
                                                           ELSE (T1.TOT_INCM_AMT - T1.TOT_EXP_AMT ) * 12 / ( T3.L12M_AST_AMT ) * 100 * T2.ASSET_WEIGHTED_AVG
                                                           END) AS PROF_VS_LST_12_MM_AVG_AST
                                           FROM   TM23_MMLY_INCM_EXP_A T1 INNER JOIN ASSET_WEIGHTED_AVG T2
                                                                                  ON T1.BAS_YM = T2.BAS_YM
                                                                                 AND T1.PCF_ID = T2.PCF_ID
                                                                          INNER JOIN TM27_MMLY_PCF_KEY_IDC_A T3
                                                                                  ON T3.BAS_YM  = T1.BAS_YM
                                                                                 AND T3.PCF_ID  = T1.PCF_ID
                                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                                          ) YY
                                       ON YY.BAS_YM    >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                      AND YY.BAS_YM    <= XX.BAS_YM
                                      AND YY.CBV_BR_CD  = XX.CBV_BR_CD
                   GROUP BY XX.BAS_YM, XX.CBV_BR_CD
                  )

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '092' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '0' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '1' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  ROUND(XX.PROF_VS_LST_12_MM_AVG_AST, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PCF_ID, PROF_VS_LST_12_MM_AVG_AST, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY PROF_VS_LST_12_MM_AVG_AST DESC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  CASE WHEN T3.L12M_AST_AMT = 0 THEN 0
                                       ELSE (T1.TOT_INCM_AMT - T1.TOT_EXP_AMT) * 12 / T3.L12M_AST_AMT * 100
                                       END  AS PROF_VS_LST_12_MM_AVG_AST
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T2.PCF_ID = T1.PCF_ID
                                                               INNER JOIN TM27_MMLY_PCF_KEY_IDC_A T3
                                                                       ON T3.BAS_YM  = T1.BAS_YM
                                                                      AND T3.PCF_ID  = T1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '092' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '0' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '2' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  ROUND(XX.PROF_VS_LST_12_MM_AVG_AST, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PCF_ID, PROF_VS_LST_12_MM_AVG_AST, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY PROF_VS_LST_12_MM_AVG_AST ASC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  CASE WHEN T3.L12M_AST_AMT = 0 THEN 0
                                       ELSE (T1.TOT_INCM_AMT - T1.TOT_EXP_AMT) * 12 / T3.L12M_AST_AMT * 100
                                       END  AS PROF_VS_LST_12_MM_AVG_AST
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T2.PCF_ID = T1.PCF_ID
                                                               INNER JOIN TM27_MMLY_PCF_KEY_IDC_A T3
                                                                       ON T3.BAS_YM  = T1.BAS_YM
                                                                      AND T3.PCF_ID  = T1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '092' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '4' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '1' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  ROUND(XX.PROF_VS_LST_12_MM_AVG_AST, 2) AS AGT_VAL
           FROM   (SELECT BAS_YM, CBV_BR_CD, PCF_ID, PROF_VS_LST_12_MM_AVG_AST, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY PROF_VS_LST_12_MM_AVG_AST DESC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  CASE WHEN T3.L12M_AST_AMT = 0 THEN 0
                                       ELSE (T1.TOT_INCM_AMT - T1.TOT_EXP_AMT) * 12 / T3.L12M_AST_AMT * 100 * T2.ASSET_WEIGHTED_AVG
                                       END  AS PROF_VS_LST_12_MM_AVG_AST
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN ASSET_WEIGHTED_AVG T2
                                                                       ON T1.BAS_YM = T2.BAS_YM
                                                                      AND T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN FROM_TM27_MMLY_PCF_KEY_IDC_A T3
                                                                       ON T3.BAS_YM  = T1.BAS_YM
                                                                      AND T3.PCF_ID  = T1.PCF_ID
                          )
                  ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '092' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '4' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '2' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  ROUND(XX.PROF_VS_LST_12_MM_AVG_AST, 2) AS AGT_VAL
           FROM   (SELECT BAS_YM, CBV_BR_CD, PCF_ID, PROF_VS_LST_12_MM_AVG_AST, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY PROF_VS_LST_12_MM_AVG_AST ASC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  CASE WHEN T3.L12M_AST_AMT = 0 THEN 0
                                       ELSE (T1.TOT_INCM_AMT - T1.TOT_EXP_AMT) * 12 / T3.L12M_AST_AMT * 100 * T2.ASSET_WEIGHTED_AVG
                                       END  AS PROF_VS_LST_12_MM_AVG_AST
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN ASSET_WEIGHTED_AVG T2
                                                                       ON T1.BAS_YM = T2.BAS_YM
                                                                      AND T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN FROM_TM27_MMLY_PCF_KEY_IDC_A T3
                                                                       ON T3.BAS_YM  = T1.BAS_YM
                                                                      AND T3.PCF_ID  = T1.PCF_ID
                          )
                  ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT BAS_YM,
                  CBV_BR_CD,
                  '092' AS KPI_TYP_CD,
                  '5' AS AGT_METH_CD,
                  '1' AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL AS PCF_ID,
                  ROUND(PROF_VS_LST_12_MM_AVG_AST,2) AS AGT_VAL
           FROM  (SELECT BAS_YM, CBV_BR_CD, PROF_VS_LST_12_MM_AVG_AST, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY PROF_VS_LST_12_MM_AVG_AST DESC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                CBV_BR_CD,
                                INI_RNK_NUM,
                                AVG(PROF_VS_LST_12_MM_AVG_AST) AS PROF_VS_LST_12_MM_AVG_AST
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, PCF_ID, PROF_VS_LST_12_MM_AVG_AST, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1, CBV_BR_CD ORDER BY PROF_VS_LST_12_MM_AVG_AST DESC NULLS LAST, PCF_ID DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               XX.PCF_ID,
                                               YY.PROF_VS_LST_12_MM_AVG_AST
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD, T1.PCF_ID
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       A.PCF_ID,
                                                                       CASE WHEN B.L12M_AST_AMT = 0 THEN 0
                                                                            ELSE (A.TOT_INCM_AMT - A.TOT_EXP_AMT) * 12 / B.L12M_AST_AMT * 100
                                                                            END AS PROF_VS_LST_12_MM_AVG_AST
                                                                FROM   TM23_MMLY_INCM_EXP_A A INNER JOIN TM27_MMLY_PCF_KEY_IDC_A B
                                                                                                      ON B.BAS_YM  = A.BAS_YM
                                                                                                     AND B.PCF_ID  = A.PCF_ID
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PCF_ID   = XX.PCF_ID
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, CBV_BR_CD, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  CBV_BR_CD,
                  '092' AS KPI_TYP_CD,
                  '5' AS AGT_METH_CD,
                  '2' AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL AS PCF_ID,
                  ROUND(PROF_VS_LST_12_MM_AVG_AST,2) AS AGT_VAL
           FROM  (SELECT BAS_YM, CBV_BR_CD, PROF_VS_LST_12_MM_AVG_AST, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY PROF_VS_LST_12_MM_AVG_AST ASC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                CBV_BR_CD,
                                INI_RNK_NUM,
                                AVG(PROF_VS_LST_12_MM_AVG_AST) AS PROF_VS_LST_12_MM_AVG_AST
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, PCF_ID, PROF_VS_LST_12_MM_AVG_AST, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1, CBV_BR_CD ORDER BY PROF_VS_LST_12_MM_AVG_AST ASC NULLS LAST, PCF_ID DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               XX.PCF_ID,
                                               YY.PROF_VS_LST_12_MM_AVG_AST
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD, T1.PCF_ID
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       A.PCF_ID,
                                                                       CASE WHEN B.L12M_AST_AMT = 0 THEN 0
                                                                            ELSE (A.TOT_INCM_AMT - A.TOT_EXP_AMT) * 12 / B.L12M_AST_AMT * 100
                                                                            END  AS PROF_VS_LST_12_MM_AVG_AST
                                                                FROM   TM23_MMLY_INCM_EXP_A A INNER JOIN TM27_MMLY_PCF_KEY_IDC_A B
                                                                                                      ON B.BAS_YM  = A.BAS_YM
                                                                                                     AND B.PCF_ID  = A.PCF_ID
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PCF_ID   = XX.PCF_ID
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, CBV_BR_CD, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  CBV_BR_CD,
                  '092' AS KPI_TYP_CD,
                  '7' AS AGT_METH_CD,
                  '1' AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL AS PCF_ID,
                  ROUND(PROF_VS_LST_12_MM_AVG_AST, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, CBV_BR_CD, PROF_VS_LST_12_MM_AVG_AST, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY PROF_VS_LST_12_MM_AVG_AST DESC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                CBV_BR_CD,
                                INI_RNK_NUM,
                                AVG(PROF_VS_LST_12_MM_AVG_AST) AS PROF_VS_LST_12_MM_AVG_AST
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, PCF_ID, PROF_VS_LST_12_MM_AVG_AST, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1, CBV_BR_CD ORDER BY PROF_VS_LST_12_MM_AVG_AST DESC NULLS LAST, PCF_ID DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               XX.PCF_ID,
                                               YY.PROF_VS_LST_12_MM_AVG_AST
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD, T1.PCF_ID
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                               ) XX INNER JOIN (SELECT T1.BAS_YM,
                                                                       T2.CBV_BR_CD,
                                                                       T1.PCF_ID,
                                                                       CASE WHEN T3.L12M_AST_AMT = 0 THEN 0
                                                                            ELSE (T1.TOT_INCM_AMT - T1.TOT_EXP_AMT) * 12 / T3.L12M_AST_AMT * 100 * T2.ASSET_WEIGHTED_AVG
                                                                            END  AS PROF_VS_LST_12_MM_AVG_AST
                                                                FROM   TM23_MMLY_INCM_EXP_A T1 INNER JOIN ASSET_WEIGHTED_AVG T2
                                                                                                       ON T1.BAS_YM = T2.BAS_YM
                                                                                                      AND T1.PCF_ID = T2.PCF_ID
                                                                                               INNER JOIN TM27_MMLY_PCF_KEY_IDC_A T3
                                                                                                       ON T3.BAS_YM = T1.BAS_YM
                                                                                                      AND T3.PCF_ID = T1.PCF_ID
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PCF_ID   = XX.PCF_ID
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, CBV_BR_CD, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  CBV_BR_CD,
                  '092' AS KPI_TYP_CD,
                  '7' AS AGT_METH_CD,
                  '2' AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL AS PCF_ID,
                  ROUND(PROF_VS_LST_12_MM_AVG_AST, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, CBV_BR_CD, PROF_VS_LST_12_MM_AVG_AST, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY PROF_VS_LST_12_MM_AVG_AST ASC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                CBV_BR_CD,
                                INI_RNK_NUM,
                                AVG(PROF_VS_LST_12_MM_AVG_AST) AS PROF_VS_LST_12_MM_AVG_AST
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, PCF_ID, PROF_VS_LST_12_MM_AVG_AST, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1, CBV_BR_CD ORDER BY PROF_VS_LST_12_MM_AVG_AST ASC NULLS LAST, PCF_ID DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               XX.PCF_ID,
                                               YY.PROF_VS_LST_12_MM_AVG_AST
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD, T1.PCF_ID
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                               ) XX INNER JOIN (SELECT T1.BAS_YM,
                                                                       T2.CBV_BR_CD,
                                                                       T1.PCF_ID,
                                                                       CASE WHEN T3.L12M_AST_AMT = 0 THEN 0
                                                                            ELSE (T1.TOT_INCM_AMT - T1.TOT_EXP_AMT) * 12 / T3.L12M_AST_AMT * 100 * T2.ASSET_WEIGHTED_AVG
                                                                            END  AS PROF_VS_LST_12_MM_AVG_AST
                                                                FROM   TM23_MMLY_INCM_EXP_A T1 INNER JOIN ASSET_WEIGHTED_AVG T2
                                                                                                       ON T1.BAS_YM = T2.BAS_YM
                                                                                                      AND T1.PCF_ID = T2.PCF_ID
                                                                                               INNER JOIN TM27_MMLY_PCF_KEY_IDC_A T3
                                                                                                       ON T3.BAS_YM = T1.BAS_YM
                                                                                                      AND T3.PCF_ID = T1.PCF_ID
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PCF_ID   = XX.PCF_ID
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, CBV_BR_CD, INI_RNK_NUM
                        )
                 )
          ),
          FROM_KPI_TYP_093 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '093' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(CASE WHEN T4.L12M_LN_AMT = 0 THEN 0
                                 ELSE ( T1.TOT_INCM_AMT - T1.TOT_EXP_AMT ) * 12 / ( T4.L12M_LN_AMT ) * 100
                                 END),2) AS AGT_VAL
           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                       ON T1.PCF_ID = T2.PCF_ID
                                               INNER JOIN FROM_TM27_MMLY_PCF_KEY_IDC_A T4
                                                       ON T4.BAS_YM  = T1.BAS_YM
                                                      AND T4.PCF_ID  = T1.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '093' AS KPI_TYP_CD,
                  '5'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(YY.PROF_VS_LST_12_MM_AVG_LN_AMT),2) AS AGT_VAL
           FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD
                   FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                               ON T1.PCF_ID = T2.PCF_ID
                   GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                  ) XX INNER JOIN (SELECT A.BAS_YM,
                                         B.CBV_BR_CD,
                                         AVG(CASE WHEN C.L12M_LN_AMT = 0 THEN 0
                                                  ELSE ( A.TOT_INCM_AMT - A.TOT_EXP_AMT ) * 12 / ( C.L12M_LN_AMT ) * 100
                                                  END) AS PROF_VS_LST_12_MM_AVG_LN_AMT
                                  FROM   TM23_MMLY_INCM_EXP_A A INNER JOIN TM00_PCF_D B
                                                                        ON B.PCF_ID = A.PCF_ID
                                                                INNER JOIN TM27_MMLY_PCF_KEY_IDC_A C
                                                                        ON C.BAS_YM  = A.BAS_YM
                                                                       AND C.PCF_ID  = A.PCF_ID
                                  GROUP BY A.BAS_YM, B.CBV_BR_CD
                                 ) YY
                              ON YY.BAS_YM    >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                             AND YY.BAS_YM    <= XX.BAS_YM
                             AND YY.CBV_BR_CD  = XX.CBV_BR_CD
           GROUP BY XX.BAS_YM, XX.CBV_BR_CD

           UNION ALL

           SELECT BAS_YM,
                  CBV_BR_CD,
                  '093' AS KPI_TYP_CD,
                  '4'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(PROF_VS_LST_12_MM_AVG_LN_AMT, 2) AS AGT_VAL
           FROM   (SELECT T1.BAS_YM,
                          T2.CBV_BR_CD,
                          SUM(CASE WHEN T3.L12M_LN_AMT = 0 THEN 0
                                   ELSE (T1.TOT_INCM_AMT - T1.TOT_EXP_AMT ) * 12 / ( T3.L12M_LN_AMT ) * 100 * T2.ASSET_WEIGHTED_AVG
                                   END) AS PROF_VS_LST_12_MM_AVG_LN_AMT
                   FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN ASSET_WEIGHTED_AVG T2
                                                               ON T2.BAS_YM = T1.BAS_YM
                                                              AND T2.PCF_ID = T1.PCF_ID
                                                       INNER JOIN TM27_MMLY_PCF_KEY_IDC_A T3
                                                               ON T3.BAS_YM  = T1.BAS_YM
                                                              AND T3.PCF_ID  = T1.PCF_ID
                   GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                  )

           UNION ALL

           SELECT BAS_YM,
                  CBV_BR_CD,
                  '093' AS KPI_TYP_CD,
                  '7'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  PROF_VS_LST_12_MM_AVG_LN_AMT AS AGT_VAL
           FROM   (SELECT XX.BAS_YM,
                          XX.CBV_BR_CD,
                          ROUND(AVG(YY.PROF_VS_LST_12_MM_AVG_LN_AMT), 2) AS PROF_VS_LST_12_MM_AVG_LN_AMT
                   FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          ) XX INNER JOIN (SELECT T1.BAS_YM,
                                                  T2.CBV_BR_CD,
                                                  SUM(CASE WHEN T3.L12M_LN_AMT = 0 THEN 0
                                                           ELSE (T1.TOT_INCM_AMT - T1.TOT_EXP_AMT ) * 12 / ( T3.L12M_LN_AMT ) * 100 * T2.ASSET_WEIGHTED_AVG
                                                           END) AS PROF_VS_LST_12_MM_AVG_LN_AMT
                                           FROM   TM23_MMLY_INCM_EXP_A T1 INNER JOIN ASSET_WEIGHTED_AVG T2
                                                                                  ON T1.BAS_YM = T2.BAS_YM
                                                                                 AND T1.PCF_ID = T2.PCF_ID
                                                                          INNER JOIN TM27_MMLY_PCF_KEY_IDC_A T3
                                                                                  ON T3.BAS_YM  = T1.BAS_YM
                                                                                 AND T3.PCF_ID  = T1.PCF_ID
                                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                                          ) YY
                                       ON YY.BAS_YM    >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                      AND YY.BAS_YM    <= XX.BAS_YM
                                      AND YY.CBV_BR_CD  = XX.CBV_BR_CD
                   GROUP BY XX.BAS_YM, XX.CBV_BR_CD
                  )

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '093' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '0' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '1' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  ROUND(XX.PROF_VS_LST_12_MM_AVG_LN_AMT, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PCF_ID, PROF_VS_LST_12_MM_AVG_LN_AMT, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY PROF_VS_LST_12_MM_AVG_LN_AMT DESC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  CASE WHEN T3.L12M_LN_AMT = 0 THEN 0
                                       ELSE (T1.TOT_INCM_AMT - T1.TOT_EXP_AMT) * 12 / T3.L12M_LN_AMT * 100
                                       END  AS PROF_VS_LST_12_MM_AVG_LN_AMT
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T2.PCF_ID = T1.PCF_ID
                                                               INNER JOIN FROM_TM27_MMLY_PCF_KEY_IDC_A T3
                                                                       ON T3.BAS_YM  = T1.BAS_YM
                                                                      AND T3.PCF_ID  = T1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '093' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '0' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '2' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  ROUND(XX.PROF_VS_LST_12_MM_AVG_LN_AMT, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PCF_ID, PROF_VS_LST_12_MM_AVG_LN_AMT, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY PROF_VS_LST_12_MM_AVG_LN_AMT ASC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  CASE WHEN T3.L12M_LN_AMT = 0 THEN 0
                                       ELSE (T1.TOT_INCM_AMT - T1.TOT_EXP_AMT) * 12 / T3.L12M_LN_AMT * 100
                                       END  AS PROF_VS_LST_12_MM_AVG_LN_AMT
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T2.PCF_ID = T1.PCF_ID
                                                               INNER JOIN FROM_TM27_MMLY_PCF_KEY_IDC_A T3
                                                                       ON T3.BAS_YM  = T1.BAS_YM
                                                                      AND T3.PCF_ID  = T1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '093' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '4' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '1' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  ROUND(XX.PROF_VS_LST_12_MM_AVG_LN_AMT, 2) AS AGT_VAL
           FROM   (SELECT BAS_YM, CBV_BR_CD, PCF_ID, PROF_VS_LST_12_MM_AVG_LN_AMT, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY PROF_VS_LST_12_MM_AVG_LN_AMT DESC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  CASE WHEN T3.L12M_LN_AMT = 0 THEN 0
                                       ELSE (T1.TOT_INCM_AMT - T1.TOT_EXP_AMT) * 12 / T3.L12M_LN_AMT * 100 * T2.ASSET_WEIGHTED_AVG
                                       END  AS PROF_VS_LST_12_MM_AVG_LN_AMT
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN ASSET_WEIGHTED_AVG T2
                                                                       ON T1.BAS_YM = T2.BAS_YM
                                                                      AND T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN FROM_TM27_MMLY_PCF_KEY_IDC_A T3
                                                                       ON T3.BAS_YM  = T1.BAS_YM
                                                                      AND T3.PCF_ID  = T1.PCF_ID
                          )
                  ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '093' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '4' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '2' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  ROUND(XX.PROF_VS_LST_12_MM_AVG_LN_AMT, 2) AS AGT_VAL
           FROM   (SELECT BAS_YM, CBV_BR_CD, PCF_ID, PROF_VS_LST_12_MM_AVG_LN_AMT, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY PROF_VS_LST_12_MM_AVG_LN_AMT ASC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  CASE WHEN T3.L12M_LN_AMT = 0 THEN 0
                                       ELSE (T1.TOT_INCM_AMT - T1.TOT_EXP_AMT) * 12 / T3.L12M_LN_AMT * 100 * T2.ASSET_WEIGHTED_AVG
                                       END  AS PROF_VS_LST_12_MM_AVG_LN_AMT
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN ASSET_WEIGHTED_AVG T2
                                                                       ON T1.BAS_YM = T2.BAS_YM
                                                                      AND T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN FROM_TM27_MMLY_PCF_KEY_IDC_A T3
                                                                       ON T3.BAS_YM  = T1.BAS_YM
                                                                      AND T3.PCF_ID  = T1.PCF_ID
                          )
                  ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT BAS_YM,
                  CBV_BR_CD,
                  '093' AS KPI_TYP_CD,
                  '5' AS AGT_METH_CD,
                  '1' AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL AS PCF_ID,
                  ROUND(PROF_VS_LST_12_MM_AVG_LN_AMT,2) AS AGT_VAL
           FROM  (SELECT BAS_YM, CBV_BR_CD, PROF_VS_LST_12_MM_AVG_LN_AMT, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY PROF_VS_LST_12_MM_AVG_LN_AMT DESC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                CBV_BR_CD,
                                INI_RNK_NUM,
                                AVG(PROF_VS_LST_12_MM_AVG_LN_AMT) AS PROF_VS_LST_12_MM_AVG_LN_AMT
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, PCF_ID, PROF_VS_LST_12_MM_AVG_LN_AMT, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1, CBV_BR_CD ORDER BY PROF_VS_LST_12_MM_AVG_LN_AMT DESC NULLS LAST, PCF_ID DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               XX.PCF_ID,
                                               YY.PROF_VS_LST_12_MM_AVG_LN_AMT
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD, T1.PCF_ID
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       A.PCF_ID,
                                                                       CASE WHEN B.L12M_LN_AMT = 0 THEN 0
                                                                            ELSE (A.TOT_INCM_AMT - A.TOT_EXP_AMT) * 12 / B.L12M_LN_AMT * 100
                                                                            END AS PROF_VS_LST_12_MM_AVG_LN_AMT
                                                                FROM   TM23_MMLY_INCM_EXP_A A INNER JOIN TM27_MMLY_PCF_KEY_IDC_A B
                                                                                                      ON B.BAS_YM  = A.BAS_YM
                                                                                                     AND B.PCF_ID  = A.PCF_ID
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PCF_ID   = XX.PCF_ID
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, CBV_BR_CD, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  CBV_BR_CD,
                  '093' AS KPI_TYP_CD,
                  '5' AS AGT_METH_CD,
                  '2' AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL AS PCF_ID,
                  ROUND(PROF_VS_LST_12_MM_AVG_LN_AMT,2) AS AGT_VAL
           FROM  (SELECT BAS_YM, CBV_BR_CD, PROF_VS_LST_12_MM_AVG_LN_AMT, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY PROF_VS_LST_12_MM_AVG_LN_AMT ASC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                CBV_BR_CD,
                                INI_RNK_NUM,
                                AVG(PROF_VS_LST_12_MM_AVG_LN_AMT) AS PROF_VS_LST_12_MM_AVG_LN_AMT
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, PCF_ID, PROF_VS_LST_12_MM_AVG_LN_AMT, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1, CBV_BR_CD ORDER BY PROF_VS_LST_12_MM_AVG_LN_AMT ASC NULLS LAST, PCF_ID DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               XX.PCF_ID,
                                               YY.PROF_VS_LST_12_MM_AVG_LN_AMT
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD, T1.PCF_ID
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       A.PCF_ID,
                                                                       CASE WHEN B.L12M_LN_AMT = 0 THEN 0
                                                                            ELSE (A.TOT_INCM_AMT - A.TOT_EXP_AMT) * 12 / B.L12M_LN_AMT * 100
                                                                            END  AS PROF_VS_LST_12_MM_AVG_LN_AMT
                                                                FROM   TM23_MMLY_INCM_EXP_A A INNER JOIN TM27_MMLY_PCF_KEY_IDC_A B
                                                                                                      ON B.BAS_YM  = A.BAS_YM
                                                                                                     AND B.PCF_ID  = A.PCF_ID
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PCF_ID   = XX.PCF_ID
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, CBV_BR_CD, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  CBV_BR_CD,
                  '093' AS KPI_TYP_CD,
                  '7' AS AGT_METH_CD,
                  '1' AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL AS PCF_ID,
                  ROUND(PROF_VS_LST_12_MM_AVG_LN_AMT, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, CBV_BR_CD, PROF_VS_LST_12_MM_AVG_LN_AMT, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY PROF_VS_LST_12_MM_AVG_LN_AMT DESC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                CBV_BR_CD,
                                INI_RNK_NUM,
                                AVG(PROF_VS_LST_12_MM_AVG_LN_AMT) AS PROF_VS_LST_12_MM_AVG_LN_AMT
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, PCF_ID, PROF_VS_LST_12_MM_AVG_LN_AMT, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1, CBV_BR_CD ORDER BY PROF_VS_LST_12_MM_AVG_LN_AMT DESC NULLS LAST, PCF_ID DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               XX.PCF_ID,
                                               YY.PROF_VS_LST_12_MM_AVG_LN_AMT
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD, T1.PCF_ID
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                               ) XX INNER JOIN (SELECT T1.BAS_YM,
                                                                       T2.CBV_BR_CD,
                                                                       T1.PCF_ID,
                                                                       CASE WHEN T3.L12M_LN_AMT = 0 THEN 0
                                                                            ELSE (T1.TOT_INCM_AMT - T1.TOT_EXP_AMT) * 12 / T3.L12M_LN_AMT * 100 * T2.ASSET_WEIGHTED_AVG
                                                                            END  AS PROF_VS_LST_12_MM_AVG_LN_AMT
                                                                FROM   TM23_MMLY_INCM_EXP_A T1 INNER JOIN ASSET_WEIGHTED_AVG T2
                                                                                                       ON T1.BAS_YM = T2.BAS_YM
                                                                                                      AND T1.PCF_ID = T2.PCF_ID
                                                                                               INNER JOIN TM27_MMLY_PCF_KEY_IDC_A T3
                                                                                                       ON T3.BAS_YM = T1.BAS_YM
                                                                                                      AND T3.PCF_ID = T1.PCF_ID
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PCF_ID   = XX.PCF_ID
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, CBV_BR_CD, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  CBV_BR_CD,
                  '093' AS KPI_TYP_CD,
                  '7' AS AGT_METH_CD,
                  '2' AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL AS PCF_ID,
                  ROUND(PROF_VS_LST_12_MM_AVG_LN_AMT, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, CBV_BR_CD, PROF_VS_LST_12_MM_AVG_LN_AMT, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY PROF_VS_LST_12_MM_AVG_LN_AMT ASC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                CBV_BR_CD,
                                INI_RNK_NUM,
                                AVG(PROF_VS_LST_12_MM_AVG_LN_AMT) AS PROF_VS_LST_12_MM_AVG_LN_AMT
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, PCF_ID, PROF_VS_LST_12_MM_AVG_LN_AMT, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1, CBV_BR_CD ORDER BY PROF_VS_LST_12_MM_AVG_LN_AMT ASC NULLS LAST, PCF_ID DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               XX.PCF_ID,
                                               YY.PROF_VS_LST_12_MM_AVG_LN_AMT
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD, T1.PCF_ID
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                               ) XX INNER JOIN (SELECT T1.BAS_YM,
                                                                       T2.CBV_BR_CD,
                                                                       T1.PCF_ID,
                                                                       CASE WHEN T3.L12M_LN_AMT = 0 THEN 0
                                                                            ELSE (T1.TOT_INCM_AMT - T1.TOT_EXP_AMT) * 12 / T3.L12M_LN_AMT * 100 * T2.ASSET_WEIGHTED_AVG
                                                                            END  AS PROF_VS_LST_12_MM_AVG_LN_AMT
                                                                FROM   TM23_MMLY_INCM_EXP_A T1 INNER JOIN ASSET_WEIGHTED_AVG T2
                                                                                                       ON T1.BAS_YM = T2.BAS_YM
                                                                                                      AND T1.PCF_ID = T2.PCF_ID
                                                                                               INNER JOIN TM27_MMLY_PCF_KEY_IDC_A T3
                                                                                                       ON T3.BAS_YM = T1.BAS_YM
                                                                                                      AND T3.PCF_ID = T1.PCF_ID
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PCF_ID   = XX.PCF_ID
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, CBV_BR_CD, INI_RNK_NUM
                        )
                 )
          ),
          FROM_KPI_TYP_094 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '094' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(CASE WHEN T4.L12M_EQT_AMT = 0 THEN 0
                                 ELSE ( T1.TOT_INCM_AMT - T1.TOT_EXP_AMT ) * 12 / ( T4.L12M_EQT_AMT ) * 100
                                 END),2) AS AGT_VAL
           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                       ON T1.PCF_ID = T2.PCF_ID
                                               INNER JOIN FROM_TM27_MMLY_PCF_KEY_IDC_A T4
                                                       ON T4.BAS_YM  = T1.BAS_YM
                                                      AND T4.PCF_ID  = T1.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '094' AS KPI_TYP_CD,
                  '5'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(YY.PROF_VS_LST_12_MM_AVG_EQT_AMT),2) AS AGT_VAL
           FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD
                   FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                               ON T1.PCF_ID = T2.PCF_ID
                   GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                  ) XX INNER JOIN (SELECT A.BAS_YM,
                                          B.CBV_BR_CD,
                                          AVG(CASE WHEN C.L12M_EQT_AMT = 0 THEN 0
                                                   ELSE ( A.TOT_INCM_AMT - A.TOT_EXP_AMT ) * 12 / ( C.L12M_EQT_AMT ) * 100
                                                   END) AS PROF_VS_LST_12_MM_AVG_EQT_AMT
                                   FROM   TM23_MMLY_INCM_EXP_A A INNER JOIN TM00_PCF_D B
                                                                         ON B.PCF_ID = A.PCF_ID
                                                                 INNER JOIN TM27_MMLY_PCF_KEY_IDC_A C
                                                                         ON C.BAS_YM  = A.BAS_YM
                                                                        AND C.PCF_ID  = A.PCF_ID
                                   GROUP BY A.BAS_YM, B.CBV_BR_CD
                                  ) YY
                               ON YY.BAS_YM    >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                              AND YY.BAS_YM    <= XX.BAS_YM
                              AND YY.CBV_BR_CD  = XX.CBV_BR_CD
           GROUP BY XX.BAS_YM, XX.CBV_BR_CD

           UNION ALL

           SELECT BAS_YM,
                  CBV_BR_CD,
                  '094' AS KPI_TYP_CD,
                  '4'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(PROF_VS_LST_12_MM_AVG_EQT_AMT, 2) AS AGT_VAL
           FROM   (SELECT T1.BAS_YM,
                          T2.CBV_BR_CD,
                          SUM(CASE WHEN T3.L12M_EQT_AMT = 0 THEN 0
                                   ELSE (T1.TOT_INCM_AMT - T1.TOT_EXP_AMT ) * 12 / ( T3.L12M_EQT_AMT ) * 100 * T2.ASSET_WEIGHTED_AVG
                                   END) AS PROF_VS_LST_12_MM_AVG_EQT_AMT
                   FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN ASSET_WEIGHTED_AVG_094 T2
                                                               ON T2.BAS_YM = T1.BAS_YM
                                                              AND T2.PCF_ID = T1.PCF_ID
                                                       INNER JOIN FROM_TM27_MMLY_PCF_KEY_IDC_A T3
                                                               ON T3.BAS_YM  = T1.BAS_YM
                                                              AND T3.PCF_ID  = T1.PCF_ID
                   GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                  )

           UNION ALL

           SELECT BAS_YM,
                  CBV_BR_CD,
                  '094' AS KPI_TYP_CD,
                  '7'   AS AGT_METH_CD,
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  PROF_VS_LST_12_MM_AVG_EQT_AMT AS AGT_VAL
           FROM   (SELECT XX.BAS_YM,
                          XX.CBV_BR_CD,
                          ROUND(AVG(YY.PROF_VS_LST_12_MM_AVG_EQT_AMT), 2) AS PROF_VS_LST_12_MM_AVG_EQT_AMT
                   FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          ) XX INNER JOIN (SELECT T1.BAS_YM,
                                                  T2.CBV_BR_CD,
                                                  SUM(CASE WHEN T3.L12M_EQT_AMT = 0 THEN 0
                                                           ELSE (T1.TOT_INCM_AMT - T1.TOT_EXP_AMT ) * 12 / ( T3.L12M_EQT_AMT ) * 100 * T2.ASSET_WEIGHTED_AVG
                                                           END) AS PROF_VS_LST_12_MM_AVG_EQT_AMT
                                           FROM   TM23_MMLY_INCM_EXP_A T1 INNER JOIN ASSET_WEIGHTED_AVG_094 T2
                                                                                  ON T1.BAS_YM = T2.BAS_YM
                                                                                 AND T1.PCF_ID = T2.PCF_ID
                                                                          INNER JOIN TM27_MMLY_PCF_KEY_IDC_A T3
                                                                                  ON T3.BAS_YM  = T1.BAS_YM
                                                                                 AND T3.PCF_ID  = T1.PCF_ID
                                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                                          ) YY
                                       ON YY.BAS_YM    >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                      AND YY.BAS_YM    <= XX.BAS_YM
                                      AND YY.CBV_BR_CD  = XX.CBV_BR_CD
                   GROUP BY XX.BAS_YM, XX.CBV_BR_CD
                  )

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '094' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '0' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '1' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  ROUND(XX.PROF_VS_LST_12_MM_AVG_EQT_AMT, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PCF_ID, PROF_VS_LST_12_MM_AVG_EQT_AMT, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY PROF_VS_LST_12_MM_AVG_EQT_AMT DESC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  CASE WHEN T3.L12M_EQT_AMT = 0 THEN 0
                                       ELSE (T1.TOT_INCM_AMT - T1.TOT_EXP_AMT) * 12 / T3.L12M_EQT_AMT * 100
                                       END AS PROF_VS_LST_12_MM_AVG_EQT_AMT
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T2.PCF_ID = T1.PCF_ID
                                                               INNER JOIN FROM_TM27_MMLY_PCF_KEY_IDC_A T3
                                                                       ON T3.BAS_YM  = T1.BAS_YM
                                                                      AND T3.PCF_ID  = T1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '094' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '0' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '2' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  ROUND(XX.PROF_VS_LST_12_MM_AVG_EQT_AMT, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PCF_ID, PROF_VS_LST_12_MM_AVG_EQT_AMT, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY PROF_VS_LST_12_MM_AVG_EQT_AMT ASC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  CASE WHEN T3.L12M_EQT_AMT = 0 THEN 0
                                       ELSE (T1.TOT_INCM_AMT - T1.TOT_EXP_AMT) * 12 / T3.L12M_EQT_AMT * 100
                                       END AS PROF_VS_LST_12_MM_AVG_EQT_AMT
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T2.PCF_ID = T1.PCF_ID
                                                               INNER JOIN FROM_TM27_MMLY_PCF_KEY_IDC_A T3
                                                                       ON T3.BAS_YM  = T1.BAS_YM
                                                                      AND T3.PCF_ID  = T1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '094' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '4' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '1' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  ROUND(XX.PROF_VS_LST_12_MM_AVG_EQT_AMT, 2) AS AGT_VAL
           FROM   (SELECT BAS_YM, CBV_BR_CD, PCF_ID, PROF_VS_LST_12_MM_AVG_EQT_AMT, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY PROF_VS_LST_12_MM_AVG_EQT_AMT DESC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  CASE WHEN T3.L12M_EQT_AMT = 0 THEN 0
                                       ELSE (T1.TOT_INCM_AMT - T1.TOT_EXP_AMT) * 12 / T3.L12M_EQT_AMT * 100 * T2.ASSET_WEIGHTED_AVG
                                       END  AS PROF_VS_LST_12_MM_AVG_EQT_AMT
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN ASSET_WEIGHTED_AVG_094 T2
                                                                       ON T1.BAS_YM = T2.BAS_YM
                                                                      AND T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN FROM_TM27_MMLY_PCF_KEY_IDC_A T3
                                                                       ON T3.BAS_YM  = T1.BAS_YM
                                                                      AND T3.PCF_ID  = T1.PCF_ID
                          )
                  ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '094' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '4' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '2' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  ROUND(XX.PROF_VS_LST_12_MM_AVG_EQT_AMT, 2) AS AGT_VAL
           FROM   (SELECT BAS_YM, CBV_BR_CD, PCF_ID, PROF_VS_LST_12_MM_AVG_EQT_AMT, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY PROF_VS_LST_12_MM_AVG_EQT_AMT ASC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  CASE WHEN T3.L12M_EQT_AMT = 0 THEN 0
                                       ELSE (T1.TOT_INCM_AMT - T1.TOT_EXP_AMT) * 12 / T3.L12M_EQT_AMT * 100 * T2.ASSET_WEIGHTED_AVG
                                       END  AS PROF_VS_LST_12_MM_AVG_EQT_AMT
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN ASSET_WEIGHTED_AVG_094 T2
                                                                       ON T1.BAS_YM = T2.BAS_YM
                                                                      AND T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN FROM_TM27_MMLY_PCF_KEY_IDC_A T3
                                                                       ON T3.BAS_YM  = T1.BAS_YM
                                                                      AND T3.PCF_ID  = T1.PCF_ID
                          )
                  ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT BAS_YM,
                  CBV_BR_CD,
                  '094' AS KPI_TYP_CD,
                  '5' AS AGT_METH_CD,
                  '1' AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL AS PCF_ID,
                  ROUND(PROF_VS_LST_12_MM_AVG_EQT_AMT,2) AS AGT_VAL
           FROM  (SELECT BAS_YM, CBV_BR_CD, PROF_VS_LST_12_MM_AVG_EQT_AMT, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY PROF_VS_LST_12_MM_AVG_EQT_AMT DESC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                CBV_BR_CD,
                                INI_RNK_NUM,
                                AVG(PROF_VS_LST_12_MM_AVG_EQT_AMT) AS PROF_VS_LST_12_MM_AVG_EQT_AMT
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, PCF_ID, PROF_VS_LST_12_MM_AVG_EQT_AMT, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1, CBV_BR_CD ORDER BY PROF_VS_LST_12_MM_AVG_EQT_AMT DESC NULLS LAST, PCF_ID DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               XX.PCF_ID,
                                               YY.PROF_VS_LST_12_MM_AVG_EQT_AMT
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD, T1.PCF_ID
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       A.PCF_ID,
                                                                       CASE WHEN B.L12M_EQT_AMT = 0 THEN 0
                                                                            ELSE (A.TOT_INCM_AMT - A.TOT_EXP_AMT) * 12 / B.L12M_EQT_AMT * 100
                                                                            END  AS PROF_VS_LST_12_MM_AVG_EQT_AMT
                                                                FROM   TM23_MMLY_INCM_EXP_A A INNER JOIN TM27_MMLY_PCF_KEY_IDC_A B
                                                                                                      ON B.BAS_YM  = A.BAS_YM
                                                                                                     AND B.PCF_ID  = A.PCF_ID
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PCF_ID   = XX.PCF_ID
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, CBV_BR_CD, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  CBV_BR_CD,
                  '094' AS KPI_TYP_CD,
                  '5' AS AGT_METH_CD,
                  '2' AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL AS PCF_ID,
                  ROUND(PROF_VS_LST_12_MM_AVG_EQT_AMT,2) AS AGT_VAL
           FROM  (SELECT BAS_YM, CBV_BR_CD, PROF_VS_LST_12_MM_AVG_EQT_AMT, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY PROF_VS_LST_12_MM_AVG_EQT_AMT ASC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                CBV_BR_CD,
                                INI_RNK_NUM,
                                AVG(PROF_VS_LST_12_MM_AVG_EQT_AMT) AS PROF_VS_LST_12_MM_AVG_EQT_AMT
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, PCF_ID, PROF_VS_LST_12_MM_AVG_EQT_AMT, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1, CBV_BR_CD ORDER BY PROF_VS_LST_12_MM_AVG_EQT_AMT ASC NULLS LAST, PCF_ID DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               XX.PCF_ID,
                                               YY.PROF_VS_LST_12_MM_AVG_EQT_AMT
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD, T1.PCF_ID
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       A.PCF_ID,
                                                                       CASE WHEN B.L12M_EQT_AMT = 0 THEN 0
                                                                            ELSE (A.TOT_INCM_AMT - A.TOT_EXP_AMT) * 12 / B.L12M_EQT_AMT * 100
                                                                            END  AS PROF_VS_LST_12_MM_AVG_EQT_AMT
                                                                FROM   TM23_MMLY_INCM_EXP_A A INNER JOIN TM27_MMLY_PCF_KEY_IDC_A B
                                                                                                      ON B.BAS_YM  = A.BAS_YM
                                                                                                     AND B.PCF_ID  = A.PCF_ID
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PCF_ID   = XX.PCF_ID
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, CBV_BR_CD, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  CBV_BR_CD,
                  '094' AS KPI_TYP_CD,
                  '7' AS AGT_METH_CD,
                  '1' AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL AS PCF_ID,
                  ROUND(PROF_VS_LST_12_MM_AVG_EQT_AMT, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, CBV_BR_CD, PROF_VS_LST_12_MM_AVG_EQT_AMT, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY PROF_VS_LST_12_MM_AVG_EQT_AMT DESC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                CBV_BR_CD,
                                INI_RNK_NUM,
                                AVG(PROF_VS_LST_12_MM_AVG_EQT_AMT) AS PROF_VS_LST_12_MM_AVG_EQT_AMT
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, PCF_ID, PROF_VS_LST_12_MM_AVG_EQT_AMT, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1, CBV_BR_CD ORDER BY PROF_VS_LST_12_MM_AVG_EQT_AMT DESC NULLS LAST, PCF_ID DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               XX.PCF_ID,
                                               YY.PROF_VS_LST_12_MM_AVG_EQT_AMT
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD, T1.PCF_ID
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                               ) XX INNER JOIN (SELECT T1.BAS_YM,
                                                                       T2.CBV_BR_CD,
                                                                       T1.PCF_ID,
                                                                       CASE WHEN T3.L12M_EQT_AMT = 0 THEN 0
                                                                            ELSE (T1.TOT_INCM_AMT - T1.TOT_EXP_AMT) * 12 / T3.L12M_EQT_AMT * 100 * T2.ASSET_WEIGHTED_AVG
                                                                            END  AS PROF_VS_LST_12_MM_AVG_EQT_AMT
                                                                FROM   TM23_MMLY_INCM_EXP_A T1 INNER JOIN ASSET_WEIGHTED_AVG_094 T2
                                                                                                       ON T1.BAS_YM = T2.BAS_YM
                                                                                                      AND T1.PCF_ID = T2.PCF_ID
                                                                                               INNER JOIN TM27_MMLY_PCF_KEY_IDC_A T3
                                                                                                       ON T3.BAS_YM = T1.BAS_YM
                                                                                                      AND T3.PCF_ID = T1.PCF_ID
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PCF_ID   = XX.PCF_ID
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, CBV_BR_CD, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  CBV_BR_CD,
                  '094' AS KPI_TYP_CD,
                  '7' AS AGT_METH_CD,
                  '2' AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL AS PCF_ID,
                  ROUND(PROF_VS_LST_12_MM_AVG_EQT_AMT, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, CBV_BR_CD, PROF_VS_LST_12_MM_AVG_EQT_AMT, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY PROF_VS_LST_12_MM_AVG_EQT_AMT ASC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                CBV_BR_CD,
                                INI_RNK_NUM,
                                AVG(PROF_VS_LST_12_MM_AVG_EQT_AMT) AS PROF_VS_LST_12_MM_AVG_EQT_AMT
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, PCF_ID, PROF_VS_LST_12_MM_AVG_EQT_AMT, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1, CBV_BR_CD ORDER BY PROF_VS_LST_12_MM_AVG_EQT_AMT ASC NULLS LAST, PCF_ID DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               XX.PCF_ID,
                                               YY.PROF_VS_LST_12_MM_AVG_EQT_AMT
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD, T1.PCF_ID
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                               ) XX INNER JOIN (SELECT T1.BAS_YM,
                                                                       T2.CBV_BR_CD,
                                                                       T1.PCF_ID,
                                                                       CASE WHEN T3.L12M_EQT_AMT = 0 THEN 0
                                                                            ELSE (T1.TOT_INCM_AMT - T1.TOT_EXP_AMT) * 12 / T3.L12M_EQT_AMT * 100 * T2.ASSET_WEIGHTED_AVG
                                                                            END  AS PROF_VS_LST_12_MM_AVG_EQT_AMT
                                                                FROM   TM23_MMLY_INCM_EXP_A T1 INNER JOIN ASSET_WEIGHTED_AVG_094 T2
                                                                                                       ON T1.BAS_YM = T2.BAS_YM
                                                                                                      AND T1.PCF_ID = T2.PCF_ID
                                                                                               INNER JOIN TM27_MMLY_PCF_KEY_IDC_A T3
                                                                                                       ON T3.BAS_YM = T1.BAS_YM
                                                                                                      AND T3.PCF_ID = T1.PCF_ID
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PCF_ID   = XX.PCF_ID
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, CBV_BR_CD, INI_RNK_NUM
                        )
                 )
          ),
          FROM_KPI_TYP_095 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '095' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.DPST_INT_INCM_AMT) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                       ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '095' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.DPST_INT_INCM_AMT),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                       ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
          ),
          FROM_KPI_TYP_096 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '096' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.LN_INT_INCM_AMT) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                       ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '096' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.LN_INT_INCM_AMT),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                       ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
          ),
          FROM_KPI_TYP_097 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '097' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.OTHR_CR_ACT_INCM_AMT) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                       ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '097' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.OTHR_CR_ACT_INCM_AMT),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                       ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
          ),
          FROM_KPI_TYP_098 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '098' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.SERV_ACT_INCM_AMT) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                       ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '098' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.SERV_ACT_INCM_AMT),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                       ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
          ),
          FROM_KPI_TYP_099 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '099' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.OTHR_INCM_AMT) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                       ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '099' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.OTHR_INCM_AMT),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                       ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
          ),
          FROM_KPI_TYP_100 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '100' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.DPST_INT_EXP_AMT) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                       ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '100' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.DPST_INT_EXP_AMT),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                       ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
          ),
          FROM_KPI_TYP_101 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '101' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.BOR_INT_EXP_AMT) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                       ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '101' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.BOR_INT_EXP_AMT),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                       ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
          ),
          FROM_KPI_TYP_102 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '102' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.OTHR_CR_ACT_EXP_AMT) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                       ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '102' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.OTHR_CR_ACT_EXP_AMT),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                       ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
          ),
          FROM_KPI_TYP_103 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '103' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.SERV_ACT_EXP_AMT) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                       ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '103' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.SERV_ACT_EXP_AMT),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                       ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
          ),
          FROM_KPI_TYP_104 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '104' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.STF_EXP_AMT) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                       ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '104' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.STF_EXP_AMT),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                       ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
          ),
          FROM_KPI_TYP_105 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '105' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.OTHR_EXP_AMT) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                       ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '105' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.OTHR_EXP_AMT),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                       ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
          ),
          FROM_KPI_TYP_106 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '106' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.TX_FEE_PAY_EXP_AMT) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                       ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '106' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.TX_FEE_PAY_EXP_AMT),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                       ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
          ),
          FROM_KPI_TYP_133 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '133' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.WIT_COLL_LN_BAL) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '133' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.WIT_COLL_LN_BAL),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '133' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '0' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '1' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.WIT_COLL_LN_BAL AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PCF_ID, WIT_COLL_LN_BAL, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY WIT_COLL_LN_BAL DESC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  T1.WIT_COLL_LN_BAL
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T2.PCF_ID = T1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '133' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '0' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '2' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.WIT_COLL_LN_BAL AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PCF_ID, WIT_COLL_LN_BAL, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY WIT_COLL_LN_BAL ASC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  T1.WIT_COLL_LN_BAL
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T2.PCF_ID = T1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3
          ),
          FROM_KPI_TYP_135 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '135' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.CBV_STBOR_AMT) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '135' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.CBV_STBOR_AMT),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '135' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '0' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '1' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.CBV_STBOR_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PCF_ID, CBV_STBOR_AMT, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY CBV_STBOR_AMT DESC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  T1.CBV_STBOR_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T2.PCF_ID = T1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '135' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '0' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '2' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.CBV_STBOR_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PCF_ID, CBV_STBOR_AMT, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY CBV_STBOR_AMT ASC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  T1.CBV_STBOR_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T2.PCF_ID = T1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3
          ),
          FROM_KPI_TYP_136 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '136' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.CBV_MED_LT_BOR_AMT) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '136' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.CBV_MED_LT_BOR_AMT),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '136' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '0' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '1' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.CBV_MED_LT_BOR_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PCF_ID, CBV_MED_LT_BOR_AMT, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY CBV_MED_LT_BOR_AMT DESC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  T1.CBV_MED_LT_BOR_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T2.PCF_ID = T1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '136' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '0' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '2' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.CBV_MED_LT_BOR_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PCF_ID, CBV_MED_LT_BOR_AMT, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY CBV_MED_LT_BOR_AMT ASC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  T1.CBV_MED_LT_BOR_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T2.PCF_ID = T1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3
          ),
          FROM_KPI_TYP_137 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '137' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.CBV_SHRT_TRM_SVG_AMT) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '137' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.CBV_SHRT_TRM_SVG_AMT),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '137' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '0' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '1' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.CBV_SHRT_TRM_SVG_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PCF_ID, CBV_SHRT_TRM_SVG_AMT, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY CBV_SHRT_TRM_SVG_AMT DESC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  T1.CBV_SHRT_TRM_SVG_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T2.PCF_ID = T1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '137' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '0' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '2' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.CBV_SHRT_TRM_SVG_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PCF_ID, CBV_SHRT_TRM_SVG_AMT, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY CBV_SHRT_TRM_SVG_AMT ASC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  T1.CBV_SHRT_TRM_SVG_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T2.PCF_ID = T1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3
          ),
          FROM_KPI_TYP_138 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '138' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.CBV_MED_LT_SVG_AMT) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '138' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.CBV_MED_LT_SVG_AMT),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '138' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '0' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '1' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.CBV_MED_LT_SVG_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PCF_ID, CBV_MED_LT_SVG_AMT, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY CBV_MED_LT_SVG_AMT DESC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  T1.CBV_MED_LT_SVG_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T2.PCF_ID = T1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '138' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '0' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '2' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.CBV_MED_LT_SVG_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PCF_ID, CBV_MED_LT_SVG_AMT, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY CBV_MED_LT_SVG_AMT ASC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  T1.CBV_MED_LT_SVG_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T2.PCF_ID = T1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3
          ),
          FROM_KPI_TYP_141 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '141' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.CBV_BOR_AMT) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '141' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.CBV_BOR_AMT),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '141' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '0' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '1' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.CBV_BOR_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PCF_ID, CBV_BOR_AMT, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY CBV_BOR_AMT DESC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  T1.CBV_BOR_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T2.PCF_ID = T1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '141' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '0' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '2' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.CBV_BOR_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PCF_ID, CBV_BOR_AMT, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY CBV_BOR_AMT ASC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  T1.CBV_BOR_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T2.PCF_ID = T1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3
          ),
          FROM_KPI_TYP_142 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '142' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.CBV_SHRT_TRM_SVG_AMT + T1.CBV_MED_LT_SVG_AMT) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '142' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.CBV_SHRT_TRM_SVG_AMT + T1.CBV_MED_LT_SVG_AMT),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '142' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '0' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '1' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.CBV_TOT_SVG_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PCF_ID, CBV_TOT_SVG_AMT, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY CBV_TOT_SVG_AMT DESC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  T1.CBV_SHRT_TRM_SVG_AMT + T1.CBV_MED_LT_SVG_AMT AS CBV_TOT_SVG_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T2.PCF_ID = T1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '142' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '0' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '2' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.CBV_TOT_SVG_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PCF_ID, CBV_TOT_SVG_AMT, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY CBV_TOT_SVG_AMT ASC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  T1.CBV_SHRT_TRM_SVG_AMT + T1.CBV_MED_LT_SVG_AMT AS CBV_TOT_SVG_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T2.PCF_ID = T1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3
          ),
          FROM_KPI_TYP_143 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '143' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.CUR_TOT_INCM_AMT - T1.CUR_TOT_EXP_AMT) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                       ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '143' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.CUR_TOT_INCM_AMT - T1.CUR_TOT_EXP_AMT),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                       ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '143' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '0' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '1' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.CUR_NET_PROFIT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PCF_ID, CUR_NET_PROFIT, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY CUR_NET_PROFIT DESC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  T1.CUR_TOT_INCM_AMT - T1.CUR_TOT_EXP_AMT AS CUR_NET_PROFIT
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T2.PCF_ID = T1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '143' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '0' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '2' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.CUR_NET_PROFIT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PCF_ID, CUR_NET_PROFIT, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY CUR_NET_PROFIT ASC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  T1.CUR_TOT_INCM_AMT - T1.CUR_TOT_EXP_AMT AS CUR_NET_PROFIT
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T2.PCF_ID = T1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3
          ),
          FROM_KPI_TYP_144 AS
          (SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '144' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.INCL_ALOSS_INCM_MNS_EXP_AMT) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT T1.BAS_YM,
                  T2.CBV_BR_CD,
                  '144' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.INCL_ALOSS_INCM_MNS_EXP_AMT),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
           GROUP BY T1.BAS_YM, T2.CBV_BR_CD

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '144' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '0' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '1' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.INCL_ALOSS_INCM_MNS_EXP_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PCF_ID, INCL_ALOSS_INCM_MNS_EXP_AMT, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY INCL_ALOSS_INCM_MNS_EXP_AMT DESC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  T1.INCL_ALOSS_INCM_MNS_EXP_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T2.PCF_ID = T1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.CBV_BR_CD,
                  '144' AS KPI_TYP_CD,    /* Code of variable corresponding to each KPI item */
                  '0' AS AGT_METH_CD,     /* Code of variable corresponding to each aggregation method */
                  '2' AS TOP_BTOM_TYP_CD, /* When extract lowest value : 2 */
                  XX.RNK_NUM,
                  XX.PCF_ID,
                  XX.INCL_ALOSS_INCM_MNS_EXP_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PCF_ID, INCL_ALOSS_INCM_MNS_EXP_AMT, RANK() OVER(PARTITION BY BAS_YM, CBV_BR_CD ORDER BY INCL_ALOSS_INCM_MNS_EXP_AMT ASC NULLS LAST, PCF_ID DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  T1.PCF_ID,
                                  T1.INCL_ALOSS_INCM_MNS_EXP_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T2.PCF_ID = T1.PCF_ID
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3
          )
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_001
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_002
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_003
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_004
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_005
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_006
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_007
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_008
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_009
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_010
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_011
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_058
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_059
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_060
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_061
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_062
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_063
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_064
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_065
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_066
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_069
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_070
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_071
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_072
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_073
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_074
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_075
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_076
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_077
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_078
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_079
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_080
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_081
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_082
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_083
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_084
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_085
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_086
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_087
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_088
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_089
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_090
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_091
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_092
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_093
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_094
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_095
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_096
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_097
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_098
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_099
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_100
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_101
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_102
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_103
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_104
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_105
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_106
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_133
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_135
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_136
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_137
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_138
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_141
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_142
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_143
          UNION ALL
          SELECT BAS_YM, CBV_BR_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_144
          ;

          ----------------------------------------------------------------------------
          --  Transaction Log
          ----------------------------------------------------------------------------
          v_step_code    := '020' ;
          v_step_desc    := v_wk_date || v_seq || ' ' || loop_bas_day.BAS_YM ||' : Inserting Data Result';
          v_time         := SYSDATE ;

          P1_BSM_PROG_EXEC_LOG(v_program_id, v_program_type_name, v_step_code, v_step_desc, v_time, sql%rowcount, NULL, NULL) ;

          COMMIT ;
          ----------------------------------------------------------------------------

          v_cnt := v_cnt+sql%rowcount;

          EXCEPTION
          WHEN OTHERS THEN
               begin
                     v_step_code    := '021' ;
                     v_step_desc    := v_wk_date || v_seq || ' : Inserting Data Error with BAS_YM = ' ;
                     v_time         := SYSDATE ;
                     P1_BSM_PROG_EXEC_LOG(v_program_id, v_program_type_name, v_step_code, v_step_desc||loop_bas_day.BAS_YM, v_time, v_cnt, TO_CHAR(SQLCODE), SQLERRM) ;
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