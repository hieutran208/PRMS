create or replace PROCEDURE            "P4_M27_MMLY_PRVN_CBV_BR_RNK_1718"
                 ( p_wkdate    IN  CHAR DEFAULT NULL
                 , p_stdate_01 IN  CHAR DEFAULT NULL
                 , p_eddate_01 IN  CHAR DEFAULT NULL
                 )
IS
    /*--------------------------------------------------------------------------
     * PROGRAM ID    : P4_M27_MMLY_PRVN_CBV_BR_RNK_1718
     * PROGRAM NAME  : A program for insert data to TM27_MMLY_PRVN_CBV_BR_RNK_A
     * SOURCE TABLE  : TM27_MMLY_PCF_KEY_IDC_A
                       TM26_MMLY_AST_QAL_A
                       TM23_MMLY_INCM_EXP_A
                       TM24_MMLY_FUND_SRC_USG_A
                       TB03_G32_006_TTGS_A
     * TARGET TABLE  : TM27_MMLY_PRVN_CBV_BR_RNK_A
     * PROGRAMER     : HIEU
     * LAST MODIFICATION DATE : 2025-12-30
     * UNIQUENESS    : N/A
     * COMMENTS      : N/A
    ----------------------------------------------------------------------------
     * Revision History : 2025-12-30 : Create
     * Revision History :
    --------------------------------------------------------------------------*/
    ----------------------------------------------------------------------------
    --  Declare Local Variables
    ----------------------------------------------------------------------------
    v_wk_date                 CHAR(8) DEFAULT NULL ;
    v_st_date_01              CHAR(8) DEFAULT NULL ;
    v_end_date_01             CHAR(8) DEFAULT NULL ;

    v_program_id              VARCHAR2(100) DEFAULT 'P4_M27_MMLY_PRVN_CBV_BR_RNK_1718' ;
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
    WHERE  BAS_YM BETWEEN '202104' AND '202208'
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
            FROM TM27_MMLY_PRVN_CBV_BR_RNK_A T1
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

          INSERT INTO TM27_MMLY_PRVN_CBV_BR_RNK_A
          (   BAS_YM
             ,RPVN_CBV_BR_TYP_CD
             ,KPI_TYP_CD
             ,AGT_METH_CD
             ,TOP_BTOM_TYP_CD
             ,RNK_NUM
             ,PRVN_CD
             ,CBV_BR_CD
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
                 ,TOT_LN_BAL
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
          ASSET_WEIGHTED_AVG_PCF_BR AS
          (SELECT A.BAS_YM,
                  B.CBV_BR_CD,
                  A.PCF_ID,
                  CASE WHEN B.CBV_BR_AST_AMT = 0 THEN 0
                       ELSE A.PCF_AST_AMT / B.CBV_BR_AST_AMT
                       END AS ASSET_WEIGHTED_AVG
           FROM   (SELECT T1.BAS_YM,
                          T2.CBV_BR_CD,
                          T1.PCF_ID,
                          T1.TOT_AST_AMT AS PCF_AST_AMT
                   FROM   TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                             ON T1.PCF_ID = T2.PCF_ID
                   WHERE  T1.TOT_AST_AMT IS NOT NULL
                  ) A INNER JOIN (SELECT T1.BAS_YM, T2.CBV_BR_CD, SUM(T1.TOT_AST_AMT) AS CBV_BR_AST_AMT
                                  FROM   TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                            ON T1.PCF_ID = T2.PCF_ID
                                  WHERE  T1.TOT_AST_AMT IS NOT NULL
                                  GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                                 ) B
                              ON B.BAS_YM    = A.BAS_YM
                             AND B.CBV_BR_CD = A.CBV_BR_CD
          ),
          ASSET_WA_PCF_BR_066 AS
          (SELECT A.BAS_YM,
                  B.CBV_BR_CD,
                  A.PCF_ID,
                  CASE WHEN B.CBV_BR_AST_AMT = 0 THEN 0
                       ELSE A.PCF_AST_AMT / B.CBV_BR_AST_AMT
                       END AS ASSET_WEIGHTED_AVG
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
          ASSET_WA_PCF_BR_070 AS
          (SELECT A.BAS_YM,
                  B.CBV_BR_CD,
                  A.PCF_ID,
                  CASE WHEN B.CBV_BR_AST_AMT = 0 THEN 0
                       ELSE A.PCF_AST_AMT / B.CBV_BR_AST_AMT
                       END AS ASSET_WEIGHTED_AVG
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
          ASSET_WA_PCF_BR_071 AS
          (SELECT A.BAS_YM,
                  B.CBV_BR_CD,
                  A.PCF_ID,
                  CASE WHEN B.CBV_BR_AST_AMT = 0 THEN 0
                       ELSE A.PCF_AST_AMT / B.CBV_BR_AST_AMT
                       END AS ASSET_WEIGHTED_AVG
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
          ASSET_WA_PCF_BR_094 AS
          (SELECT A.BAS_YM,
                  B.CBV_BR_CD,
                  A.PCF_ID,
                  CASE WHEN B.CBV_BR_AST_AMT = 0 THEN 0
                       ELSE A.PCF_AST_AMT / B.CBV_BR_AST_AMT
                       END AS ASSET_WEIGHTED_AVG
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
          ASSET_WEIGHTED_AVG_PCF_PRVN AS
          (SELECT A.BAS_YM,
                  B.PRVN_CD,
                  A.PCF_ID,
                  CASE WHEN B.PRVN_AST_AMT = 0 THEN 0
                       ELSE A.PCF_AST_AMT / B.PRVN_AST_AMT
                       END AS ASSET_WEIGHTED_AVG
           FROM   (SELECT T1.BAS_YM,
                          T3.PRVN_CD,
                          T1.PCF_ID,
                          T1.TOT_AST_AMT AS PCF_AST_AMT
                   FROM   TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                             ON T1.PCF_ID = T2.PCF_ID
                                                     INNER JOIN TM00_LOC_D T3
                                                             ON T2.LOC_ID = T3.LOC_ID
                   WHERE  T1.TOT_AST_AMT IS NOT NULL
                  ) A INNER JOIN (SELECT T1.BAS_YM, T3.PRVN_CD, SUM(T1.TOT_AST_AMT) AS PRVN_AST_AMT
                                  FROM   TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                            ON T1.PCF_ID = T2.PCF_ID
                                                                    INNER JOIN TM00_LOC_D T3
                                                                            ON T2.LOC_ID = T3.LOC_ID
                                  WHERE  T1.TOT_AST_AMT IS NOT NULL
                                  GROUP BY T1.BAS_YM, T3.PRVN_CD
                                 ) B
                              ON B.BAS_YM  = A.BAS_YM
                             AND B.PRVN_CD = A.PRVN_CD
          ),
          ASSET_WA_PCF_PRVN_066 AS
          (SELECT A.BAS_YM,
                  B.PRVN_CD,
                  A.PCF_ID,
                  CASE WHEN B.PRVN_AST_AMT = 0 THEN 0
                       ELSE A.PCF_AST_AMT / B.PRVN_AST_AMT
                       END AS ASSET_WEIGHTED_AVG
           FROM   (SELECT T1.BAS_YM,
                          T3.PRVN_CD,
                          T1.PCF_ID,
                          T1.TOT_AST_AMT AS PCF_AST_AMT
                   FROM   TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                             ON T1.PCF_ID = T2.PCF_ID
                                                     INNER JOIN TM00_LOC_D T3
                                                             ON T2.LOC_ID = T3.LOC_ID
                                                     INNER JOIN MED_AND_LONG_TERM_LENDING T4
                                                             ON T4.BAS_YM = T1.BAS_YM
                                                            AND T4.PCF_ID = T1.PCF_ID
                   WHERE  T1.TOT_AST_AMT IS NOT NULL
                  ) A INNER JOIN (SELECT T1.BAS_YM, T3.PRVN_CD, SUM(T1.TOT_AST_AMT) AS PRVN_AST_AMT
                                  FROM   TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                            ON T1.PCF_ID = T2.PCF_ID
                                                                    INNER JOIN TM00_LOC_D T3
                                                                            ON T2.LOC_ID = T3.LOC_ID
                                                                    INNER JOIN MED_AND_LONG_TERM_LENDING T4
                                                                            ON T4.BAS_YM = T1.BAS_YM
                                                                           AND T4.PCF_ID = T1.PCF_ID
                                  WHERE  T1.TOT_AST_AMT IS NOT NULL
                                  GROUP BY T1.BAS_YM, T3.PRVN_CD
                                 ) B
                              ON B.BAS_YM  = A.BAS_YM
                             AND B.PRVN_CD = A.PRVN_CD
          ),
          ASSET_WA_PCF_PRVN_070 AS
          (SELECT A.BAS_YM,
                  B.PRVN_CD,
                  A.PCF_ID,
                  CASE WHEN B.PRVN_AST_AMT = 0 THEN 0
                       ELSE A.PCF_AST_AMT / B.PRVN_AST_AMT
                       END AS ASSET_WEIGHTED_AVG
           FROM   (SELECT T1.BAS_YM,
                          T3.PRVN_CD,
                          T1.PCF_ID,
                          T1.TOT_AST_AMT AS PCF_AST_AMT
                   FROM   TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                             ON T1.PCF_ID = T2.PCF_ID
                                                     INNER JOIN TM00_LOC_D T3
                                                             ON T2.LOC_ID = T3.LOC_ID
                                                     INNER JOIN TM26_MMLY_AST_QAL_A T4
                                                             ON T4.BAS_YM = T1.BAS_YM
                                                            AND T4.PCF_ID = T1.PCF_ID
                                                            AND T4.TOT_COLL_VAL IS NOT NULL
                   WHERE  T1.TOT_AST_AMT IS NOT NULL
                  ) A INNER JOIN (SELECT T1.BAS_YM, T3.PRVN_CD, SUM(T1.TOT_AST_AMT) AS PRVN_AST_AMT
                                  FROM   TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                            ON T1.PCF_ID = T2.PCF_ID
                                                                    INNER JOIN TM00_LOC_D T3
                                                                            ON T2.LOC_ID = T3.LOC_ID
                                                                    INNER JOIN TM26_MMLY_AST_QAL_A T4
                                                                            ON T4.BAS_YM = T1.BAS_YM
                                                                           AND T4.PCF_ID = T1.PCF_ID
                                                                           AND T4.TOT_COLL_VAL IS NOT NULL
                                  WHERE  T1.TOT_AST_AMT IS NOT NULL
                                  GROUP BY T1.BAS_YM, T3.PRVN_CD
                                 ) B
                              ON B.BAS_YM  = A.BAS_YM
                             AND B.PRVN_CD = A.PRVN_CD
          ),
          ASSET_WA_PCF_PRVN_071 AS
          (SELECT A.BAS_YM,
                  B.PRVN_CD,
                  A.PCF_ID,
                  CASE WHEN B.PRVN_AST_AMT = 0 THEN 0
                       ELSE A.PCF_AST_AMT / B.PRVN_AST_AMT
                       END AS ASSET_WEIGHTED_AVG
           FROM   (SELECT T1.BAS_YM,
                          T3.PRVN_CD,
                          T1.PCF_ID,
                          T1.TOT_AST_AMT AS PCF_AST_AMT
                   FROM   TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                             ON T1.PCF_ID = T2.PCF_ID
                                                     INNER JOIN TM00_LOC_D T3
                                                             ON T2.LOC_ID = T3.LOC_ID
                                                     INNER JOIN TM26_MMLY_AST_QAL_A T4
                                                             ON T4.BAS_YM = T1.BAS_YM
                                                            AND T4.PCF_ID = T1.PCF_ID
                                                            AND T4.TOT_COLL_VAL IS NOT NULL
                   WHERE  T1.TOT_AST_AMT IS NOT NULL
                  ) A INNER JOIN (SELECT T1.BAS_YM, T3.PRVN_CD, SUM(T1.TOT_AST_AMT) AS PRVN_AST_AMT
                                  FROM   TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                            ON T1.PCF_ID = T2.PCF_ID
                                                                    INNER JOIN TM00_LOC_D T3
                                                                            ON T2.LOC_ID = T3.LOC_ID
                                                                    INNER JOIN TM26_MMLY_AST_QAL_A T4
                                                                            ON T4.BAS_YM = T1.BAS_YM
                                                                           AND T4.PCF_ID = T1.PCF_ID
                                                                           AND T4.TOT_COLL_VAL IS NOT NULL
                                  WHERE  T1.TOT_AST_AMT IS NOT NULL
                                  GROUP BY T1.BAS_YM, T3.PRVN_CD
                                 ) B
                              ON B.BAS_YM  = A.BAS_YM
                             AND B.PRVN_CD = A.PRVN_CD
          ),
          ASSET_WA_PCF_PRVN_094 AS
          (SELECT A.BAS_YM,
                  B.PRVN_CD,
                  A.PCF_ID,
                  CASE WHEN B.PRVN_AST_AMT = 0 THEN 0
                       ELSE A.PCF_AST_AMT / B.PRVN_AST_AMT
                       END AS ASSET_WEIGHTED_AVG
           FROM   (SELECT T1.BAS_YM,
                          T3.PRVN_CD,
                          T1.PCF_ID,
                          T1.TOT_AST_AMT AS PCF_AST_AMT
                   FROM   TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                             ON T1.PCF_ID = T2.PCF_ID
                                                     INNER JOIN TM00_LOC_D T3
                                                             ON T2.LOC_ID = T3.LOC_ID
                   WHERE  T1.TOT_AST_AMT IS NOT NULL
                   AND    T1.EQT_AMT IS NOT NULL
                  ) A INNER JOIN (SELECT T1.BAS_YM, T3.PRVN_CD, SUM(T1.TOT_AST_AMT) AS PRVN_AST_AMT
                                  FROM   TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                            ON T1.PCF_ID = T2.PCF_ID
                                                                    INNER JOIN TM00_LOC_D T3
                                                                            ON T2.LOC_ID = T3.LOC_ID
                                  WHERE  T1.TOT_AST_AMT IS NOT NULL
                                  AND    T1.EQT_AMT IS NOT NULL
                                  GROUP BY T1.BAS_YM, T3.PRVN_CD
                                 ) B
                              ON B.BAS_YM  = A.BAS_YM
                             AND B.PRVN_CD = A.PRVN_CD
          ),
          PCF_CNT_WEIGHTED_AVG AS
          (SELECT A.BAS_YM,
                  A.CBV_BR_CD,
                  CASE WHEN B.TOT_PCF_MBR_AT_NETWK = 0 THEN 0
                       ELSE A.TOT_PCF_MBR_AT_BR / B.TOT_PCF_MBR_AT_NETWK
                       END AS PCF_CNT_WEIGHTED_AVG
           FROM   (SELECT T1.BAS_YM,
                          T2.CBV_BR_CD,
                          COUNT(DISTINCT T1.PCF_ID) AS TOT_PCF_MBR_AT_BR
                   FROM   TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                             ON T1.PCF_ID = T2.PCF_ID
                   GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                  ) A INNER JOIN (SELECT BAS_YM, COUNT(DISTINCT PCF_ID) AS TOT_PCF_MBR_AT_NETWK
                                  FROM   TM27_MMLY_PCF_KEY_IDC_A
                                  GROUP BY BAS_YM
                                 ) B
                              ON B.BAS_YM = A.BAS_YM
          ),
          FROM_KPI_TYP_001 AS
          (SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '001' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  XX.TOT_PCF_MBR_NUM_CNT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, TOT_PCF_MBR_NUM_CNT, RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_PCF_MBR_NUM_CNT DESC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  SUM(T1.TOT_PCF_MBR_NUM_CNT) AS TOT_PCF_MBR_NUM_CNT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                                                                  INNER JOIN TM00_LOC_D T3
                                                                          ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '001' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  XX.TOT_PCF_MBR_NUM_CNT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, TOT_PCF_MBR_NUM_CNT, RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_PCF_MBR_NUM_CNT ASC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  SUM(T1.TOT_PCF_MBR_NUM_CNT) AS TOT_PCF_MBR_NUM_CNT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                                                                  INNER JOIN TM00_LOC_D T3
                                                                          ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '001' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.TOT_PCF_MBR_NUM_CNT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, TOT_PCF_MBR_NUM_CNT, RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_PCF_MBR_NUM_CNT DESC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(T1.TOT_PCF_MBR_NUM_CNT) AS TOT_PCF_MBR_NUM_CNT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '001' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.TOT_PCF_MBR_NUM_CNT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, TOT_PCF_MBR_NUM_CNT, RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_PCF_MBR_NUM_CNT ASC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(T1.TOT_PCF_MBR_NUM_CNT) AS TOT_PCF_MBR_NUM_CNT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '001' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  XX.TOT_PCF_MBR_NUM_CNT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, TOT_PCF_MBR_NUM_CNT, RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_PCF_MBR_NUM_CNT DESC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT), 0) AS TOT_PCF_MBR_NUM_CNT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                                                                  INNER JOIN TM00_LOC_D T3
                                                                          ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '001' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  XX.TOT_PCF_MBR_NUM_CNT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, TOT_PCF_MBR_NUM_CNT, RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_PCF_MBR_NUM_CNT ASC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT), 0) AS TOT_PCF_MBR_NUM_CNT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                                                                  INNER JOIN TM00_LOC_D T3
                                                                          ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '001' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.TOT_PCF_MBR_NUM_CNT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, TOT_PCF_MBR_NUM_CNT, RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_PCF_MBR_NUM_CNT DESC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT), 0) AS TOT_PCF_MBR_NUM_CNT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '001' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.TOT_PCF_MBR_NUM_CNT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, TOT_PCF_MBR_NUM_CNT, RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_PCF_MBR_NUM_CNT ASC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT), 0) AS TOT_PCF_MBR_NUM_CNT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3
          ),
          FROM_KPI_TYP_002 AS
          (SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '002' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  XX.TDP_BAL AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, TDP_BAL, RANK() OVER(PARTITION BY BAS_YM ORDER BY TDP_BAL DESC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  SUM(T1.TDP_BAL) AS TDP_BAL
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                                                                  INNER JOIN TM00_LOC_D T3
                                                                          ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '002' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  XX.TDP_BAL AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, TDP_BAL, RANK() OVER(PARTITION BY BAS_YM ORDER BY TDP_BAL ASC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  SUM(T1.TDP_BAL) AS TDP_BAL
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                                                                  INNER JOIN TM00_LOC_D T3
                                                                          ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '002' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.TDP_BAL AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, TDP_BAL, RANK() OVER(PARTITION BY BAS_YM ORDER BY TDP_BAL DESC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(T1.TDP_BAL) AS TDP_BAL
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '002' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.TDP_BAL AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, TDP_BAL, RANK() OVER(PARTITION BY BAS_YM ORDER BY TDP_BAL ASC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(T1.TDP_BAL) AS TDP_BAL
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '002' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  XX.TDP_BAL AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, TDP_BAL, RANK() OVER(PARTITION BY BAS_YM ORDER BY TDP_BAL DESC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  ROUND(AVG(T1.TDP_BAL), 0) AS TDP_BAL
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                                                                  INNER JOIN TM00_LOC_D T3
                                                                          ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '002' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  XX.TDP_BAL AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, TDP_BAL, RANK() OVER(PARTITION BY BAS_YM ORDER BY TDP_BAL ASC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  ROUND(AVG(T1.TDP_BAL), 0) AS TDP_BAL
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                                                                  INNER JOIN TM00_LOC_D T3
                                                                          ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '002' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.TDP_BAL AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, TDP_BAL, RANK() OVER(PARTITION BY BAS_YM ORDER BY TDP_BAL DESC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  ROUND(AVG(T1.TDP_BAL), 0) AS TDP_BAL
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '002' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.TDP_BAL AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, TDP_BAL, RANK() OVER(PARTITION BY BAS_YM ORDER BY TDP_BAL ASC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  ROUND(AVG(T1.TDP_BAL), 0) AS TDP_BAL
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3
          ),
          FROM_KPI_TYP_003 AS
          (SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '003' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  XX.TOT_LN_BAL AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, TOT_LN_BAL, RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_LN_BAL DESC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  SUM(T1.TOT_LN_BAL) AS TOT_LN_BAL
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                                                                  INNER JOIN TM00_LOC_D T3
                                                                          ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '003' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  XX.TOT_LN_BAL AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, TOT_LN_BAL, RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_LN_BAL ASC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  SUM(T1.TOT_LN_BAL) AS TOT_LN_BAL
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                                                                  INNER JOIN TM00_LOC_D T3
                                                                          ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '003' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.TOT_LN_BAL AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, TOT_LN_BAL, RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_LN_BAL DESC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(T1.TOT_LN_BAL) AS TOT_LN_BAL
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '003' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.TOT_LN_BAL AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, TOT_LN_BAL, RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_LN_BAL ASC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(T1.TOT_LN_BAL) AS TOT_LN_BAL
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '003' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  XX.TOT_LN_BAL AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, TOT_LN_BAL, RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_LN_BAL DESC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  ROUND(AVG(T1.TOT_LN_BAL), 0) AS TOT_LN_BAL
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                                                                  INNER JOIN TM00_LOC_D T3
                                                                          ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '003' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  XX.TOT_LN_BAL AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, TOT_LN_BAL, RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_LN_BAL ASC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  ROUND(AVG(T1.TOT_LN_BAL), 0) AS TOT_LN_BAL
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                                                                  INNER JOIN TM00_LOC_D T3
                                                                          ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '003' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.TOT_LN_BAL AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, TOT_LN_BAL, RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_LN_BAL DESC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  ROUND(AVG(T1.TOT_LN_BAL), 0) AS TOT_LN_BAL
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '003' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.TOT_LN_BAL AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, TOT_LN_BAL, RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_LN_BAL ASC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  ROUND(AVG(T1.TOT_LN_BAL), 0) AS TOT_LN_BAL
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3
          ),
          FROM_KPI_TYP_004 AS
          (SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '004' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  XX.WO_COLL_LN_BAL AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, WO_COLL_LN_BAL, RANK() OVER(PARTITION BY BAS_YM ORDER BY WO_COLL_LN_BAL DESC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  SUM(T1.WO_COLL_LN_BAL) AS WO_COLL_LN_BAL
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                                                                  INNER JOIN TM00_LOC_D T3
                                                                          ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '004' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  XX.WO_COLL_LN_BAL AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, WO_COLL_LN_BAL, RANK() OVER(PARTITION BY BAS_YM ORDER BY WO_COLL_LN_BAL ASC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  SUM(T1.WO_COLL_LN_BAL) AS WO_COLL_LN_BAL
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                                                                  INNER JOIN TM00_LOC_D T3
                                                                          ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '004' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.WO_COLL_LN_BAL AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, WO_COLL_LN_BAL, RANK() OVER(PARTITION BY BAS_YM ORDER BY WO_COLL_LN_BAL DESC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(T1.WO_COLL_LN_BAL) AS WO_COLL_LN_BAL
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '004' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.WO_COLL_LN_BAL AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, WO_COLL_LN_BAL, RANK() OVER(PARTITION BY BAS_YM ORDER BY WO_COLL_LN_BAL ASC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(T1.WO_COLL_LN_BAL) AS WO_COLL_LN_BAL
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '004' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  XX.WO_COLL_LN_BAL AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, WO_COLL_LN_BAL, RANK() OVER(PARTITION BY BAS_YM ORDER BY WO_COLL_LN_BAL DESC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  ROUND(AVG(T1.WO_COLL_LN_BAL), 0) AS WO_COLL_LN_BAL
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                                                                  INNER JOIN TM00_LOC_D T3
                                                                          ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '004' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  XX.WO_COLL_LN_BAL AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, WO_COLL_LN_BAL, RANK() OVER(PARTITION BY BAS_YM ORDER BY WO_COLL_LN_BAL ASC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  ROUND(AVG(T1.WO_COLL_LN_BAL), 0) AS WO_COLL_LN_BAL
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                                                                  INNER JOIN TM00_LOC_D T3
                                                                          ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '004' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.WO_COLL_LN_BAL AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, WO_COLL_LN_BAL, RANK() OVER(PARTITION BY BAS_YM ORDER BY WO_COLL_LN_BAL DESC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  ROUND(AVG(T1.WO_COLL_LN_BAL), 0) AS WO_COLL_LN_BAL
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '004' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.WO_COLL_LN_BAL AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, WO_COLL_LN_BAL, RANK() OVER(PARTITION BY BAS_YM ORDER BY WO_COLL_LN_BAL ASC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  ROUND(AVG(T1.WO_COLL_LN_BAL), 0) AS WO_COLL_LN_BAL
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3
          ),
          FROM_KPI_TYP_005 AS
          (SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '005' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  XX.BAD_DBT_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, BAD_DBT_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY BAD_DBT_AMT DESC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  SUM(T1.BAD_DBT_AMT) AS BAD_DBT_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                                                                  INNER JOIN TM00_LOC_D T3
                                                                          ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '005' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  XX.BAD_DBT_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, BAD_DBT_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY BAD_DBT_AMT ASC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  SUM(T1.BAD_DBT_AMT) AS BAD_DBT_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                                                                  INNER JOIN TM00_LOC_D T3
                                                                          ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '005' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.BAD_DBT_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, BAD_DBT_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY BAD_DBT_AMT DESC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(T1.BAD_DBT_AMT) AS BAD_DBT_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '005' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.BAD_DBT_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, BAD_DBT_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY BAD_DBT_AMT ASC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(T1.BAD_DBT_AMT) AS BAD_DBT_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '005' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  XX.BAD_DBT_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, BAD_DBT_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY BAD_DBT_AMT DESC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  ROUND(AVG(T1.BAD_DBT_AMT), 0) AS BAD_DBT_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                                                                  INNER JOIN TM00_LOC_D T3
                                                                          ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '005' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  XX.BAD_DBT_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, BAD_DBT_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY BAD_DBT_AMT ASC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  ROUND(AVG(T1.BAD_DBT_AMT), 0) AS BAD_DBT_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                                                                  INNER JOIN TM00_LOC_D T3
                                                                          ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '005' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.BAD_DBT_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, BAD_DBT_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY BAD_DBT_AMT DESC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  ROUND(AVG(T1.BAD_DBT_AMT), 0) AS BAD_DBT_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '005' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.BAD_DBT_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, BAD_DBT_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY BAD_DBT_AMT ASC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  ROUND(AVG(T1.BAD_DBT_AMT), 0) AS BAD_DBT_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3
          ),
          FROM_KPI_TYP_006 AS
          (SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '006' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  XX.EQT_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, EQT_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY EQT_AMT DESC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  SUM(T1.EQT_AMT) AS EQT_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                                                                  INNER JOIN TM00_LOC_D T3
                                                                          ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '006' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  XX.EQT_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, EQT_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY EQT_AMT ASC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  SUM(T1.EQT_AMT) AS EQT_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                                                                  INNER JOIN TM00_LOC_D T3
                                                                          ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '006' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.EQT_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, EQT_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY EQT_AMT DESC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(T1.EQT_AMT) AS EQT_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '006' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.EQT_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, EQT_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY EQT_AMT ASC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(T1.EQT_AMT) AS EQT_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '006' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  XX.EQT_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, EQT_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY EQT_AMT DESC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  ROUND(AVG(T1.EQT_AMT), 0) AS EQT_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                                                                  INNER JOIN TM00_LOC_D T3
                                                                          ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '006' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  XX.EQT_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, EQT_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY EQT_AMT ASC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  ROUND(AVG(T1.EQT_AMT), 0) AS EQT_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                                                                  INNER JOIN TM00_LOC_D T3
                                                                          ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '006' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.EQT_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, EQT_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY EQT_AMT DESC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  ROUND(AVG(T1.EQT_AMT), 0) AS EQT_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '006' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.EQT_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, EQT_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY EQT_AMT ASC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  ROUND(AVG(T1.EQT_AMT), 0) AS EQT_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3
          ),
          FROM_KPI_TYP_007 AS
          (SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '007' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  XX.CCAP_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, CCAP_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY CCAP_AMT DESC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  SUM(T1.CCAP_AMT) AS CCAP_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                                                                  INNER JOIN TM00_LOC_D T3
                                                                          ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '007' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  XX.CCAP_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, CCAP_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY CCAP_AMT ASC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  SUM(T1.CCAP_AMT) AS CCAP_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                                                                  INNER JOIN TM00_LOC_D T3
                                                                          ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '007' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.CCAP_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, CCAP_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY CCAP_AMT DESC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(T1.CCAP_AMT) AS CCAP_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '007' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.CCAP_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, CCAP_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY CCAP_AMT ASC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(T1.CCAP_AMT) AS CCAP_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '007' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  XX.CCAP_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, CCAP_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY CCAP_AMT DESC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  ROUND(AVG(T1.CCAP_AMT), 0) AS CCAP_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                                                                  INNER JOIN TM00_LOC_D T3
                                                                          ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '007' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  XX.CCAP_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, CCAP_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY CCAP_AMT ASC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  ROUND(AVG(T1.CCAP_AMT), 0) AS CCAP_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                                                                  INNER JOIN TM00_LOC_D T3
                                                                          ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '007' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.CCAP_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, CCAP_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY CCAP_AMT DESC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  ROUND(AVG(T1.CCAP_AMT), 0) AS CCAP_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '007' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.CCAP_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, CCAP_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY CCAP_AMT ASC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  ROUND(AVG(T1.CCAP_AMT), 0) AS CCAP_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3
          ),
          FROM_KPI_TYP_008 AS
          (SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '008' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  XX.CBV_BOR_EXCL_SFTY_FUND_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, CBV_BOR_EXCL_SFTY_FUND_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY CBV_BOR_EXCL_SFTY_FUND_AMT DESC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  SUM(T1.CBV_BOR_EXCL_SFTY_FUND_AMT) AS CBV_BOR_EXCL_SFTY_FUND_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                                                                  INNER JOIN TM00_LOC_D T3
                                                                          ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '008' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  XX.CBV_BOR_EXCL_SFTY_FUND_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, CBV_BOR_EXCL_SFTY_FUND_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY CBV_BOR_EXCL_SFTY_FUND_AMT ASC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  SUM(T1.CBV_BOR_EXCL_SFTY_FUND_AMT) AS CBV_BOR_EXCL_SFTY_FUND_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                                                                  INNER JOIN TM00_LOC_D T3
                                                                          ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '008' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.CBV_BOR_EXCL_SFTY_FUND_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, CBV_BOR_EXCL_SFTY_FUND_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY CBV_BOR_EXCL_SFTY_FUND_AMT DESC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(T1.CBV_BOR_EXCL_SFTY_FUND_AMT) AS CBV_BOR_EXCL_SFTY_FUND_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '008' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.CBV_BOR_EXCL_SFTY_FUND_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, CBV_BOR_EXCL_SFTY_FUND_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY CBV_BOR_EXCL_SFTY_FUND_AMT ASC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(T1.CBV_BOR_EXCL_SFTY_FUND_AMT) AS CBV_BOR_EXCL_SFTY_FUND_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '008' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  XX.CBV_BOR_EXCL_SFTY_FUND_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, CBV_BOR_EXCL_SFTY_FUND_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY CBV_BOR_EXCL_SFTY_FUND_AMT DESC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  ROUND(AVG(T1.CBV_BOR_EXCL_SFTY_FUND_AMT), 0) AS CBV_BOR_EXCL_SFTY_FUND_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                                                                  INNER JOIN TM00_LOC_D T3
                                                                          ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '008' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  XX.CBV_BOR_EXCL_SFTY_FUND_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, CBV_BOR_EXCL_SFTY_FUND_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY CBV_BOR_EXCL_SFTY_FUND_AMT ASC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  ROUND(AVG(T1.CBV_BOR_EXCL_SFTY_FUND_AMT), 0) AS CBV_BOR_EXCL_SFTY_FUND_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                                                                  INNER JOIN TM00_LOC_D T3
                                                                          ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '008' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.CBV_BOR_EXCL_SFTY_FUND_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, CBV_BOR_EXCL_SFTY_FUND_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY CBV_BOR_EXCL_SFTY_FUND_AMT DESC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  ROUND(AVG(T1.CBV_BOR_EXCL_SFTY_FUND_AMT), 0) AS CBV_BOR_EXCL_SFTY_FUND_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '008' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.CBV_BOR_EXCL_SFTY_FUND_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, CBV_BOR_EXCL_SFTY_FUND_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY CBV_BOR_EXCL_SFTY_FUND_AMT ASC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  ROUND(AVG(T1.CBV_BOR_EXCL_SFTY_FUND_AMT), 0) AS CBV_BOR_EXCL_SFTY_FUND_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3
          ),
          FROM_KPI_TYP_009 AS
          (SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '009' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  XX.CBV_SVG_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, CBV_SVG_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY CBV_SVG_AMT DESC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  SUM(T1.CBV_SVG_AMT) AS CBV_SVG_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                                                                  INNER JOIN TM00_LOC_D T3
                                                                          ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '009' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  XX.CBV_SVG_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, CBV_SVG_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY CBV_SVG_AMT ASC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  SUM(T1.CBV_SVG_AMT) AS CBV_SVG_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                                                                  INNER JOIN TM00_LOC_D T3
                                                                          ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '009' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.CBV_SVG_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, CBV_SVG_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY CBV_SVG_AMT DESC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(T1.CBV_SVG_AMT) AS CBV_SVG_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '009' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.CBV_SVG_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, CBV_SVG_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY CBV_SVG_AMT ASC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(T1.CBV_SVG_AMT) AS CBV_SVG_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '009' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  XX.CBV_SVG_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, CBV_SVG_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY CBV_SVG_AMT DESC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  ROUND(AVG(T1.CBV_SVG_AMT), 0) AS CBV_SVG_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                                                                  INNER JOIN TM00_LOC_D T3
                                                                          ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '009' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  XX.CBV_SVG_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, CBV_SVG_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY CBV_SVG_AMT ASC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  ROUND(AVG(T1.CBV_SVG_AMT), 0) AS CBV_SVG_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                                                                  INNER JOIN TM00_LOC_D T3
                                                                          ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '009' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.CBV_SVG_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, CBV_SVG_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY CBV_SVG_AMT DESC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  ROUND(AVG(T1.CBV_SVG_AMT), 0) AS CBV_SVG_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '009' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.CBV_SVG_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, CBV_SVG_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY CBV_SVG_AMT ASC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  ROUND(AVG(T1.CBV_SVG_AMT), 0) AS CBV_SVG_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3
          ),
          FROM_KPI_TYP_010 AS
          (SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '010' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  XX.OTHR_INST_SVG_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, OTHR_INST_SVG_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY OTHR_INST_SVG_AMT DESC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  SUM(T1.OTHR_INST_SVG_AMT) AS OTHR_INST_SVG_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                                                                  INNER JOIN TM00_LOC_D T3
                                                                          ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '010' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  XX.OTHR_INST_SVG_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, OTHR_INST_SVG_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY OTHR_INST_SVG_AMT ASC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  SUM(T1.OTHR_INST_SVG_AMT) AS OTHR_INST_SVG_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                                                                  INNER JOIN TM00_LOC_D T3
                                                                          ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '010' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.OTHR_INST_SVG_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, OTHR_INST_SVG_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY OTHR_INST_SVG_AMT DESC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(T1.OTHR_INST_SVG_AMT) AS OTHR_INST_SVG_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '010' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.OTHR_INST_SVG_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, OTHR_INST_SVG_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY OTHR_INST_SVG_AMT ASC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(T1.OTHR_INST_SVG_AMT) AS OTHR_INST_SVG_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '010' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  XX.OTHR_INST_SVG_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, OTHR_INST_SVG_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY OTHR_INST_SVG_AMT DESC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  ROUND(AVG(T1.OTHR_INST_SVG_AMT), 0) AS OTHR_INST_SVG_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                                                                  INNER JOIN TM00_LOC_D T3
                                                                          ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '010' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  XX.OTHR_INST_SVG_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, OTHR_INST_SVG_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY OTHR_INST_SVG_AMT ASC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  ROUND(AVG(T1.OTHR_INST_SVG_AMT), 0) AS OTHR_INST_SVG_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                                                                  INNER JOIN TM00_LOC_D T3
                                                                          ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '010' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.OTHR_INST_SVG_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, OTHR_INST_SVG_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY OTHR_INST_SVG_AMT DESC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  ROUND(AVG(T1.OTHR_INST_SVG_AMT), 0) AS OTHR_INST_SVG_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '010' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.OTHR_INST_SVG_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, OTHR_INST_SVG_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY OTHR_INST_SVG_AMT ASC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  ROUND(AVG(T1.OTHR_INST_SVG_AMT), 0) AS OTHR_INST_SVG_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3
          ),
          FROM_KPI_TYP_066 AS
          (SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '066' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  XX.STFND_USE_MTLTLD_RTO AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, STFND_USE_MTLTLD_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY STFND_USE_MTLTLD_RTO DESC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  CASE WHEN SUM(T1.MTLTLD_AMT - T1.MED_AND_LONG_TERM_CAPITAL) =  0 THEN 0
                                       WHEN SUM(T1.MTLTLD_AMT - T1.MED_AND_LONG_TERM_CAPITAL) <> 0 AND SUM(T1.SHORT_TERM_CAPITAL) = 0 THEN NULL
                                       ELSE ROUND(SUM(T1.MTLTLD_AMT - T1.MED_AND_LONG_TERM_CAPITAL) / SUM(T1.SHORT_TERM_CAPITAL) * 100, 2)
                                       END AS STFND_USE_MTLTLD_RTO
                           FROM   MED_AND_LONG_TERM_LENDING T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN TM00_LOC_D T3
                                                                       ON T2.LOC_ID = T3.LOC_ID
                           WHERE  T1.BAS_YM = loop_bas_day.BAS_YM
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '066' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  XX.STFND_USE_MTLTLD_RTO AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, STFND_USE_MTLTLD_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY STFND_USE_MTLTLD_RTO ASC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  CASE WHEN SUM(T1.MTLTLD_AMT - T1.MED_AND_LONG_TERM_CAPITAL) =  0 THEN 0
                                       WHEN SUM(T1.MTLTLD_AMT - T1.MED_AND_LONG_TERM_CAPITAL) <> 0 AND SUM(T1.SHORT_TERM_CAPITAL) = 0 THEN NULL
                                       ELSE ROUND(SUM(T1.MTLTLD_AMT - T1.MED_AND_LONG_TERM_CAPITAL) / SUM(T1.SHORT_TERM_CAPITAL) * 100, 2)
                                       END AS STFND_USE_MTLTLD_RTO
                           FROM   MED_AND_LONG_TERM_LENDING T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN TM00_LOC_D T3
                                                                       ON T2.LOC_ID = T3.LOC_ID
                           WHERE  T1.BAS_YM = loop_bas_day.BAS_YM
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '066' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.STFND_USE_MTLTLD_RTO AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, STFND_USE_MTLTLD_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY STFND_USE_MTLTLD_RTO DESC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  CASE WHEN SUM(T1.MTLTLD_AMT - T1.MED_AND_LONG_TERM_CAPITAL) =  0 THEN 0
                                       WHEN SUM(T1.MTLTLD_AMT - T1.MED_AND_LONG_TERM_CAPITAL) <> 0 AND SUM(T1.SHORT_TERM_CAPITAL) = 0 THEN NULL
                                       ELSE ROUND(SUM(T1.MTLTLD_AMT - T1.MED_AND_LONG_TERM_CAPITAL) / SUM(T1.SHORT_TERM_CAPITAL) * 100, 2)
                                       END AS STFND_USE_MTLTLD_RTO
                           FROM   MED_AND_LONG_TERM_LENDING T1 INNER JOIN TM00_PCF_D T2
                                                                     ON T1.PCF_ID = T2.PCF_ID
                           WHERE  T1.BAS_YM = loop_bas_day.BAS_YM
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '066' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.STFND_USE_MTLTLD_RTO AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, STFND_USE_MTLTLD_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY STFND_USE_MTLTLD_RTO ASC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  CASE WHEN SUM(T1.MTLTLD_AMT - T1.MED_AND_LONG_TERM_CAPITAL) =  0 THEN 0
                                       WHEN SUM(T1.MTLTLD_AMT - T1.MED_AND_LONG_TERM_CAPITAL) <> 0 AND SUM(T1.SHORT_TERM_CAPITAL) = 0 THEN NULL
                                       ELSE ROUND(SUM(T1.MTLTLD_AMT - T1.MED_AND_LONG_TERM_CAPITAL) / SUM(T1.SHORT_TERM_CAPITAL) * 100, 2)
                                       END AS STFND_USE_MTLTLD_RTO
                           FROM   MED_AND_LONG_TERM_LENDING T1 INNER JOIN TM00_PCF_D T2
                                                                     ON T1.PCF_ID = T2.PCF_ID
                           WHERE  T1.BAS_YM = loop_bas_day.BAS_YM
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '066' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  XX.STFND_USE_MTLTLD_RTO AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, STFND_USE_MTLTLD_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY STFND_USE_MTLTLD_RTO DESC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  ROUND(AVG(STFND_USE_MTLTLD_RTO), 2) AS STFND_USE_MTLTLD_RTO
                           FROM   FROM_TM24_MMLY_FUND_SRC_USG_A T1 INNER JOIN TM00_PCF_D T2
                                                                           ON T1.PCF_ID = T2.PCF_ID
                                                                   INNER JOIN TM00_LOC_D T3
                                                                           ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '066' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  XX.STFND_USE_MTLTLD_RTO AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, STFND_USE_MTLTLD_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY STFND_USE_MTLTLD_RTO ASC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  ROUND(AVG(STFND_USE_MTLTLD_RTO), 2) AS STFND_USE_MTLTLD_RTO
                           FROM   FROM_TM24_MMLY_FUND_SRC_USG_A T1 INNER JOIN TM00_PCF_D T2
                                                                           ON T1.PCF_ID = T2.PCF_ID
                                                                   INNER JOIN TM00_LOC_D T3
                                                                           ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '066' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.STFND_USE_MTLTLD_RTO AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, STFND_USE_MTLTLD_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY STFND_USE_MTLTLD_RTO DESC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  ROUND(AVG(STFND_USE_MTLTLD_RTO), 2) AS STFND_USE_MTLTLD_RTO
                           FROM   FROM_TM24_MMLY_FUND_SRC_USG_A T1 INNER JOIN TM00_PCF_D T2
                                                                           ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '066' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.STFND_USE_MTLTLD_RTO AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, STFND_USE_MTLTLD_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY STFND_USE_MTLTLD_RTO ASC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  ROUND(AVG(STFND_USE_MTLTLD_RTO), 2) AS STFND_USE_MTLTLD_RTO
                           FROM   FROM_TM24_MMLY_FUND_SRC_USG_A T1 INNER JOIN TM00_PCF_D T2
                                                                           ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '066' AS KPI_TYP_CD,
                  '5'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(STFND_USE_MTLTLD_RTO, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, STFND_USE_MTLTLD_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY STFND_USE_MTLTLD_RTO DESC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(STFND_USE_MTLTLD_RTO) AS STFND_USE_MTLTLD_RTO
                         FROM
                               (SELECT BAS_YM, BAS_YM1, PRVN_CD, STFND_USE_MTLTLD_RTO, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY STFND_USE_MTLTLD_RTO DESC NULLS LAST, PRVN_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.PRVN_CD,
                                               YY.STFND_USE_MTLTLD_RTO
                                        FROM   (SELECT T1.BAS_YM, T3.PRVN_CD
                                                FROM   FROM_TM24_MMLY_FUND_SRC_USG_A T1 INNER JOIN TM00_PCF_D T2
                                                                                                ON T1.PCF_ID = T2.PCF_ID
                                                                                        INNER JOIN TM00_LOC_D T3
                                                                                                ON T2.LOC_ID = T3.LOC_ID
                                                GROUP BY T1.BAS_YM, T3.PRVN_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       C.PRVN_CD,
                                                                       ROUND(AVG(STFND_USE_MTLTLD_RTO), 2) AS STFND_USE_MTLTLD_RTO
                                                                FROM   TM24_MMLY_FUND_SRC_USG_A A INNER JOIN TM00_PCF_D B
                                                                                                          ON A.PCF_ID = B.PCF_ID
                                                                                                  INNER JOIN TM00_LOC_D C
                                                                                                          ON B.LOC_ID = C.LOC_ID
                                                                GROUP BY A.BAS_YM, C.PRVN_CD
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PRVN_CD  = XX.PRVN_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '066' AS KPI_TYP_CD,
                  '5'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(STFND_USE_MTLTLD_RTO, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, STFND_USE_MTLTLD_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY STFND_USE_MTLTLD_RTO ASC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(STFND_USE_MTLTLD_RTO) AS STFND_USE_MTLTLD_RTO
                         FROM
                               (SELECT BAS_YM, BAS_YM1, PRVN_CD, STFND_USE_MTLTLD_RTO, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY STFND_USE_MTLTLD_RTO ASC NULLS LAST, PRVN_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.PRVN_CD,
                                               YY.STFND_USE_MTLTLD_RTO
                                        FROM   (SELECT T1.BAS_YM, T3.PRVN_CD
                                                FROM   FROM_TM24_MMLY_FUND_SRC_USG_A T1 INNER JOIN TM00_PCF_D T2
                                                                                                ON T1.PCF_ID = T2.PCF_ID
                                                                                        INNER JOIN TM00_LOC_D T3
                                                                                                ON T2.LOC_ID = T3.LOC_ID
                                                GROUP BY T1.BAS_YM, T3.PRVN_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       C.PRVN_CD,
                                                                       ROUND(AVG(STFND_USE_MTLTLD_RTO), 2) AS STFND_USE_MTLTLD_RTO
                                                                FROM   TM24_MMLY_FUND_SRC_USG_A A INNER JOIN TM00_PCF_D B
                                                                                                          ON A.PCF_ID = B.PCF_ID
                                                                                                  INNER JOIN TM00_LOC_D C
                                                                                                          ON B.LOC_ID = C.LOC_ID
                                                                GROUP BY A.BAS_YM, C.PRVN_CD
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PRVN_CD  = XX.PRVN_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '066' AS KPI_TYP_CD,
                  '5'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(STFND_USE_MTLTLD_RTO, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, STFND_USE_MTLTLD_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY STFND_USE_MTLTLD_RTO DESC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(STFND_USE_MTLTLD_RTO) AS STFND_USE_MTLTLD_RTO
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, STFND_USE_MTLTLD_RTO, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY STFND_USE_MTLTLD_RTO DESC NULLS LAST, CBV_BR_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               YY.STFND_USE_MTLTLD_RTO
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD
                                                FROM   FROM_TM24_MMLY_FUND_SRC_USG_A T1 INNER JOIN TM00_PCF_D T2
                                                                                                ON T1.PCF_ID = T2.PCF_ID
                                                GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       B.CBV_BR_CD,
                                                                       ROUND(AVG(STFND_USE_MTLTLD_RTO), 2) AS STFND_USE_MTLTLD_RTO
                                                                FROM   TM24_MMLY_FUND_SRC_USG_A A INNER JOIN TM00_PCF_D B
                                                                                                          ON A.PCF_ID = B.PCF_ID
                                                                GROUP BY A.BAS_YM, B.CBV_BR_CD
                                                               ) YY
                                                            ON YY.BAS_YM   >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM   <= XX.BAS_YM
                                                           AND YY.CBV_BR_CD = XX.CBV_BR_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '066' AS KPI_TYP_CD,
                  '5'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(STFND_USE_MTLTLD_RTO, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, STFND_USE_MTLTLD_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY STFND_USE_MTLTLD_RTO ASC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(STFND_USE_MTLTLD_RTO) AS STFND_USE_MTLTLD_RTO
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, STFND_USE_MTLTLD_RTO, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY STFND_USE_MTLTLD_RTO ASC NULLS LAST, CBV_BR_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               YY.STFND_USE_MTLTLD_RTO
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD
                                                FROM   FROM_TM24_MMLY_FUND_SRC_USG_A T1 INNER JOIN TM00_PCF_D T2
                                                                                                ON T1.PCF_ID = T2.PCF_ID
                                                GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       B.CBV_BR_CD,
                                                                       ROUND(AVG(STFND_USE_MTLTLD_RTO), 2) AS STFND_USE_MTLTLD_RTO
                                                                FROM   TM24_MMLY_FUND_SRC_USG_A A INNER JOIN TM00_PCF_D B
                                                                                                          ON A.PCF_ID = B.PCF_ID
                                                                GROUP BY A.BAS_YM, B.CBV_BR_CD
                                                               ) YY
                                                            ON YY.BAS_YM   >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM   <= XX.BAS_YM
                                                           AND YY.CBV_BR_CD = XX.CBV_BR_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '066' AS KPI_TYP_CD,
                  '4'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(STFND_USE_MTLTLD_RTO, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, STFND_USE_MTLTLD_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY STFND_USE_MTLTLD_RTO DESC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  SUM(CASE WHEN T1.MTLTLD_AMT - T1.MED_AND_LONG_TERM_CAPITAL =  0 THEN 0
                                           WHEN T1.MTLTLD_AMT - T1.MED_AND_LONG_TERM_CAPITAL <> 0 AND T1.SHORT_TERM_CAPITAL = 0 THEN NULL
                                           ELSE CASE WHEN T1.SHORT_TERM_CAPITAL = 0 THEN 0
                                                     ELSE ROUND((T1.MTLTLD_AMT - T1.MED_AND_LONG_TERM_CAPITAL) / (T1.SHORT_TERM_CAPITAL) * 100, 2) * T4.ASSET_WEIGHTED_AVG
                                                     END
                                           END) AS STFND_USE_MTLTLD_RTO
                           FROM   MED_AND_LONG_TERM_LENDING T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN TM00_LOC_D T3
                                                                       ON T2.LOC_ID = T3.LOC_ID
                                                               INNER JOIN ASSET_WA_PCF_PRVN_066 T4
                                                                       ON T4.BAS_YM = T1.BAS_YM
                                                                      AND T4.PCF_ID = T1.PCF_ID
                           WHERE  T1.BAS_YM = loop_bas_day.BAS_YM
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   )
           WHERE RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '066' AS KPI_TYP_CD,
                  '4'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(STFND_USE_MTLTLD_RTO, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, STFND_USE_MTLTLD_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY STFND_USE_MTLTLD_RTO ASC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  SUM(CASE WHEN T1.MTLTLD_AMT - T1.MED_AND_LONG_TERM_CAPITAL =  0 THEN 0
                                           WHEN T1.MTLTLD_AMT - T1.MED_AND_LONG_TERM_CAPITAL <> 0 AND T1.SHORT_TERM_CAPITAL = 0 THEN NULL
                                           ELSE CASE WHEN T1.SHORT_TERM_CAPITAL = 0 THEN 0
                                                     ELSE ROUND((T1.MTLTLD_AMT - T1.MED_AND_LONG_TERM_CAPITAL) / (T1.SHORT_TERM_CAPITAL) * 100, 2) * T4.ASSET_WEIGHTED_AVG
                                                     END
                                           END) AS STFND_USE_MTLTLD_RTO
                           FROM   MED_AND_LONG_TERM_LENDING T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN TM00_LOC_D T3
                                                                       ON T2.LOC_ID = T3.LOC_ID
                                                               INNER JOIN ASSET_WA_PCF_PRVN_066 T4
                                                                       ON T4.BAS_YM = T1.BAS_YM
                                                                      AND T4.PCF_ID = T1.PCF_ID
                           WHERE  T1.BAS_YM = loop_bas_day.BAS_YM
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   )
           WHERE RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '066' AS KPI_TYP_CD,
                  '4'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  CBV_BR_CD,
                  ROUND(STFND_USE_MTLTLD_RTO, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, STFND_USE_MTLTLD_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY STFND_USE_MTLTLD_RTO DESC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(CASE WHEN T1.MTLTLD_AMT - T1.MED_AND_LONG_TERM_CAPITAL =  0 THEN 0
                                           WHEN T1.MTLTLD_AMT - T1.MED_AND_LONG_TERM_CAPITAL <> 0 AND T1.SHORT_TERM_CAPITAL = 0 THEN NULL
                                           ELSE CASE WHEN T1.SHORT_TERM_CAPITAL = 0 THEN 0
                                                     ELSE ROUND((T1.MTLTLD_AMT - T1.MED_AND_LONG_TERM_CAPITAL) / (T1.SHORT_TERM_CAPITAL) * 100, 2) * T3.ASSET_WEIGHTED_AVG
                                                     END
                                           END) AS STFND_USE_MTLTLD_RTO
                           FROM   MED_AND_LONG_TERM_LENDING T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN ASSET_WA_PCF_BR_066 T3
                                                                       ON T3.BAS_YM = T1.BAS_YM
                                                                      AND T3.PCF_ID = T1.PCF_ID
                           WHERE  T1.BAS_YM = loop_bas_day.BAS_YM
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   )
           WHERE RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '066' AS KPI_TYP_CD,
                  '4'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  CBV_BR_CD,
                  ROUND(STFND_USE_MTLTLD_RTO, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, STFND_USE_MTLTLD_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY STFND_USE_MTLTLD_RTO ASC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(CASE WHEN T1.MTLTLD_AMT - T1.MED_AND_LONG_TERM_CAPITAL =  0 THEN 0
                                           WHEN T1.MTLTLD_AMT - T1.MED_AND_LONG_TERM_CAPITAL <> 0 AND T1.SHORT_TERM_CAPITAL = 0 THEN NULL
                                           ELSE CASE WHEN T1.SHORT_TERM_CAPITAL = 0 THEN 0
                                                     ELSE ROUND((T1.MTLTLD_AMT - T1.MED_AND_LONG_TERM_CAPITAL) / (T1.SHORT_TERM_CAPITAL) * 100, 2) * T3.ASSET_WEIGHTED_AVG
                                                     END
                                           END) AS STFND_USE_MTLTLD_RTO
                           FROM   MED_AND_LONG_TERM_LENDING T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN ASSET_WA_PCF_BR_066 T3
                                                                       ON T3.BAS_YM = T1.BAS_YM
                                                                      AND T3.PCF_ID = T1.PCF_ID
                           WHERE  T1.BAS_YM = loop_bas_day.BAS_YM
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   )
           WHERE RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '066' AS KPI_TYP_CD,
                  '7'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(STFND_USE_MTLTLD_RTO, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, STFND_USE_MTLTLD_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY STFND_USE_MTLTLD_RTO DESC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(STFND_USE_MTLTLD_RTO) AS STFND_USE_MTLTLD_RTO
                         FROM
                               (SELECT BAS_YM, BAS_YM1, PRVN_CD, STFND_USE_MTLTLD_RTO, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY STFND_USE_MTLTLD_RTO DESC NULLS LAST, PRVN_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.PRVN_CD,
                                               YY.STFND_USE_MTLTLD_RTO
                                        FROM   (SELECT T1.BAS_YM, T3.PRVN_CD
                                                FROM   MED_AND_LONG_TERM_LENDING  T1 INNER JOIN TM00_PCF_D T2
                                                                                             ON T1.PCF_ID = T2.PCF_ID
                                                                                     INNER JOIN TM00_LOC_D T3
                                                                                             ON T2.LOC_ID = T3.LOC_ID
                                                WHERE  T1.BAS_YM = loop_bas_day.BAS_YM
                                                GROUP BY T1.BAS_YM, T3.PRVN_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       C.PRVN_CD,
                                                                       SUM(CASE WHEN A.MTLTLD_AMT - A.MED_AND_LONG_TERM_CAPITAL =  0 THEN 0
                                                                                WHEN A.MTLTLD_AMT - A.MED_AND_LONG_TERM_CAPITAL <> 0 AND A.SHORT_TERM_CAPITAL = 0 THEN NULL
                                                                                ELSE CASE WHEN A.SHORT_TERM_CAPITAL = 0 THEN 0
                                                                                          ELSE ROUND((A.MTLTLD_AMT - A.MED_AND_LONG_TERM_CAPITAL) / (A.SHORT_TERM_CAPITAL) * 100, 2) * D.ASSET_WEIGHTED_AVG
                                                                                          END
                                                                                END) AS STFND_USE_MTLTLD_RTO
                                                                FROM   MED_AND_LONG_TERM_LENDING  A INNER JOIN TM00_PCF_D B
                                                                                                            ON A.PCF_ID = B.PCF_ID
                                                                                                    INNER JOIN TM00_LOC_D C
                                                                                                            ON B.LOC_ID = C.LOC_ID
                                                                                                    INNER JOIN ASSET_WA_PCF_PRVN_066 D
                                                                                                            ON D.BAS_YM = A.BAS_YM
                                                                                                           AND D.PCF_ID = A.PCF_ID
                                                                GROUP BY A.BAS_YM, C.PRVN_CD
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PRVN_CD  = XX.PRVN_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '066' AS KPI_TYP_CD,
                  '7'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(STFND_USE_MTLTLD_RTO, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, STFND_USE_MTLTLD_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY STFND_USE_MTLTLD_RTO ASC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(STFND_USE_MTLTLD_RTO) AS STFND_USE_MTLTLD_RTO
                         FROM
                               (SELECT BAS_YM, BAS_YM1, PRVN_CD, STFND_USE_MTLTLD_RTO, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY STFND_USE_MTLTLD_RTO ASC NULLS LAST, PRVN_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.PRVN_CD,
                                               YY.STFND_USE_MTLTLD_RTO
                                        FROM   (SELECT T1.BAS_YM, T3.PRVN_CD
                                                FROM   MED_AND_LONG_TERM_LENDING  T1 INNER JOIN TM00_PCF_D T2
                                                                                             ON T1.PCF_ID = T2.PCF_ID
                                                                                     INNER JOIN TM00_LOC_D T3
                                                                                             ON T2.LOC_ID = T3.LOC_ID
                                                WHERE  T1.BAS_YM = loop_bas_day.BAS_YM
                                                GROUP BY T1.BAS_YM, T3.PRVN_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       C.PRVN_CD,
                                                                       SUM(CASE WHEN A.MTLTLD_AMT - A.MED_AND_LONG_TERM_CAPITAL =  0 THEN 0
                                                                                WHEN A.MTLTLD_AMT - A.MED_AND_LONG_TERM_CAPITAL <> 0 AND A.SHORT_TERM_CAPITAL = 0 THEN NULL
                                                                                ELSE CASE WHEN A.SHORT_TERM_CAPITAL = 0 THEN 0
                                                                                          ELSE ROUND((A.MTLTLD_AMT - A.MED_AND_LONG_TERM_CAPITAL) / (A.SHORT_TERM_CAPITAL) * 100, 2) * D.ASSET_WEIGHTED_AVG
                                                                                          END
                                                                                END) AS STFND_USE_MTLTLD_RTO
                                                                FROM   MED_AND_LONG_TERM_LENDING  A INNER JOIN TM00_PCF_D B
                                                                                                            ON A.PCF_ID = B.PCF_ID
                                                                                                    INNER JOIN TM00_LOC_D C
                                                                                                            ON B.LOC_ID = C.LOC_ID
                                                                                                    INNER JOIN ASSET_WA_PCF_PRVN_066 D
                                                                                                            ON D.BAS_YM = A.BAS_YM
                                                                                                           AND D.PCF_ID = A.PCF_ID
                                                                GROUP BY A.BAS_YM, C.PRVN_CD
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PRVN_CD  = XX.PRVN_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '066' AS KPI_TYP_CD,
                  '7'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(STFND_USE_MTLTLD_RTO, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, STFND_USE_MTLTLD_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY STFND_USE_MTLTLD_RTO DESC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(STFND_USE_MTLTLD_RTO) AS STFND_USE_MTLTLD_RTO
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, STFND_USE_MTLTLD_RTO, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY STFND_USE_MTLTLD_RTO DESC NULLS LAST, CBV_BR_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               YY.STFND_USE_MTLTLD_RTO
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD
                                                FROM   MED_AND_LONG_TERM_LENDING  T1 INNER JOIN TM00_PCF_D T2
                                                                                             ON T1.PCF_ID = T2.PCF_ID
                                                WHERE  T1.BAS_YM = loop_bas_day.BAS_YM
                                                GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       B.CBV_BR_CD,
                                                                       SUM(CASE WHEN A.MTLTLD_AMT - A.MED_AND_LONG_TERM_CAPITAL =  0 THEN 0
                                                                                WHEN A.MTLTLD_AMT - A.MED_AND_LONG_TERM_CAPITAL <> 0 AND A.SHORT_TERM_CAPITAL = 0 THEN NULL
                                                                                ELSE CASE WHEN A.SHORT_TERM_CAPITAL = 0 THEN 0
                                                                                          ELSE ROUND((A.MTLTLD_AMT - A.MED_AND_LONG_TERM_CAPITAL) / (A.SHORT_TERM_CAPITAL) * 100, 2) * C.ASSET_WEIGHTED_AVG
                                                                                          END
                                                                                END) AS STFND_USE_MTLTLD_RTO
                                                                FROM   MED_AND_LONG_TERM_LENDING  A INNER JOIN TM00_PCF_D B
                                                                                                            ON A.PCF_ID = B.PCF_ID
                                                                                                    INNER JOIN ASSET_WA_PCF_BR_066 C
                                                                                                            ON C.BAS_YM = A.BAS_YM
                                                                                                           AND C.PCF_ID = A.PCF_ID
                                                                GROUP BY A.BAS_YM, B.CBV_BR_CD
                                                               ) YY
                                                            ON YY.BAS_YM   >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM   <= XX.BAS_YM
                                                           AND YY.CBV_BR_CD = XX.CBV_BR_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '066' AS KPI_TYP_CD,
                  '7'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(STFND_USE_MTLTLD_RTO, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, STFND_USE_MTLTLD_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY STFND_USE_MTLTLD_RTO ASC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(STFND_USE_MTLTLD_RTO) AS STFND_USE_MTLTLD_RTO
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, STFND_USE_MTLTLD_RTO, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY STFND_USE_MTLTLD_RTO ASC NULLS LAST, CBV_BR_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               YY.STFND_USE_MTLTLD_RTO
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD
                                                FROM   MED_AND_LONG_TERM_LENDING  T1 INNER JOIN TM00_PCF_D T2
                                                                                             ON T1.PCF_ID = T2.PCF_ID
                                                WHERE  T1.BAS_YM = loop_bas_day.BAS_YM
                                                GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       B.CBV_BR_CD,
                                                                       SUM(CASE WHEN A.MTLTLD_AMT - A.MED_AND_LONG_TERM_CAPITAL =  0 THEN 0
                                                                                WHEN A.MTLTLD_AMT - A.MED_AND_LONG_TERM_CAPITAL <> 0 AND A.SHORT_TERM_CAPITAL = 0 THEN NULL
                                                                                ELSE CASE WHEN A.SHORT_TERM_CAPITAL = 0 THEN 0
                                                                                          ELSE ROUND((A.MTLTLD_AMT - A.MED_AND_LONG_TERM_CAPITAL) / (A.SHORT_TERM_CAPITAL) * 100, 2) * C.ASSET_WEIGHTED_AVG
                                                                                          END
                                                                                END) AS STFND_USE_MTLTLD_RTO
                                                                FROM   MED_AND_LONG_TERM_LENDING  A INNER JOIN TM00_PCF_D B
                                                                                                            ON A.PCF_ID = B.PCF_ID
                                                                                                    INNER JOIN ASSET_WA_PCF_BR_066 C
                                                                                                            ON C.BAS_YM = A.BAS_YM
                                                                                                           AND C.PCF_ID = A.PCF_ID
                                                                GROUP BY A.BAS_YM, B.CBV_BR_CD
                                                               ) YY
                                                            ON YY.BAS_YM   >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM   <= XX.BAS_YM
                                                           AND YY.CBV_BR_CD = XX.CBV_BR_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )
          ),
          FROM_KPI_TYP_069 AS
          (SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '069' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(XX.BAD_DBT_RTO, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, BAD_DBT_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY BAD_DBT_RTO DESC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  SUM(T1.BAD_DBT_AMT) / SUM(T1.TOT_LN_BAL) * 100 AS BAD_DBT_RTO
                           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                                      ON T1.PCF_ID = T2.PCF_ID
                                                              INNER JOIN TM00_LOC_D T3
                                                                      ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '069' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(XX.BAD_DBT_RTO, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, BAD_DBT_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY BAD_DBT_RTO ASC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  SUM(T1.BAD_DBT_AMT) / SUM(T1.TOT_LN_BAL) * 100 AS BAD_DBT_RTO
                           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                                      ON T1.PCF_ID = T2.PCF_ID
                                                              INNER JOIN TM00_LOC_D T3
                                                                      ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '069' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  ROUND(XX.BAD_DBT_RTO, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, BAD_DBT_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY BAD_DBT_RTO DESC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(T1.BAD_DBT_AMT) / SUM(T1.TOT_LN_BAL) * 100 AS BAD_DBT_RTO
                           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                                     ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '069' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  ROUND(XX.BAD_DBT_RTO, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, BAD_DBT_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY BAD_DBT_RTO ASC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(T1.BAD_DBT_AMT) / SUM(T1.TOT_LN_BAL) * 100 AS BAD_DBT_RTO
                           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                                      ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '069' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(XX.BAD_DBT_RTO, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, BAD_DBT_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY BAD_DBT_RTO DESC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  AVG(CASE WHEN T1.TOT_LN_BAL = 0 AND T1.BAD_DBT_AMT = 0 THEN 0
                                           ELSE CASE WHEN T1.TOT_LN_BAL = 0 THEN 0
                                                     ELSE T1.BAD_DBT_AMT / T1.TOT_LN_BAL * 100
                                                     END
                                           END) AS BAD_DBT_RTO
                           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                                      ON T1.PCF_ID = T2.PCF_ID
                                                              INNER JOIN TM00_LOC_D T3
                                                                      ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '069' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(XX.BAD_DBT_RTO, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, BAD_DBT_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY BAD_DBT_RTO ASC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  AVG(CASE WHEN T1.TOT_LN_BAL = 0 AND T1.BAD_DBT_AMT = 0 THEN 0
                                           ELSE CASE WHEN T1.TOT_LN_BAL = 0 THEN 0
                                                     ELSE T1.BAD_DBT_AMT / T1.TOT_LN_BAL * 100
                                                     END
                                           END) AS BAD_DBT_RTO
                           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                                      ON T1.PCF_ID = T2.PCF_ID
                                                              INNER JOIN TM00_LOC_D T3
                                                                      ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '069' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  ROUND(XX.BAD_DBT_RTO, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, BAD_DBT_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY BAD_DBT_RTO DESC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  AVG(CASE WHEN T1.TOT_LN_BAL = 0 AND T1.BAD_DBT_AMT = 0 THEN 0
                                           ELSE CASE WHEN T1.TOT_LN_BAL = 0 THEN 0
                                                     ELSE T1.BAD_DBT_AMT / T1.TOT_LN_BAL * 100
                                                     END
                                           END) AS BAD_DBT_RTO
                           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                                      ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '069' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  ROUND(XX.BAD_DBT_RTO, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, BAD_DBT_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY BAD_DBT_RTO ASC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  AVG(CASE WHEN T1.TOT_LN_BAL = 0 AND T1.BAD_DBT_AMT = 0 THEN 0
                                           ELSE CASE WHEN T1.TOT_LN_BAL = 0 THEN 0
                                                     ELSE T1.BAD_DBT_AMT / T1.TOT_LN_BAL * 100
                                                     END
                                           END) AS BAD_DBT_RTO
                           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                                      ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '069' AS KPI_TYP_CD,
                  '4'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(BAD_DBT_RTO, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, BAD_DBT_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY BAD_DBT_RTO DESC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  SUM(CASE WHEN (T1.TOT_LN_BAL) = 0 AND (T1.BAD_DBT_AMT) = 0 THEN 0
                                           ELSE CASE WHEN T1.TOT_LN_BAL = 0 THEN 0
                                                     ELSE (T1.BAD_DBT_AMT) / (T1.TOT_LN_BAL) * 100 * T4.ASSET_WEIGHTED_AVG
                                                     END
                                           END) AS BAD_DBT_RTO
                           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                                      ON T1.PCF_ID = T2.PCF_ID
                                                              INNER JOIN TM00_LOC_D T3
                                                                      ON T2.LOC_ID = T3.LOC_ID
                                                              INNER JOIN ASSET_WEIGHTED_AVG_PCF_PRVN T4
                                                                      ON T4.BAS_YM = T1.BAS_YM
                                                                     AND T4.PCF_ID = T1.PCF_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   )
           WHERE RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '069' AS KPI_TYP_CD,
                  '4'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(BAD_DBT_RTO, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, BAD_DBT_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY BAD_DBT_RTO ASC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  SUM(CASE WHEN (T1.TOT_LN_BAL) = 0 AND (T1.BAD_DBT_AMT) = 0 THEN 0
                                           ELSE CASE WHEN T1.TOT_LN_BAL = 0 THEN 0
                                                     ELSE (T1.BAD_DBT_AMT) / (T1.TOT_LN_BAL) * 100 * T4.ASSET_WEIGHTED_AVG
                                                     END
                                           END) AS BAD_DBT_RTO
                           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                                      ON T1.PCF_ID = T2.PCF_ID
                                                              INNER JOIN TM00_LOC_D T3
                                                                      ON T2.LOC_ID = T3.LOC_ID
                                                              INNER JOIN ASSET_WEIGHTED_AVG_PCF_PRVN T4
                                                                      ON T4.BAS_YM = T1.BAS_YM
                                                                     AND T4.PCF_ID = T1.PCF_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   )
           WHERE RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '069' AS KPI_TYP_CD,
                  '4'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  CBV_BR_CD,
                  ROUND(BAD_DBT_RTO, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, BAD_DBT_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY BAD_DBT_RTO DESC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(CASE WHEN (T1.TOT_LN_BAL) = 0 AND (T1.BAD_DBT_AMT) = 0 THEN 0
                                           ELSE CASE WHEN T1.TOT_LN_BAL = 0 THEN 0
                                                     ELSE (T1.BAD_DBT_AMT) / (T1.TOT_LN_BAL) * 100 * T3.ASSET_WEIGHTED_AVG
                                                     END
                                           END) AS BAD_DBT_RTO
                           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                                      ON T1.PCF_ID = T2.PCF_ID
                                                              INNER JOIN ASSET_WEIGHTED_AVG_PCF_BR T3
                                                                      ON T3.BAS_YM = T1.BAS_YM
                                                                     AND T3.PCF_ID = T1.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   )
           WHERE RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '069' AS KPI_TYP_CD,
                  '4'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  CBV_BR_CD,
                  ROUND(BAD_DBT_RTO, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, BAD_DBT_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY BAD_DBT_RTO ASC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(CASE WHEN (T1.TOT_LN_BAL) = 0 AND (T1.BAD_DBT_AMT) = 0 THEN 0
                                           ELSE CASE WHEN T1.TOT_LN_BAL = 0 THEN 0
                                                     ELSE (T1.BAD_DBT_AMT) / (T1.TOT_LN_BAL) * 100 * T3.ASSET_WEIGHTED_AVG
                                                     END
                                           END) AS BAD_DBT_RTO
                           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                                      ON T1.PCF_ID = T2.PCF_ID
                                                              INNER JOIN ASSET_WEIGHTED_AVG_PCF_BR T3
                                                                      ON T3.BAS_YM = T1.BAS_YM
                                                                     AND T3.PCF_ID = T1.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   )
           WHERE RNK_NUM BETWEEN 1 AND 3
          ),
          FROM_KPI_TYP_070 AS
          (SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '070' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(XX.COLL_RTO, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, COLL_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY COLL_RTO DESC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  SUM(T1.TOT_COLL_VAL) / SUM(T1.TOT_LN_BAL) * 100 AS COLL_RTO
                           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                                      ON T1.PCF_ID = T2.PCF_ID
                                                              INNER JOIN TM00_LOC_D T3
                                                                      ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '070' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(XX.COLL_RTO, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, COLL_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY COLL_RTO ASC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  SUM(T1.TOT_COLL_VAL) / SUM(T1.TOT_LN_BAL) * 100 AS COLL_RTO
                           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                                      ON T1.PCF_ID = T2.PCF_ID
                                                              INNER JOIN TM00_LOC_D T3
                                                                      ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '070' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  ROUND(XX.COLL_RTO, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, COLL_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY COLL_RTO DESC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(T1.TOT_COLL_VAL) / SUM(T1.TOT_LN_BAL) * 100 AS COLL_RTO
                           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                                      ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '070' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  ROUND(XX.COLL_RTO, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, COLL_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY COLL_RTO ASC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(T1.TOT_COLL_VAL) / SUM(T1.TOT_LN_BAL) * 100 AS COLL_RTO
                           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                                      ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '070' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(XX.COLL_RTO, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, COLL_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY COLL_RTO DESC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  AVG(CASE WHEN T1.TOT_LN_BAL = 0 THEN 0
                                           ELSE T1.TOT_COLL_VAL / T1.TOT_LN_BAL * 100
                                           END) AS COLL_RTO
                           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                                      ON T1.PCF_ID = T2.PCF_ID
                                                              INNER JOIN TM00_LOC_D T3
                                                                      ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '070' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(XX.COLL_RTO, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, COLL_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY COLL_RTO ASC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  AVG(CASE WHEN T1.TOT_LN_BAL = 0 THEN 0
                                           ELSE T1.TOT_COLL_VAL / T1.TOT_LN_BAL * 100
                                           END) AS COLL_RTO
                           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                                      ON T1.PCF_ID = T2.PCF_ID
                                                              INNER JOIN TM00_LOC_D T3
                                                                      ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '070' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  ROUND(XX.COLL_RTO, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, COLL_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY COLL_RTO DESC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  AVG(CASE WHEN T1.TOT_LN_BAL = 0 THEN 0
                                           ELSE T1.TOT_COLL_VAL / T1.TOT_LN_BAL * 100
                                           END) AS COLL_RTO
                           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                                      ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '070' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  ROUND(XX.COLL_RTO, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, COLL_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY COLL_RTO ASC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  AVG(CASE WHEN T1.TOT_LN_BAL = 0 THEN 0
                                           ELSE T1.TOT_COLL_VAL / T1.TOT_LN_BAL * 100
                                           END) AS COLL_RTO
                           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                                      ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '070' AS KPI_TYP_CD,
                  '4'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(COLL_RTO, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, COLL_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY COLL_RTO DESC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  SUM(CASE WHEN T1.TOT_LN_BAL = 0 THEN 0
                                           ELSE (T1.TOT_COLL_VAL) / (T1.TOT_LN_BAL) * 100 * T4.ASSET_WEIGHTED_AVG
                                           END) AS COLL_RTO
                           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                                      ON T1.PCF_ID = T2.PCF_ID
                                                              INNER JOIN TM00_LOC_D T3
                                                                      ON T2.LOC_ID = T3.LOC_ID
                                                              INNER JOIN ASSET_WA_PCF_PRVN_070 T4
                                                                      ON T4.BAS_YM = T1.BAS_YM
                                                                     AND T4.PCF_ID = T1.PCF_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   )
           WHERE RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '070' AS KPI_TYP_CD,
                  '4'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(COLL_RTO, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, COLL_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY COLL_RTO ASC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  SUM(CASE WHEN T1.TOT_LN_BAL = 0 THEN 0
                                           ELSE (T1.TOT_COLL_VAL) / (T1.TOT_LN_BAL) * 100 * T4.ASSET_WEIGHTED_AVG
                                           END) AS COLL_RTO
                           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                                      ON T1.PCF_ID = T2.PCF_ID
                                                              INNER JOIN TM00_LOC_D T3
                                                                      ON T2.LOC_ID = T3.LOC_ID
                                                              INNER JOIN ASSET_WA_PCF_PRVN_070 T4
                                                                      ON T4.BAS_YM = T1.BAS_YM
                                                                     AND T4.PCF_ID = T1.PCF_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   )
           WHERE RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '070' AS KPI_TYP_CD,
                  '4'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  CBV_BR_CD,
                  ROUND(COLL_RTO, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, COLL_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY COLL_RTO DESC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(CASE WHEN T1.TOT_LN_BAL = 0 THEN 0
                                           ELSE (T1.TOT_COLL_VAL) / (T1.TOT_LN_BAL) * 100 * T3.ASSET_WEIGHTED_AVG
                                           END) AS COLL_RTO
                           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                                      ON T1.PCF_ID = T2.PCF_ID
                                                              INNER JOIN ASSET_WA_PCF_BR_070 T3
                                                                      ON T3.BAS_YM = T1.BAS_YM
                                                                     AND T3.PCF_ID = T1.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   )
           WHERE RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '070' AS KPI_TYP_CD,
                  '4'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  CBV_BR_CD,
                  ROUND(COLL_RTO, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, COLL_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY COLL_RTO ASC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(CASE WHEN T1.TOT_LN_BAL = 0 THEN 0
                                           ELSE (T1.TOT_COLL_VAL) / (T1.TOT_LN_BAL) * 100 * T3.ASSET_WEIGHTED_AVG
                                           END) AS COLL_RTO
                           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                                      ON T1.PCF_ID = T2.PCF_ID
                                                              INNER JOIN ASSET_WA_PCF_BR_070 T3
                                                                      ON T3.BAS_YM = T1.BAS_YM
                                                                     AND T3.PCF_ID = T1.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   )
           WHERE RNK_NUM BETWEEN 1 AND 3
          ),
          FROM_KPI_TYP_071 AS
          (SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '071' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(XX.PRVS_RTO, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, PRVS_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY PRVS_RTO DESC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  SUM(T1.TOT_PRVS_AMT) / SUM(T1.TOT_LN_BAL) * 100 AS PRVS_RTO
                           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                                      ON T1.PCF_ID = T2.PCF_ID
                                                              INNER JOIN TM00_LOC_D T3
                                                                      ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '071' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(XX.PRVS_RTO, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, PRVS_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY PRVS_RTO ASC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  SUM(T1.TOT_PRVS_AMT) / SUM(T1.TOT_LN_BAL) * 100 AS PRVS_RTO
                           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                                     ON T1.PCF_ID = T2.PCF_ID
                                                              INNER JOIN TM00_LOC_D T3
                                                                      ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '071' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  ROUND(XX.PRVS_RTO, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PRVS_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY PRVS_RTO DESC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(T1.TOT_PRVS_AMT) / SUM(T1.TOT_LN_BAL) * 100 AS PRVS_RTO
                           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                                      ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '071' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  ROUND(XX.PRVS_RTO, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PRVS_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY PRVS_RTO ASC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(T1.TOT_PRVS_AMT) / SUM(T1.TOT_LN_BAL) * 100 AS PRVS_RTO
                           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                                      ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '071' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(XX.PRVS_RTO, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, PRVS_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY PRVS_RTO DESC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  AVG(CASE WHEN T1.TOT_LN_BAL = 0 THEN 0
                                           ELSE T1.TOT_PRVS_AMT / T1.TOT_LN_BAL * 100
                                           END) AS PRVS_RTO
                           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                                      ON T1.PCF_ID = T2.PCF_ID
                                                              INNER JOIN TM00_LOC_D T3
                                                                      ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '071' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(XX.PRVS_RTO, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, PRVS_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY PRVS_RTO ASC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  AVG(CASE WHEN T1.TOT_LN_BAL = 0 THEN 0
                                           ELSE T1.TOT_PRVS_AMT / T1.TOT_LN_BAL * 100
                                           END) AS PRVS_RTO
                           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                                      ON T1.PCF_ID = T2.PCF_ID
                                                              INNER JOIN TM00_LOC_D T3
                                                                      ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '071' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  ROUND(XX.PRVS_RTO, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PRVS_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY PRVS_RTO DESC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  AVG(CASE WHEN T1.TOT_LN_BAL = 0 THEN 0
                                           ELSE T1.TOT_PRVS_AMT / T1.TOT_LN_BAL * 100
                                           END) AS PRVS_RTO
                           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                                      ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '071' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  ROUND(XX.PRVS_RTO, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PRVS_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY PRVS_RTO ASC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  AVG(CASE WHEN T1.TOT_LN_BAL = 0 THEN 0
                                           ELSE T1.TOT_PRVS_AMT / T1.TOT_LN_BAL * 100
                                           END) AS PRVS_RTO
                           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                                      ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '071' AS KPI_TYP_CD,
                  '4'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PRVS_RTO, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, PRVS_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY PRVS_RTO DESC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  SUM(CASE WHEN T1.TOT_LN_BAL = 0 THEN 0
                                           ELSE (T1.TOT_PRVS_AMT) / (T1.TOT_LN_BAL) * 100 * T4.ASSET_WEIGHTED_AVG
                                           END) AS PRVS_RTO
                           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                                      ON T1.PCF_ID = T2.PCF_ID
                                                              INNER JOIN TM00_LOC_D T3
                                                                      ON T2.LOC_ID = T3.LOC_ID
                                                              INNER JOIN ASSET_WA_PCF_PRVN_071 T4
                                                                      ON T4.BAS_YM = T1.BAS_YM
                                                                     AND T4.PCF_ID = T1.PCF_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   )
           WHERE RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '071' AS KPI_TYP_CD,
                  '4'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PRVS_RTO, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, PRVS_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY PRVS_RTO ASC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  SUM(CASE WHEN T1.TOT_LN_BAL = 0 THEN 0
                                           ELSE (T1.TOT_PRVS_AMT) / (T1.TOT_LN_BAL) * 100 * T4.ASSET_WEIGHTED_AVG
                                           END) AS PRVS_RTO
                           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                                      ON T1.PCF_ID = T2.PCF_ID
                                                              INNER JOIN TM00_LOC_D T3
                                                                      ON T2.LOC_ID = T3.LOC_ID
                                                              INNER JOIN ASSET_WA_PCF_PRVN_071 T4
                                                                      ON T4.BAS_YM = T1.BAS_YM
                                                                     AND T4.PCF_ID = T1.PCF_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   )
           WHERE RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '071' AS KPI_TYP_CD,
                  '4'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  CBV_BR_CD,
                  ROUND(PRVS_RTO, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PRVS_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY PRVS_RTO DESC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(CASE WHEN T1.TOT_LN_BAL = 0 THEN 0
                                           ELSE (T1.TOT_PRVS_AMT) / (T1.TOT_LN_BAL) * 100 * T3.ASSET_WEIGHTED_AVG
                                           END) AS PRVS_RTO
                           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                                      ON T1.PCF_ID = T2.PCF_ID
                                                              INNER JOIN ASSET_WA_PCF_BR_071 T3
                                                                      ON T3.BAS_YM = T1.BAS_YM
                                                                     AND T3.PCF_ID = T1.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   )
           WHERE RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '071' AS KPI_TYP_CD,
                  '4'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  CBV_BR_CD,
                  ROUND(PRVS_RTO, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PRVS_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY PRVS_RTO ASC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(CASE WHEN T1.TOT_LN_BAL = 0 THEN 0
                                           ELSE (T1.TOT_PRVS_AMT) / (T1.TOT_LN_BAL) * 100 * T3.ASSET_WEIGHTED_AVG
                                           END) AS PRVS_RTO
                           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN TM00_PCF_D T2
                                                                      ON T1.PCF_ID = T2.PCF_ID
                                                              INNER JOIN ASSET_WA_PCF_BR_071 T3
                                                                      ON T3.BAS_YM = T1.BAS_YM
                                                                     AND T3.PCF_ID = T1.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   )
           WHERE RNK_NUM BETWEEN 1 AND 3
          ),
          FROM_KPI_TYP_088 AS
          (SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '088' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  XX.PRFBL_AST_VS_TOT_AST_RTO AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, PRFBL_AST_VS_TOT_AST_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY PRFBL_AST_VS_TOT_AST_RTO DESC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  ROUND(SUM(T1.PRFBL_AST_AMT) / SUM(T1.TOT_AST_AMT) * 100, 2) AS PRFBL_AST_VS_TOT_AST_RTO
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                                                                  INNER JOIN TM00_LOC_D T3
                                                                          ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '088' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  XX.PRFBL_AST_VS_TOT_AST_RTO AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, PRFBL_AST_VS_TOT_AST_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY PRFBL_AST_VS_TOT_AST_RTO ASC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  ROUND(SUM(T1.PRFBL_AST_AMT) / SUM(T1.TOT_AST_AMT) * 100, 2) AS PRFBL_AST_VS_TOT_AST_RTO
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                                                                  INNER JOIN TM00_LOC_D T3
                                                                          ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '088' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.PRFBL_AST_VS_TOT_AST_RTO AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PRFBL_AST_VS_TOT_AST_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY PRFBL_AST_VS_TOT_AST_RTO DESC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  ROUND(SUM(T1.PRFBL_AST_AMT) / SUM(T1.TOT_AST_AMT) * 100, 2) AS PRFBL_AST_VS_TOT_AST_RTO
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '088' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.PRFBL_AST_VS_TOT_AST_RTO AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PRFBL_AST_VS_TOT_AST_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY PRFBL_AST_VS_TOT_AST_RTO ASC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  ROUND(SUM(T1.PRFBL_AST_AMT) / SUM(T1.TOT_AST_AMT) * 100, 2) AS PRFBL_AST_VS_TOT_AST_RTO
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '088' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  XX.PRFBL_AST_VS_TOT_AST_RTO AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, PRFBL_AST_VS_TOT_AST_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY PRFBL_AST_VS_TOT_AST_RTO DESC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  ROUND(AVG(T1.PRFBL_AST_VS_TOT_AST_RTO), 2) AS PRFBL_AST_VS_TOT_AST_RTO
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                                                                  INNER JOIN TM00_LOC_D T3
                                                                          ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '088' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  XX.PRFBL_AST_VS_TOT_AST_RTO AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, PRFBL_AST_VS_TOT_AST_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY PRFBL_AST_VS_TOT_AST_RTO ASC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  ROUND(AVG(T1.PRFBL_AST_VS_TOT_AST_RTO), 2) AS PRFBL_AST_VS_TOT_AST_RTO
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                                                                  INNER JOIN TM00_LOC_D T3
                                                                          ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '088' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.PRFBL_AST_VS_TOT_AST_RTO AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PRFBL_AST_VS_TOT_AST_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY PRFBL_AST_VS_TOT_AST_RTO DESC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  ROUND(AVG(T1.PRFBL_AST_VS_TOT_AST_RTO), 2) AS PRFBL_AST_VS_TOT_AST_RTO
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '088' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  ROUND(XX.PRFBL_AST_VS_TOT_AST_RTO, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PRFBL_AST_VS_TOT_AST_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY PRFBL_AST_VS_TOT_AST_RTO ASC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  ROUND(AVG(T1.PRFBL_AST_VS_TOT_AST_RTO), 2) AS PRFBL_AST_VS_TOT_AST_RTO
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '088' AS KPI_TYP_CD,
                  '4'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PRFBL_AST_VS_TOT_AST_RTO, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, PRFBL_AST_VS_TOT_AST_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY PRFBL_AST_VS_TOT_AST_RTO DESC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  SUM(CASE WHEN T1.TOT_AST_AMT = 0 THEN 0
                                           ELSE T1.PRFBL_AST_AMT / T1.TOT_AST_AMT * 100 * T4.ASSET_WEIGHTED_AVG
                                           END) AS PRFBL_AST_VS_TOT_AST_RTO
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                                                                  INNER JOIN TM00_LOC_D T3
                                                                          ON T2.LOC_ID = T3.LOC_ID
                                                                  INNER JOIN ASSET_WEIGHTED_AVG_PCF_PRVN T4
                                                                          ON T4.BAS_YM = T1.BAS_YM
                                                                         AND T4.PCF_ID = T1.PCF_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   )
           WHERE RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '088' AS KPI_TYP_CD,
                  '4'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PRFBL_AST_VS_TOT_AST_RTO, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, PRFBL_AST_VS_TOT_AST_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY PRFBL_AST_VS_TOT_AST_RTO ASC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  SUM(CASE WHEN T1.TOT_AST_AMT = 0 THEN 0
                                           ELSE T1.PRFBL_AST_AMT / T1.TOT_AST_AMT * 100 * T4.ASSET_WEIGHTED_AVG
                                           END) AS PRFBL_AST_VS_TOT_AST_RTO
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                                                                  INNER JOIN TM00_LOC_D T3
                                                                          ON T2.LOC_ID = T3.LOC_ID
                                                                  INNER JOIN ASSET_WEIGHTED_AVG_PCF_PRVN T4
                                                                          ON T4.BAS_YM = T1.BAS_YM
                                                                         AND T4.PCF_ID = T1.PCF_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   )
           WHERE RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '088' AS KPI_TYP_CD,
                  '4'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  CBV_BR_CD,
                  ROUND(PRFBL_AST_VS_TOT_AST_RTO, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PRFBL_AST_VS_TOT_AST_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY PRFBL_AST_VS_TOT_AST_RTO DESC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(CASE WHEN T1.TOT_AST_AMT = 0 THEN 0
                                           ELSE T1.PRFBL_AST_AMT / T1.TOT_AST_AMT * 100 * ASSET_WEIGHTED_AVG
                                           END) AS PRFBL_AST_VS_TOT_AST_RTO
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T2.PCF_ID = T1.PCF_ID
                                                                  INNER JOIN ASSET_WEIGHTED_AVG_PCF_BR  T3
                                                                          ON T3.BAS_YM = T1.BAS_YM
                                                                         AND T3.PCF_ID = T1.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   )
           WHERE RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '088' AS KPI_TYP_CD,
                  '4'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  CBV_BR_CD,
                  ROUND(PRFBL_AST_VS_TOT_AST_RTO, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PRFBL_AST_VS_TOT_AST_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY PRFBL_AST_VS_TOT_AST_RTO ASC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(CASE WHEN T1.TOT_AST_AMT = 0 THEN 0
                                           ELSE T1.PRFBL_AST_AMT / T1.TOT_AST_AMT * 100 * ASSET_WEIGHTED_AVG
                                           END) AS PRFBL_AST_VS_TOT_AST_RTO
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T2.PCF_ID = T1.PCF_ID
                                                                  INNER JOIN ASSET_WEIGHTED_AVG_PCF_BR  T3
                                                                          ON T3.BAS_YM = T1.BAS_YM
                                                                         AND T3.PCF_ID = T1.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   )
           WHERE RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '088' AS KPI_TYP_CD,
                  '5'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PRFBL_AST_VS_TOT_AST_RTO, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, PRFBL_AST_VS_TOT_AST_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY PRFBL_AST_VS_TOT_AST_RTO DESC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(PRFBL_AST_VS_TOT_AST_RTO) AS PRFBL_AST_VS_TOT_AST_RTO
                         FROM
                               (SELECT BAS_YM, BAS_YM1, PRVN_CD, PRFBL_AST_VS_TOT_AST_RTO, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY PRFBL_AST_VS_TOT_AST_RTO DESC NULLS LAST, PRVN_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.PRVN_CD,
                                               YY.PRFBL_AST_VS_TOT_AST_RTO
                                        FROM   (SELECT T1.BAS_YM, T3.PRVN_CD
                                                FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                                               ON T1.PCF_ID = T2.PCF_ID
                                                                                       INNER JOIN TM00_LOC_D T3
                                                                                               ON T2.LOC_ID = T3.LOC_ID
                                                GROUP BY T1.BAS_YM, T3.PRVN_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       C.PRVN_CD,
                                                                       ROUND(AVG(A.PRFBL_AST_VS_TOT_AST_RTO), 2) AS PRFBL_AST_VS_TOT_AST_RTO
                                                                FROM   TM27_MMLY_PCF_KEY_IDC_A A INNER JOIN TM00_PCF_D B
                                                                                                         ON A.PCF_ID = B.PCF_ID
                                                                                                 INNER JOIN TM00_LOC_D C
                                                                                                         ON B.LOC_ID = C.LOC_ID
                                                                GROUP BY A.BAS_YM, C.PRVN_CD
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PRVN_CD  = XX.PRVN_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '088' AS KPI_TYP_CD,
                  '5'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PRFBL_AST_VS_TOT_AST_RTO, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, PRFBL_AST_VS_TOT_AST_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY PRFBL_AST_VS_TOT_AST_RTO ASC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(PRFBL_AST_VS_TOT_AST_RTO) AS PRFBL_AST_VS_TOT_AST_RTO
                         FROM
                               (SELECT BAS_YM, BAS_YM1, PRVN_CD, PRFBL_AST_VS_TOT_AST_RTO, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY PRFBL_AST_VS_TOT_AST_RTO ASC NULLS LAST, PRVN_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.PRVN_CD,
                                               YY.PRFBL_AST_VS_TOT_AST_RTO
                                        FROM   (SELECT T1.BAS_YM, T3.PRVN_CD
                                                FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                                               ON T1.PCF_ID = T2.PCF_ID
                                                                                       INNER JOIN TM00_LOC_D T3
                                                                                               ON T2.LOC_ID = T3.LOC_ID
                                                GROUP BY T1.BAS_YM, T3.PRVN_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       C.PRVN_CD,
                                                                       ROUND(AVG(A.PRFBL_AST_VS_TOT_AST_RTO), 2) AS PRFBL_AST_VS_TOT_AST_RTO
                                                                FROM   TM27_MMLY_PCF_KEY_IDC_A A INNER JOIN TM00_PCF_D B
                                                                                                         ON A.PCF_ID = B.PCF_ID
                                                                                                 INNER JOIN TM00_LOC_D C
                                                                                                         ON B.LOC_ID = C.LOC_ID
                                                                GROUP BY A.BAS_YM, C.PRVN_CD
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PRVN_CD  = XX.PRVN_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '088' AS KPI_TYP_CD,
                  '5'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PRFBL_AST_VS_TOT_AST_RTO, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, PRFBL_AST_VS_TOT_AST_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY PRFBL_AST_VS_TOT_AST_RTO DESC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(PRFBL_AST_VS_TOT_AST_RTO) AS PRFBL_AST_VS_TOT_AST_RTO
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, PRFBL_AST_VS_TOT_AST_RTO, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY PRFBL_AST_VS_TOT_AST_RTO DESC NULLS LAST, CBV_BR_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               YY.PRFBL_AST_VS_TOT_AST_RTO
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD
                                                FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                                               ON T1.PCF_ID = T2.PCF_ID
                                                GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       B.CBV_BR_CD,
                                                                       ROUND(AVG(A.PRFBL_AST_VS_TOT_AST_RTO), 2) AS PRFBL_AST_VS_TOT_AST_RTO
                                                                FROM   TM27_MMLY_PCF_KEY_IDC_A A INNER JOIN TM00_PCF_D B
                                                                                                         ON A.PCF_ID = B.PCF_ID
                                                                GROUP BY A.BAS_YM, B.CBV_BR_CD
                                                               ) YY
                                                            ON YY.BAS_YM   >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM   <= XX.BAS_YM
                                                           AND YY.CBV_BR_CD = XX.CBV_BR_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '088' AS KPI_TYP_CD,
                  '5'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PRFBL_AST_VS_TOT_AST_RTO, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, PRFBL_AST_VS_TOT_AST_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY PRFBL_AST_VS_TOT_AST_RTO ASC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(PRFBL_AST_VS_TOT_AST_RTO) AS PRFBL_AST_VS_TOT_AST_RTO
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, PRFBL_AST_VS_TOT_AST_RTO, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY PRFBL_AST_VS_TOT_AST_RTO ASC NULLS LAST, CBV_BR_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               YY.PRFBL_AST_VS_TOT_AST_RTO
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD
                                                FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                                               ON T1.PCF_ID = T2.PCF_ID
                                                GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       B.CBV_BR_CD,
                                                                       ROUND(AVG(A.PRFBL_AST_VS_TOT_AST_RTO), 2) AS PRFBL_AST_VS_TOT_AST_RTO
                                                                FROM   TM27_MMLY_PCF_KEY_IDC_A A INNER JOIN TM00_PCF_D B
                                                                                                         ON A.PCF_ID = B.PCF_ID
                                                                GROUP BY A.BAS_YM, B.CBV_BR_CD
                                                               ) YY
                                                            ON YY.BAS_YM   >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM   <= XX.BAS_YM
                                                           AND YY.CBV_BR_CD = XX.CBV_BR_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '088' AS KPI_TYP_CD,
                  '7'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PRFBL_AST_VS_TOT_AST_RTO, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, PRFBL_AST_VS_TOT_AST_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY PRFBL_AST_VS_TOT_AST_RTO DESC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(PRFBL_AST_VS_TOT_AST_RTO) AS PRFBL_AST_VS_TOT_AST_RTO
                         FROM
                               (SELECT BAS_YM, BAS_YM1, PRVN_CD, PRFBL_AST_VS_TOT_AST_RTO, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY PRFBL_AST_VS_TOT_AST_RTO DESC NULLS LAST, PRVN_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.PRVN_CD,
                                               YY.PRFBL_AST_VS_TOT_AST_RTO
                                        FROM   (SELECT T1.BAS_YM, T3.PRVN_CD
                                                FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                                               ON T1.PCF_ID = T2.PCF_ID
                                                                                       INNER JOIN TM00_LOC_D T3
                                                                                               ON T2.LOC_ID = T3.LOC_ID
                                                GROUP BY T1.BAS_YM, T3.PRVN_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       C.PRVN_CD,
                                                                       SUM(CASE WHEN A.TOT_AST_AMT = 0 THEN 0
                                                                                ELSE A.PRFBL_AST_AMT / A.TOT_AST_AMT * 100 * D.ASSET_WEIGHTED_AVG
                                                                                END)  AS PRFBL_AST_VS_TOT_AST_RTO
                                                                FROM   TM27_MMLY_PCF_KEY_IDC_A A INNER JOIN TM00_PCF_D B
                                                                                                         ON A.PCF_ID = B.PCF_ID
                                                                                                 INNER JOIN TM00_LOC_D C
                                                                                                         ON B.LOC_ID = C.LOC_ID
                                                                                                 INNER JOIN ASSET_WEIGHTED_AVG_PCF_PRVN D
                                                                                                         ON D.BAS_YM = A.BAS_YM
                                                                                                        AND D.PCF_ID = A.PCF_ID
                                                                GROUP BY A.BAS_YM, C.PRVN_CD
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PRVN_CD  = XX.PRVN_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '088' AS KPI_TYP_CD,
                  '7'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PRFBL_AST_VS_TOT_AST_RTO, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, PRFBL_AST_VS_TOT_AST_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY PRFBL_AST_VS_TOT_AST_RTO ASC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(PRFBL_AST_VS_TOT_AST_RTO) AS PRFBL_AST_VS_TOT_AST_RTO
                         FROM
                               (SELECT BAS_YM, BAS_YM1, PRVN_CD, PRFBL_AST_VS_TOT_AST_RTO, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY PRFBL_AST_VS_TOT_AST_RTO ASC NULLS LAST, PRVN_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.PRVN_CD,
                                               YY.PRFBL_AST_VS_TOT_AST_RTO
                                        FROM   (SELECT T1.BAS_YM, T3.PRVN_CD
                                                FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                                               ON T1.PCF_ID = T2.PCF_ID
                                                                                       INNER JOIN TM00_LOC_D T3
                                                                                               ON T2.LOC_ID = T3.LOC_ID
                                                GROUP BY T1.BAS_YM, T3.PRVN_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       C.PRVN_CD,
                                                                       SUM(CASE WHEN A.TOT_AST_AMT = 0 THEN 0
                                                                                ELSE A.PRFBL_AST_AMT / A.TOT_AST_AMT * 100 * D.ASSET_WEIGHTED_AVG
                                                                                END)  AS PRFBL_AST_VS_TOT_AST_RTO
                                                                FROM   TM27_MMLY_PCF_KEY_IDC_A A INNER JOIN TM00_PCF_D B
                                                                                                         ON A.PCF_ID = B.PCF_ID
                                                                                                 INNER JOIN TM00_LOC_D C
                                                                                                         ON B.LOC_ID = C.LOC_ID
                                                                                                 INNER JOIN ASSET_WEIGHTED_AVG_PCF_PRVN D
                                                                                                         ON D.BAS_YM = A.BAS_YM
                                                                                                        AND D.PCF_ID = A.PCF_ID
                                                                GROUP BY A.BAS_YM, C.PRVN_CD
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PRVN_CD  = XX.PRVN_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '088' AS KPI_TYP_CD,
                  '7'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PRFBL_AST_VS_TOT_AST_RTO, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, PRFBL_AST_VS_TOT_AST_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY PRFBL_AST_VS_TOT_AST_RTO DESC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(PRFBL_AST_VS_TOT_AST_RTO) AS PRFBL_AST_VS_TOT_AST_RTO
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, PRFBL_AST_VS_TOT_AST_RTO, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY PRFBL_AST_VS_TOT_AST_RTO DESC NULLS LAST, CBV_BR_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               YY.PRFBL_AST_VS_TOT_AST_RTO
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD
                                                FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                                               ON T1.PCF_ID = T2.PCF_ID
                                                GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       B.CBV_BR_CD,
                                                                       SUM(CASE WHEN A.TOT_AST_AMT = 0 THEN 0
                                                                                ELSE A.PRFBL_AST_AMT / A.TOT_AST_AMT * 100 * C.ASSET_WEIGHTED_AVG
                                                                                END) AS PRFBL_AST_VS_TOT_AST_RTO
                                                                FROM   TM27_MMLY_PCF_KEY_IDC_A A INNER JOIN TM00_PCF_D B
                                                                                                         ON A.PCF_ID = B.PCF_ID
                                                                                                 INNER JOIN ASSET_WEIGHTED_AVG_PCF_BR C
                                                                                                         ON C.BAS_YM = A.BAS_YM
                                                                                                        AND C.PCF_ID = A.PCF_ID
                                                                GROUP BY A.BAS_YM, B.CBV_BR_CD
                                                               ) YY
                                                            ON YY.BAS_YM   >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM   <= XX.BAS_YM
                                                           AND YY.CBV_BR_CD = XX.CBV_BR_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '088' AS KPI_TYP_CD,
                  '7'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PRFBL_AST_VS_TOT_AST_RTO, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, PRFBL_AST_VS_TOT_AST_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY PRFBL_AST_VS_TOT_AST_RTO ASC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(PRFBL_AST_VS_TOT_AST_RTO) AS PRFBL_AST_VS_TOT_AST_RTO
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, PRFBL_AST_VS_TOT_AST_RTO, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY PRFBL_AST_VS_TOT_AST_RTO ASC NULLS LAST, CBV_BR_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               YY.PRFBL_AST_VS_TOT_AST_RTO
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD
                                                FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                                               ON T1.PCF_ID = T2.PCF_ID
                                                GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       B.CBV_BR_CD,
                                                                       SUM(CASE WHEN A.TOT_AST_AMT = 0 THEN 0
                                                                                ELSE A.PRFBL_AST_AMT / A.TOT_AST_AMT * 100 * C.ASSET_WEIGHTED_AVG
                                                                                END) AS PRFBL_AST_VS_TOT_AST_RTO
                                                                FROM   TM27_MMLY_PCF_KEY_IDC_A A INNER JOIN TM00_PCF_D B
                                                                                                         ON A.PCF_ID = B.PCF_ID
                                                                                                 INNER JOIN ASSET_WEIGHTED_AVG_PCF_BR C
                                                                                                         ON C.BAS_YM = A.BAS_YM
                                                                                                        AND C.PCF_ID = A.PCF_ID
                                                                GROUP BY A.BAS_YM, B.CBV_BR_CD
                                                               ) YY
                                                            ON YY.BAS_YM   >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM   <= XX.BAS_YM
                                                           AND YY.CBV_BR_CD = XX.CBV_BR_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '088' AS KPI_TYP_CD,
                  '8'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PRFBL_AST_VS_TOT_AST_RTO, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, PRFBL_AST_VS_TOT_AST_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY PRFBL_AST_VS_TOT_AST_RTO DESC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(PRFBL_AST_VS_TOT_AST_RTO) AS PRFBL_AST_VS_TOT_AST_RTO
                         FROM
                               (SELECT BAS_YM, BAS_YM1, PRVN_CD, PRFBL_AST_VS_TOT_AST_RTO, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY PRFBL_AST_VS_TOT_AST_RTO DESC NULLS LAST, PRVN_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.PRVN_CD,
                                               YY.PRFBL_AST_VS_TOT_AST_RTO
                                        FROM   (SELECT T1.BAS_YM, T3.PRVN_CD
                                                FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                                               ON T1.PCF_ID = T2.PCF_ID
                                                                                       INNER JOIN TM00_LOC_D T3
                                                                                               ON T2.LOC_ID = T3.LOC_ID
                                                GROUP BY T1.BAS_YM, T3.PRVN_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       C.PRVN_CD,
                                                                       ROUND(SUM(A.PRFBL_AST_AMT) / SUM(A.TOT_AST_AMT) * 100, 2) AS PRFBL_AST_VS_TOT_AST_RTO
                                                                FROM   TM27_MMLY_PCF_KEY_IDC_A A INNER JOIN TM00_PCF_D B
                                                                                                         ON A.PCF_ID = B.PCF_ID
                                                                                                 INNER JOIN TM00_LOC_D C
                                                                                                         ON B.LOC_ID = C.LOC_ID
                                                                GROUP BY A.BAS_YM, C.PRVN_CD
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PRVN_CD  = XX.PRVN_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '088' AS KPI_TYP_CD,
                  '8'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PRFBL_AST_VS_TOT_AST_RTO, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, PRFBL_AST_VS_TOT_AST_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY PRFBL_AST_VS_TOT_AST_RTO ASC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(PRFBL_AST_VS_TOT_AST_RTO) AS PRFBL_AST_VS_TOT_AST_RTO
                         FROM
                               (SELECT BAS_YM, BAS_YM1, PRVN_CD, PRFBL_AST_VS_TOT_AST_RTO, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY PRFBL_AST_VS_TOT_AST_RTO ASC NULLS LAST, PRVN_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.PRVN_CD,
                                               YY.PRFBL_AST_VS_TOT_AST_RTO
                                        FROM   (SELECT T1.BAS_YM, T3.PRVN_CD
                                                FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                                               ON T1.PCF_ID = T2.PCF_ID
                                                                                       INNER JOIN TM00_LOC_D T3
                                                                                               ON T2.LOC_ID = T3.LOC_ID
                                                GROUP BY T1.BAS_YM, T3.PRVN_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       C.PRVN_CD,
                                                                       ROUND(SUM(A.PRFBL_AST_AMT) / SUM(A.TOT_AST_AMT) * 100, 2) AS PRFBL_AST_VS_TOT_AST_RTO
                                                                FROM   TM27_MMLY_PCF_KEY_IDC_A A INNER JOIN TM00_PCF_D B
                                                                                                         ON A.PCF_ID = B.PCF_ID
                                                                                                 INNER JOIN TM00_LOC_D C
                                                                                                         ON B.LOC_ID = C.LOC_ID
                                                                GROUP BY A.BAS_YM, C.PRVN_CD
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PRVN_CD  = XX.PRVN_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '088' AS KPI_TYP_CD,
                  '8'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PRFBL_AST_VS_TOT_AST_RTO, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, PRFBL_AST_VS_TOT_AST_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY PRFBL_AST_VS_TOT_AST_RTO DESC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(PRFBL_AST_VS_TOT_AST_RTO) AS PRFBL_AST_VS_TOT_AST_RTO
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, PRFBL_AST_VS_TOT_AST_RTO, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY PRFBL_AST_VS_TOT_AST_RTO DESC NULLS LAST, CBV_BR_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               YY.PRFBL_AST_VS_TOT_AST_RTO
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD
                                                FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                                               ON T1.PCF_ID = T2.PCF_ID
                                                GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       B.CBV_BR_CD,
                                                                       ROUND(SUM(A.PRFBL_AST_AMT) / SUM(A.TOT_AST_AMT) * 100, 2) AS PRFBL_AST_VS_TOT_AST_RTO
                                                                FROM   TM27_MMLY_PCF_KEY_IDC_A A INNER JOIN TM00_PCF_D B
                                                                                                         ON A.PCF_ID = B.PCF_ID
                                                                GROUP BY A.BAS_YM, B.CBV_BR_CD
                                                               ) YY
                                                            ON YY.BAS_YM   >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM   <= XX.BAS_YM
                                                           AND YY.CBV_BR_CD = XX.CBV_BR_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '088' AS KPI_TYP_CD,
                  '8'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PRFBL_AST_VS_TOT_AST_RTO, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, PRFBL_AST_VS_TOT_AST_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY PRFBL_AST_VS_TOT_AST_RTO ASC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(PRFBL_AST_VS_TOT_AST_RTO) AS PRFBL_AST_VS_TOT_AST_RTO
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, PRFBL_AST_VS_TOT_AST_RTO, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY PRFBL_AST_VS_TOT_AST_RTO ASC NULLS LAST, CBV_BR_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               YY.PRFBL_AST_VS_TOT_AST_RTO
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD
                                                FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                                               ON T1.PCF_ID = T2.PCF_ID
                                                GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       B.CBV_BR_CD,
                                                                       ROUND(SUM(A.PRFBL_AST_AMT) / SUM(A.TOT_AST_AMT) * 100, 2) AS PRFBL_AST_VS_TOT_AST_RTO
                                                                FROM   TM27_MMLY_PCF_KEY_IDC_A A INNER JOIN TM00_PCF_D B
                                                                                                         ON A.PCF_ID = B.PCF_ID
                                                                GROUP BY A.BAS_YM, B.CBV_BR_CD
                                                               ) YY
                                                            ON YY.BAS_YM   >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM   <= XX.BAS_YM
                                                           AND YY.CBV_BR_CD = XX.CBV_BR_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )
          ),
          FROM_KPI_TYP_090 AS
          (SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '090' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  XX.NET_PROFIT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, NET_PROFIT, RANK() OVER(PARTITION BY BAS_YM ORDER BY NET_PROFIT DESC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  SUM(T1.TOT_INCM_AMT - T1.TOT_EXP_AMT) AS NET_PROFIT
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN TM00_LOC_D T3
                                                                       ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '090' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  XX.NET_PROFIT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, NET_PROFIT, RANK() OVER(PARTITION BY BAS_YM ORDER BY NET_PROFIT ASC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  SUM(T1.TOT_INCM_AMT - T1.TOT_EXP_AMT) AS NET_PROFIT
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN TM00_LOC_D T3
                                                                       ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '090' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.NET_PROFIT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, NET_PROFIT, RANK() OVER(PARTITION BY BAS_YM ORDER BY NET_PROFIT DESC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(T1.TOT_INCM_AMT - T1.TOT_EXP_AMT) AS NET_PROFIT
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '090' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.NET_PROFIT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, NET_PROFIT, RANK() OVER(PARTITION BY BAS_YM ORDER BY NET_PROFIT ASC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(T1.TOT_INCM_AMT - T1.TOT_EXP_AMT) AS NET_PROFIT
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '090' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  XX.NET_PROFIT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, NET_PROFIT, RANK() OVER(PARTITION BY BAS_YM ORDER BY NET_PROFIT DESC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  ROUND(AVG(T1.TOT_INCM_AMT - T1.TOT_EXP_AMT), 0) AS NET_PROFIT
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN TM00_LOC_D T3
                                                                       ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '090' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  XX.NET_PROFIT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, NET_PROFIT, RANK() OVER(PARTITION BY BAS_YM ORDER BY NET_PROFIT ASC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  ROUND(AVG(T1.TOT_INCM_AMT - T1.TOT_EXP_AMT), 0) AS NET_PROFIT
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN TM00_LOC_D T3
                                                                       ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '090' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.NET_PROFIT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, NET_PROFIT, RANK() OVER(PARTITION BY BAS_YM ORDER BY NET_PROFIT DESC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  ROUND(AVG(T1.TOT_INCM_AMT - T1.TOT_EXP_AMT), 0) AS NET_PROFIT
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '090' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.NET_PROFIT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, NET_PROFIT, RANK() OVER(PARTITION BY BAS_YM ORDER BY NET_PROFIT ASC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  ROUND(AVG(T1.TOT_INCM_AMT - T1.TOT_EXP_AMT), 0) AS NET_PROFIT
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '090' AS KPI_TYP_CD,
                  '4'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(NET_PROFIT, 0) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, NET_PROFIT, RANK() OVER(PARTITION BY BAS_YM ORDER BY NET_PROFIT DESC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  SUM((T1.TOT_INCM_AMT - T1.TOT_EXP_AMT) * ASSET_WEIGHTED_AVG) AS NET_PROFIT
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN TM00_LOC_D T3
                                                                       ON T2.LOC_ID = T3.LOC_ID
                                                               INNER JOIN ASSET_WEIGHTED_AVG_PCF_PRVN T4
                                                                       ON T4.BAS_YM = T1.BAS_YM
                                                                      AND T4.PCF_ID = T1.PCF_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   )
           WHERE RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '090' AS KPI_TYP_CD,
                  '4'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(NET_PROFIT, 0) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, NET_PROFIT, RANK() OVER(PARTITION BY BAS_YM ORDER BY NET_PROFIT ASC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  SUM((T1.TOT_INCM_AMT - T1.TOT_EXP_AMT) * ASSET_WEIGHTED_AVG) AS NET_PROFIT
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN TM00_LOC_D T3
                                                                       ON T2.LOC_ID = T3.LOC_ID
                                                               INNER JOIN ASSET_WEIGHTED_AVG_PCF_PRVN T4
                                                                       ON T4.BAS_YM = T1.BAS_YM
                                                                      AND T4.PCF_ID = T1.PCF_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   )
           WHERE RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '090' AS KPI_TYP_CD,
                  '4'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  CBV_BR_CD,
                  ROUND(NET_PROFIT, 0) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, NET_PROFIT, RANK() OVER(PARTITION BY BAS_YM ORDER BY NET_PROFIT DESC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM((T1.TOT_INCM_AMT - T1.TOT_EXP_AMT) * ASSET_WEIGHTED_AVG) AS NET_PROFIT
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN ASSET_WEIGHTED_AVG_PCF_BR  T3
                                                                       ON T3.BAS_YM = T1.BAS_YM
                                                                      AND T3.PCF_ID = T1.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   )
           WHERE RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '090' AS KPI_TYP_CD,
                  '4'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  CBV_BR_CD,
                  ROUND(NET_PROFIT, 0) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, NET_PROFIT, RANK() OVER(PARTITION BY BAS_YM ORDER BY NET_PROFIT ASC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM((T1.TOT_INCM_AMT - T1.TOT_EXP_AMT) * ASSET_WEIGHTED_AVG) AS NET_PROFIT
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN ASSET_WEIGHTED_AVG_PCF_BR  T3
                                                                       ON T3.BAS_YM = T1.BAS_YM
                                                                      AND T3.PCF_ID = T1.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   )
           WHERE RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '090' AS KPI_TYP_CD,
                  '5'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(NET_PROFIT, 0) AS AGT_VAL
           FROM  (SELECT BAS_YM, NET_PROFIT, RANK() OVER(PARTITION BY BAS_YM ORDER BY NET_PROFIT DESC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(NET_PROFIT) AS NET_PROFIT
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, NET_PROFIT, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY NET_PROFIT DESC NULLS LAST, CBV_BR_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               YY.NET_PROFIT
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                                GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       B.CBV_BR_CD,
                                                                       ROUND(AVG(A.TOT_INCM_AMT - A.TOT_EXP_AMT),0) AS NET_PROFIT
                                                                FROM   TM23_MMLY_INCM_EXP_A A INNER JOIN TM00_PCF_D B
                                                                                                      ON A.PCF_ID = B.PCF_ID
                                                                GROUP BY A.BAS_YM, B.CBV_BR_CD
                                                               ) YY
                                                            ON YY.BAS_YM   >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM   <= XX.BAS_YM
                                                           AND YY.CBV_BR_CD = XX.CBV_BR_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '090' AS KPI_TYP_CD,
                  '5'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(NET_PROFIT, 0) AS AGT_VAL
           FROM  (SELECT BAS_YM, NET_PROFIT, RANK() OVER(PARTITION BY BAS_YM ORDER BY NET_PROFIT ASC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(NET_PROFIT) AS NET_PROFIT
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, NET_PROFIT, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY NET_PROFIT ASC NULLS LAST, CBV_BR_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               YY.NET_PROFIT
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                                GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       B.CBV_BR_CD,
                                                                       ROUND(AVG(A.TOT_INCM_AMT - A.TOT_EXP_AMT),0) AS NET_PROFIT
                                                                FROM   TM23_MMLY_INCM_EXP_A A INNER JOIN TM00_PCF_D B
                                                                                                      ON A.PCF_ID = B.PCF_ID
                                                                GROUP BY A.BAS_YM, B.CBV_BR_CD
                                                               ) YY
                                                            ON YY.BAS_YM   >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM   <= XX.BAS_YM
                                                           AND YY.CBV_BR_CD = XX.CBV_BR_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '090' AS KPI_TYP_CD,
                  '7'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(NET_PROFIT, 0) AS AGT_VAL
           FROM  (SELECT BAS_YM, NET_PROFIT, RANK() OVER(PARTITION BY BAS_YM ORDER BY NET_PROFIT DESC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(NET_PROFIT) AS NET_PROFIT
                         FROM
                               (SELECT BAS_YM, BAS_YM1, PRVN_CD, NET_PROFIT, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY NET_PROFIT DESC NULLS LAST, PRVN_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.PRVN_CD,
                                               YY.NET_PROFIT
                                        FROM   (SELECT T1.BAS_YM, T3.PRVN_CD
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                                                                    INNER JOIN TM00_LOC_D T3
                                                                                            ON T2.LOC_ID = T3.LOC_ID
                                                GROUP BY T1.BAS_YM, T3.PRVN_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       C.PRVN_CD,
                                                                       SUM((A.TOT_INCM_AMT - A.TOT_EXP_AMT) * D.ASSET_WEIGHTED_AVG) AS NET_PROFIT
                                                                FROM   TM23_MMLY_INCM_EXP_A A INNER JOIN TM00_PCF_D B
                                                                                                      ON A.PCF_ID = B.PCF_ID
                                                                                              INNER JOIN TM00_LOC_D C
                                                                                                      ON B.LOC_ID = C.LOC_ID
                                                                                              INNER JOIN ASSET_WEIGHTED_AVG_PCF_PRVN D
                                                                                                      ON D.BAS_YM = A.BAS_YM
                                                                                                     AND D.PCF_ID = A.PCF_ID
                                                                GROUP BY A.BAS_YM, C.PRVN_CD
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PRVN_CD  = XX.PRVN_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '090' AS KPI_TYP_CD,
                  '7'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(NET_PROFIT, 0) AS AGT_VAL
           FROM  (SELECT BAS_YM, NET_PROFIT, RANK() OVER(PARTITION BY BAS_YM ORDER BY NET_PROFIT ASC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(NET_PROFIT) AS NET_PROFIT
                         FROM
                               (SELECT BAS_YM, BAS_YM1, PRVN_CD, NET_PROFIT, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY NET_PROFIT ASC NULLS LAST, PRVN_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.PRVN_CD,
                                               YY.NET_PROFIT
                                        FROM   (SELECT T1.BAS_YM, T3.PRVN_CD
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                                                                    INNER JOIN TM00_LOC_D T3
                                                                                            ON T2.LOC_ID = T3.LOC_ID
                                                GROUP BY T1.BAS_YM, T3.PRVN_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       C.PRVN_CD,
                                                                       SUM((A.TOT_INCM_AMT - A.TOT_EXP_AMT) * D.ASSET_WEIGHTED_AVG) AS NET_PROFIT
                                                                FROM   TM23_MMLY_INCM_EXP_A A INNER JOIN TM00_PCF_D B
                                                                                                      ON A.PCF_ID = B.PCF_ID
                                                                                              INNER JOIN TM00_LOC_D C
                                                                                                      ON B.LOC_ID = C.LOC_ID
                                                                                              INNER JOIN ASSET_WEIGHTED_AVG_PCF_PRVN D
                                                                                                      ON D.BAS_YM = A.BAS_YM
                                                                                                     AND D.PCF_ID = A.PCF_ID
                                                                GROUP BY A.BAS_YM, C.PRVN_CD
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PRVN_CD  = XX.PRVN_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '090' AS KPI_TYP_CD,
                  '7'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(NET_PROFIT, 0) AS AGT_VAL
           FROM  (SELECT BAS_YM, NET_PROFIT, RANK() OVER(PARTITION BY BAS_YM ORDER BY NET_PROFIT DESC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(NET_PROFIT) AS NET_PROFIT
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, NET_PROFIT, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY NET_PROFIT DESC NULLS LAST, CBV_BR_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               YY.NET_PROFIT
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                                GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       B.CBV_BR_CD,
                                                                       SUM((A.TOT_INCM_AMT - A.TOT_EXP_AMT) * C.ASSET_WEIGHTED_AVG) AS NET_PROFIT
                                                                FROM   TM23_MMLY_INCM_EXP_A A INNER JOIN TM00_PCF_D B
                                                                                                      ON A.PCF_ID = B.PCF_ID
                                                                                              INNER JOIN ASSET_WEIGHTED_AVG_PCF_BR C
                                                                                                      ON C.BAS_YM = A.BAS_YM
                                                                                                     AND C.PCF_ID = A.PCF_ID
                                                                GROUP BY A.BAS_YM, B.CBV_BR_CD
                                                               ) YY
                                                            ON YY.BAS_YM   >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM   <= XX.BAS_YM
                                                           AND YY.CBV_BR_CD = XX.CBV_BR_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '090' AS KPI_TYP_CD,
                  '7'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(NET_PROFIT, 0) AS AGT_VAL
           FROM  (SELECT BAS_YM, NET_PROFIT, RANK() OVER(PARTITION BY BAS_YM ORDER BY NET_PROFIT ASC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(NET_PROFIT) AS NET_PROFIT
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, NET_PROFIT, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY NET_PROFIT ASC NULLS LAST, CBV_BR_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               YY.NET_PROFIT
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                                GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       B.CBV_BR_CD,
                                                                       SUM((A.TOT_INCM_AMT - A.TOT_EXP_AMT) * C.ASSET_WEIGHTED_AVG) AS NET_PROFIT
                                                                FROM   TM23_MMLY_INCM_EXP_A A INNER JOIN TM00_PCF_D B
                                                                                                      ON A.PCF_ID = B.PCF_ID
                                                                                              INNER JOIN ASSET_WEIGHTED_AVG_PCF_BR C
                                                                                                      ON C.BAS_YM = A.BAS_YM
                                                                                                     AND C.PCF_ID = A.PCF_ID
                                                                GROUP BY A.BAS_YM, B.CBV_BR_CD
                                                               ) YY
                                                            ON YY.BAS_YM   >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM   <= XX.BAS_YM
                                                           AND YY.CBV_BR_CD = XX.CBV_BR_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '090' AS KPI_TYP_CD,
                  '8'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(NET_PROFIT, 0) AS AGT_VAL
           FROM  (SELECT BAS_YM, NET_PROFIT, RANK() OVER(PARTITION BY BAS_YM ORDER BY NET_PROFIT DESC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(NET_PROFIT) AS NET_PROFIT
                         FROM
                               (SELECT BAS_YM, BAS_YM1, PRVN_CD, NET_PROFIT, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY NET_PROFIT DESC NULLS LAST, PRVN_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.PRVN_CD,
                                               YY.NET_PROFIT
                                        FROM   (SELECT T1.BAS_YM, T3.PRVN_CD
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                                                                    INNER JOIN TM00_LOC_D T3
                                                                                            ON T2.LOC_ID = T3.LOC_ID
                                                GROUP BY T1.BAS_YM, T3.PRVN_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       C.PRVN_CD,
                                                                       SUM(A.TOT_INCM_AMT) - SUM(A.TOT_EXP_AMT) AS NET_PROFIT
                                                                FROM   TM23_MMLY_INCM_EXP_A A INNER JOIN TM00_PCF_D B
                                                                                                      ON A.PCF_ID = B.PCF_ID
                                                                                              INNER JOIN TM00_LOC_D C
                                                                                                      ON B.LOC_ID = C.LOC_ID
                                                                GROUP BY A.BAS_YM, C.PRVN_CD
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PRVN_CD  = XX.PRVN_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '090' AS KPI_TYP_CD,
                  '8'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(NET_PROFIT, 0) AS AGT_VAL
           FROM  (SELECT BAS_YM, NET_PROFIT, RANK() OVER(PARTITION BY BAS_YM ORDER BY NET_PROFIT ASC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(NET_PROFIT) AS NET_PROFIT
                         FROM
                               (SELECT BAS_YM, BAS_YM1, PRVN_CD, NET_PROFIT, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY NET_PROFIT ASC NULLS LAST, PRVN_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.PRVN_CD,
                                               YY.NET_PROFIT
                                        FROM   (SELECT T1.BAS_YM, T3.PRVN_CD
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                                                                    INNER JOIN TM00_LOC_D T3
                                                                                            ON T2.LOC_ID = T3.LOC_ID
                                                GROUP BY T1.BAS_YM, T3.PRVN_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       C.PRVN_CD,
                                                                       SUM(A.TOT_INCM_AMT) - SUM(A.TOT_EXP_AMT) AS NET_PROFIT
                                                                FROM   TM23_MMLY_INCM_EXP_A A INNER JOIN TM00_PCF_D B
                                                                                                      ON A.PCF_ID = B.PCF_ID
                                                                                              INNER JOIN TM00_LOC_D C
                                                                                                      ON B.LOC_ID = C.LOC_ID
                                                                GROUP BY A.BAS_YM, C.PRVN_CD
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PRVN_CD  = XX.PRVN_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '090' AS KPI_TYP_CD,
                  '8'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(NET_PROFIT, 0) AS AGT_VAL
           FROM  (SELECT BAS_YM, NET_PROFIT, RANK() OVER(PARTITION BY BAS_YM ORDER BY NET_PROFIT DESC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(NET_PROFIT) AS NET_PROFIT
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, NET_PROFIT, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY NET_PROFIT DESC NULLS LAST, CBV_BR_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               YY.NET_PROFIT
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                                GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       B.CBV_BR_CD,
                                                                       SUM(A.TOT_INCM_AMT) - SUM(A.TOT_EXP_AMT) AS NET_PROFIT
                                                                FROM   TM23_MMLY_INCM_EXP_A A INNER JOIN TM00_PCF_D B
                                                                                                      ON A.PCF_ID = B.PCF_ID
                                                                GROUP BY A.BAS_YM, B.CBV_BR_CD
                                                               ) YY
                                                            ON YY.BAS_YM   >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM   <= XX.BAS_YM
                                                           AND YY.CBV_BR_CD = XX.CBV_BR_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '090' AS KPI_TYP_CD,
                  '8'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(NET_PROFIT, 0) AS AGT_VAL
           FROM  (SELECT BAS_YM, NET_PROFIT, RANK() OVER(PARTITION BY BAS_YM ORDER BY NET_PROFIT ASC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(NET_PROFIT) AS NET_PROFIT
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, NET_PROFIT, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY NET_PROFIT ASC NULLS LAST, CBV_BR_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               YY.NET_PROFIT
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                                GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       B.CBV_BR_CD,
                                                                       SUM(A.TOT_INCM_AMT) - SUM(A.TOT_EXP_AMT) AS NET_PROFIT
                                                                FROM   TM23_MMLY_INCM_EXP_A A INNER JOIN TM00_PCF_D B
                                                                                                      ON A.PCF_ID = B.PCF_ID
                                                                GROUP BY A.BAS_YM, B.CBV_BR_CD
                                                               ) YY
                                                            ON YY.BAS_YM   >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM   <= XX.BAS_YM
                                                           AND YY.CBV_BR_CD = XX.CBV_BR_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )
          ),
          FROM_KPI_TYP_091 AS
          (SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '091' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(XX.PROF_VS_TOT_INCM_RTO, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, PROF_VS_TOT_INCM_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_TOT_INCM_RTO DESC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  CASE WHEN SUM(T1.TOT_INCM_AMT - T1.TOT_EXP_AMT) = 0 THEN 0
                                       WHEN SUM(T1.TOT_INCM_AMT - T1.TOT_EXP_AMT) <> 0 AND SUM(T1.TOT_INCM_AMT) = 0 THEN NULL
                                       ELSE SUM(T1.TOT_INCM_AMT - T1.TOT_EXP_AMT) / SUM(T1.TOT_INCM_AMT) * 100
                                       END AS PROF_VS_TOT_INCM_RTO
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN TM00_LOC_D T3
                                                                       ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '091' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(XX.PROF_VS_TOT_INCM_RTO, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, PROF_VS_TOT_INCM_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_TOT_INCM_RTO ASC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  CASE WHEN SUM(T1.TOT_INCM_AMT - T1.TOT_EXP_AMT) = 0 THEN 0
                                       WHEN SUM(T1.TOT_INCM_AMT - T1.TOT_EXP_AMT) <> 0 AND SUM(T1.TOT_INCM_AMT) = 0 THEN NULL
                                       ELSE SUM(T1.TOT_INCM_AMT - T1.TOT_EXP_AMT) / SUM(T1.TOT_INCM_AMT) * 100
                                       END AS PROF_VS_TOT_INCM_RTO
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN TM00_LOC_D T3
                                                                       ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '091' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  ROUND(XX.PROF_VS_TOT_INCM_RTO, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PROF_VS_TOT_INCM_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_TOT_INCM_RTO DESC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  CASE WHEN SUM(T1.TOT_INCM_AMT - T1.TOT_EXP_AMT) = 0 THEN 0
                                       WHEN SUM(T1.TOT_INCM_AMT - T1.TOT_EXP_AMT) <> 0 AND SUM(T1.TOT_INCM_AMT) = 0 THEN NULL
                                       ELSE SUM(T1.TOT_INCM_AMT - T1.TOT_EXP_AMT) / SUM(T1.TOT_INCM_AMT) * 100
                                       END AS PROF_VS_TOT_INCM_RTO
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '091' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  ROUND(XX.PROF_VS_TOT_INCM_RTO, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PROF_VS_TOT_INCM_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_TOT_INCM_RTO ASC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  CASE WHEN SUM(T1.TOT_INCM_AMT - T1.TOT_EXP_AMT) = 0 THEN 0
                                       WHEN SUM(T1.TOT_INCM_AMT - T1.TOT_EXP_AMT) <> 0 AND SUM(T1.TOT_INCM_AMT) = 0 THEN NULL
                                       ELSE SUM(T1.TOT_INCM_AMT - T1.TOT_EXP_AMT) / SUM(T1.TOT_INCM_AMT) * 100
                                       END AS PROF_VS_TOT_INCM_RTO
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '091' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(XX.PROF_VS_TOT_INCM_RTO, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, PROF_VS_TOT_INCM_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_TOT_INCM_RTO DESC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  AVG(CASE WHEN T1.TOT_INCM_AMT - T1.TOT_EXP_AMT = 0 THEN 0
                                           WHEN T1.TOT_INCM_AMT - T1.TOT_EXP_AMT <> 0 AND T1.TOT_INCM_AMT = 0 THEN NULL
                                           ELSE CASE WHEN T1.TOT_INCM_AMT = 0 THEN 0
                                                     ELSE ( T1.TOT_INCM_AMT - T1.TOT_EXP_AMT ) / ( T1.TOT_INCM_AMT ) * 100
                                                     END
                                           END) AS PROF_VS_TOT_INCM_RTO
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN TM00_LOC_D T3
                                                                       ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '091' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(XX.PROF_VS_TOT_INCM_RTO, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, PROF_VS_TOT_INCM_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_TOT_INCM_RTO ASC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  AVG(CASE WHEN T1.TOT_INCM_AMT - T1.TOT_EXP_AMT = 0 THEN 0
                                           WHEN T1.TOT_INCM_AMT - T1.TOT_EXP_AMT <> 0 AND T1.TOT_INCM_AMT = 0 THEN NULL
                                           ELSE CASE WHEN T1.TOT_INCM_AMT = 0 THEN 0
                                                     ELSE ( T1.TOT_INCM_AMT - T1.TOT_EXP_AMT ) / ( T1.TOT_INCM_AMT ) * 100
                                                     END
                                           END) AS PROF_VS_TOT_INCM_RTO
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN TM00_LOC_D T3
                                                                       ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '091' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  ROUND(XX.PROF_VS_TOT_INCM_RTO, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PROF_VS_TOT_INCM_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_TOT_INCM_RTO DESC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  AVG(CASE WHEN T1.TOT_INCM_AMT - T1.TOT_EXP_AMT = 0 THEN 0
                                           WHEN T1.TOT_INCM_AMT - T1.TOT_EXP_AMT <> 0 AND T1.TOT_INCM_AMT = 0 THEN NULL
                                           ELSE CASE WHEN T1.TOT_INCM_AMT = 0 THEN 0
                                                     ELSE ( T1.TOT_INCM_AMT - T1.TOT_EXP_AMT ) / ( T1.TOT_INCM_AMT ) * 100
                                                     END
                                           END) AS PROF_VS_TOT_INCM_RTO
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '091' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  ROUND(XX.PROF_VS_TOT_INCM_RTO, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PROF_VS_TOT_INCM_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_TOT_INCM_RTO ASC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  AVG(CASE WHEN T1.TOT_INCM_AMT - T1.TOT_EXP_AMT = 0 THEN 0
                                           WHEN T1.TOT_INCM_AMT - T1.TOT_EXP_AMT <> 0 AND T1.TOT_INCM_AMT = 0 THEN NULL
                                           ELSE CASE WHEN T1.TOT_INCM_AMT = 0 THEN 0
                                                     ELSE ( T1.TOT_INCM_AMT - T1.TOT_EXP_AMT ) / ( T1.TOT_INCM_AMT ) * 100
                                                     END
                                           END) AS PROF_VS_TOT_INCM_RTO
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '091' AS KPI_TYP_CD,
                  '4'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PROF_VS_TOT_INCM_RTO, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, PROF_VS_TOT_INCM_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_TOT_INCM_RTO DESC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  SUM(CASE WHEN T1.TOT_INCM_AMT - T1.TOT_EXP_AMT = 0 THEN 0
                                           WHEN T1.TOT_INCM_AMT - T1.TOT_EXP_AMT <> 0 AND T1.TOT_INCM_AMT = 0 THEN NULL
                                           ELSE CASE WHEN T1.TOT_INCM_AMT = 0 THEN 0
                                                     ELSE ( T1.TOT_INCM_AMT - T1.TOT_EXP_AMT ) / ( T1.TOT_INCM_AMT ) * 100
                                                     END
                                           END * ASSET_WEIGHTED_AVG) AS PROF_VS_TOT_INCM_RTO
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN TM00_LOC_D T3
                                                                       ON T2.LOC_ID = T3.LOC_ID
                                                               INNER JOIN ASSET_WEIGHTED_AVG_PCF_PRVN T4
                                                                       ON T4.BAS_YM = T1.BAS_YM
                                                                      AND T4.PCF_ID = T1.PCF_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   )
           WHERE RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '091' AS KPI_TYP_CD,
                  '4'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PROF_VS_TOT_INCM_RTO, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, PROF_VS_TOT_INCM_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_TOT_INCM_RTO ASC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  SUM(CASE WHEN T1.TOT_INCM_AMT - T1.TOT_EXP_AMT = 0 THEN 0
                                           WHEN T1.TOT_INCM_AMT - T1.TOT_EXP_AMT <> 0 AND T1.TOT_INCM_AMT = 0 THEN NULL
                                           ELSE CASE WHEN T1.TOT_INCM_AMT = 0 THEN 0
                                                     ELSE ( T1.TOT_INCM_AMT - T1.TOT_EXP_AMT ) / ( T1.TOT_INCM_AMT ) * 100
                                                     END
                                           END * ASSET_WEIGHTED_AVG) AS PROF_VS_TOT_INCM_RTO
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN TM00_LOC_D T3
                                                                       ON T2.LOC_ID = T3.LOC_ID
                                                               INNER JOIN ASSET_WEIGHTED_AVG_PCF_PRVN T4
                                                                       ON T4.BAS_YM = T1.BAS_YM
                                                                      AND T4.PCF_ID = T1.PCF_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   )
           WHERE RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '091' AS KPI_TYP_CD,
                  '4'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  CBV_BR_CD,
                  ROUND(PROF_VS_TOT_INCM_RTO, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PROF_VS_TOT_INCM_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_TOT_INCM_RTO DESC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(CASE WHEN T1.TOT_INCM_AMT - T1.TOT_EXP_AMT = 0 THEN 0
                                           WHEN T1.TOT_INCM_AMT - T1.TOT_EXP_AMT <> 0 AND T1.TOT_INCM_AMT = 0 THEN NULL
                                           ELSE CASE WHEN T1.TOT_INCM_AMT = 0 THEN 0
                                                     ELSE ( T1.TOT_INCM_AMT - T1.TOT_EXP_AMT ) / ( T1.TOT_INCM_AMT ) * 100
                                                     END
                                           END * ASSET_WEIGHTED_AVG) AS PROF_VS_TOT_INCM_RTO
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN ASSET_WEIGHTED_AVG_PCF_BR T3
                                                                       ON T3.BAS_YM = T1.BAS_YM
                                                                      AND T3.PCF_ID = T1.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   )
           WHERE RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '091' AS KPI_TYP_CD,
                  '4'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  CBV_BR_CD,
                  ROUND(PROF_VS_TOT_INCM_RTO, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PROF_VS_TOT_INCM_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_TOT_INCM_RTO ASC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(CASE WHEN T1.TOT_INCM_AMT - T1.TOT_EXP_AMT = 0 THEN 0
                                           WHEN T1.TOT_INCM_AMT - T1.TOT_EXP_AMT <> 0 AND T1.TOT_INCM_AMT = 0 THEN NULL
                                           ELSE CASE WHEN T1.TOT_INCM_AMT = 0 THEN 0
                                                     ELSE ( T1.TOT_INCM_AMT - T1.TOT_EXP_AMT ) / ( T1.TOT_INCM_AMT ) * 100
                                                     END
                                           END * ASSET_WEIGHTED_AVG) AS PROF_VS_TOT_INCM_RTO
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN ASSET_WEIGHTED_AVG_PCF_BR T3
                                                                       ON T3.BAS_YM = T1.BAS_YM
                                                                      AND T3.PCF_ID = T1.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   )
           WHERE RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '091' AS KPI_TYP_CD,
                  '5'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PROF_VS_TOT_INCM_RTO, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, PROF_VS_TOT_INCM_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_TOT_INCM_RTO DESC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(PROF_VS_TOT_INCM_RTO) AS PROF_VS_TOT_INCM_RTO
                         FROM
                               (SELECT BAS_YM, BAS_YM1, PRVN_CD, PROF_VS_TOT_INCM_RTO, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY PROF_VS_TOT_INCM_RTO DESC NULLS LAST, PRVN_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.PRVN_CD,
                                               YY.PROF_VS_TOT_INCM_RTO
                                        FROM   (SELECT T1.BAS_YM, T3.PRVN_CD
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                                                                    INNER JOIN TM00_LOC_D T3
                                                                                            ON T2.LOC_ID = T3.LOC_ID
                                                GROUP BY T1.BAS_YM, T3.PRVN_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       C.PRVN_CD,
                                                                       AVG(CASE WHEN A.TOT_INCM_AMT - A.TOT_EXP_AMT = 0 THEN 0
                                                                                WHEN A.TOT_INCM_AMT - A.TOT_EXP_AMT <> 0 AND A.TOT_INCM_AMT = 0 THEN NULL
                                                                                ELSE CASE WHEN A.TOT_INCM_AMT = 0 THEN 0
                                                                                          ELSE ( A.TOT_INCM_AMT - A.TOT_EXP_AMT ) / ( A.TOT_INCM_AMT ) * 100
                                                                                          END
                                                                                END) AS PROF_VS_TOT_INCM_RTO
                                                                FROM   TM23_MMLY_INCM_EXP_A A INNER JOIN TM00_PCF_D B
                                                                                                      ON A.PCF_ID = B.PCF_ID
                                                                                              INNER JOIN TM00_LOC_D C
                                                                                                      ON B.LOC_ID = C.LOC_ID
                                                                GROUP BY A.BAS_YM, C.PRVN_CD
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PRVN_CD  = XX.PRVN_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '091' AS KPI_TYP_CD,
                  '5'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PROF_VS_TOT_INCM_RTO, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, PROF_VS_TOT_INCM_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_TOT_INCM_RTO ASC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(PROF_VS_TOT_INCM_RTO) AS PROF_VS_TOT_INCM_RTO
                         FROM
                               (SELECT BAS_YM, BAS_YM1, PRVN_CD, PROF_VS_TOT_INCM_RTO, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY PROF_VS_TOT_INCM_RTO ASC NULLS LAST, PRVN_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.PRVN_CD,
                                               YY.PROF_VS_TOT_INCM_RTO
                                        FROM   (SELECT T1.BAS_YM, T3.PRVN_CD
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                                                                    INNER JOIN TM00_LOC_D T3
                                                                                            ON T2.LOC_ID = T3.LOC_ID
                                                GROUP BY T1.BAS_YM, T3.PRVN_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       C.PRVN_CD,
                                                                       AVG(CASE WHEN A.TOT_INCM_AMT - A.TOT_EXP_AMT = 0 THEN 0
                                                                                WHEN A.TOT_INCM_AMT - A.TOT_EXP_AMT <> 0 AND A.TOT_INCM_AMT = 0 THEN NULL
                                                                                ELSE CASE WHEN A.TOT_INCM_AMT = 0 THEN 0
                                                                                          ELSE ( A.TOT_INCM_AMT - A.TOT_EXP_AMT ) / ( A.TOT_INCM_AMT ) * 100
                                                                                          END
                                                                                END) AS PROF_VS_TOT_INCM_RTO
                                                                FROM   TM23_MMLY_INCM_EXP_A A INNER JOIN TM00_PCF_D B
                                                                                                      ON A.PCF_ID = B.PCF_ID
                                                                                              INNER JOIN TM00_LOC_D C
                                                                                                      ON B.LOC_ID = C.LOC_ID
                                                                GROUP BY A.BAS_YM, C.PRVN_CD
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PRVN_CD  = XX.PRVN_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '091' AS KPI_TYP_CD,
                  '5'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PROF_VS_TOT_INCM_RTO, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, PROF_VS_TOT_INCM_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_TOT_INCM_RTO DESC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(PROF_VS_TOT_INCM_RTO) AS PROF_VS_TOT_INCM_RTO
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, PROF_VS_TOT_INCM_RTO, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY PROF_VS_TOT_INCM_RTO DESC NULLS LAST, CBV_BR_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               YY.PROF_VS_TOT_INCM_RTO
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                                GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       B.CBV_BR_CD,
                                                                       AVG(CASE WHEN A.TOT_INCM_AMT - A.TOT_EXP_AMT = 0 THEN 0
                                                                                WHEN A.TOT_INCM_AMT - A.TOT_EXP_AMT <> 0 AND A.TOT_INCM_AMT = 0 THEN NULL
                                                                                ELSE CASE WHEN A.TOT_INCM_AMT = 0 THEN 0
                                                                                          ELSE ( A.TOT_INCM_AMT - A.TOT_EXP_AMT ) / ( A.TOT_INCM_AMT ) * 100
                                                                                          END
                                                                                END) AS PROF_VS_TOT_INCM_RTO
                                                                FROM   TM23_MMLY_INCM_EXP_A A INNER JOIN TM00_PCF_D B
                                                                                                      ON A.PCF_ID = B.PCF_ID
                                                                GROUP BY A.BAS_YM, B.CBV_BR_CD
                                                               ) YY
                                                            ON YY.BAS_YM   >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM   <= XX.BAS_YM
                                                           AND YY.CBV_BR_CD = XX.CBV_BR_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '091' AS KPI_TYP_CD,
                  '5'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PROF_VS_TOT_INCM_RTO, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, PROF_VS_TOT_INCM_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_TOT_INCM_RTO ASC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(PROF_VS_TOT_INCM_RTO) AS PROF_VS_TOT_INCM_RTO
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, PROF_VS_TOT_INCM_RTO, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY PROF_VS_TOT_INCM_RTO ASC NULLS LAST, CBV_BR_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               YY.PROF_VS_TOT_INCM_RTO
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                                GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       B.CBV_BR_CD,
                                                                       AVG(CASE WHEN A.TOT_INCM_AMT - A.TOT_EXP_AMT = 0 THEN 0
                                                                                WHEN A.TOT_INCM_AMT - A.TOT_EXP_AMT <> 0 AND A.TOT_INCM_AMT = 0 THEN NULL
                                                                                ELSE CASE WHEN A.TOT_INCM_AMT = 0 THEN 0
                                                                                          ELSE ( A.TOT_INCM_AMT - A.TOT_EXP_AMT ) / ( A.TOT_INCM_AMT ) * 100
                                                                                          END
                                                                                END) AS PROF_VS_TOT_INCM_RTO
                                                                FROM   TM23_MMLY_INCM_EXP_A A INNER JOIN TM00_PCF_D B
                                                                                                      ON A.PCF_ID = B.PCF_ID
                                                                GROUP BY A.BAS_YM, B.CBV_BR_CD
                                                               ) YY
                                                            ON YY.BAS_YM   >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM   <= XX.BAS_YM
                                                           AND YY.CBV_BR_CD = XX.CBV_BR_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '091' AS KPI_TYP_CD,
                  '7'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PROF_VS_TOT_INCM_RTO, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, PROF_VS_TOT_INCM_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_TOT_INCM_RTO DESC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(PROF_VS_TOT_INCM_RTO) AS PROF_VS_TOT_INCM_RTO
                         FROM
                               (SELECT BAS_YM, BAS_YM1, PRVN_CD, PROF_VS_TOT_INCM_RTO, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY PROF_VS_TOT_INCM_RTO DESC NULLS LAST, PRVN_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.PRVN_CD,
                                               YY.PROF_VS_TOT_INCM_RTO
                                        FROM   (SELECT T1.BAS_YM, T3.PRVN_CD
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                                                                    INNER JOIN TM00_LOC_D T3
                                                                                            ON T2.LOC_ID = T3.LOC_ID
                                                GROUP BY T1.BAS_YM, T3.PRVN_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       C.PRVN_CD,
                                                                       SUM(CASE WHEN A.TOT_INCM_AMT - A.TOT_EXP_AMT = 0 THEN 0
                                                                                WHEN A.TOT_INCM_AMT - A.TOT_EXP_AMT <> 0 AND A.TOT_INCM_AMT = 0 THEN NULL
                                                                                ELSE CASE WHEN A.TOT_INCM_AMT = 0 THEN 0
                                                                                          ELSE ( A.TOT_INCM_AMT - A.TOT_EXP_AMT ) / ( A.TOT_INCM_AMT ) * 100
                                                                                          END
                                                                                END * D.ASSET_WEIGHTED_AVG) AS PROF_VS_TOT_INCM_RTO
                                                                FROM   TM23_MMLY_INCM_EXP_A A INNER JOIN TM00_PCF_D B
                                                                                                      ON A.PCF_ID = B.PCF_ID
                                                                                              INNER JOIN TM00_LOC_D C
                                                                                                      ON B.LOC_ID = C.LOC_ID
                                                                                              INNER JOIN ASSET_WEIGHTED_AVG_PCF_PRVN D
                                                                                                      ON D.BAS_YM = A.BAS_YM
                                                                                                     AND D.PCF_ID = A.PCF_ID
                                                                GROUP BY A.BAS_YM, C.PRVN_CD
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PRVN_CD  = XX.PRVN_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '091' AS KPI_TYP_CD,
                  '7'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PROF_VS_TOT_INCM_RTO, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, PROF_VS_TOT_INCM_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_TOT_INCM_RTO ASC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(PROF_VS_TOT_INCM_RTO) AS PROF_VS_TOT_INCM_RTO
                         FROM
                               (SELECT BAS_YM, BAS_YM1, PRVN_CD, PROF_VS_TOT_INCM_RTO, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY PROF_VS_TOT_INCM_RTO ASC NULLS LAST, PRVN_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.PRVN_CD,
                                               YY.PROF_VS_TOT_INCM_RTO
                                        FROM   (SELECT T1.BAS_YM, T3.PRVN_CD
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                                                                    INNER JOIN TM00_LOC_D T3
                                                                                            ON T2.LOC_ID = T3.LOC_ID
                                                GROUP BY T1.BAS_YM, T3.PRVN_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       C.PRVN_CD,
                                                                       SUM(CASE WHEN A.TOT_INCM_AMT - A.TOT_EXP_AMT = 0 THEN 0
                                                                                WHEN A.TOT_INCM_AMT - A.TOT_EXP_AMT <> 0 AND A.TOT_INCM_AMT = 0 THEN NULL
                                                                                ELSE CASE WHEN A.TOT_INCM_AMT = 0 THEN 0
                                                                                          ELSE ( A.TOT_INCM_AMT - A.TOT_EXP_AMT ) / ( A.TOT_INCM_AMT ) * 100
                                                                                          END
                                                                                END * D.ASSET_WEIGHTED_AVG) AS PROF_VS_TOT_INCM_RTO
                                                                FROM   TM23_MMLY_INCM_EXP_A A INNER JOIN TM00_PCF_D B
                                                                                                      ON A.PCF_ID = B.PCF_ID
                                                                                              INNER JOIN TM00_LOC_D C
                                                                                                      ON B.LOC_ID = C.LOC_ID
                                                                                              INNER JOIN ASSET_WEIGHTED_AVG_PCF_PRVN D
                                                                                                      ON D.BAS_YM = A.BAS_YM
                                                                                                     AND D.PCF_ID = A.PCF_ID
                                                                GROUP BY A.BAS_YM, C.PRVN_CD
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PRVN_CD  = XX.PRVN_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '091' AS KPI_TYP_CD,
                  '7'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PROF_VS_TOT_INCM_RTO, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, PROF_VS_TOT_INCM_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_TOT_INCM_RTO DESC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(PROF_VS_TOT_INCM_RTO) AS PROF_VS_TOT_INCM_RTO
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, PROF_VS_TOT_INCM_RTO, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY PROF_VS_TOT_INCM_RTO DESC NULLS LAST, CBV_BR_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               YY.PROF_VS_TOT_INCM_RTO
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                                GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       B.CBV_BR_CD,
                                                                       SUM(CASE WHEN A.TOT_INCM_AMT - A.TOT_EXP_AMT = 0 THEN 0
                                                                                WHEN A.TOT_INCM_AMT - A.TOT_EXP_AMT <> 0 AND A.TOT_INCM_AMT = 0 THEN NULL
                                                                                ELSE CASE WHEN A.TOT_INCM_AMT = 0 THEN 0
                                                                                          ELSE ( A.TOT_INCM_AMT - A.TOT_EXP_AMT ) / ( A.TOT_INCM_AMT ) * 100
                                                                                          END
                                                                                END * C.ASSET_WEIGHTED_AVG) AS PROF_VS_TOT_INCM_RTO
                                                                FROM   TM23_MMLY_INCM_EXP_A A INNER JOIN TM00_PCF_D B
                                                                                                      ON A.PCF_ID = B.PCF_ID
                                                                                              INNER JOIN ASSET_WEIGHTED_AVG_PCF_BR C
                                                                                                      ON C.BAS_YM = A.BAS_YM
                                                                                                     AND C.PCF_ID = A.PCF_ID
                                                                GROUP BY A.BAS_YM, B.CBV_BR_CD
                                                               ) YY
                                                            ON YY.BAS_YM   >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM   <= XX.BAS_YM
                                                           AND YY.CBV_BR_CD = XX.CBV_BR_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '091' AS KPI_TYP_CD,
                  '7'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PROF_VS_TOT_INCM_RTO, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, PROF_VS_TOT_INCM_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_TOT_INCM_RTO ASC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(PROF_VS_TOT_INCM_RTO) AS PROF_VS_TOT_INCM_RTO
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, PROF_VS_TOT_INCM_RTO, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY PROF_VS_TOT_INCM_RTO ASC NULLS LAST, CBV_BR_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               YY.PROF_VS_TOT_INCM_RTO
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                                GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       B.CBV_BR_CD,
                                                                       SUM(CASE WHEN A.TOT_INCM_AMT - A.TOT_EXP_AMT = 0 THEN 0
                                                                                WHEN A.TOT_INCM_AMT - A.TOT_EXP_AMT <> 0 AND A.TOT_INCM_AMT = 0 THEN NULL
                                                                                ELSE CASE WHEN A.TOT_INCM_AMT = 0 THEN 0
                                                                                          ELSE ( A.TOT_INCM_AMT - A.TOT_EXP_AMT ) / ( A.TOT_INCM_AMT ) * 100
                                                                                          END
                                                                                END * C.ASSET_WEIGHTED_AVG) AS PROF_VS_TOT_INCM_RTO
                                                                FROM   TM23_MMLY_INCM_EXP_A A INNER JOIN TM00_PCF_D B
                                                                                                      ON A.PCF_ID = B.PCF_ID
                                                                                              INNER JOIN ASSET_WEIGHTED_AVG_PCF_BR C
                                                                                                      ON C.BAS_YM = A.BAS_YM
                                                                                                     AND C.PCF_ID = A.PCF_ID
                                                                GROUP BY A.BAS_YM, B.CBV_BR_CD
                                                               ) YY
                                                            ON YY.BAS_YM   >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM   <= XX.BAS_YM
                                                           AND YY.CBV_BR_CD = XX.CBV_BR_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '091' AS KPI_TYP_CD,
                  '8'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PROF_VS_TOT_INCM_RTO, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, PROF_VS_TOT_INCM_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_TOT_INCM_RTO DESC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(PROF_VS_TOT_INCM_RTO) AS PROF_VS_TOT_INCM_RTO
                         FROM
                               (SELECT BAS_YM, BAS_YM1, PRVN_CD, PROF_VS_TOT_INCM_RTO, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY PROF_VS_TOT_INCM_RTO DESC NULLS LAST, PRVN_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.PRVN_CD,
                                               YY.PROF_VS_TOT_INCM_RTO
                                        FROM   (SELECT T1.BAS_YM, T3.PRVN_CD
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                                                                    INNER JOIN TM00_LOC_D T3
                                                                                            ON T2.LOC_ID = T3.LOC_ID
                                                GROUP BY T1.BAS_YM, T3.PRVN_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       C.PRVN_CD,
                                                                       CASE WHEN SUM(A.TOT_INCM_AMT - A.TOT_EXP_AMT) = 0 THEN 0
                                                                            WHEN SUM(A.TOT_INCM_AMT - A.TOT_EXP_AMT) <> 0 AND SUM(A.TOT_INCM_AMT) = 0 THEN NULL
                                                                            ELSE SUM(A.TOT_INCM_AMT - A.TOT_EXP_AMT) / SUM(A.TOT_INCM_AMT) * 100
                                                                            END  AS PROF_VS_TOT_INCM_RTO
                                                                FROM   TM23_MMLY_INCM_EXP_A A INNER JOIN TM00_PCF_D B
                                                                                                      ON A.PCF_ID = B.PCF_ID
                                                                                              INNER JOIN TM00_LOC_D C
                                                                                                      ON B.LOC_ID = C.LOC_ID
                                                                GROUP BY A.BAS_YM, C.PRVN_CD
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PRVN_CD  = XX.PRVN_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '091' AS KPI_TYP_CD,
                  '8'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PROF_VS_TOT_INCM_RTO, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, PROF_VS_TOT_INCM_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_TOT_INCM_RTO ASC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(PROF_VS_TOT_INCM_RTO) AS PROF_VS_TOT_INCM_RTO
                         FROM
                               (SELECT BAS_YM, BAS_YM1, PRVN_CD, PROF_VS_TOT_INCM_RTO, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY PROF_VS_TOT_INCM_RTO ASC NULLS LAST, PRVN_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.PRVN_CD,
                                               YY.PROF_VS_TOT_INCM_RTO
                                        FROM   (SELECT T1.BAS_YM, T3.PRVN_CD
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                                                                    INNER JOIN TM00_LOC_D T3
                                                                                            ON T2.LOC_ID = T3.LOC_ID
                                                GROUP BY T1.BAS_YM, T3.PRVN_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       C.PRVN_CD,
                                                                       CASE WHEN SUM(A.TOT_INCM_AMT - A.TOT_EXP_AMT) = 0 THEN 0
                                                                            WHEN SUM(A.TOT_INCM_AMT - A.TOT_EXP_AMT) <> 0 AND SUM(A.TOT_INCM_AMT) = 0 THEN NULL
                                                                            ELSE SUM(A.TOT_INCM_AMT - A.TOT_EXP_AMT) / SUM(A.TOT_INCM_AMT) * 100
                                                                            END  AS PROF_VS_TOT_INCM_RTO
                                                                FROM   TM23_MMLY_INCM_EXP_A A INNER JOIN TM00_PCF_D B
                                                                                                      ON A.PCF_ID = B.PCF_ID
                                                                                              INNER JOIN TM00_LOC_D C
                                                                                                      ON B.LOC_ID = C.LOC_ID
                                                                GROUP BY A.BAS_YM, C.PRVN_CD
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PRVN_CD  = XX.PRVN_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '091' AS KPI_TYP_CD,
                  '8'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PROF_VS_TOT_INCM_RTO, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, PROF_VS_TOT_INCM_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_TOT_INCM_RTO DESC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(PROF_VS_TOT_INCM_RTO) AS PROF_VS_TOT_INCM_RTO
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, PROF_VS_TOT_INCM_RTO, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY PROF_VS_TOT_INCM_RTO DESC NULLS LAST, CBV_BR_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               YY.PROF_VS_TOT_INCM_RTO
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                                GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       B.CBV_BR_CD,
                                                                       CASE WHEN SUM(A.TOT_INCM_AMT - A.TOT_EXP_AMT) = 0 THEN 0
                                                                            WHEN SUM(A.TOT_INCM_AMT - A.TOT_EXP_AMT) <> 0 AND SUM(A.TOT_INCM_AMT) = 0 THEN NULL
                                                                            ELSE SUM(A.TOT_INCM_AMT - A.TOT_EXP_AMT) / SUM(A.TOT_INCM_AMT) * 100
                                                                            END  AS PROF_VS_TOT_INCM_RTO
                                                                FROM   TM23_MMLY_INCM_EXP_A A INNER JOIN TM00_PCF_D B
                                                                                                      ON A.PCF_ID = B.PCF_ID
                                                                GROUP BY A.BAS_YM, B.CBV_BR_CD
                                                               ) YY
                                                            ON YY.BAS_YM   >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM   <= XX.BAS_YM
                                                           AND YY.CBV_BR_CD = XX.CBV_BR_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '091' AS KPI_TYP_CD,
                  '8'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PROF_VS_TOT_INCM_RTO, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, PROF_VS_TOT_INCM_RTO, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_TOT_INCM_RTO ASC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(PROF_VS_TOT_INCM_RTO) AS PROF_VS_TOT_INCM_RTO
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, PROF_VS_TOT_INCM_RTO, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY PROF_VS_TOT_INCM_RTO ASC NULLS LAST, CBV_BR_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               YY.PROF_VS_TOT_INCM_RTO
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                                GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       B.CBV_BR_CD,
                                                                       CASE WHEN SUM(A.TOT_INCM_AMT - A.TOT_EXP_AMT) = 0 THEN 0
                                                                            WHEN SUM(A.TOT_INCM_AMT - A.TOT_EXP_AMT) <> 0 AND SUM(A.TOT_INCM_AMT) = 0 THEN NULL
                                                                            ELSE SUM(A.TOT_INCM_AMT - A.TOT_EXP_AMT) / SUM(A.TOT_INCM_AMT) * 100
                                                                            END  AS PROF_VS_TOT_INCM_RTO
                                                                FROM   TM23_MMLY_INCM_EXP_A A INNER JOIN TM00_PCF_D B
                                                                                                      ON A.PCF_ID = B.PCF_ID
                                                                GROUP BY A.BAS_YM, B.CBV_BR_CD
                                                               ) YY
                                                            ON YY.BAS_YM   >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM   <= XX.BAS_YM
                                                           AND YY.CBV_BR_CD = XX.CBV_BR_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )
          ),
          FROM_KPI_TYP_092 AS
          (SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '092' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(XX.PROF_VS_LST_12_MM_AVG_AST, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, PROF_VS_LST_12_MM_AVG_AST, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_AST DESC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  SUM(T1.TOT_INCM_AMT - T1.TOT_EXP_AMT) * 12 / SUM(T4.L12M_AST_AMT) * 100 AS PROF_VS_LST_12_MM_AVG_AST
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                  ON T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN TM00_LOC_D T3
                                                                       ON T2.LOC_ID = T3.LOC_ID
                                                               INNER JOIN FROM_TM27_MMLY_PCF_KEY_IDC_A T4
                                                                       ON T4.BAS_YM  = T1.BAS_YM
                                                                      AND T4.PCF_ID  = T1.PCF_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '092' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(XX.PROF_VS_LST_12_MM_AVG_AST, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, PROF_VS_LST_12_MM_AVG_AST, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_AST ASC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  SUM(T1.TOT_INCM_AMT - T1.TOT_EXP_AMT) * 12 / SUM(T4.L12M_AST_AMT) * 100 AS PROF_VS_LST_12_MM_AVG_AST
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN TM00_LOC_D T3
                                                                       ON T2.LOC_ID = T3.LOC_ID
                                                               INNER JOIN FROM_TM27_MMLY_PCF_KEY_IDC_A T4
                                                                       ON T4.BAS_YM  = T1.BAS_YM
                                                                      AND T4.PCF_ID  = T1.PCF_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '092' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  ROUND(XX.PROF_VS_LST_12_MM_AVG_AST, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PROF_VS_LST_12_MM_AVG_AST, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_AST DESC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(T1.TOT_INCM_AMT - T1.TOT_EXP_AMT) * 12 / SUM(T3.L12M_AST_AMT) * 100 AS PROF_VS_LST_12_MM_AVG_AST
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN FROM_TM27_MMLY_PCF_KEY_IDC_A T3
                                                                       ON T3.BAS_YM  = T1.BAS_YM
                                                                      AND T3.PCF_ID  = T1.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '092' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  ROUND(XX.PROF_VS_LST_12_MM_AVG_AST, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PROF_VS_LST_12_MM_AVG_AST, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_AST ASC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(T1.TOT_INCM_AMT - T1.TOT_EXP_AMT) * 12 / SUM(T3.L12M_AST_AMT) * 100 AS PROF_VS_LST_12_MM_AVG_AST
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN FROM_TM27_MMLY_PCF_KEY_IDC_A T3
                                                                       ON T3.BAS_YM  = T1.BAS_YM
                                                                      AND T3.PCF_ID  = T1.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '092' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(XX.PROF_VS_LST_12_MM_AVG_AST, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, PROF_VS_LST_12_MM_AVG_AST, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_AST DESC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  AVG(CASE WHEN T4.L12M_AST_AMT = 0 THEN 0
                                           ELSE ( T1.TOT_INCM_AMT - T1.TOT_EXP_AMT ) * 12 / ( T4.L12M_AST_AMT ) * 100
                                           END) AS PROF_VS_LST_12_MM_AVG_AST
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN TM00_LOC_D T3
                                                                       ON T2.LOC_ID = T3.LOC_ID
                                                               INNER JOIN FROM_TM27_MMLY_PCF_KEY_IDC_A T4
                                                                       ON T4.BAS_YM  = T1.BAS_YM
                                                                      AND T4.PCF_ID  = T1.PCF_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '092' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(XX.PROF_VS_LST_12_MM_AVG_AST, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, PROF_VS_LST_12_MM_AVG_AST, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_AST ASC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  AVG(CASE WHEN T4.L12M_AST_AMT = 0 THEN 0
                                           ELSE ( T1.TOT_INCM_AMT - T1.TOT_EXP_AMT ) * 12 / ( T4.L12M_AST_AMT ) * 100
                                           END) AS PROF_VS_LST_12_MM_AVG_AST
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN TM00_LOC_D T3
                                                                       ON T2.LOC_ID = T3.LOC_ID
                                                               INNER JOIN FROM_TM27_MMLY_PCF_KEY_IDC_A T4
                                                                       ON T4.BAS_YM  = T1.BAS_YM
                                                                      AND T4.PCF_ID  = T1.PCF_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '092' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  ROUND(XX.PROF_VS_LST_12_MM_AVG_AST, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PROF_VS_LST_12_MM_AVG_AST, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_AST DESC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  AVG(CASE WHEN T3.L12M_AST_AMT = 0 THEN 0
                                           ELSE ( T1.TOT_INCM_AMT - T1.TOT_EXP_AMT ) * 12 / ( T3.L12M_AST_AMT ) * 100
                                           END) AS PROF_VS_LST_12_MM_AVG_AST
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN FROM_TM27_MMLY_PCF_KEY_IDC_A T3
                                                                       ON T3.BAS_YM  = T1.BAS_YM
                                                                      AND T3.PCF_ID  = T1.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '092' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  ROUND(XX.PROF_VS_LST_12_MM_AVG_AST, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PROF_VS_LST_12_MM_AVG_AST, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_AST ASC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  AVG(CASE WHEN T3.L12M_AST_AMT = 0 THEN 0
                                           ELSE ( T1.TOT_INCM_AMT - T1.TOT_EXP_AMT ) * 12 / ( T3.L12M_AST_AMT ) * 100
                                           END) AS PROF_VS_LST_12_MM_AVG_AST
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN FROM_TM27_MMLY_PCF_KEY_IDC_A T3
                                                                       ON T3.BAS_YM  = T1.BAS_YM
                                                                      AND T3.PCF_ID  = T1.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '092' AS KPI_TYP_CD,
                  '4'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PROF_VS_LST_12_MM_AVG_AST, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, PROF_VS_LST_12_MM_AVG_AST, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_AST DESC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  SUM(CASE WHEN T4.L12M_AST_AMT = 0 THEN 0
                                           ELSE ( T1.TOT_INCM_AMT - T1.TOT_EXP_AMT ) * 12 / ( T4.L12M_AST_AMT ) * 100 * T5.ASSET_WEIGHTED_AVG
                                           END) AS PROF_VS_LST_12_MM_AVG_AST
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN TM00_LOC_D T3
                                                                       ON T2.LOC_ID = T3.LOC_ID
                                                               INNER JOIN FROM_TM27_MMLY_PCF_KEY_IDC_A T4
                                                                       ON T4.BAS_YM  = T1.BAS_YM
                                                                      AND T4.PCF_ID  = T1.PCF_ID
                                                               INNER JOIN ASSET_WEIGHTED_AVG_PCF_PRVN T5
                                                                       ON T5.BAS_YM = T1.BAS_YM
                                                                      AND T5.PCF_ID = T1.PCF_ID

                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   )
           WHERE RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '092' AS KPI_TYP_CD,
                  '4'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PROF_VS_LST_12_MM_AVG_AST, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, PROF_VS_LST_12_MM_AVG_AST, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_AST ASC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  SUM(CASE WHEN T4.L12M_AST_AMT = 0 THEN 0
                                           ELSE ( T1.TOT_INCM_AMT - T1.TOT_EXP_AMT ) * 12 / ( T4.L12M_AST_AMT ) * 100 * T5.ASSET_WEIGHTED_AVG
                                           END) AS PROF_VS_LST_12_MM_AVG_AST
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN TM00_LOC_D T3
                                                                       ON T2.LOC_ID = T3.LOC_ID
                                                               INNER JOIN FROM_TM27_MMLY_PCF_KEY_IDC_A T4
                                                                       ON T4.BAS_YM  = T1.BAS_YM
                                                                      AND T4.PCF_ID  = T1.PCF_ID
                                                               INNER JOIN ASSET_WEIGHTED_AVG_PCF_PRVN T5
                                                                       ON T5.BAS_YM = T1.BAS_YM
                                                                      AND T5.PCF_ID = T1.PCF_ID

                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   )
           WHERE RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '092' AS KPI_TYP_CD,
                  '4'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  CBV_BR_CD,
                  ROUND(PROF_VS_LST_12_MM_AVG_AST, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PROF_VS_LST_12_MM_AVG_AST, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_AST DESC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(CASE WHEN T3.L12M_AST_AMT = 0 THEN 0
                                           ELSE ( T1.TOT_INCM_AMT - T1.TOT_EXP_AMT ) * 12 / ( T3.L12M_AST_AMT ) * 100 * T4.ASSET_WEIGHTED_AVG
                                           END)AS PROF_VS_LST_12_MM_AVG_AST
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN FROM_TM27_MMLY_PCF_KEY_IDC_A T3
                                                                       ON T3.BAS_YM  = T1.BAS_YM
                                                                      AND T3.PCF_ID  = T1.PCF_ID
                                                               INNER JOIN ASSET_WEIGHTED_AVG_PCF_BR T4
                                                                       ON T4.BAS_YM = T1.BAS_YM
                                                                      AND T4.PCF_ID = T1.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   )
           WHERE RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '092' AS KPI_TYP_CD,
                  '4'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  CBV_BR_CD,
                  ROUND(PROF_VS_LST_12_MM_AVG_AST, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PROF_VS_LST_12_MM_AVG_AST, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_AST ASC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(CASE WHEN T3.L12M_AST_AMT = 0 THEN 0
                                           ELSE ( T1.TOT_INCM_AMT - T1.TOT_EXP_AMT ) * 12 / ( T3.L12M_AST_AMT ) * 100 * T4.ASSET_WEIGHTED_AVG
                                           END)AS PROF_VS_LST_12_MM_AVG_AST
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN FROM_TM27_MMLY_PCF_KEY_IDC_A T3
                                                                       ON T3.BAS_YM  = T1.BAS_YM
                                                                      AND T3.PCF_ID  = T1.PCF_ID
                                                               INNER JOIN ASSET_WEIGHTED_AVG_PCF_BR T4
                                                                       ON T4.BAS_YM = T1.BAS_YM
                                                                      AND T4.PCF_ID = T1.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   )
           WHERE RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '092' AS KPI_TYP_CD,
                  '5'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PROF_VS_LST_12_MM_AVG_AST, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, PROF_VS_LST_12_MM_AVG_AST, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_AST DESC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(PROF_VS_LST_12_MM_AVG_AST) AS PROF_VS_LST_12_MM_AVG_AST
                         FROM
                               (SELECT BAS_YM, BAS_YM1, PRVN_CD, PROF_VS_LST_12_MM_AVG_AST, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY PROF_VS_LST_12_MM_AVG_AST DESC NULLS LAST, PRVN_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.PRVN_CD,
                                               YY.PROF_VS_LST_12_MM_AVG_AST
                                        FROM   (SELECT T1.BAS_YM, T3.PRVN_CD
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                                                                    INNER JOIN TM00_LOC_D T3
                                                                                            ON T2.LOC_ID = T3.LOC_ID
                                                GROUP BY T1.BAS_YM, T3.PRVN_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       C.PRVN_CD,
                                                                       AVG(CASE WHEN D.L12M_AST_AMT = 0 THEN 0
                                                                                ELSE ( A.TOT_INCM_AMT - A.TOT_EXP_AMT ) * 12 / ( D.L12M_AST_AMT ) * 100
                                                                                END) AS PROF_VS_LST_12_MM_AVG_AST
                                                                FROM   TM23_MMLY_INCM_EXP_A A INNER JOIN TM00_PCF_D B
                                                                                                      ON A.PCF_ID = B.PCF_ID
                                                                                              INNER JOIN TM00_LOC_D C
                                                                                                      ON B.LOC_ID = C.LOC_ID
                                                                                              INNER JOIN TM27_MMLY_PCF_KEY_IDC_A D
                                                                                                      ON D.BAS_YM  = A.BAS_YM
                                                                                                     AND D.PCF_ID  = A.PCF_ID
                                                                GROUP BY A.BAS_YM, C.PRVN_CD
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PRVN_CD  = XX.PRVN_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '092' AS KPI_TYP_CD,
                  '5'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PROF_VS_LST_12_MM_AVG_AST, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, PROF_VS_LST_12_MM_AVG_AST, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_AST ASC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(PROF_VS_LST_12_MM_AVG_AST) AS PROF_VS_LST_12_MM_AVG_AST
                         FROM
                               (SELECT BAS_YM, BAS_YM1, PRVN_CD, PROF_VS_LST_12_MM_AVG_AST, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY PROF_VS_LST_12_MM_AVG_AST ASC NULLS LAST, PRVN_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.PRVN_CD,
                                               YY.PROF_VS_LST_12_MM_AVG_AST
                                        FROM   (SELECT T1.BAS_YM, T3.PRVN_CD
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                                                                    INNER JOIN TM00_LOC_D T3
                                                                                            ON T2.LOC_ID = T3.LOC_ID
                                                GROUP BY T1.BAS_YM, T3.PRVN_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       C.PRVN_CD,
                                                                       AVG(CASE WHEN D.L12M_AST_AMT = 0 THEN 0
                                                                                ELSE ( A.TOT_INCM_AMT - A.TOT_EXP_AMT ) * 12 / ( D.L12M_AST_AMT ) * 100
                                                                                END) AS PROF_VS_LST_12_MM_AVG_AST
                                                                FROM   TM23_MMLY_INCM_EXP_A A INNER JOIN TM00_PCF_D B
                                                                                                      ON A.PCF_ID = B.PCF_ID
                                                                                              INNER JOIN TM00_LOC_D C
                                                                                                      ON B.LOC_ID = C.LOC_ID
                                                                                              INNER JOIN TM27_MMLY_PCF_KEY_IDC_A D
                                                                                                      ON D.BAS_YM  = A.BAS_YM
                                                                                                     AND D.PCF_ID  = A.PCF_ID
                                                                GROUP BY A.BAS_YM, C.PRVN_CD
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PRVN_CD  = XX.PRVN_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '092' AS KPI_TYP_CD,
                  '5'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PROF_VS_LST_12_MM_AVG_AST, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, PROF_VS_LST_12_MM_AVG_AST, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_AST DESC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(PROF_VS_LST_12_MM_AVG_AST) AS PROF_VS_LST_12_MM_AVG_AST
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, PROF_VS_LST_12_MM_AVG_AST, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY PROF_VS_LST_12_MM_AVG_AST DESC NULLS LAST, CBV_BR_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               YY.PROF_VS_LST_12_MM_AVG_AST
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                                GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       B.CBV_BR_CD,
                                                                       AVG(CASE WHEN C.L12M_AST_AMT = 0 THEN 0
                                                                                ELSE ( A.TOT_INCM_AMT - A.TOT_EXP_AMT ) * 12 / ( C.L12M_AST_AMT ) * 100
                                                                                END) AS PROF_VS_LST_12_MM_AVG_AST
                                                                FROM   TM23_MMLY_INCM_EXP_A A INNER JOIN TM00_PCF_D B
                                                                                                      ON A.PCF_ID = B.PCF_ID
                                                                                              INNER JOIN TM27_MMLY_PCF_KEY_IDC_A C
                                                                                                      ON C.BAS_YM  = A.BAS_YM
                                                                                                     AND C.PCF_ID  = A.PCF_ID
                                                                GROUP BY A.BAS_YM, B.CBV_BR_CD
                                                               ) YY
                                                            ON YY.BAS_YM   >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM   <= XX.BAS_YM
                                                           AND YY.CBV_BR_CD = XX.CBV_BR_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '092' AS KPI_TYP_CD,
                  '5'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PROF_VS_LST_12_MM_AVG_AST, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, PROF_VS_LST_12_MM_AVG_AST, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_AST ASC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(PROF_VS_LST_12_MM_AVG_AST) AS PROF_VS_LST_12_MM_AVG_AST
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, PROF_VS_LST_12_MM_AVG_AST, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY PROF_VS_LST_12_MM_AVG_AST ASC NULLS LAST, CBV_BR_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               YY.PROF_VS_LST_12_MM_AVG_AST
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                                GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       B.CBV_BR_CD,
                                                                       AVG(CASE WHEN C.L12M_AST_AMT = 0 THEN 0
                                                                                ELSE ( A.TOT_INCM_AMT - A.TOT_EXP_AMT ) * 12 / ( C.L12M_AST_AMT ) * 100
                                                                                END) AS PROF_VS_LST_12_MM_AVG_AST
                                                                FROM   TM23_MMLY_INCM_EXP_A A INNER JOIN TM00_PCF_D B
                                                                                                      ON A.PCF_ID = B.PCF_ID
                                                                                              INNER JOIN TM27_MMLY_PCF_KEY_IDC_A C
                                                                                                      ON C.BAS_YM  = A.BAS_YM
                                                                                                     AND C.PCF_ID  = A.PCF_ID
                                                                GROUP BY A.BAS_YM, B.CBV_BR_CD
                                                               ) YY
                                                            ON YY.BAS_YM   >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM   <= XX.BAS_YM
                                                           AND YY.CBV_BR_CD = XX.CBV_BR_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '092' AS KPI_TYP_CD,
                  '7'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PROF_VS_LST_12_MM_AVG_AST, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, PROF_VS_LST_12_MM_AVG_AST, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_AST DESC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(PROF_VS_LST_12_MM_AVG_AST) AS PROF_VS_LST_12_MM_AVG_AST
                         FROM
                               (SELECT BAS_YM, BAS_YM1, PRVN_CD, PROF_VS_LST_12_MM_AVG_AST, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY PROF_VS_LST_12_MM_AVG_AST DESC NULLS LAST, PRVN_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.PRVN_CD,
                                               YY.PROF_VS_LST_12_MM_AVG_AST
                                        FROM   (SELECT T1.BAS_YM, T3.PRVN_CD
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                                                                    INNER JOIN TM00_LOC_D T3
                                                                                            ON T2.LOC_ID = T3.LOC_ID
                                                GROUP BY T1.BAS_YM, T3.PRVN_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       C.PRVN_CD,
                                                                       SUM(CASE WHEN D.L12M_AST_AMT = 0 THEN 0
                                                                                ELSE ( A.TOT_INCM_AMT - A.TOT_EXP_AMT ) * 12 / ( D.L12M_AST_AMT ) * 100 * E.ASSET_WEIGHTED_AVG
                                                                                END) AS PROF_VS_LST_12_MM_AVG_AST
                                                                FROM   TM23_MMLY_INCM_EXP_A A INNER JOIN TM00_PCF_D B
                                                                                                      ON A.PCF_ID = B.PCF_ID
                                                                                              INNER JOIN TM00_LOC_D C
                                                                                                      ON B.LOC_ID = C.LOC_ID
                                                                                              INNER JOIN TM27_MMLY_PCF_KEY_IDC_A D
                                                                                                      ON D.BAS_YM  = A.BAS_YM
                                                                                                     AND D.PCF_ID  = A.PCF_ID
                                                                                              INNER JOIN ASSET_WEIGHTED_AVG_PCF_PRVN E
                                                                                                      ON E.BAS_YM = A.BAS_YM
                                                                                                     AND E.PCF_ID = A.PCF_ID
                                                                GROUP BY A.BAS_YM, C.PRVN_CD
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PRVN_CD  = XX.PRVN_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '092' AS KPI_TYP_CD,
                  '7'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PROF_VS_LST_12_MM_AVG_AST, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, PROF_VS_LST_12_MM_AVG_AST, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_AST ASC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(PROF_VS_LST_12_MM_AVG_AST) AS PROF_VS_LST_12_MM_AVG_AST
                         FROM
                               (SELECT BAS_YM, BAS_YM1, PRVN_CD, PROF_VS_LST_12_MM_AVG_AST, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY PROF_VS_LST_12_MM_AVG_AST ASC NULLS LAST, PRVN_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.PRVN_CD,
                                               YY.PROF_VS_LST_12_MM_AVG_AST
                                        FROM   (SELECT T1.BAS_YM, T3.PRVN_CD
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                                                                    INNER JOIN TM00_LOC_D T3
                                                                                            ON T2.LOC_ID = T3.LOC_ID
                                                GROUP BY T1.BAS_YM, T3.PRVN_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       C.PRVN_CD,
                                                                       SUM(CASE WHEN D.L12M_AST_AMT = 0 THEN 0
                                                                                ELSE ( A.TOT_INCM_AMT - A.TOT_EXP_AMT ) * 12 / ( D.L12M_AST_AMT ) * 100 * E.ASSET_WEIGHTED_AVG
                                                                                END) AS PROF_VS_LST_12_MM_AVG_AST
                                                                FROM   TM23_MMLY_INCM_EXP_A A INNER JOIN TM00_PCF_D B
                                                                                                      ON A.PCF_ID = B.PCF_ID
                                                                                              INNER JOIN TM00_LOC_D C
                                                                                                      ON B.LOC_ID = C.LOC_ID
                                                                                              INNER JOIN TM27_MMLY_PCF_KEY_IDC_A D
                                                                                                      ON D.BAS_YM  = A.BAS_YM
                                                                                                     AND D.PCF_ID  = A.PCF_ID
                                                                                              INNER JOIN ASSET_WEIGHTED_AVG_PCF_PRVN E
                                                                                                      ON E.BAS_YM = A.BAS_YM
                                                                                                     AND E.PCF_ID = A.PCF_ID
                                                                GROUP BY A.BAS_YM, C.PRVN_CD
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PRVN_CD  = XX.PRVN_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '092' AS KPI_TYP_CD,
                  '7'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PROF_VS_LST_12_MM_AVG_AST, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, PROF_VS_LST_12_MM_AVG_AST, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_AST DESC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(PROF_VS_LST_12_MM_AVG_AST) AS PROF_VS_LST_12_MM_AVG_AST
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, PROF_VS_LST_12_MM_AVG_AST, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY PROF_VS_LST_12_MM_AVG_AST DESC NULLS LAST, CBV_BR_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               YY.PROF_VS_LST_12_MM_AVG_AST
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                                GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       B.CBV_BR_CD,
                                                                       SUM(CASE WHEN C.L12M_AST_AMT = 0 THEN 0
                                                                                ELSE ( A.TOT_INCM_AMT - A.TOT_EXP_AMT ) * 12 / ( C.L12M_AST_AMT ) * 100 * D.ASSET_WEIGHTED_AVG
                                                                                END) AS PROF_VS_LST_12_MM_AVG_AST
                                                                FROM   TM23_MMLY_INCM_EXP_A A INNER JOIN TM00_PCF_D B
                                                                                                      ON A.PCF_ID = B.PCF_ID
                                                                                              INNER JOIN TM27_MMLY_PCF_KEY_IDC_A C
                                                                                                      ON C.BAS_YM  = A.BAS_YM
                                                                                                     AND C.PCF_ID  = A.PCF_ID
                                                                                              INNER JOIN ASSET_WEIGHTED_AVG_PCF_BR D
                                                                                                      ON D.BAS_YM = A.BAS_YM
                                                                                                     AND D.PCF_ID = A.PCF_ID
                                                                GROUP BY A.BAS_YM, B.CBV_BR_CD
                                                               ) YY
                                                            ON YY.BAS_YM   >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM   <= XX.BAS_YM
                                                           AND YY.CBV_BR_CD = XX.CBV_BR_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '092' AS KPI_TYP_CD,
                  '7'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PROF_VS_LST_12_MM_AVG_AST, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, PROF_VS_LST_12_MM_AVG_AST, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_AST ASC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(PROF_VS_LST_12_MM_AVG_AST) AS PROF_VS_LST_12_MM_AVG_AST
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, PROF_VS_LST_12_MM_AVG_AST, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY PROF_VS_LST_12_MM_AVG_AST ASC NULLS LAST, CBV_BR_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               YY.PROF_VS_LST_12_MM_AVG_AST
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                                GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       B.CBV_BR_CD,
                                                                       SUM(CASE WHEN C.L12M_AST_AMT = 0 THEN 0
                                                                                ELSE ( A.TOT_INCM_AMT - A.TOT_EXP_AMT ) * 12 / ( C.L12M_AST_AMT ) * 100 * D.ASSET_WEIGHTED_AVG
                                                                                END) AS PROF_VS_LST_12_MM_AVG_AST
                                                                FROM   TM23_MMLY_INCM_EXP_A A INNER JOIN TM00_PCF_D B
                                                                                                      ON A.PCF_ID = B.PCF_ID
                                                                                              INNER JOIN TM27_MMLY_PCF_KEY_IDC_A C
                                                                                                      ON C.BAS_YM  = A.BAS_YM
                                                                                                     AND C.PCF_ID  = A.PCF_ID
                                                                                              INNER JOIN ASSET_WEIGHTED_AVG_PCF_BR D
                                                                                                      ON D.BAS_YM = A.BAS_YM
                                                                                                     AND D.PCF_ID = A.PCF_ID
                                                                GROUP BY A.BAS_YM, B.CBV_BR_CD
                                                               ) YY
                                                            ON YY.BAS_YM   >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM   <= XX.BAS_YM
                                                           AND YY.CBV_BR_CD = XX.CBV_BR_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '092' AS KPI_TYP_CD,
                  '8'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PROF_VS_LST_12_MM_AVG_AST, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, PROF_VS_LST_12_MM_AVG_AST, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_AST DESC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(PROF_VS_LST_12_MM_AVG_AST) AS PROF_VS_LST_12_MM_AVG_AST
                         FROM
                               (SELECT BAS_YM, BAS_YM1, PRVN_CD, PROF_VS_LST_12_MM_AVG_AST, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY PROF_VS_LST_12_MM_AVG_AST DESC NULLS LAST, PRVN_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.PRVN_CD,
                                               YY.PROF_VS_LST_12_MM_AVG_AST
                                        FROM   (SELECT T1.BAS_YM, T3.PRVN_CD
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                                                                    INNER JOIN TM00_LOC_D T3
                                                                                            ON T2.LOC_ID = T3.LOC_ID
                                                GROUP BY T1.BAS_YM, T3.PRVN_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       C.PRVN_CD,
                                                                       SUM(A.TOT_INCM_AMT - A.TOT_EXP_AMT) * 12 / SUM(D.L12M_AST_AMT) * 100 AS PROF_VS_LST_12_MM_AVG_AST
                                                                FROM   TM23_MMLY_INCM_EXP_A A INNER JOIN TM00_PCF_D B
                                                                                                      ON A.PCF_ID = B.PCF_ID
                                                                                              INNER JOIN TM00_LOC_D C
                                                                                                      ON B.LOC_ID = C.LOC_ID
                                                                                              INNER JOIN TM27_MMLY_PCF_KEY_IDC_A D
                                                                                                      ON D.BAS_YM  = A.BAS_YM
                                                                                                     AND D.PCF_ID  = A.PCF_ID
                                                                GROUP BY A.BAS_YM, C.PRVN_CD
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PRVN_CD  = XX.PRVN_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '092' AS KPI_TYP_CD,
                  '8'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PROF_VS_LST_12_MM_AVG_AST, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, PROF_VS_LST_12_MM_AVG_AST, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_AST ASC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(PROF_VS_LST_12_MM_AVG_AST) AS PROF_VS_LST_12_MM_AVG_AST
                         FROM
                               (SELECT BAS_YM, BAS_YM1, PRVN_CD, PROF_VS_LST_12_MM_AVG_AST, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY PROF_VS_LST_12_MM_AVG_AST ASC NULLS LAST, PRVN_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.PRVN_CD,
                                               YY.PROF_VS_LST_12_MM_AVG_AST
                                        FROM   (SELECT T1.BAS_YM, T3.PRVN_CD
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                                                                    INNER JOIN TM00_LOC_D T3
                                                                                            ON T2.LOC_ID = T3.LOC_ID
                                                GROUP BY T1.BAS_YM, T3.PRVN_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       C.PRVN_CD,
                                                                       SUM(A.TOT_INCM_AMT - A.TOT_EXP_AMT) * 12 / SUM(D.L12M_AST_AMT) * 100 AS PROF_VS_LST_12_MM_AVG_AST
                                                                FROM   TM23_MMLY_INCM_EXP_A A INNER JOIN TM00_PCF_D B
                                                                                                      ON A.PCF_ID = B.PCF_ID
                                                                                              INNER JOIN TM00_LOC_D C
                                                                                                      ON B.LOC_ID = C.LOC_ID
                                                                                              INNER JOIN TM27_MMLY_PCF_KEY_IDC_A D
                                                                                                      ON D.BAS_YM  = A.BAS_YM
                                                                                                     AND D.PCF_ID  = A.PCF_ID
                                                                GROUP BY A.BAS_YM, C.PRVN_CD
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PRVN_CD  = XX.PRVN_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '092' AS KPI_TYP_CD,
                  '8'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PROF_VS_LST_12_MM_AVG_AST, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, PROF_VS_LST_12_MM_AVG_AST, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_AST DESC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(PROF_VS_LST_12_MM_AVG_AST) AS PROF_VS_LST_12_MM_AVG_AST
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, PROF_VS_LST_12_MM_AVG_AST, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY PROF_VS_LST_12_MM_AVG_AST DESC NULLS LAST, CBV_BR_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               YY.PROF_VS_LST_12_MM_AVG_AST
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                                GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       B.CBV_BR_CD,
                                                                       SUM(A.TOT_INCM_AMT - A.TOT_EXP_AMT) * 12 / SUM(C.L12M_AST_AMT) * 100 AS PROF_VS_LST_12_MM_AVG_AST
                                                                FROM   TM23_MMLY_INCM_EXP_A A INNER JOIN TM00_PCF_D B
                                                                                                      ON A.PCF_ID = B.PCF_ID
                                                                                              INNER JOIN TM27_MMLY_PCF_KEY_IDC_A C
                                                                                                      ON C.BAS_YM  = A.BAS_YM
                                                                                                     AND C.PCF_ID  = A.PCF_ID
                                                                GROUP BY A.BAS_YM, B.CBV_BR_CD
                                                               ) YY
                                                            ON YY.BAS_YM   >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM   <= XX.BAS_YM
                                                           AND YY.CBV_BR_CD = XX.CBV_BR_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '092' AS KPI_TYP_CD,
                  '8'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PROF_VS_LST_12_MM_AVG_AST, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, PROF_VS_LST_12_MM_AVG_AST, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_AST ASC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(PROF_VS_LST_12_MM_AVG_AST) AS PROF_VS_LST_12_MM_AVG_AST
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, PROF_VS_LST_12_MM_AVG_AST, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY PROF_VS_LST_12_MM_AVG_AST ASC NULLS LAST, CBV_BR_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               YY.PROF_VS_LST_12_MM_AVG_AST
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                                GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       B.CBV_BR_CD,
                                                                       SUM(A.TOT_INCM_AMT - A.TOT_EXP_AMT) * 12 / SUM(C.L12M_AST_AMT) * 100 AS PROF_VS_LST_12_MM_AVG_AST
                                                                FROM   TM23_MMLY_INCM_EXP_A A INNER JOIN TM00_PCF_D B
                                                                                                      ON A.PCF_ID = B.PCF_ID
                                                                                              INNER JOIN TM27_MMLY_PCF_KEY_IDC_A C
                                                                                                      ON C.BAS_YM  = A.BAS_YM
                                                                                                     AND C.PCF_ID  = A.PCF_ID
                                                                GROUP BY A.BAS_YM, B.CBV_BR_CD
                                                               ) YY
                                                            ON YY.BAS_YM   >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM   <= XX.BAS_YM
                                                           AND YY.CBV_BR_CD = XX.CBV_BR_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )
          ),
          FROM_KPI_TYP_093 AS
          (SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '093' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(XX.PROF_VS_LST_12_MM_AVG_LN_AMT, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, PROF_VS_LST_12_MM_AVG_LN_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_LN_AMT DESC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  SUM(T1.TOT_INCM_AMT - T1.TOT_EXP_AMT) * 12 / SUM(T4.L12M_LN_AMT) * 100 AS PROF_VS_LST_12_MM_AVG_LN_AMT
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN TM00_LOC_D T3
                                                                       ON T2.LOC_ID = T3.LOC_ID
                                                               INNER JOIN FROM_TM27_MMLY_PCF_KEY_IDC_A T4
                                                                       ON T4.BAS_YM  = T1.BAS_YM
                                                                      AND T4.PCF_ID  = T1.PCF_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '093' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(XX.PROF_VS_LST_12_MM_AVG_LN_AMT, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, PROF_VS_LST_12_MM_AVG_LN_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_LN_AMT ASC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  SUM(T1.TOT_INCM_AMT - T1.TOT_EXP_AMT) * 12 / SUM(T4.L12M_LN_AMT) * 100 AS PROF_VS_LST_12_MM_AVG_LN_AMT
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN TM00_LOC_D T3
                                                                       ON T2.LOC_ID = T3.LOC_ID
                                                               INNER JOIN FROM_TM27_MMLY_PCF_KEY_IDC_A T4
                                                                       ON T4.BAS_YM  = T1.BAS_YM
                                                                      AND T4.PCF_ID  = T1.PCF_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '093' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  ROUND(XX.PROF_VS_LST_12_MM_AVG_LN_AMT, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PROF_VS_LST_12_MM_AVG_LN_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_LN_AMT DESC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(T1.TOT_INCM_AMT - T1.TOT_EXP_AMT) * 12 / SUM(T3.L12M_LN_AMT) * 100 AS PROF_VS_LST_12_MM_AVG_LN_AMT
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN FROM_TM27_MMLY_PCF_KEY_IDC_A T3
                                                                       ON T3.BAS_YM  = T1.BAS_YM
                                                                      AND T3.PCF_ID  = T1.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '093' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  ROUND(XX.PROF_VS_LST_12_MM_AVG_LN_AMT, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PROF_VS_LST_12_MM_AVG_LN_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_LN_AMT ASC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(T1.TOT_INCM_AMT - T1.TOT_EXP_AMT) * 12 / SUM(T3.L12M_LN_AMT) * 100 AS PROF_VS_LST_12_MM_AVG_LN_AMT
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN FROM_TM27_MMLY_PCF_KEY_IDC_A T3
                                                                       ON T3.BAS_YM  = T1.BAS_YM
                                                                      AND T3.PCF_ID  = T1.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '093' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(XX.PROF_VS_LST_12_MM_AVG_LN_AMT, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, PROF_VS_LST_12_MM_AVG_LN_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_LN_AMT DESC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  AVG(CASE WHEN T4.L12M_LN_AMT = 0 THEN 0
                                           ELSE ( T1.TOT_INCM_AMT - T1.TOT_EXP_AMT ) * 12 / ( T4.L12M_LN_AMT ) * 100
                                           END) AS PROF_VS_LST_12_MM_AVG_LN_AMT
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN TM00_LOC_D T3
                                                                       ON T2.LOC_ID = T3.LOC_ID
                                                               INNER JOIN FROM_TM27_MMLY_PCF_KEY_IDC_A T4
                                                                       ON T4.BAS_YM  = T1.BAS_YM
                                                                      AND T4.PCF_ID  = T1.PCF_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '093' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(XX.PROF_VS_LST_12_MM_AVG_LN_AMT, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, PROF_VS_LST_12_MM_AVG_LN_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_LN_AMT ASC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  AVG(CASE WHEN T4.L12M_LN_AMT = 0 THEN 0
                                           ELSE ( T1.TOT_INCM_AMT - T1.TOT_EXP_AMT ) * 12 / ( T4.L12M_LN_AMT ) * 100
                                           END) AS PROF_VS_LST_12_MM_AVG_LN_AMT
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN TM00_LOC_D T3
                                                                       ON T2.LOC_ID = T3.LOC_ID
                                                               INNER JOIN FROM_TM27_MMLY_PCF_KEY_IDC_A T4
                                                                       ON T4.BAS_YM  = T1.BAS_YM
                                                                      AND T4.PCF_ID  = T1.PCF_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '093' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  ROUND(XX.PROF_VS_LST_12_MM_AVG_LN_AMT, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PROF_VS_LST_12_MM_AVG_LN_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_LN_AMT DESC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  AVG(CASE WHEN T3.L12M_LN_AMT = 0 THEN 0
                                           ELSE ( T1.TOT_INCM_AMT - T1.TOT_EXP_AMT ) * 12 / ( T3.L12M_LN_AMT ) * 100
                                           END) AS PROF_VS_LST_12_MM_AVG_LN_AMT
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN FROM_TM27_MMLY_PCF_KEY_IDC_A T3
                                                                       ON T3.BAS_YM  = T1.BAS_YM
                                                                      AND T3.PCF_ID  = T1.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '093' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  ROUND(XX.PROF_VS_LST_12_MM_AVG_LN_AMT, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PROF_VS_LST_12_MM_AVG_LN_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_LN_AMT ASC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  AVG(CASE WHEN T3.L12M_LN_AMT = 0 THEN 0
                                           ELSE ( T1.TOT_INCM_AMT - T1.TOT_EXP_AMT ) * 12 / ( T3.L12M_LN_AMT ) * 100
                                           END) AS PROF_VS_LST_12_MM_AVG_LN_AMT
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN FROM_TM27_MMLY_PCF_KEY_IDC_A T3
                                                                       ON T3.BAS_YM  = T1.BAS_YM
                                                                      AND T3.PCF_ID  = T1.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '093' AS KPI_TYP_CD,
                  '4'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PROF_VS_LST_12_MM_AVG_LN_AMT, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, PROF_VS_LST_12_MM_AVG_LN_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_LN_AMT DESC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  SUM(CASE WHEN T4.L12M_LN_AMT = 0 THEN 0
                                           ELSE ( T1.TOT_INCM_AMT - T1.TOT_EXP_AMT ) * 12 / ( T4.L12M_LN_AMT ) * 100 * T5.ASSET_WEIGHTED_AVG
                                           END) AS PROF_VS_LST_12_MM_AVG_LN_AMT
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN TM00_LOC_D T3
                                                                       ON T2.LOC_ID = T3.LOC_ID
                                                               INNER JOIN FROM_TM27_MMLY_PCF_KEY_IDC_A T4
                                                                       ON T4.BAS_YM  = T1.BAS_YM
                                                                      AND T4.PCF_ID  = T1.PCF_ID
                                                               INNER JOIN ASSET_WEIGHTED_AVG_PCF_PRVN T5
                                                                       ON T5.BAS_YM = T1.BAS_YM
                                                                      AND T5.PCF_ID = T1.PCF_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   )
           WHERE RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '093' AS KPI_TYP_CD,
                  '4'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PROF_VS_LST_12_MM_AVG_LN_AMT, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, PROF_VS_LST_12_MM_AVG_LN_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_LN_AMT ASC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  SUM(CASE WHEN T4.L12M_LN_AMT = 0 THEN 0
                                           ELSE ( T1.TOT_INCM_AMT - T1.TOT_EXP_AMT ) * 12 / ( T4.L12M_LN_AMT ) * 100 * T5.ASSET_WEIGHTED_AVG
                                           END) AS PROF_VS_LST_12_MM_AVG_LN_AMT
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN TM00_LOC_D T3
                                                                       ON T2.LOC_ID = T3.LOC_ID
                                                               INNER JOIN FROM_TM27_MMLY_PCF_KEY_IDC_A T4
                                                                       ON T4.BAS_YM  = T1.BAS_YM
                                                                      AND T4.PCF_ID  = T1.PCF_ID
                                                               INNER JOIN ASSET_WEIGHTED_AVG_PCF_PRVN T5
                                                                       ON T5.BAS_YM = T1.BAS_YM
                                                                      AND T5.PCF_ID = T1.PCF_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   )
           WHERE RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '093' AS KPI_TYP_CD,
                  '4'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  CBV_BR_CD,
                  ROUND(PROF_VS_LST_12_MM_AVG_LN_AMT, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PROF_VS_LST_12_MM_AVG_LN_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_LN_AMT DESC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(CASE WHEN T3.L12M_LN_AMT = 0 THEN 0
                                           ELSE ( T1.TOT_INCM_AMT - T1.TOT_EXP_AMT ) * 12 / ( T3.L12M_LN_AMT ) * 100 * T4.ASSET_WEIGHTED_AVG
                                           END)AS PROF_VS_LST_12_MM_AVG_LN_AMT
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN FROM_TM27_MMLY_PCF_KEY_IDC_A T3
                                                                       ON T3.BAS_YM  = T1.BAS_YM
                                                                      AND T3.PCF_ID  = T1.PCF_ID
                                                               INNER JOIN ASSET_WEIGHTED_AVG_PCF_BR T4
                                                                       ON T4.BAS_YM = T1.BAS_YM
                                                                      AND T4.PCF_ID = T1.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   )
           WHERE RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '093' AS KPI_TYP_CD,
                  '4'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  CBV_BR_CD,
                  ROUND(PROF_VS_LST_12_MM_AVG_LN_AMT, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PROF_VS_LST_12_MM_AVG_LN_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_LN_AMT ASC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(CASE WHEN T3.L12M_LN_AMT = 0 THEN 0
                                           ELSE ( T1.TOT_INCM_AMT - T1.TOT_EXP_AMT ) * 12 / ( T3.L12M_LN_AMT ) * 100 * T4.ASSET_WEIGHTED_AVG
                                           END)AS PROF_VS_LST_12_MM_AVG_LN_AMT
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN FROM_TM27_MMLY_PCF_KEY_IDC_A T3
                                                                       ON T3.BAS_YM  = T1.BAS_YM
                                                                      AND T3.PCF_ID  = T1.PCF_ID
                                                               INNER JOIN ASSET_WEIGHTED_AVG_PCF_BR T4
                                                                       ON T4.BAS_YM = T1.BAS_YM
                                                                      AND T4.PCF_ID = T1.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   )
           WHERE RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '093' AS KPI_TYP_CD,
                  '5'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PROF_VS_LST_12_MM_AVG_LN_AMT, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, PROF_VS_LST_12_MM_AVG_LN_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_LN_AMT DESC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(PROF_VS_LST_12_MM_AVG_LN_AMT) AS PROF_VS_LST_12_MM_AVG_LN_AMT
                         FROM
                               (SELECT BAS_YM, BAS_YM1, PRVN_CD, PROF_VS_LST_12_MM_AVG_LN_AMT, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY PROF_VS_LST_12_MM_AVG_LN_AMT DESC NULLS LAST, PRVN_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.PRVN_CD,
                                               YY.PROF_VS_LST_12_MM_AVG_LN_AMT
                                        FROM   (SELECT T1.BAS_YM, T3.PRVN_CD
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                                                                    INNER JOIN TM00_LOC_D T3
                                                                                            ON T2.LOC_ID = T3.LOC_ID
                                                GROUP BY T1.BAS_YM, T3.PRVN_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       C.PRVN_CD,
                                                                       AVG(CASE WHEN D.L12M_LN_AMT = 0 THEN 0
                                                                                ELSE ( A.TOT_INCM_AMT - A.TOT_EXP_AMT ) * 12 / ( D.L12M_LN_AMT ) * 100
                                                                                END) AS PROF_VS_LST_12_MM_AVG_LN_AMT
                                                                FROM   TM23_MMLY_INCM_EXP_A A INNER JOIN TM00_PCF_D B
                                                                                                      ON A.PCF_ID = B.PCF_ID
                                                                                              INNER JOIN TM00_LOC_D C
                                                                                                      ON B.LOC_ID = C.LOC_ID
                                                                                              INNER JOIN TM27_MMLY_PCF_KEY_IDC_A D
                                                                                                      ON D.BAS_YM  = A.BAS_YM
                                                                                                     AND D.PCF_ID  = A.PCF_ID
                                                                GROUP BY A.BAS_YM, C.PRVN_CD
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PRVN_CD  = XX.PRVN_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '093' AS KPI_TYP_CD,
                  '5'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PROF_VS_LST_12_MM_AVG_LN_AMT, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, PROF_VS_LST_12_MM_AVG_LN_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_LN_AMT ASC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(PROF_VS_LST_12_MM_AVG_LN_AMT) AS PROF_VS_LST_12_MM_AVG_LN_AMT
                         FROM
                               (SELECT BAS_YM, BAS_YM1, PRVN_CD, PROF_VS_LST_12_MM_AVG_LN_AMT, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY PROF_VS_LST_12_MM_AVG_LN_AMT ASC NULLS LAST, PRVN_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.PRVN_CD,
                                               YY.PROF_VS_LST_12_MM_AVG_LN_AMT
                                        FROM   (SELECT T1.BAS_YM, T3.PRVN_CD
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                                                                    INNER JOIN TM00_LOC_D T3
                                                                                            ON T2.LOC_ID = T3.LOC_ID
                                                GROUP BY T1.BAS_YM, T3.PRVN_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       C.PRVN_CD,
                                                                       AVG(CASE WHEN D.L12M_LN_AMT = 0 THEN 0
                                                                                ELSE ( A.TOT_INCM_AMT - A.TOT_EXP_AMT ) * 12 / ( D.L12M_LN_AMT ) * 100
                                                                                END) AS PROF_VS_LST_12_MM_AVG_LN_AMT
                                                                FROM   TM23_MMLY_INCM_EXP_A A INNER JOIN TM00_PCF_D B
                                                                                                      ON A.PCF_ID = B.PCF_ID
                                                                                              INNER JOIN TM00_LOC_D C
                                                                                                      ON B.LOC_ID = C.LOC_ID
                                                                                              INNER JOIN TM27_MMLY_PCF_KEY_IDC_A D
                                                                                                      ON D.BAS_YM  = A.BAS_YM
                                                                                                     AND D.PCF_ID  = A.PCF_ID
                                                                GROUP BY A.BAS_YM, C.PRVN_CD
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PRVN_CD  = XX.PRVN_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '093' AS KPI_TYP_CD,
                  '5'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PROF_VS_LST_12_MM_AVG_LN_AMT, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, PROF_VS_LST_12_MM_AVG_LN_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_LN_AMT DESC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(PROF_VS_LST_12_MM_AVG_LN_AMT) AS PROF_VS_LST_12_MM_AVG_LN_AMT
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, PROF_VS_LST_12_MM_AVG_LN_AMT, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY PROF_VS_LST_12_MM_AVG_LN_AMT DESC NULLS LAST, CBV_BR_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               YY.PROF_VS_LST_12_MM_AVG_LN_AMT
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
                                                                                                      ON A.PCF_ID = B.PCF_ID
                                                                                              INNER JOIN TM27_MMLY_PCF_KEY_IDC_A C
                                                                                                      ON C.BAS_YM  = A.BAS_YM
                                                                                                     AND C.PCF_ID  = A.PCF_ID
                                                                GROUP BY A.BAS_YM, B.CBV_BR_CD
                                                               ) YY
                                                            ON YY.BAS_YM   >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM   <= XX.BAS_YM
                                                           AND YY.CBV_BR_CD = XX.CBV_BR_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '093' AS KPI_TYP_CD,
                  '5'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PROF_VS_LST_12_MM_AVG_LN_AMT, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, PROF_VS_LST_12_MM_AVG_LN_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_LN_AMT ASC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(PROF_VS_LST_12_MM_AVG_LN_AMT) AS PROF_VS_LST_12_MM_AVG_LN_AMT
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, PROF_VS_LST_12_MM_AVG_LN_AMT, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY PROF_VS_LST_12_MM_AVG_LN_AMT ASC NULLS LAST, CBV_BR_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               YY.PROF_VS_LST_12_MM_AVG_LN_AMT
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
                                                                                                      ON A.PCF_ID = B.PCF_ID
                                                                                              INNER JOIN TM27_MMLY_PCF_KEY_IDC_A C
                                                                                                      ON C.BAS_YM  = A.BAS_YM
                                                                                                     AND C.PCF_ID  = A.PCF_ID
                                                                GROUP BY A.BAS_YM, B.CBV_BR_CD
                                                               ) YY
                                                            ON YY.BAS_YM   >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM   <= XX.BAS_YM
                                                           AND YY.CBV_BR_CD = XX.CBV_BR_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '093' AS KPI_TYP_CD,
                  '7'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PROF_VS_LST_12_MM_AVG_LN_AMT, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, PROF_VS_LST_12_MM_AVG_LN_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_LN_AMT DESC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(PROF_VS_LST_12_MM_AVG_LN_AMT) AS PROF_VS_LST_12_MM_AVG_LN_AMT
                         FROM
                               (SELECT BAS_YM, BAS_YM1, PRVN_CD, PROF_VS_LST_12_MM_AVG_LN_AMT, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY PROF_VS_LST_12_MM_AVG_LN_AMT DESC NULLS LAST, PRVN_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.PRVN_CD,
                                               YY.PROF_VS_LST_12_MM_AVG_LN_AMT
                                        FROM   (SELECT T1.BAS_YM, T3.PRVN_CD
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                                                                    INNER JOIN TM00_LOC_D T3
                                                                                            ON T2.LOC_ID = T3.LOC_ID
                                                GROUP BY T1.BAS_YM, T3.PRVN_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       C.PRVN_CD,
                                                                       SUM(CASE WHEN D.L12M_LN_AMT = 0 THEN 0
                                                                                ELSE ( A.TOT_INCM_AMT - A.TOT_EXP_AMT ) * 12 / ( D.L12M_LN_AMT ) * 100 * E.ASSET_WEIGHTED_AVG
                                                                                END)AS PROF_VS_LST_12_MM_AVG_LN_AMT
                                                                FROM   TM23_MMLY_INCM_EXP_A A INNER JOIN TM00_PCF_D B
                                                                                                      ON A.PCF_ID = B.PCF_ID
                                                                                              INNER JOIN TM00_LOC_D C
                                                                                                      ON B.LOC_ID = C.LOC_ID
                                                                                              INNER JOIN TM27_MMLY_PCF_KEY_IDC_A D
                                                                                                      ON D.BAS_YM  = A.BAS_YM
                                                                                                     AND D.PCF_ID  = A.PCF_ID
                                                                                              INNER JOIN ASSET_WEIGHTED_AVG_PCF_PRVN E
                                                                                                      ON E.BAS_YM = A.BAS_YM
                                                                                                     AND E.PCF_ID = A.PCF_ID
                                                                GROUP BY A.BAS_YM, C.PRVN_CD
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PRVN_CD  = XX.PRVN_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '093' AS KPI_TYP_CD,
                  '7'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PROF_VS_LST_12_MM_AVG_LN_AMT, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, PROF_VS_LST_12_MM_AVG_LN_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_LN_AMT ASC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(PROF_VS_LST_12_MM_AVG_LN_AMT) AS PROF_VS_LST_12_MM_AVG_LN_AMT
                         FROM
                               (SELECT BAS_YM, BAS_YM1, PRVN_CD, PROF_VS_LST_12_MM_AVG_LN_AMT, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY PROF_VS_LST_12_MM_AVG_LN_AMT ASC NULLS LAST, PRVN_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.PRVN_CD,
                                               YY.PROF_VS_LST_12_MM_AVG_LN_AMT
                                        FROM   (SELECT T1.BAS_YM, T3.PRVN_CD
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                                                                    INNER JOIN TM00_LOC_D T3
                                                                                            ON T2.LOC_ID = T3.LOC_ID
                                                GROUP BY T1.BAS_YM, T3.PRVN_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       C.PRVN_CD,
                                                                       SUM(CASE WHEN D.L12M_LN_AMT = 0 THEN 0
                                                                                ELSE ( A.TOT_INCM_AMT - A.TOT_EXP_AMT ) * 12 / ( D.L12M_LN_AMT ) * 100 * E.ASSET_WEIGHTED_AVG
                                                                                END)AS PROF_VS_LST_12_MM_AVG_LN_AMT
                                                                FROM   TM23_MMLY_INCM_EXP_A A INNER JOIN TM00_PCF_D B
                                                                                                      ON A.PCF_ID = B.PCF_ID
                                                                                              INNER JOIN TM00_LOC_D C
                                                                                                      ON B.LOC_ID = C.LOC_ID
                                                                                              INNER JOIN TM27_MMLY_PCF_KEY_IDC_A D
                                                                                                      ON D.BAS_YM  = A.BAS_YM
                                                                                                     AND D.PCF_ID  = A.PCF_ID
                                                                                              INNER JOIN ASSET_WEIGHTED_AVG_PCF_PRVN E
                                                                                                      ON E.BAS_YM = A.BAS_YM
                                                                                                     AND E.PCF_ID = A.PCF_ID
                                                                GROUP BY A.BAS_YM, C.PRVN_CD
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PRVN_CD  = XX.PRVN_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '093' AS KPI_TYP_CD,
                  '7'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PROF_VS_LST_12_MM_AVG_LN_AMT, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, PROF_VS_LST_12_MM_AVG_LN_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_LN_AMT DESC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(PROF_VS_LST_12_MM_AVG_LN_AMT) AS PROF_VS_LST_12_MM_AVG_LN_AMT
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, PROF_VS_LST_12_MM_AVG_LN_AMT, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY PROF_VS_LST_12_MM_AVG_LN_AMT DESC NULLS LAST, CBV_BR_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               YY.PROF_VS_LST_12_MM_AVG_LN_AMT
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                                GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       B.CBV_BR_CD,
                                                                       SUM(CASE WHEN C.L12M_LN_AMT = 0 THEN 0
                                                                                ELSE ( A.TOT_INCM_AMT - A.TOT_EXP_AMT ) * 12 / ( C.L12M_LN_AMT ) * 100 * D.ASSET_WEIGHTED_AVG
                                                                                END)AS PROF_VS_LST_12_MM_AVG_LN_AMT
                                                                FROM   TM23_MMLY_INCM_EXP_A A INNER JOIN TM00_PCF_D B
                                                                                                      ON A.PCF_ID = B.PCF_ID
                                                                                              INNER JOIN TM27_MMLY_PCF_KEY_IDC_A C
                                                                                                      ON C.BAS_YM  = A.BAS_YM
                                                                                                     AND C.PCF_ID  = A.PCF_ID
                                                                                              INNER JOIN ASSET_WEIGHTED_AVG_PCF_BR D
                                                                                                      ON D.BAS_YM = A.BAS_YM
                                                                                                     AND D.PCF_ID = A.PCF_ID
                                                                GROUP BY A.BAS_YM, B.CBV_BR_CD
                                                               ) YY
                                                            ON YY.BAS_YM   >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM   <= XX.BAS_YM
                                                           AND YY.CBV_BR_CD = XX.CBV_BR_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '093' AS KPI_TYP_CD,
                  '7'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PROF_VS_LST_12_MM_AVG_LN_AMT, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, PROF_VS_LST_12_MM_AVG_LN_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_LN_AMT ASC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(PROF_VS_LST_12_MM_AVG_LN_AMT) AS PROF_VS_LST_12_MM_AVG_LN_AMT
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, PROF_VS_LST_12_MM_AVG_LN_AMT, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY PROF_VS_LST_12_MM_AVG_LN_AMT ASC NULLS LAST, CBV_BR_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               YY.PROF_VS_LST_12_MM_AVG_LN_AMT
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                                GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       B.CBV_BR_CD,
                                                                       SUM(CASE WHEN C.L12M_LN_AMT = 0 THEN 0
                                                                                ELSE ( A.TOT_INCM_AMT - A.TOT_EXP_AMT ) * 12 / ( C.L12M_LN_AMT ) * 100 * D.ASSET_WEIGHTED_AVG
                                                                                END)AS PROF_VS_LST_12_MM_AVG_LN_AMT
                                                                FROM   TM23_MMLY_INCM_EXP_A A INNER JOIN TM00_PCF_D B
                                                                                                      ON A.PCF_ID = B.PCF_ID
                                                                                              INNER JOIN TM27_MMLY_PCF_KEY_IDC_A C
                                                                                                      ON C.BAS_YM  = A.BAS_YM
                                                                                                     AND C.PCF_ID  = A.PCF_ID
                                                                                              INNER JOIN ASSET_WEIGHTED_AVG_PCF_BR D
                                                                                                      ON D.BAS_YM = A.BAS_YM
                                                                                                     AND D.PCF_ID = A.PCF_ID
                                                                GROUP BY A.BAS_YM, B.CBV_BR_CD
                                                               ) YY
                                                            ON YY.BAS_YM   >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM   <= XX.BAS_YM
                                                           AND YY.CBV_BR_CD = XX.CBV_BR_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '093' AS KPI_TYP_CD,
                  '8'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PROF_VS_LST_12_MM_AVG_LN_AMT, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, PROF_VS_LST_12_MM_AVG_LN_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_LN_AMT DESC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(PROF_VS_LST_12_MM_AVG_LN_AMT) AS PROF_VS_LST_12_MM_AVG_LN_AMT
                         FROM
                               (SELECT BAS_YM, BAS_YM1, PRVN_CD, PROF_VS_LST_12_MM_AVG_LN_AMT, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY PROF_VS_LST_12_MM_AVG_LN_AMT DESC NULLS LAST, PRVN_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.PRVN_CD,
                                               YY.PROF_VS_LST_12_MM_AVG_LN_AMT
                                        FROM   (SELECT T1.BAS_YM, T3.PRVN_CD
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                                                                    INNER JOIN TM00_LOC_D T3
                                                                                            ON T2.LOC_ID = T3.LOC_ID
                                                GROUP BY T1.BAS_YM, T3.PRVN_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       C.PRVN_CD,
                                                                       SUM(A.TOT_INCM_AMT - A.TOT_EXP_AMT) * 12 / SUM(D.L12M_LN_AMT) * 100 AS PROF_VS_LST_12_MM_AVG_LN_AMT
                                                                FROM   TM23_MMLY_INCM_EXP_A A INNER JOIN TM00_PCF_D B
                                                                                                      ON A.PCF_ID = B.PCF_ID
                                                                                              INNER JOIN TM00_LOC_D C
                                                                                                      ON B.LOC_ID = C.LOC_ID
                                                                                              INNER JOIN TM27_MMLY_PCF_KEY_IDC_A D
                                                                                                      ON D.BAS_YM  = A.BAS_YM
                                                                                                     AND D.PCF_ID  = A.PCF_ID
                                                                GROUP BY A.BAS_YM, C.PRVN_CD
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PRVN_CD  = XX.PRVN_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '093' AS KPI_TYP_CD,
                  '8'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PROF_VS_LST_12_MM_AVG_LN_AMT, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, PROF_VS_LST_12_MM_AVG_LN_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_LN_AMT ASC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(PROF_VS_LST_12_MM_AVG_LN_AMT) AS PROF_VS_LST_12_MM_AVG_LN_AMT
                         FROM
                               (SELECT BAS_YM, BAS_YM1, PRVN_CD, PROF_VS_LST_12_MM_AVG_LN_AMT, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY PROF_VS_LST_12_MM_AVG_LN_AMT ASC NULLS LAST, PRVN_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.PRVN_CD,
                                               YY.PROF_VS_LST_12_MM_AVG_LN_AMT
                                        FROM   (SELECT T1.BAS_YM, T3.PRVN_CD
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                                                                    INNER JOIN TM00_LOC_D T3
                                                                                            ON T2.LOC_ID = T3.LOC_ID
                                                GROUP BY T1.BAS_YM, T3.PRVN_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       C.PRVN_CD,
                                                                       SUM(A.TOT_INCM_AMT - A.TOT_EXP_AMT) * 12 / SUM(D.L12M_LN_AMT) * 100 AS PROF_VS_LST_12_MM_AVG_LN_AMT
                                                                FROM   TM23_MMLY_INCM_EXP_A A INNER JOIN TM00_PCF_D B
                                                                                                      ON A.PCF_ID = B.PCF_ID
                                                                                              INNER JOIN TM00_LOC_D C
                                                                                                      ON B.LOC_ID = C.LOC_ID
                                                                                              INNER JOIN TM27_MMLY_PCF_KEY_IDC_A D
                                                                                                      ON D.BAS_YM  = A.BAS_YM
                                                                                                     AND D.PCF_ID  = A.PCF_ID
                                                                GROUP BY A.BAS_YM, C.PRVN_CD
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PRVN_CD  = XX.PRVN_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '093' AS KPI_TYP_CD,
                  '8'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PROF_VS_LST_12_MM_AVG_LN_AMT, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, PROF_VS_LST_12_MM_AVG_LN_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_LN_AMT DESC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(PROF_VS_LST_12_MM_AVG_LN_AMT) AS PROF_VS_LST_12_MM_AVG_LN_AMT
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, PROF_VS_LST_12_MM_AVG_LN_AMT, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY PROF_VS_LST_12_MM_AVG_LN_AMT DESC NULLS LAST, CBV_BR_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               YY.PROF_VS_LST_12_MM_AVG_LN_AMT
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                                GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       B.CBV_BR_CD,
                                                                       SUM(A.TOT_INCM_AMT - A.TOT_EXP_AMT) * 12 / SUM(C.L12M_LN_AMT) * 100 AS PROF_VS_LST_12_MM_AVG_LN_AMT
                                                                FROM   TM23_MMLY_INCM_EXP_A A INNER JOIN TM00_PCF_D B
                                                                                                      ON A.PCF_ID = B.PCF_ID
                                                                                              INNER JOIN TM27_MMLY_PCF_KEY_IDC_A C
                                                                                                      ON C.BAS_YM  = A.BAS_YM
                                                                                                     AND C.PCF_ID  = A.PCF_ID
                                                                GROUP BY A.BAS_YM, B.CBV_BR_CD
                                                               ) YY
                                                            ON YY.BAS_YM   >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM   <= XX.BAS_YM
                                                           AND YY.CBV_BR_CD = XX.CBV_BR_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '093' AS KPI_TYP_CD,
                  '8'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PROF_VS_LST_12_MM_AVG_LN_AMT, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, PROF_VS_LST_12_MM_AVG_LN_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_LN_AMT ASC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(PROF_VS_LST_12_MM_AVG_LN_AMT) AS PROF_VS_LST_12_MM_AVG_LN_AMT
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, PROF_VS_LST_12_MM_AVG_LN_AMT, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY PROF_VS_LST_12_MM_AVG_LN_AMT ASC NULLS LAST, CBV_BR_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               YY.PROF_VS_LST_12_MM_AVG_LN_AMT
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                                GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       B.CBV_BR_CD,
                                                                       SUM(A.TOT_INCM_AMT - A.TOT_EXP_AMT) * 12 / SUM(C.L12M_LN_AMT) * 100 AS PROF_VS_LST_12_MM_AVG_LN_AMT
                                                                FROM   TM23_MMLY_INCM_EXP_A A INNER JOIN TM00_PCF_D B
                                                                                                      ON A.PCF_ID = B.PCF_ID
                                                                                              INNER JOIN TM27_MMLY_PCF_KEY_IDC_A C
                                                                                                      ON C.BAS_YM  = A.BAS_YM
                                                                                                     AND C.PCF_ID  = A.PCF_ID
                                                                GROUP BY A.BAS_YM, B.CBV_BR_CD
                                                               ) YY
                                                            ON YY.BAS_YM   >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM   <= XX.BAS_YM
                                                           AND YY.CBV_BR_CD = XX.CBV_BR_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )
          ),
          FROM_KPI_TYP_094 AS
          (SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '094' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(XX.PROF_VS_LST_12_MM_AVG_EQT_AMT, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, PROF_VS_LST_12_MM_AVG_EQT_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_EQT_AMT DESC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  SUM(T1.TOT_INCM_AMT - T1.TOT_EXP_AMT) * 12 / SUM(T4.L12M_EQT_AMT) * 100 AS PROF_VS_LST_12_MM_AVG_EQT_AMT
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN TM00_LOC_D T3
                                                                       ON T2.LOC_ID = T3.LOC_ID
                                                               INNER JOIN FROM_TM27_MMLY_PCF_KEY_IDC_A T4
                                                                       ON T4.BAS_YM  = T1.BAS_YM
                                                                      AND T4.PCF_ID  = T1.PCF_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '094' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(XX.PROF_VS_LST_12_MM_AVG_EQT_AMT, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, PROF_VS_LST_12_MM_AVG_EQT_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_EQT_AMT ASC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  SUM(T1.TOT_INCM_AMT - T1.TOT_EXP_AMT) * 12 / SUM(T4.L12M_EQT_AMT) * 100 AS PROF_VS_LST_12_MM_AVG_EQT_AMT
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN TM00_LOC_D T3
                                                                       ON T2.LOC_ID = T3.LOC_ID
                                                               INNER JOIN FROM_TM27_MMLY_PCF_KEY_IDC_A T4
                                                                       ON T4.BAS_YM  = T1.BAS_YM
                                                                      AND T4.PCF_ID  = T1.PCF_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '094' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  ROUND(XX.PROF_VS_LST_12_MM_AVG_EQT_AMT, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PROF_VS_LST_12_MM_AVG_EQT_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_EQT_AMT DESC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(T1.TOT_INCM_AMT - T1.TOT_EXP_AMT) * 12 / SUM(T3.L12M_EQT_AMT) * 100 AS PROF_VS_LST_12_MM_AVG_EQT_AMT
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN FROM_TM27_MMLY_PCF_KEY_IDC_A T3
                                                                       ON T3.BAS_YM  = T1.BAS_YM
                                                                      AND T3.PCF_ID  = T1.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '094' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  ROUND(XX.PROF_VS_LST_12_MM_AVG_EQT_AMT, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PROF_VS_LST_12_MM_AVG_EQT_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_EQT_AMT ASC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(T1.TOT_INCM_AMT - T1.TOT_EXP_AMT) * 12 / SUM(T3.L12M_EQT_AMT) * 100 AS PROF_VS_LST_12_MM_AVG_EQT_AMT
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN FROM_TM27_MMLY_PCF_KEY_IDC_A T3
                                                                       ON T3.BAS_YM  = T1.BAS_YM
                                                                      AND T3.PCF_ID  = T1.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '094' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(XX.PROF_VS_LST_12_MM_AVG_EQT_AMT, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, PROF_VS_LST_12_MM_AVG_EQT_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_EQT_AMT DESC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  AVG(CASE WHEN T4.L12M_EQT_AMT = 0 THEN 0
                                           ELSE ( T1.TOT_INCM_AMT - T1.TOT_EXP_AMT ) * 12 / ( T4.L12M_EQT_AMT ) * 100
                                           END) AS PROF_VS_LST_12_MM_AVG_EQT_AMT
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN TM00_LOC_D T3
                                                                       ON T2.LOC_ID = T3.LOC_ID
                                                               INNER JOIN FROM_TM27_MMLY_PCF_KEY_IDC_A T4
                                                                       ON T4.BAS_YM  = T1.BAS_YM
                                                                      AND T4.PCF_ID  = T1.PCF_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '094' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(XX.PROF_VS_LST_12_MM_AVG_EQT_AMT, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, PROF_VS_LST_12_MM_AVG_EQT_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_EQT_AMT ASC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  AVG(CASE WHEN T4.L12M_EQT_AMT = 0 THEN 0
                                           ELSE ( T1.TOT_INCM_AMT - T1.TOT_EXP_AMT ) * 12 / ( T4.L12M_EQT_AMT ) * 100
                                           END) AS PROF_VS_LST_12_MM_AVG_EQT_AMT
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN TM00_LOC_D T3
                                                                       ON T2.LOC_ID = T3.LOC_ID
                                                               INNER JOIN FROM_TM27_MMLY_PCF_KEY_IDC_A T4
                                                                       ON T4.BAS_YM  = T1.BAS_YM
                                                                      AND T4.PCF_ID  = T1.PCF_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '094' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  ROUND(XX.PROF_VS_LST_12_MM_AVG_EQT_AMT, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PROF_VS_LST_12_MM_AVG_EQT_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_EQT_AMT DESC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  AVG(CASE WHEN T3.L12M_EQT_AMT = 0 THEN 0
                                           ELSE ( T1.TOT_INCM_AMT - T1.TOT_EXP_AMT ) * 12 / ( T3.L12M_EQT_AMT ) * 100
                                           END) AS PROF_VS_LST_12_MM_AVG_EQT_AMT
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN FROM_TM27_MMLY_PCF_KEY_IDC_A T3
                                                                       ON T3.BAS_YM  = T1.BAS_YM
                                                                      AND T3.PCF_ID  = T1.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '094' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  ROUND(XX.PROF_VS_LST_12_MM_AVG_EQT_AMT, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PROF_VS_LST_12_MM_AVG_EQT_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_EQT_AMT ASC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  AVG(CASE WHEN T3.L12M_EQT_AMT = 0 THEN 0
                                           ELSE ( T1.TOT_INCM_AMT - T1.TOT_EXP_AMT ) * 12 / ( T3.L12M_EQT_AMT ) * 100
                                           END) AS PROF_VS_LST_12_MM_AVG_EQT_AMT
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN FROM_TM27_MMLY_PCF_KEY_IDC_A T3
                                                                       ON T3.BAS_YM  = T1.BAS_YM
                                                                      AND T3.PCF_ID  = T1.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '094' AS KPI_TYP_CD,
                  '4'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PROF_VS_LST_12_MM_AVG_EQT_AMT, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, PROF_VS_LST_12_MM_AVG_EQT_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_EQT_AMT DESC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  SUM(CASE WHEN T4.L12M_EQT_AMT = 0 THEN 0
                                           ELSE ( T1.TOT_INCM_AMT - T1.TOT_EXP_AMT ) * 12 / ( T4.L12M_EQT_AMT ) * 100 * T5.ASSET_WEIGHTED_AVG
                                           END) AS PROF_VS_LST_12_MM_AVG_EQT_AMT
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN TM00_LOC_D T3
                                                                       ON T2.LOC_ID = T3.LOC_ID
                                                               INNER JOIN FROM_TM27_MMLY_PCF_KEY_IDC_A T4
                                                                       ON T4.BAS_YM  = T1.BAS_YM
                                                                      AND T4.PCF_ID  = T1.PCF_ID
                                                               INNER JOIN ASSET_WA_PCF_PRVN_094 T5
                                                                       ON T5.BAS_YM = T1.BAS_YM
                                                                      AND T5.PCF_ID = T1.PCF_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   )
           WHERE RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '094' AS KPI_TYP_CD,
                  '4'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PROF_VS_LST_12_MM_AVG_EQT_AMT, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, PROF_VS_LST_12_MM_AVG_EQT_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_EQT_AMT ASC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  SUM(CASE WHEN T4.L12M_EQT_AMT = 0 THEN 0
                                           ELSE ( T1.TOT_INCM_AMT - T1.TOT_EXP_AMT ) * 12 / ( T4.L12M_EQT_AMT ) * 100 * T5.ASSET_WEIGHTED_AVG
                                           END) AS PROF_VS_LST_12_MM_AVG_EQT_AMT
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN TM00_LOC_D T3
                                                                       ON T2.LOC_ID = T3.LOC_ID
                                                               INNER JOIN FROM_TM27_MMLY_PCF_KEY_IDC_A T4
                                                                       ON T4.BAS_YM  = T1.BAS_YM
                                                                      AND T4.PCF_ID  = T1.PCF_ID
                                                               INNER JOIN ASSET_WA_PCF_PRVN_094 T5
                                                                       ON T5.BAS_YM = T1.BAS_YM
                                                                      AND T5.PCF_ID = T1.PCF_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   )
           WHERE RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '094' AS KPI_TYP_CD,
                  '4'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  CBV_BR_CD,
                  ROUND(PROF_VS_LST_12_MM_AVG_EQT_AMT, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PROF_VS_LST_12_MM_AVG_EQT_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_EQT_AMT DESC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(CASE WHEN T3.L12M_EQT_AMT = 0 THEN 0
                                           ELSE ( T1.TOT_INCM_AMT - T1.TOT_EXP_AMT ) * 12 / ( T3.L12M_EQT_AMT ) * 100 * T4.ASSET_WEIGHTED_AVG
                                           END)AS PROF_VS_LST_12_MM_AVG_EQT_AMT
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN FROM_TM27_MMLY_PCF_KEY_IDC_A T3
                                                                       ON T3.BAS_YM  = T1.BAS_YM
                                                                      AND T3.PCF_ID  = T1.PCF_ID
                                                               INNER JOIN ASSET_WA_PCF_BR_094 T4
                                                                       ON T4.BAS_YM = T1.BAS_YM
                                                                      AND T4.PCF_ID = T1.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   )
           WHERE RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '094' AS KPI_TYP_CD,
                  '4'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  CBV_BR_CD,
                  ROUND(PROF_VS_LST_12_MM_AVG_EQT_AMT, 2) AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, PROF_VS_LST_12_MM_AVG_EQT_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_EQT_AMT ASC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(CASE WHEN T3.L12M_EQT_AMT = 0 THEN 0
                                           ELSE ( T1.TOT_INCM_AMT - T1.TOT_EXP_AMT ) * 12 / ( T3.L12M_EQT_AMT ) * 100 * T4.ASSET_WEIGHTED_AVG
                                           END)AS PROF_VS_LST_12_MM_AVG_EQT_AMT
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN FROM_TM27_MMLY_PCF_KEY_IDC_A T3
                                                                       ON T3.BAS_YM  = T1.BAS_YM
                                                                      AND T3.PCF_ID  = T1.PCF_ID
                                                               INNER JOIN ASSET_WA_PCF_BR_094 T4
                                                                       ON T4.BAS_YM = T1.BAS_YM
                                                                      AND T4.PCF_ID = T1.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   )
           WHERE RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '094' AS KPI_TYP_CD,
                  '5'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PROF_VS_LST_12_MM_AVG_EQT_AMT, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, PROF_VS_LST_12_MM_AVG_EQT_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_EQT_AMT DESC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(PROF_VS_LST_12_MM_AVG_EQT_AMT) AS PROF_VS_LST_12_MM_AVG_EQT_AMT
                         FROM
                               (SELECT BAS_YM, BAS_YM1, PRVN_CD, PROF_VS_LST_12_MM_AVG_EQT_AMT, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY PROF_VS_LST_12_MM_AVG_EQT_AMT DESC NULLS LAST, PRVN_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.PRVN_CD,
                                               YY.PROF_VS_LST_12_MM_AVG_EQT_AMT
                                        FROM   (SELECT T1.BAS_YM, T3.PRVN_CD
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                                                                    INNER JOIN TM00_LOC_D T3
                                                                                            ON T2.LOC_ID = T3.LOC_ID
                                                GROUP BY T1.BAS_YM, T3.PRVN_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       C.PRVN_CD,
                                                                       AVG(CASE WHEN D.L12M_EQT_AMT = 0 THEN 0
                                                                                ELSE ( A.TOT_INCM_AMT - A.TOT_EXP_AMT ) * 12 / ( D.L12M_EQT_AMT ) * 100
                                                                                END) AS PROF_VS_LST_12_MM_AVG_EQT_AMT
                                                                FROM   TM23_MMLY_INCM_EXP_A A INNER JOIN TM00_PCF_D B
                                                                                                      ON A.PCF_ID = B.PCF_ID
                                                                                              INNER JOIN TM00_LOC_D C
                                                                                                      ON B.LOC_ID = C.LOC_ID
                                                                                              INNER JOIN TM27_MMLY_PCF_KEY_IDC_A D
                                                                                                      ON D.BAS_YM  = A.BAS_YM
                                                                                                     AND D.PCF_ID  = A.PCF_ID
                                                                GROUP BY A.BAS_YM, C.PRVN_CD
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PRVN_CD  = XX.PRVN_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '094' AS KPI_TYP_CD,
                  '5'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PROF_VS_LST_12_MM_AVG_EQT_AMT, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, PROF_VS_LST_12_MM_AVG_EQT_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_EQT_AMT ASC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(PROF_VS_LST_12_MM_AVG_EQT_AMT) AS PROF_VS_LST_12_MM_AVG_EQT_AMT
                         FROM
                               (SELECT BAS_YM, BAS_YM1, PRVN_CD, PROF_VS_LST_12_MM_AVG_EQT_AMT, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY PROF_VS_LST_12_MM_AVG_EQT_AMT ASC NULLS LAST, PRVN_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.PRVN_CD,
                                               YY.PROF_VS_LST_12_MM_AVG_EQT_AMT
                                        FROM   (SELECT T1.BAS_YM, T3.PRVN_CD
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                                                                    INNER JOIN TM00_LOC_D T3
                                                                                            ON T2.LOC_ID = T3.LOC_ID
                                                GROUP BY T1.BAS_YM, T3.PRVN_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       C.PRVN_CD,
                                                                       AVG(CASE WHEN D.L12M_EQT_AMT = 0 THEN 0
                                                                                ELSE ( A.TOT_INCM_AMT - A.TOT_EXP_AMT ) * 12 / ( D.L12M_EQT_AMT ) * 100
                                                                                END) AS PROF_VS_LST_12_MM_AVG_EQT_AMT
                                                                FROM   TM23_MMLY_INCM_EXP_A A INNER JOIN TM00_PCF_D B
                                                                                                      ON A.PCF_ID = B.PCF_ID
                                                                                              INNER JOIN TM00_LOC_D C
                                                                                                      ON B.LOC_ID = C.LOC_ID
                                                                                              INNER JOIN TM27_MMLY_PCF_KEY_IDC_A D
                                                                                                      ON D.BAS_YM  = A.BAS_YM
                                                                                                     AND D.PCF_ID  = A.PCF_ID
                                                                GROUP BY A.BAS_YM, C.PRVN_CD
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PRVN_CD  = XX.PRVN_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '094' AS KPI_TYP_CD,
                  '5'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PROF_VS_LST_12_MM_AVG_EQT_AMT, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, PROF_VS_LST_12_MM_AVG_EQT_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_EQT_AMT DESC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(PROF_VS_LST_12_MM_AVG_EQT_AMT) AS PROF_VS_LST_12_MM_AVG_EQT_AMT
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, PROF_VS_LST_12_MM_AVG_EQT_AMT, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY PROF_VS_LST_12_MM_AVG_EQT_AMT DESC NULLS LAST, CBV_BR_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               YY.PROF_VS_LST_12_MM_AVG_EQT_AMT
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
                                                                                                      ON A.PCF_ID = B.PCF_ID
                                                                                              INNER JOIN TM27_MMLY_PCF_KEY_IDC_A C
                                                                                                      ON C.BAS_YM  = A.BAS_YM
                                                                                                     AND C.PCF_ID  = A.PCF_ID
                                                                GROUP BY A.BAS_YM, B.CBV_BR_CD
                                                               ) YY
                                                            ON YY.BAS_YM   >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM   <= XX.BAS_YM
                                                           AND YY.CBV_BR_CD = XX.CBV_BR_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '094' AS KPI_TYP_CD,
                  '5'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PROF_VS_LST_12_MM_AVG_EQT_AMT, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, PROF_VS_LST_12_MM_AVG_EQT_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_EQT_AMT ASC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(PROF_VS_LST_12_MM_AVG_EQT_AMT) AS PROF_VS_LST_12_MM_AVG_EQT_AMT
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, PROF_VS_LST_12_MM_AVG_EQT_AMT, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY PROF_VS_LST_12_MM_AVG_EQT_AMT ASC NULLS LAST, CBV_BR_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               YY.PROF_VS_LST_12_MM_AVG_EQT_AMT
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
                                                                                                      ON A.PCF_ID = B.PCF_ID
                                                                                              INNER JOIN TM27_MMLY_PCF_KEY_IDC_A C
                                                                                                      ON C.BAS_YM  = A.BAS_YM
                                                                                                     AND C.PCF_ID  = A.PCF_ID
                                                                GROUP BY A.BAS_YM, B.CBV_BR_CD
                                                               ) YY
                                                            ON YY.BAS_YM   >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM   <= XX.BAS_YM
                                                           AND YY.CBV_BR_CD = XX.CBV_BR_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '094' AS KPI_TYP_CD,
                  '7'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PROF_VS_LST_12_MM_AVG_EQT_AMT, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, PROF_VS_LST_12_MM_AVG_EQT_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_EQT_AMT DESC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(PROF_VS_LST_12_MM_AVG_EQT_AMT) AS PROF_VS_LST_12_MM_AVG_EQT_AMT
                         FROM
                               (SELECT BAS_YM, BAS_YM1, PRVN_CD, PROF_VS_LST_12_MM_AVG_EQT_AMT, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY PROF_VS_LST_12_MM_AVG_EQT_AMT DESC NULLS LAST, PRVN_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.PRVN_CD,
                                               YY.PROF_VS_LST_12_MM_AVG_EQT_AMT
                                        FROM   (SELECT T1.BAS_YM, T3.PRVN_CD
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                                                                    INNER JOIN TM00_LOC_D T3
                                                                                            ON T2.LOC_ID = T3.LOC_ID
                                                GROUP BY T1.BAS_YM, T3.PRVN_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       C.PRVN_CD,
                                                                       SUM(CASE WHEN D.L12M_EQT_AMT = 0 THEN 0
                                                                                ELSE ( A.TOT_INCM_AMT - A.TOT_EXP_AMT ) * 12 / ( D.L12M_EQT_AMT ) * 100 * E.ASSET_WEIGHTED_AVG
                                                                                END) AS PROF_VS_LST_12_MM_AVG_EQT_AMT
                                                                FROM   TM23_MMLY_INCM_EXP_A A INNER JOIN TM00_PCF_D B
                                                                                                      ON A.PCF_ID = B.PCF_ID
                                                                                              INNER JOIN TM00_LOC_D C
                                                                                                      ON B.LOC_ID = C.LOC_ID
                                                                                              INNER JOIN TM27_MMLY_PCF_KEY_IDC_A D
                                                                                                      ON D.BAS_YM  = A.BAS_YM
                                                                                                     AND D.PCF_ID  = A.PCF_ID
                                                                                              INNER JOIN ASSET_WA_PCF_PRVN_094 E
                                                                                                      ON E.BAS_YM = A.BAS_YM
                                                                                                     AND E.PCF_ID = A.PCF_ID
                                                                GROUP BY A.BAS_YM, C.PRVN_CD
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PRVN_CD  = XX.PRVN_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '094' AS KPI_TYP_CD,
                  '7'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PROF_VS_LST_12_MM_AVG_EQT_AMT, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, PROF_VS_LST_12_MM_AVG_EQT_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_EQT_AMT ASC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(PROF_VS_LST_12_MM_AVG_EQT_AMT) AS PROF_VS_LST_12_MM_AVG_EQT_AMT
                         FROM
                               (SELECT BAS_YM, BAS_YM1, PRVN_CD, PROF_VS_LST_12_MM_AVG_EQT_AMT, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY PROF_VS_LST_12_MM_AVG_EQT_AMT ASC NULLS LAST, PRVN_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.PRVN_CD,
                                               YY.PROF_VS_LST_12_MM_AVG_EQT_AMT
                                        FROM   (SELECT T1.BAS_YM, T3.PRVN_CD
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                                                                    INNER JOIN TM00_LOC_D T3
                                                                                            ON T2.LOC_ID = T3.LOC_ID
                                                GROUP BY T1.BAS_YM, T3.PRVN_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       C.PRVN_CD,
                                                                       SUM(CASE WHEN D.L12M_EQT_AMT = 0 THEN 0
                                                                                ELSE ( A.TOT_INCM_AMT - A.TOT_EXP_AMT ) * 12 / ( D.L12M_EQT_AMT ) * 100 * E.ASSET_WEIGHTED_AVG
                                                                                END) AS PROF_VS_LST_12_MM_AVG_EQT_AMT
                                                                FROM   TM23_MMLY_INCM_EXP_A A INNER JOIN TM00_PCF_D B
                                                                                                      ON A.PCF_ID = B.PCF_ID
                                                                                              INNER JOIN TM00_LOC_D C
                                                                                                      ON B.LOC_ID = C.LOC_ID
                                                                                              INNER JOIN TM27_MMLY_PCF_KEY_IDC_A D
                                                                                                      ON D.BAS_YM  = A.BAS_YM
                                                                                                     AND D.PCF_ID  = A.PCF_ID
                                                                                              INNER JOIN ASSET_WA_PCF_PRVN_094 E
                                                                                                      ON E.BAS_YM = A.BAS_YM
                                                                                                     AND E.PCF_ID = A.PCF_ID
                                                                GROUP BY A.BAS_YM, C.PRVN_CD
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PRVN_CD  = XX.PRVN_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '094' AS KPI_TYP_CD,
                  '7'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PROF_VS_LST_12_MM_AVG_EQT_AMT, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, PROF_VS_LST_12_MM_AVG_EQT_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_EQT_AMT DESC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(PROF_VS_LST_12_MM_AVG_EQT_AMT) AS PROF_VS_LST_12_MM_AVG_EQT_AMT
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, PROF_VS_LST_12_MM_AVG_EQT_AMT, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY PROF_VS_LST_12_MM_AVG_EQT_AMT DESC NULLS LAST, CBV_BR_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               YY.PROF_VS_LST_12_MM_AVG_EQT_AMT
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                                GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       B.CBV_BR_CD,
                                                                       SUM(CASE WHEN C.L12M_EQT_AMT = 0 THEN 0
                                                                                ELSE ( A.TOT_INCM_AMT - A.TOT_EXP_AMT ) * 12 / ( C.L12M_EQT_AMT ) * 100 * D.ASSET_WEIGHTED_AVG
                                                                                END)AS PROF_VS_LST_12_MM_AVG_EQT_AMT
                                                                FROM   TM23_MMLY_INCM_EXP_A A INNER JOIN TM00_PCF_D B
                                                                                                      ON A.PCF_ID = B.PCF_ID
                                                                                              INNER JOIN TM27_MMLY_PCF_KEY_IDC_A C
                                                                                                      ON C.BAS_YM  = A.BAS_YM
                                                                                                     AND C.PCF_ID  = A.PCF_ID
                                                                                              INNER JOIN ASSET_WA_PCF_BR_094 D
                                                                                                      ON D.BAS_YM = A.BAS_YM
                                                                                                     AND D.PCF_ID = A.PCF_ID
                                                                GROUP BY A.BAS_YM, B.CBV_BR_CD
                                                               ) YY
                                                            ON YY.BAS_YM   >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM   <= XX.BAS_YM
                                                           AND YY.CBV_BR_CD = XX.CBV_BR_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '094' AS KPI_TYP_CD,
                  '7'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PROF_VS_LST_12_MM_AVG_EQT_AMT, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, PROF_VS_LST_12_MM_AVG_EQT_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_EQT_AMT ASC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(PROF_VS_LST_12_MM_AVG_EQT_AMT) AS PROF_VS_LST_12_MM_AVG_EQT_AMT
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, PROF_VS_LST_12_MM_AVG_EQT_AMT, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY PROF_VS_LST_12_MM_AVG_EQT_AMT ASC NULLS LAST, CBV_BR_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               YY.PROF_VS_LST_12_MM_AVG_EQT_AMT
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                                GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       B.CBV_BR_CD,
                                                                       SUM(CASE WHEN C.L12M_EQT_AMT = 0 THEN 0
                                                                                ELSE ( A.TOT_INCM_AMT - A.TOT_EXP_AMT ) * 12 / ( C.L12M_EQT_AMT ) * 100 * D.ASSET_WEIGHTED_AVG
                                                                                END)AS PROF_VS_LST_12_MM_AVG_EQT_AMT
                                                                FROM   TM23_MMLY_INCM_EXP_A A INNER JOIN TM00_PCF_D B
                                                                                                      ON A.PCF_ID = B.PCF_ID
                                                                                              INNER JOIN TM27_MMLY_PCF_KEY_IDC_A C
                                                                                                      ON C.BAS_YM  = A.BAS_YM
                                                                                                     AND C.PCF_ID  = A.PCF_ID
                                                                                              INNER JOIN ASSET_WA_PCF_BR_094 D
                                                                                                      ON D.BAS_YM = A.BAS_YM
                                                                                                     AND D.PCF_ID = A.PCF_ID
                                                                GROUP BY A.BAS_YM, B.CBV_BR_CD
                                                               ) YY
                                                            ON YY.BAS_YM   >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM   <= XX.BAS_YM
                                                           AND YY.CBV_BR_CD = XX.CBV_BR_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '094' AS KPI_TYP_CD,
                  '8'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PROF_VS_LST_12_MM_AVG_EQT_AMT, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, PROF_VS_LST_12_MM_AVG_EQT_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_EQT_AMT DESC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(PROF_VS_LST_12_MM_AVG_EQT_AMT) AS PROF_VS_LST_12_MM_AVG_EQT_AMT
                         FROM
                               (SELECT BAS_YM, BAS_YM1, PRVN_CD, PROF_VS_LST_12_MM_AVG_EQT_AMT, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY PROF_VS_LST_12_MM_AVG_EQT_AMT DESC NULLS LAST, PRVN_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.PRVN_CD,
                                               YY.PROF_VS_LST_12_MM_AVG_EQT_AMT
                                        FROM   (SELECT T1.BAS_YM, T3.PRVN_CD
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                                                                    INNER JOIN TM00_LOC_D T3
                                                                                            ON T2.LOC_ID = T3.LOC_ID
                                                GROUP BY T1.BAS_YM, T3.PRVN_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       C.PRVN_CD,
                                                                       SUM(A.TOT_INCM_AMT - A.TOT_EXP_AMT) * 12 / SUM(D.L12M_EQT_AMT) * 100 AS PROF_VS_LST_12_MM_AVG_EQT_AMT
                                                                FROM   TM23_MMLY_INCM_EXP_A A INNER JOIN TM00_PCF_D B
                                                                                                      ON A.PCF_ID = B.PCF_ID
                                                                                              INNER JOIN TM00_LOC_D C
                                                                                                      ON B.LOC_ID = C.LOC_ID
                                                                                              INNER JOIN TM27_MMLY_PCF_KEY_IDC_A D
                                                                                                      ON D.BAS_YM  = A.BAS_YM
                                                                                                     AND D.PCF_ID  = A.PCF_ID
                                                                GROUP BY A.BAS_YM, C.PRVN_CD
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PRVN_CD  = XX.PRVN_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '094' AS KPI_TYP_CD,
                  '8'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PROF_VS_LST_12_MM_AVG_EQT_AMT, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, PROF_VS_LST_12_MM_AVG_EQT_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_EQT_AMT ASC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(PROF_VS_LST_12_MM_AVG_EQT_AMT) AS PROF_VS_LST_12_MM_AVG_EQT_AMT
                         FROM
                               (SELECT BAS_YM, BAS_YM1, PRVN_CD, PROF_VS_LST_12_MM_AVG_EQT_AMT, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY PROF_VS_LST_12_MM_AVG_EQT_AMT ASC NULLS LAST, PRVN_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.PRVN_CD,
                                               YY.PROF_VS_LST_12_MM_AVG_EQT_AMT
                                        FROM   (SELECT T1.BAS_YM, T3.PRVN_CD
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                                                                    INNER JOIN TM00_LOC_D T3
                                                                                            ON T2.LOC_ID = T3.LOC_ID
                                                GROUP BY T1.BAS_YM, T3.PRVN_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       C.PRVN_CD,
                                                                       SUM(A.TOT_INCM_AMT - A.TOT_EXP_AMT) * 12 / SUM(D.L12M_EQT_AMT) * 100 AS PROF_VS_LST_12_MM_AVG_EQT_AMT
                                                                FROM   TM23_MMLY_INCM_EXP_A A INNER JOIN TM00_PCF_D B
                                                                                                      ON A.PCF_ID = B.PCF_ID
                                                                                              INNER JOIN TM00_LOC_D C
                                                                                                      ON B.LOC_ID = C.LOC_ID
                                                                                              INNER JOIN TM27_MMLY_PCF_KEY_IDC_A D
                                                                                                      ON D.BAS_YM  = A.BAS_YM
                                                                                                     AND D.PCF_ID  = A.PCF_ID
                                                                GROUP BY A.BAS_YM, C.PRVN_CD
                                                               ) YY
                                                            ON YY.BAS_YM  >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM  <= XX.BAS_YM
                                                           AND YY.PRVN_CD  = XX.PRVN_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '094' AS KPI_TYP_CD,
                  '8'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PROF_VS_LST_12_MM_AVG_EQT_AMT, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, PROF_VS_LST_12_MM_AVG_EQT_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_EQT_AMT DESC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(PROF_VS_LST_12_MM_AVG_EQT_AMT) AS PROF_VS_LST_12_MM_AVG_EQT_AMT
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, PROF_VS_LST_12_MM_AVG_EQT_AMT, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY PROF_VS_LST_12_MM_AVG_EQT_AMT DESC NULLS LAST, CBV_BR_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               YY.PROF_VS_LST_12_MM_AVG_EQT_AMT
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                                GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       B.CBV_BR_CD,
                                                                       SUM(A.TOT_INCM_AMT - A.TOT_EXP_AMT) * 12 / SUM(C.L12M_EQT_AMT) * 100 AS PROF_VS_LST_12_MM_AVG_EQT_AMT
                                                                FROM   TM23_MMLY_INCM_EXP_A A INNER JOIN TM00_PCF_D B
                                                                                                      ON A.PCF_ID = B.PCF_ID
                                                                                              INNER JOIN TM27_MMLY_PCF_KEY_IDC_A C
                                                                                                      ON C.BAS_YM  = A.BAS_YM
                                                                                                     AND C.PCF_ID  = A.PCF_ID
                                                                GROUP BY A.BAS_YM, B.CBV_BR_CD
                                                               ) YY
                                                            ON YY.BAS_YM   >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM   <= XX.BAS_YM
                                                           AND YY.CBV_BR_CD = XX.CBV_BR_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )

           UNION ALL

           SELECT BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '094' AS KPI_TYP_CD,
                  '8'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  RNK_NUM,
                  NULL  AS PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  ROUND(PROF_VS_LST_12_MM_AVG_EQT_AMT, 2) AS AGT_VAL
           FROM  (SELECT BAS_YM, PROF_VS_LST_12_MM_AVG_EQT_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY PROF_VS_LST_12_MM_AVG_EQT_AMT ASC NULLS LAST, INI_RNK_NUM DESC) AS RNK_NUM
                  FROM  (SELECT BAS_YM,
                                INI_RNK_NUM,
                                AVG(PROF_VS_LST_12_MM_AVG_EQT_AMT) AS PROF_VS_LST_12_MM_AVG_EQT_AMT
                         FROM
                               (SELECT BAS_YM, BAS_YM1, CBV_BR_CD, PROF_VS_LST_12_MM_AVG_EQT_AMT, RANK() OVER(PARTITION BY BAS_YM, BAS_YM1 ORDER BY PROF_VS_LST_12_MM_AVG_EQT_AMT ASC NULLS LAST, CBV_BR_CD DESC) AS INI_RNK_NUM
                                FROM   (SELECT XX.BAS_YM,
                                               YY.BAS_YM AS BAS_YM1,
                                               XX.CBV_BR_CD,
                                               YY.PROF_VS_LST_12_MM_AVG_EQT_AMT
                                        FROM   (SELECT T1.BAS_YM, T2.CBV_BR_CD
                                                FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                                            ON T1.PCF_ID = T2.PCF_ID
                                                GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                                               ) XX INNER JOIN (SELECT A.BAS_YM,
                                                                       B.CBV_BR_CD,
                                                                       SUM(A.TOT_INCM_AMT - A.TOT_EXP_AMT) * 12 / SUM(C.L12M_EQT_AMT) * 100 AS PROF_VS_LST_12_MM_AVG_EQT_AMT
                                                                FROM   TM23_MMLY_INCM_EXP_A A INNER JOIN TM00_PCF_D B
                                                                                                      ON A.PCF_ID = B.PCF_ID
                                                                                              INNER JOIN TM27_MMLY_PCF_KEY_IDC_A C
                                                                                                      ON C.BAS_YM  = A.BAS_YM
                                                                                                     AND C.PCF_ID  = A.PCF_ID
                                                                GROUP BY A.BAS_YM, B.CBV_BR_CD
                                                               ) YY
                                                            ON YY.BAS_YM   >= TO_CHAR(ADD_MONTHS(TO_DATE(XX.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                           AND YY.BAS_YM   <= XX.BAS_YM
                                                           AND YY.CBV_BR_CD = XX.CBV_BR_CD
                                       )
                               )
                         WHERE INI_RNK_NUM BETWEEN 1 AND 3
                         GROUP BY BAS_YM, INI_RNK_NUM
                        )
                 )
          ),
          FROM_KPI_TYP_133 AS
          (SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '133' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  XX.WIT_COLL_LN_BAL AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, WIT_COLL_LN_BAL, RANK() OVER(PARTITION BY BAS_YM ORDER BY WIT_COLL_LN_BAL DESC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  SUM(T1.WIT_COLL_LN_BAL) AS WIT_COLL_LN_BAL
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                                                                  INNER JOIN TM00_LOC_D T3
                                                                          ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '133' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  XX.WIT_COLL_LN_BAL AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, WIT_COLL_LN_BAL, RANK() OVER(PARTITION BY BAS_YM ORDER BY WIT_COLL_LN_BAL ASC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  SUM(T1.WIT_COLL_LN_BAL) AS WIT_COLL_LN_BAL
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                                                                  INNER JOIN TM00_LOC_D T3
                                                                          ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '133' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.WIT_COLL_LN_BAL AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, WIT_COLL_LN_BAL, RANK() OVER(PARTITION BY BAS_YM ORDER BY WIT_COLL_LN_BAL DESC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(T1.WIT_COLL_LN_BAL) AS WIT_COLL_LN_BAL
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '133' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.WIT_COLL_LN_BAL AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, WIT_COLL_LN_BAL, RANK() OVER(PARTITION BY BAS_YM ORDER BY WIT_COLL_LN_BAL ASC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(T1.WIT_COLL_LN_BAL) AS WIT_COLL_LN_BAL
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '133' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  XX.WIT_COLL_LN_BAL AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, WIT_COLL_LN_BAL, RANK() OVER(PARTITION BY BAS_YM ORDER BY WIT_COLL_LN_BAL DESC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  ROUND(AVG(T1.WIT_COLL_LN_BAL), 0) AS WIT_COLL_LN_BAL
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                                                                  INNER JOIN TM00_LOC_D T3
                                                                          ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '133' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  XX.WIT_COLL_LN_BAL AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, WIT_COLL_LN_BAL, RANK() OVER(PARTITION BY BAS_YM ORDER BY WIT_COLL_LN_BAL ASC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  ROUND(AVG(T1.WIT_COLL_LN_BAL), 0) AS WIT_COLL_LN_BAL
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                                                                  INNER JOIN TM00_LOC_D T3
                                                                          ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '133' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.WIT_COLL_LN_BAL AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, WIT_COLL_LN_BAL, RANK() OVER(PARTITION BY BAS_YM ORDER BY WIT_COLL_LN_BAL DESC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  ROUND(AVG(T1.WIT_COLL_LN_BAL), 0) AS WIT_COLL_LN_BAL
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '133' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.WIT_COLL_LN_BAL AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, WIT_COLL_LN_BAL, RANK() OVER(PARTITION BY BAS_YM ORDER BY WIT_COLL_LN_BAL ASC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  ROUND(AVG(T1.WIT_COLL_LN_BAL), 0) AS WIT_COLL_LN_BAL
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3
          ),
          FROM_KPI_TYP_135 AS
          (SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '135' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.CBV_STBOR_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, CBV_STBOR_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY CBV_STBOR_AMT DESC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(T1.CBV_STBOR_AMT) AS CBV_STBOR_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '135' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.CBV_STBOR_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, CBV_STBOR_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY CBV_STBOR_AMT ASC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(T1.CBV_STBOR_AMT) AS CBV_STBOR_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3
          ),
          FROM_KPI_TYP_136 AS
          (SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '136' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.CBV_MED_LT_BOR_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, CBV_MED_LT_BOR_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY CBV_MED_LT_BOR_AMT DESC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(T1.CBV_MED_LT_BOR_AMT) AS CBV_MED_LT_BOR_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '136' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.CBV_MED_LT_BOR_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, CBV_MED_LT_BOR_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY CBV_MED_LT_BOR_AMT ASC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(T1.CBV_MED_LT_BOR_AMT) AS CBV_MED_LT_BOR_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3
          ),
          FROM_KPI_TYP_137 AS
          (SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '137' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.CBV_SHRT_TRM_SVG_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, CBV_SHRT_TRM_SVG_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY CBV_SHRT_TRM_SVG_AMT DESC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(T1.CBV_SHRT_TRM_SVG_AMT) AS CBV_SHRT_TRM_SVG_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '137' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.CBV_SHRT_TRM_SVG_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, CBV_SHRT_TRM_SVG_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY CBV_SHRT_TRM_SVG_AMT ASC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(T1.CBV_SHRT_TRM_SVG_AMT) AS CBV_SHRT_TRM_SVG_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3
          ),
          FROM_KPI_TYP_138 AS
          (SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '138' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.CBV_MED_LT_SVG_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, CBV_MED_LT_SVG_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY CBV_MED_LT_SVG_AMT DESC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(T1.CBV_MED_LT_SVG_AMT) AS CBV_MED_LT_SVG_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '138' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.CBV_MED_LT_SVG_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, CBV_MED_LT_SVG_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY CBV_MED_LT_SVG_AMT ASC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(T1.CBV_MED_LT_SVG_AMT) AS CBV_MED_LT_SVG_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3
          ),
          FROM_KPI_TYP_141 AS
          (SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '141' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.CBV_BOR_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, CBV_BOR_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY CBV_BOR_AMT DESC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(T1.CBV_BOR_AMT) AS CBV_BOR_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '141' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.CBV_BOR_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, CBV_BOR_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY CBV_BOR_AMT ASC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(T1.CBV_BOR_AMT) AS CBV_BOR_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3
          ),
          FROM_KPI_TYP_142 AS
          (SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '142' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.CBV_TOT_SVG_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, CBV_TOT_SVG_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY CBV_TOT_SVG_AMT DESC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(T1.CBV_SHRT_TRM_SVG_AMT + T1.CBV_MED_LT_SVG_AMT) AS CBV_TOT_SVG_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '142' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.CBV_TOT_SVG_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, CBV_TOT_SVG_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY CBV_TOT_SVG_AMT ASC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(T1.CBV_SHRT_TRM_SVG_AMT + T1.CBV_MED_LT_SVG_AMT) AS CBV_TOT_SVG_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3
          ),
          FROM_KPI_TYP_143 AS
          (SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '143' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  XX.CUR_NET_PROFIT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, CUR_NET_PROFIT, RANK() OVER(PARTITION BY BAS_YM ORDER BY CUR_NET_PROFIT DESC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  SUM(T1.CUR_TOT_INCM_AMT - T1.CUR_TOT_EXP_AMT) AS CUR_NET_PROFIT
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN TM00_LOC_D T3
                                                                       ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '143' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  XX.CUR_NET_PROFIT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, CUR_NET_PROFIT, RANK() OVER(PARTITION BY BAS_YM ORDER BY CUR_NET_PROFIT ASC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  SUM(T1.CUR_TOT_INCM_AMT - T1.CUR_TOT_EXP_AMT) AS CUR_NET_PROFIT
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN TM00_LOC_D T3
                                                                       ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '143' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.CUR_NET_PROFIT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, CUR_NET_PROFIT, RANK() OVER(PARTITION BY BAS_YM ORDER BY CUR_NET_PROFIT DESC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(T1.CUR_TOT_INCM_AMT - T1.CUR_TOT_EXP_AMT) AS CUR_NET_PROFIT
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '143' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.CUR_NET_PROFIT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, CUR_NET_PROFIT, RANK() OVER(PARTITION BY BAS_YM ORDER BY CUR_NET_PROFIT ASC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(T1.CUR_TOT_INCM_AMT - T1.CUR_TOT_EXP_AMT) AS CUR_NET_PROFIT
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '143' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  XX.CUR_NET_PROFIT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, CUR_NET_PROFIT, RANK() OVER(PARTITION BY BAS_YM ORDER BY CUR_NET_PROFIT DESC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  ROUND(AVG(T1.CUR_TOT_INCM_AMT - T1.CUR_TOT_EXP_AMT), 0) AS CUR_NET_PROFIT
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN TM00_LOC_D T3
                                                                       ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '143' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  XX.CUR_NET_PROFIT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, CUR_NET_PROFIT, RANK() OVER(PARTITION BY BAS_YM ORDER BY CUR_NET_PROFIT ASC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  ROUND(AVG(T1.CUR_TOT_INCM_AMT - T1.CUR_TOT_EXP_AMT), 0) AS CUR_NET_PROFIT
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                                                               INNER JOIN TM00_LOC_D T3
                                                                       ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '143' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.CUR_NET_PROFIT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, CUR_NET_PROFIT, RANK() OVER(PARTITION BY BAS_YM ORDER BY CUR_NET_PROFIT DESC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  ROUND(AVG(T1.CUR_TOT_INCM_AMT - T1.CUR_TOT_EXP_AMT), 0) AS CUR_NET_PROFIT
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '143' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.CUR_NET_PROFIT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, CUR_NET_PROFIT, RANK() OVER(PARTITION BY BAS_YM ORDER BY CUR_NET_PROFIT ASC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  ROUND(AVG(T1.CUR_TOT_INCM_AMT - T1.CUR_TOT_EXP_AMT), 0) AS CUR_NET_PROFIT
                           FROM   FROM_TM23_MMLY_INCM_EXP_A T1 INNER JOIN TM00_PCF_D T2
                                                                       ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3
          ),
          FROM_KPI_TYP_144 AS
          (SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '144' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  XX.INCL_ALOSS_INCM_MNS_EXP_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, INCL_ALOSS_INCM_MNS_EXP_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY INCL_ALOSS_INCM_MNS_EXP_AMT DESC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  SUM(T1.INCL_ALOSS_INCM_MNS_EXP_AMT) AS INCL_ALOSS_INCM_MNS_EXP_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                                                                  INNER JOIN TM00_LOC_D T3
                                                                          ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '144' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  XX.INCL_ALOSS_INCM_MNS_EXP_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, INCL_ALOSS_INCM_MNS_EXP_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY INCL_ALOSS_INCM_MNS_EXP_AMT ASC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  SUM(T1.INCL_ALOSS_INCM_MNS_EXP_AMT) AS INCL_ALOSS_INCM_MNS_EXP_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                                                                  INNER JOIN TM00_LOC_D T3
                                                                          ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '144' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.INCL_ALOSS_INCM_MNS_EXP_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, INCL_ALOSS_INCM_MNS_EXP_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY INCL_ALOSS_INCM_MNS_EXP_AMT DESC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(T1.INCL_ALOSS_INCM_MNS_EXP_AMT) AS INCL_ALOSS_INCM_MNS_EXP_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '144' AS KPI_TYP_CD,
                  '1'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.INCL_ALOSS_INCM_MNS_EXP_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, INCL_ALOSS_INCM_MNS_EXP_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY INCL_ALOSS_INCM_MNS_EXP_AMT ASC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  SUM(T1.INCL_ALOSS_INCM_MNS_EXP_AMT) AS INCL_ALOSS_INCM_MNS_EXP_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '144' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  XX.INCL_ALOSS_INCM_MNS_EXP_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, INCL_ALOSS_INCM_MNS_EXP_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY INCL_ALOSS_INCM_MNS_EXP_AMT DESC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  ROUND(AVG(T1.INCL_ALOSS_INCM_MNS_EXP_AMT), 0) AS INCL_ALOSS_INCM_MNS_EXP_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                                                                  INNER JOIN TM00_LOC_D T3
                                                                          ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '1'   AS RPVN_CBV_BR_TYP_CD,
                  '144' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  XX.PRVN_CD,
                  NULL  AS CBV_BR_CD,
                  XX.INCL_ALOSS_INCM_MNS_EXP_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, PRVN_CD, INCL_ALOSS_INCM_MNS_EXP_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY INCL_ALOSS_INCM_MNS_EXP_AMT ASC NULLS LAST, PRVN_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T3.PRVN_CD,
                                  ROUND(AVG(T1.INCL_ALOSS_INCM_MNS_EXP_AMT), 0) AS INCL_ALOSS_INCM_MNS_EXP_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                                                                  INNER JOIN TM00_LOC_D T3
                                                                          ON T2.LOC_ID = T3.LOC_ID
                           GROUP BY T1.BAS_YM, T3.PRVN_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '144' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '1'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.INCL_ALOSS_INCM_MNS_EXP_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, INCL_ALOSS_INCM_MNS_EXP_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY INCL_ALOSS_INCM_MNS_EXP_AMT DESC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  ROUND(AVG(T1.INCL_ALOSS_INCM_MNS_EXP_AMT), 0) AS INCL_ALOSS_INCM_MNS_EXP_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3

           UNION ALL

           SELECT XX.BAS_YM,
                  '2'   AS RPVN_CBV_BR_TYP_CD,
                  '144' AS KPI_TYP_CD,
                  '2'   AS AGT_METH_CD,
                  '2'   AS TOP_BTOM_TYP_CD,
                  XX.RNK_NUM,
                  NULL  AS PRVN_CD,
                  XX.CBV_BR_CD,
                  XX.INCL_ALOSS_INCM_MNS_EXP_AMT AS AGT_VAL
           FROM   (
                   SELECT BAS_YM, CBV_BR_CD, INCL_ALOSS_INCM_MNS_EXP_AMT, RANK() OVER(PARTITION BY BAS_YM ORDER BY INCL_ALOSS_INCM_MNS_EXP_AMT ASC NULLS LAST, CBV_BR_CD DESC) AS RNK_NUM
                   FROM   (
                           SELECT T1.BAS_YM,
                                  T2.CBV_BR_CD,
                                  ROUND(AVG(T1.INCL_ALOSS_INCM_MNS_EXP_AMT), 0) AS INCL_ALOSS_INCM_MNS_EXP_AMT
                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                                          ON T1.PCF_ID = T2.PCF_ID
                           GROUP BY T1.BAS_YM, T2.CBV_BR_CD
                          )
                   ) XX
           WHERE XX.RNK_NUM BETWEEN 1 AND 3
          )
          SELECT BAS_YM, RPVN_CBV_BR_TYP_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PRVN_CD, CBV_BR_CD, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_001
          UNION ALL
          SELECT BAS_YM, RPVN_CBV_BR_TYP_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PRVN_CD, CBV_BR_CD, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_002
          UNION ALL
          SELECT BAS_YM, RPVN_CBV_BR_TYP_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PRVN_CD, CBV_BR_CD, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_003
          UNION ALL
          SELECT BAS_YM, RPVN_CBV_BR_TYP_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PRVN_CD, CBV_BR_CD, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_004
          UNION ALL
          SELECT BAS_YM, RPVN_CBV_BR_TYP_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PRVN_CD, CBV_BR_CD, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_005
          UNION ALL
          SELECT BAS_YM, RPVN_CBV_BR_TYP_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PRVN_CD, CBV_BR_CD, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_006
          UNION ALL
          SELECT BAS_YM, RPVN_CBV_BR_TYP_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PRVN_CD, CBV_BR_CD, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_007
          UNION ALL
          SELECT BAS_YM, RPVN_CBV_BR_TYP_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PRVN_CD, CBV_BR_CD, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_008
          UNION ALL
          SELECT BAS_YM, RPVN_CBV_BR_TYP_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PRVN_CD, CBV_BR_CD, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_009
          UNION ALL
          SELECT BAS_YM, RPVN_CBV_BR_TYP_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PRVN_CD, CBV_BR_CD, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_010
          UNION ALL
          SELECT BAS_YM, RPVN_CBV_BR_TYP_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PRVN_CD, CBV_BR_CD, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_066
          UNION ALL
          SELECT BAS_YM, RPVN_CBV_BR_TYP_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PRVN_CD, CBV_BR_CD, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_069
          UNION ALL
          SELECT BAS_YM, RPVN_CBV_BR_TYP_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PRVN_CD, CBV_BR_CD, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_070
          UNION ALL
          SELECT BAS_YM, RPVN_CBV_BR_TYP_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PRVN_CD, CBV_BR_CD, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_071
          UNION ALL
          SELECT BAS_YM, RPVN_CBV_BR_TYP_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PRVN_CD, CBV_BR_CD, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_088
          UNION ALL
          SELECT BAS_YM, RPVN_CBV_BR_TYP_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PRVN_CD, CBV_BR_CD, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_090
          UNION ALL
          SELECT BAS_YM, RPVN_CBV_BR_TYP_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PRVN_CD, CBV_BR_CD, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_091
          UNION ALL
          SELECT BAS_YM, RPVN_CBV_BR_TYP_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PRVN_CD, CBV_BR_CD, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_092
          UNION ALL
          SELECT BAS_YM, RPVN_CBV_BR_TYP_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PRVN_CD, CBV_BR_CD, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_093
          UNION ALL
          SELECT BAS_YM, RPVN_CBV_BR_TYP_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PRVN_CD, CBV_BR_CD, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_094
          UNION ALL
          SELECT BAS_YM, RPVN_CBV_BR_TYP_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PRVN_CD, CBV_BR_CD, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_133
          UNION ALL
          SELECT BAS_YM, RPVN_CBV_BR_TYP_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PRVN_CD, CBV_BR_CD, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_135
          UNION ALL
          SELECT BAS_YM, RPVN_CBV_BR_TYP_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PRVN_CD, CBV_BR_CD, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_136
          UNION ALL
          SELECT BAS_YM, RPVN_CBV_BR_TYP_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PRVN_CD, CBV_BR_CD, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_137
          UNION ALL
          SELECT BAS_YM, RPVN_CBV_BR_TYP_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PRVN_CD, CBV_BR_CD, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_138
          UNION ALL
          SELECT BAS_YM, RPVN_CBV_BR_TYP_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PRVN_CD, CBV_BR_CD, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_141
          UNION ALL
          SELECT BAS_YM, RPVN_CBV_BR_TYP_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PRVN_CD, CBV_BR_CD, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_142
          UNION ALL
          SELECT BAS_YM, RPVN_CBV_BR_TYP_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PRVN_CD, CBV_BR_CD, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_143
          UNION ALL
          SELECT BAS_YM, RPVN_CBV_BR_TYP_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PRVN_CD, CBV_BR_CD, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_144
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