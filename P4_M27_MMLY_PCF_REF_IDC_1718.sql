create or replace PROCEDURE            "P4_M27_MMLY_PCF_REF_IDC_1718"
                 ( p_wkdate    IN  CHAR DEFAULT NULL
                 , p_stdate_01 IN  CHAR DEFAULT NULL
                 , p_eddate_01 IN  CHAR DEFAULT NULL
                 )
IS
    /*--------------------------------------------------------------------------
     * PROGRAM ID    : P4_M27_MMLY_PCF_REF_IDC_1718
     * PROGRAM NAME  : A program for insert data to TM27_MMLY_PCF_REF_IDC_A
     * SOURCE TABLE  : TM27_MMLY_PCF_KEY_IDC_A
                       TM26_MMLY_AST_QAL_A
                       TM23_MMLY_INCM_EXP_A
                       TM24_MMLY_FUND_SRC_USG_A
                       TB03_G32_006_TTGS_A
     * TARGET TABLE  : TM27_MMLY_PCF_REF_IDC_A
     * PROGRAMER     : HIEU
     * LAST MODIFICATION DATE : 2025-12-31
     * UNIQUENESS    : N/A
     * COMMENTS      : N/A
    ----------------------------------------------------------------------------
     * Revision History : 2025-12-31 : Create
     * Revision History :
    --------------------------------------------------------------------------*/
    ----------------------------------------------------------------------------
    --  Declare Local Variables
    ----------------------------------------------------------------------------
    v_wk_date                 CHAR(8) DEFAULT NULL ;
    v_st_date_01              CHAR(8) DEFAULT NULL ;
    v_end_date_01             CHAR(8) DEFAULT NULL ;

    v_program_id              VARCHAR2(100) DEFAULT 'P4_M27_MMLY_PCF_REF_IDC_1718' ;
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
    WHERE  BAS_YM BETWEEN '202105' AND '202208'
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
            FROM TM27_MMLY_PCF_REF_IDC_A T1
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

          INSERT INTO TM27_MMLY_PCF_REF_IDC_A
          (   BAS_YM
             ,PCF_ID
             ,KPI_TYP_CD
             ,AGT_METH_CD
             ,REF_IDC_TYP_CD
             ,REF_GRP_RNK_NUM
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
          ASSET_WEIGHTED_AVG_PCF AS
          (SELECT A.BAS_YM,
                  A.PRVN_CD,
                  A.CBV_BR_CD,
                  A.PCF_ID,
                  CASE WHEN B.TOT_AST_AMT = 0 THEN 0
                       ELSE A.TOT_AST_AMT / B.TOT_AST_AMT
                       END AS ASSET_WEIGHTED_AVG
           FROM   (SELECT T1.BAS_YM,
                          T3.PRVN_CD,
                          T2.CBV_BR_CD,
                          T1.PCF_ID,
                          T1.TOT_AST_AMT
                   FROM   TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                             ON T1.PCF_ID = T2.PCF_ID
                                                     INNER JOIN TM00_LOC_D T3
                                                             ON T2.LOC_ID = T3.LOC_ID
                   WHERE  T1.TOT_AST_AMT IS NOT NULL
                  ) A INNER JOIN (SELECT BAS_YM, SUM(TOT_AST_AMT) AS TOT_AST_AMT
                                  FROM   TM27_MMLY_PCF_KEY_IDC_A
                                  WHERE  TOT_AST_AMT IS NOT NULL
                                  GROUP BY BAS_YM
                                 ) B
                              ON B.BAS_YM = A.BAS_YM
          ),
          FROM_KPI_TYP_001 AS
          (SELECT XX.BAS_YM,
                  XX.PCF_ID,
                  '001'    AS KPI_TYP_CD,
                  '2'      AS AGT_METH_CD,
                  '001007' AS REF_IDC_TYP_CD,
                  XX.RANK_GRP,
                  YY.AGT_VAL
           FROM   (SELECT BAS_YM,
                          PCF_ID,
                          CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY CCAP_AMT ASC NULLS LAST, PCF_ID ASC) /100, 1) = 0
                                    THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY CCAP_AMT ASC NULLS LAST, PCF_ID ASC) / 100
                               ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY CCAP_AMT ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                               END AS RANK_GRP
                   FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A
                   WHERE  TOT_PCF_MBR_NUM_CNT IS NOT NULL
                   AND    CCAP_AMT IS NOT NULL
                  ) XX INNER JOIN (SELECT BAS_YM,
                                          RANK_GRP,
                                          ROUND(AVG(TOT_PCF_MBR_NUM_CNT), 0) AS AGT_VAL
                                   FROM   (SELECT BAS_YM,
                                                  PCF_ID,
                                                  TOT_PCF_MBR_NUM_CNT,
                                                  CCAP_AMT,
                                                  RANK() OVER(PARTITION BY BAS_YM ORDER BY CCAP_AMT ASC NULLS LAST, PCF_ID ASC) AS RNK_NUM,
                                                  CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY CCAP_AMT ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                                            THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY CCAP_AMT ASC NULLS LAST, PCF_ID ASC) / 100
                                                       ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY CCAP_AMT ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                                                       END AS RANK_GRP
                                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A
                                           WHERE  TOT_PCF_MBR_NUM_CNT IS NOT NULL
                                           AND    CCAP_AMT IS NOT NULL
                                          )
                                   GROUP BY BAS_YM, RANK_GRP
                                  ) YY
                               ON YY.BAS_YM   = XX.BAS_YM
                              AND YY.RANK_GRP = XX.RANK_GRP
          ),
          FROM_KPI_TYP_002 AS
          (SELECT XX.BAS_YM,
                  XX.PCF_ID,
                  '002'    AS KPI_TYP_CD,
                  '2'      AS AGT_METH_CD,
                  '002001' AS REF_IDC_TYP_CD,
                  XX.RANK_GRP,
                  YY.AGT_VAL
           FROM   (SELECT BAS_YM,
                          PCF_ID,
                          CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_PCF_MBR_NUM_CNT ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                    THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_PCF_MBR_NUM_CNT ASC NULLS LAST, PCF_ID ASC) / 100
                               ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_PCF_MBR_NUM_CNT ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                               END AS RANK_GRP
                    FROM  FROM_TM27_MMLY_PCF_KEY_IDC_A
                    WHERE TOT_PCF_MBR_NUM_CNT IS NOT NULL
                    AND   TDP_BAL IS NOT NULL
                  ) XX INNER JOIN (SELECT BAS_YM,
                                          RANK_GRP,
                                          ROUND(AVG(TDP_BAL), 0) AS AGT_VAL
                                   FROM   (SELECT BAS_YM,
                                                  PCF_ID,
                                                  TDP_BAL,
                                                  TOT_PCF_MBR_NUM_CNT,
                                                  RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_PCF_MBR_NUM_CNT ASC NULLS LAST, PCF_ID ASC) AS RNK_NUM,
                                                  CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_PCF_MBR_NUM_CNT ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                                            THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_PCF_MBR_NUM_CNT ASC NULLS LAST, PCF_ID ASC) / 100
                                                       ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_PCF_MBR_NUM_CNT ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                                                       END AS RANK_GRP
                                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A
                                           WHERE  TOT_PCF_MBR_NUM_CNT IS NOT NULL
                                           AND    TDP_BAL IS NOT NULL
                                          )
                                   GROUP BY BAS_YM, RANK_GRP
                                  ) YY
                               ON YY.BAS_YM   = XX.BAS_YM
                              AND YY.RANK_GRP = XX.RANK_GRP
          ),
          FROM_KPI_TYP_003 AS
          (SELECT XX.BAS_YM,
                  XX.PCF_ID,
                  '003'    AS KPI_TYP_CD,
                  '2'      AS AGT_METH_CD,
                  '003011' AS REF_IDC_TYP_CD,
                  XX.RANK_GRP,
                  YY.AGT_VAL
           FROM   (SELECT BAS_YM,
                          PCF_ID,
                          CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                    THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100
                               ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                               END AS RANK_GRP
                    FROM  FROM_TM27_MMLY_PCF_KEY_IDC_A
                    WHERE TOT_AST_AMT IS NOT NULL
                    AND   TOT_LN_BAL IS NOT NULL
                  ) XX INNER JOIN (SELECT BAS_YM,
                                          RANK_GRP,
                                          ROUND(AVG(TOT_LN_BAL), 0) AS AGT_VAL
                                   FROM   (SELECT BAS_YM,
                                                  PCF_ID,
                                                  TOT_LN_BAL,
                                                  TOT_AST_AMT,
                                                  RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) AS RNK_NUM,
                                                  CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                                            THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100
                                                       ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                                                       END AS RANK_GRP
                                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A
                                          WHERE   TOT_AST_AMT IS NOT NULL
                                            AND   TOT_LN_BAL IS NOT NULL
                                          )
                                   GROUP BY BAS_YM, RANK_GRP
                                  ) YY
                               ON YY.BAS_YM   = XX.BAS_YM
                              AND YY.RANK_GRP = XX.RANK_GRP
          ),
          FROM_KPI_TYP_004 AS
          (SELECT XX.BAS_YM,
                  XX.PCF_ID,
                  '004'    AS KPI_TYP_CD,
                  '2'      AS AGT_METH_CD,
                  '004011' AS REF_IDC_TYP_CD,
                  XX.RANK_GRP,
                  YY.AGT_VAL
           FROM   (SELECT BAS_YM,
                          PCF_ID,
                          CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                    THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100
                               ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                               END AS RANK_GRP
                   FROM  FROM_TM27_MMLY_PCF_KEY_IDC_A
                   WHERE TOT_AST_AMT IS NOT NULL
                   AND   WO_COLL_LN_BAL IS NOT NULL
                  ) XX INNER JOIN (SELECT BAS_YM,
                                          RANK_GRP,
                                          ROUND(AVG(WO_COLL_LN_BAL), 0) AS AGT_VAL
                                   FROM   (SELECT BAS_YM,
                                                  PCF_ID,
                                                  WO_COLL_LN_BAL,
                                                  TOT_AST_AMT,
                                                  RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) AS RNK_NUM,
                                                  CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                                            THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100
                                                       ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                                                       END AS RANK_GRP
                                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A
                                           WHERE  TOT_AST_AMT IS NOT NULL
                                           AND    WO_COLL_LN_BAL IS NOT NULL
                                          )
                                   GROUP BY BAS_YM, RANK_GRP
                                  ) YY
                               ON YY.BAS_YM   = XX.BAS_YM
                              AND YY.RANK_GRP = XX.RANK_GRP
          ),
          FROM_KPI_TYP_005 AS
          (SELECT XX.BAS_YM,
                  XX.PCF_ID,
                  '005'    AS KPI_TYP_CD,
                  '2'      AS AGT_METH_CD,
                  '005003' AS REF_IDC_TYP_CD,
                  XX.RANK_GRP,
                  YY.AGT_VAL
           FROM   (SELECT BAS_YM,
                          PCF_ID,
                          CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_LN_BAL ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                    THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_LN_BAL ASC NULLS LAST, PCF_ID ASC) / 100
                               ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_LN_BAL ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                               END AS RANK_GRP
                    FROM  FROM_TM27_MMLY_PCF_KEY_IDC_A
                    WHERE TOT_LN_BAL IS NOT NULL
                    AND   BAD_DBT_AMT IS NOT NULL
                  ) XX INNER JOIN (SELECT BAS_YM,
                                          RANK_GRP,
                                          ROUND(AVG(BAD_DBT_AMT), 0) AS AGT_VAL
                                   FROM   (SELECT BAS_YM,
                                                  PCF_ID,
                                                  BAD_DBT_AMT,
                                                  TOT_LN_BAL,
                                                  RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_LN_BAL ASC NULLS LAST, PCF_ID ASC) AS RNK_NUM,
                                                  CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_LN_BAL ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                                            THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_LN_BAL ASC NULLS LAST, PCF_ID ASC) / 100
                                                       ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_LN_BAL ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                                                       END AS RANK_GRP
                                            FROM  FROM_TM27_MMLY_PCF_KEY_IDC_A
                                            WHERE TOT_LN_BAL IS NOT NULL
                                            AND   BAD_DBT_AMT IS NOT NULL
                                          )
                                   GROUP BY BAS_YM, RANK_GRP
                                  ) YY
                               ON YY.BAS_YM   = XX.BAS_YM
                              AND YY.RANK_GRP = XX.RANK_GRP
          ),
          FROM_KPI_TYP_006 AS
          (SELECT XX.BAS_YM,
                  XX.PCF_ID,
                  '006'    AS KPI_TYP_CD,
                  '2'      AS AGT_METH_CD,
                  '006011' AS REF_IDC_TYP_CD,
                  XX.RANK_GRP,
                  YY.AGT_VAL
           FROM   (SELECT BAS_YM,
                          PCF_ID,
                          CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                    THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100
                               ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                               END AS RANK_GRP
                    FROM  FROM_TM27_MMLY_PCF_KEY_IDC_A
                    WHERE TOT_AST_AMT IS NOT NULL
                    AND   EQT_AMT IS NOT NULL
                  ) XX INNER JOIN (SELECT BAS_YM,
                                          RANK_GRP,
                                          ROUND(AVG(EQT_AMT), 0) AS AGT_VAL
                                   FROM   (SELECT BAS_YM,
                                                  PCF_ID,
                                                  EQT_AMT,
                                                  TOT_AST_AMT,
                                                  RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) AS RNK_NUM,
                                                  CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                                            THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100
                                                       ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                                                       END AS RANK_GRP
                                            FROM  FROM_TM27_MMLY_PCF_KEY_IDC_A
                                            WHERE TOT_AST_AMT IS NOT NULL
                                            AND   EQT_AMT IS NOT NULL
                                          )
                                   GROUP BY BAS_YM, RANK_GRP
                                  ) YY
                               ON YY.BAS_YM   = XX.BAS_YM
                              AND YY.RANK_GRP = XX.RANK_GRP
          ),
          FROM_KPI_TYP_007 AS
          (SELECT XX.BAS_YM,
                  XX.PCF_ID,
                  '007'    AS KPI_TYP_CD,
                  '2'      AS AGT_METH_CD,
                  '007006' AS REF_IDC_TYP_CD,
                  XX.RANK_GRP,
                  YY.AGT_VAL
           FROM   (SELECT BAS_YM,
                          PCF_ID,
                          CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY EQT_AMT ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                    THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY EQT_AMT ASC NULLS LAST, PCF_ID ASC) / 100
                               ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY EQT_AMT ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                               END AS RANK_GRP
                    FROM  FROM_TM27_MMLY_PCF_KEY_IDC_A
                    WHERE EQT_AMT IS NOT NULL
                    AND   CCAP_AMT IS NOT NULL
                  ) XX INNER JOIN (SELECT BAS_YM,
                                          RANK_GRP,
                                          ROUND(AVG(CCAP_AMT), 0) AS AGT_VAL
                                   FROM   (SELECT BAS_YM,
                                                  PCF_ID,
                                                  CCAP_AMT,
                                                  EQT_AMT,
                                                  RANK() OVER(PARTITION BY BAS_YM ORDER BY EQT_AMT ASC NULLS LAST, PCF_ID ASC) AS RNK_NUM,
                                                  CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY EQT_AMT ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                                            THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY EQT_AMT ASC NULLS LAST, PCF_ID ASC) / 100
                                                       ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY EQT_AMT ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                                                       END AS RANK_GRP
                                            FROM  FROM_TM27_MMLY_PCF_KEY_IDC_A
                                            WHERE EQT_AMT IS NOT NULL
                                            AND   CCAP_AMT IS NOT NULL
                                          )
                                   GROUP BY BAS_YM, RANK_GRP
                                  ) YY
                               ON YY.BAS_YM   = XX.BAS_YM
                              AND YY.RANK_GRP = XX.RANK_GRP
          ),
          FROM_KPI_TYP_008 AS
          (SELECT XX.BAS_YM,
                  XX.PCF_ID,
                  '008'    AS KPI_TYP_CD,
                  '2'      AS AGT_METH_CD,
                  '008006' AS REF_IDC_TYP_CD,
                  XX.RANK_GRP,
                  YY.AGT_VAL
           FROM   (SELECT BAS_YM,
                          PCF_ID,
                          CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY EQT_AMT ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                    THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY EQT_AMT ASC NULLS LAST, PCF_ID ASC) / 100
                               ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY EQT_AMT ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                               END AS RANK_GRP
                    FROM  FROM_TM27_MMLY_PCF_KEY_IDC_A
                    WHERE EQT_AMT IS NOT NULL
                    AND   CBV_BOR_EXCL_SFTY_FUND_AMT IS NOT NULL
                  ) XX INNER JOIN (SELECT BAS_YM,
                                          RANK_GRP,
                                          ROUND(AVG(CBV_BOR_EXCL_SFTY_FUND_AMT), 0) AS AGT_VAL
                                   FROM   (SELECT BAS_YM,
                                                  PCF_ID,
                                                  CBV_BOR_EXCL_SFTY_FUND_AMT,
                                                  EQT_AMT,
                                                  RANK() OVER(PARTITION BY BAS_YM ORDER BY EQT_AMT ASC NULLS LAST, PCF_ID ASC) AS RNK_NUM,
                                                  CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY EQT_AMT ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                                            THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY EQT_AMT ASC NULLS LAST, PCF_ID ASC) / 100
                                                       ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY EQT_AMT ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                                                       END AS RANK_GRP
                                            FROM  FROM_TM27_MMLY_PCF_KEY_IDC_A
                                            WHERE EQT_AMT IS NOT NULL
                                            AND   CBV_BOR_EXCL_SFTY_FUND_AMT IS NOT NULL
                                          )
                                   GROUP BY BAS_YM, RANK_GRP
                                  ) YY
                               ON YY.BAS_YM   = XX.BAS_YM
                              AND YY.RANK_GRP = XX.RANK_GRP
          ),
          FROM_KPI_TYP_009 AS
          (SELECT XX.BAS_YM,
                  XX.PCF_ID,
                  '009'    AS KPI_TYP_CD,
                  '2'      AS AGT_METH_CD,
                  '009011' AS REF_IDC_TYP_CD,
                  XX.RANK_GRP,
                  YY.AGT_VAL
           FROM   (SELECT BAS_YM,
                          PCF_ID,
                          CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                    THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100
                               ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                               END AS RANK_GRP
                    FROM  FROM_TM27_MMLY_PCF_KEY_IDC_A
                    WHERE TOT_AST_AMT IS NOT NULL
                    AND   CBV_SVG_AMT IS NOT NULL
                  ) XX INNER JOIN (SELECT BAS_YM,
                                          RANK_GRP,
                                          ROUND(AVG(CBV_SVG_AMT), 0) AS AGT_VAL
                                   FROM   (SELECT BAS_YM,
                                                  PCF_ID,
                                                  CBV_SVG_AMT,
                                                  TOT_AST_AMT,
                                                  RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) AS RNK_NUM,
                                                  CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                                            THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100
                                                       ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                                                       END AS RANK_GRP
                                            FROM  FROM_TM27_MMLY_PCF_KEY_IDC_A
                                            WHERE TOT_AST_AMT IS NOT NULL
                                            AND   CBV_SVG_AMT IS NOT NULL
                                          )
                                   GROUP BY BAS_YM, RANK_GRP
                                  ) YY
                               ON YY.BAS_YM   = XX.BAS_YM
                              AND YY.RANK_GRP = XX.RANK_GRP
          ),
          FROM_KPI_TYP_010 AS
          (SELECT XX.BAS_YM,
                  XX.PCF_ID,
                  '010'    AS KPI_TYP_CD,
                  '2'      AS AGT_METH_CD,
                  '010011' AS REF_IDC_TYP_CD,
                  XX.RANK_GRP,
                  YY.AGT_VAL
           FROM   (SELECT BAS_YM,
                          PCF_ID,
                          CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                    THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100
                               ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                               END AS RANK_GRP
                    FROM  FROM_TM27_MMLY_PCF_KEY_IDC_A
                    WHERE TOT_AST_AMT IS NOT NULL
                    AND   OTHR_INST_SVG_AMT IS NOT NULL
                  ) XX INNER JOIN (SELECT BAS_YM,
                                          RANK_GRP,
                                          ROUND(AVG(OTHR_INST_SVG_AMT), 0) AS AGT_VAL
                                   FROM   (SELECT BAS_YM,
                                                  PCF_ID,
                                                  OTHR_INST_SVG_AMT,
                                                  TOT_AST_AMT,
                                                  RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) AS RNK_NUM,
                                                  CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                                            THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100
                                                       ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                                                       END AS RANK_GRP
                                            FROM  FROM_TM27_MMLY_PCF_KEY_IDC_A
                                            WHERE TOT_AST_AMT IS NOT NULL
                                            AND   OTHR_INST_SVG_AMT IS NOT NULL
                                          )
                                   GROUP BY BAS_YM, RANK_GRP
                                  ) YY
                               ON YY.BAS_YM   = XX.BAS_YM
                              AND YY.RANK_GRP = XX.RANK_GRP
          ),
          FROM_KPI_TYP_058 AS
          (SELECT XX.BAS_YM,
                  XX.PCF_ID,
                  '058'    AS KPI_TYP_CD,
                  '2'      AS AGT_METH_CD,
                  '058011' AS REF_IDC_TYP_CD,
                  XX.RANK_GRP,
                  YY.AGT_VAL
           FROM   (SELECT BAS_YM,
                          PCF_ID,
                          CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                    THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100
                               ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                               END AS RANK_GRP
                    FROM  FROM_TM27_MMLY_PCF_KEY_IDC_A
                    WHERE TOT_AST_AMT IS NOT NULL
                    AND   CAR IS NOT NULL
                  ) XX INNER JOIN (SELECT BAS_YM,
                                          RANK_GRP,
                                          ROUND(AVG(CAR), 2) AS AGT_VAL
                                   FROM   (SELECT BAS_YM,
                                                  PCF_ID,
                                                  CAR,
                                                  TOT_AST_AMT,
                                                  RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) AS RNK_NUM,
                                                  CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                                            THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100
                                                       ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                                                       END AS RANK_GRP
                                            FROM  FROM_TM27_MMLY_PCF_KEY_IDC_A
                                            WHERE TOT_AST_AMT IS NOT NULL
                                            AND   CAR IS NOT NULL
                                          )
                                   GROUP BY BAS_YM, RANK_GRP
                                  ) YY
                               ON YY.BAS_YM   = XX.BAS_YM
                              AND YY.RANK_GRP = XX.RANK_GRP

           UNION ALL

           SELECT BAS_YM,
                  PCF_ID,
                  '058'    AS KPI_TYP_CD,
                  '5'      AS AGT_METH_CD,
                  '058011' AS REF_IDC_TYP_CD,
                  RANK_GRP,
                  AGT_VAL
           FROM  (
                  SELECT T1.BAS_YM,
                         T1.PCF_ID,
                         MAX(T2.RANK_GRP) AS RANK_GRP,
                         ROUND(AVG(T2.AGT_VAL), 2) AS AGT_VAL
                    FROM FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN (SELECT XX.BAS_YM,
                                                                            XX.PCF_ID,
                                                                            XX.RANK_GRP,
                                                                            YY.AGT_VAL
                                                                     FROM   (SELECT BAS_YM,
                                                                                    PCF_ID,
                                                                                    CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                                                                              THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100
                                                                                         ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                                                                                         END AS RANK_GRP
                                                                              FROM  TM27_MMLY_PCF_KEY_IDC_A
                                                                              WHERE TOT_AST_AMT IS NOT NULL
                                                                              AND   CAR IS NOT NULL
                                                                            ) XX INNER JOIN (SELECT BAS_YM,
                                                                                                    RANK_GRP,
                                                                                                    AVG(CAR) AS AGT_VAL
                                                                                             FROM   (SELECT BAS_YM,
                                                                                                            PCF_ID,
                                                                                                            CAR,
                                                                                                            TOT_AST_AMT,
                                                                                                            RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) AS RNK_NUM,
                                                                                                            CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                                                                                                      THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100
                                                                                                                 ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                                                                                                                 END AS RANK_GRP
                                                                                                      FROM  TM27_MMLY_PCF_KEY_IDC_A
                                                                                                      WHERE TOT_AST_AMT IS NOT NULL
                                                                                                      AND   CAR IS NOT NULL
                                                                                                    )
                                                                                             GROUP BY BAS_YM, RANK_GRP
                                                                                            ) YY
                                                                                         ON YY.BAS_YM   = XX.BAS_YM
                                                                                        AND YY.RANK_GRP = XX.RANK_GRP
                                                                    ) T2
                                                                 ON T2.BAS_YM   >= TO_CHAR(ADD_MONTHS(TO_DATE(T1.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                                AND T2.BAS_YM   <= T1.BAS_YM
                                                                AND T2.PCF_ID   = T1.PCF_ID
                   WHERE T1.TOT_AST_AMT IS NOT NULL
                   AND   T1.CAR IS NOT NULL
                   GROUP BY T1.BAS_YM, T1.PCF_ID
                 )
          ),
          FROM_KPI_TYP_066 AS
          (SELECT XX.BAS_YM,
                  XX.PCF_ID,
                  '066'    AS KPI_TYP_CD,
                  '2'      AS AGT_METH_CD,
                  '066011' AS REF_IDC_TYP_CD,
                  XX.RANK_GRP,
                  YY.AGT_VAL
           FROM   (SELECT T1.BAS_YM,
                          T1.PCF_ID,
                          CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                    THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                               ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                               END AS RANK_GRP
                   FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN FROM_TM24_MMLY_FUND_SRC_USG_A T2
                                                                  ON T2.BAS_YM = T1.BAS_YM
                                                                 AND T2.PCF_ID = T1.PCF_ID
                                                                 AND T2.STFND_USE_MTLTLD_RTO IS NOT NULL
                   WHERE  T1.TOT_AST_AMT IS NOT NULL

                  ) XX INNER JOIN (SELECT BAS_YM,
                                          RANK_GRP,
                                          ROUND(AVG(STFND_USE_MTLTLD_RTO), 2) AS AGT_VAL
                                   FROM   (SELECT T1.BAS_YM,
                                                  T1.PCF_ID,
                                                  T1.STFND_USE_MTLTLD_RTO AS STFND_USE_MTLTLD_RTO,
                                                  T2.TOT_AST_AMT,
                                                  RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T2.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) AS RNK_NUM,
                                                  CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T2.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                                            THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T2.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                                                       ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T2.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                                                       END AS RANK_GRP
                                           FROM   FROM_TM24_MMLY_FUND_SRC_USG_A T1 INNER JOIN FROM_TM27_MMLY_PCF_KEY_IDC_A T2
                                                                                           ON T2.BAS_YM = T1.BAS_YM
                                                                                          AND T2.PCF_ID = T1.PCF_ID
                                                                                          AND T2.TOT_AST_AMT IS NOT NULL
                                           WHERE  T1.STFND_USE_MTLTLD_RTO IS NOT NULL
                                          )
                                   GROUP BY BAS_YM, RANK_GRP
                                  ) YY
                               ON YY.BAS_YM   = XX.BAS_YM
                              AND YY.RANK_GRP = XX.RANK_GRP

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.PCF_ID,
                  '066'    AS KPI_TYP_CD,
                  '4'      AS AGT_METH_CD,
                  '066011' AS REF_IDC_TYP_CD,
                  XX.RANK_GRP,
                  YY.AGT_VAL
           FROM   (SELECT T1.BAS_YM,
                          T1.PCF_ID,
                          CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                    THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                               ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                               END AS RANK_GRP
                   FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN MED_AND_LONG_TERM_LENDING T2
                                                                  ON T2.BAS_YM = T1.BAS_YM
                                                                 AND T2.PCF_ID = T1.PCF_ID
                   WHERE  T1.TOT_AST_AMT IS NOT NULL
                  ) XX INNER JOIN (SELECT BAS_YM,
                                          RANK_GRP,
                                          ROUND(SUM(STFND_USE_MTLTLD_RTO), 2) AS AGT_VAL
                                   FROM   (SELECT T1.BAS_YM,
                                                  T1.PCF_ID,
                                                  CASE WHEN T1.MTLTLD_AMT - T1.MED_AND_LONG_TERM_CAPITAL =  0 THEN 0
                                                       WHEN T1.MTLTLD_AMT - T1.MED_AND_LONG_TERM_CAPITAL <> 0 AND T1.SHORT_TERM_CAPITAL = 0 THEN NULL
                                                       ELSE CASE WHEN T1.SHORT_TERM_CAPITAL = 0 THEN 0
                                                                 ELSE ROUND((T1.MTLTLD_AMT - T1.MED_AND_LONG_TERM_CAPITAL) / T1.SHORT_TERM_CAPITAL * 100, 2)
                                                                 END
                                                       END * T3.ASSET_WEIGHTED_AVG AS STFND_USE_MTLTLD_RTO,
                                                  T2.TOT_AST_AMT,
                                                  RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T2.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) AS RNK_NUM,
                                                  CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T2.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                                            THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T2.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                                                       ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T2.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                                                       END AS RANK_GRP
                                           FROM   MED_AND_LONG_TERM_LENDING  T1 INNER JOIN FROM_TM27_MMLY_PCF_KEY_IDC_A T2
                                                                                        ON T2.BAS_YM = T1.BAS_YM
                                                                                       AND T2.PCF_ID = T1.PCF_ID
                                                                                       AND T2.TOT_AST_AMT IS NOT NULL
                                                                                INNER JOIN (SELECT A.BAS_YM,
                                                                                                   A.PCF_ID,
                                                                                                   CASE WHEN B.TOT_AST_AMT = 0 THEN 0
                                                                                                        ELSE A.TOT_AST_AMT / B.TOT_AST_AMT
                                                                                                        END AS ASSET_WEIGHTED_AVG
                                                                                            FROM   (SELECT T1.BAS_YM,
                                                                                                           CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                                                                                                     THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                                                                                                                ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                                                                                                                END AS RANK_GRP,
                                                                                                           T1.PCF_ID,
                                                                                                           T1.TOT_AST_AMT
                                                                                                    FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN MED_AND_LONG_TERM_LENDING T2
                                                                                                                                                   ON T2.BAS_YM = T1.BAS_YM
                                                                                                                                                  AND T2.PCF_ID = T1.PCF_ID
                                                                                                    WHERE  T1.TOT_AST_AMT IS NOT NULL
                                                                                                   ) A INNER JOIN (SELECT BAS_YM,
                                                                                                                          RANK_GRP,
                                                                                                                          SUM(TOT_AST_AMT) AS TOT_AST_AMT
                                                                                                                   FROM   (SELECT T1.BAS_YM,
                                                                                                                                  CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                                                                                                                            THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                                                                                                                                       ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                                                                                                                                       END AS RANK_GRP,
                                                                                                                                  T1.TOT_AST_AMT
                                                                                                                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN MED_AND_LONG_TERM_LENDING T2
                                                                                                                                                                          ON T2.BAS_YM = T1.BAS_YM
                                                                                                                                                                         AND T2.PCF_ID = T1.PCF_ID
                                                                                                                           WHERE  T1.TOT_AST_AMT IS NOT NULL
                                                                                                                          )
                                                                                                                   GROUP BY BAS_YM, RANK_GRP
                                                                                                                  ) B
                                                                                                               ON B.BAS_YM   = A.BAS_YM
                                                                                                              AND B.RANK_GRP = A.RANK_GRP
                                                                                           ) T3
                                                                                        ON T3.BAS_YM = T1.BAS_YM
                                                                                       AND T3.PCF_ID = T1.PCF_ID

                                          )
                                   GROUP BY BAS_YM, RANK_GRP
                                  ) YY
                               ON YY.BAS_YM   = XX.BAS_YM
                              AND YY.RANK_GRP = XX.RANK_GRP

           UNION ALL

           SELECT BAS_YM,
                  PCF_ID,
                  '066'    AS KPI_TYP_CD,
                  '5'      AS AGT_METH_CD,
                  '066011' AS REF_IDC_TYP_CD,
                  RANK_GRP,
                  AGT_VAL
           FROM  (
                  SELECT T1.BAS_YM,
                         T1.PCF_ID,
                         MAX(T2.RANK_GRP) AS RANK_GRP,
                         ROUND(AVG(T2.AGT_VAL), 2) AS AGT_VAL
                  FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN (SELECT XX.BAS_YM,
                                                                            XX.PCF_ID,
                                                                            XX.RANK_GRP,
                                                                            YY.AGT_VAL
                                                                     FROM   (SELECT T1.BAS_YM,
                                                                                    T1.PCF_ID,
                                                                                    CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                                                                              THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                                                                                         ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                                                                                         END AS RANK_GRP
                                                                             FROM   TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN TM24_MMLY_FUND_SRC_USG_A T2
                                                                                                                       ON T2.BAS_YM = T1.BAS_YM
                                                                                                                      AND T2.PCF_ID = T1.PCF_ID
                                                                                                                      AND T2.STFND_USE_MTLTLD_RTO IS NOT NULL
                                                                             WHERE  T1.TOT_AST_AMT IS NOT NULL
                                                                            ) XX INNER JOIN (SELECT BAS_YM,
                                                                                                    RANK_GRP,
                                                                                                    ROUND(AVG(STFND_USE_MTLTLD_RTO), 2) AS AGT_VAL
                                                                                             FROM   (SELECT T1.BAS_YM,
                                                                                                            T1.PCF_ID,
                                                                                                            T1.STFND_USE_MTLTLD_RTO AS STFND_USE_MTLTLD_RTO,
                                                                                                            T2.TOT_AST_AMT,
                                                                                                            RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T2.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) AS RNK_NUM,
                                                                                                            CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T2.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                                                                                                      THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T2.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                                                                                                                 ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T2.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                                                                                                                 END AS RANK_GRP
                                                                                                     FROM   TM24_MMLY_FUND_SRC_USG_A T1 INNER JOIN TM27_MMLY_PCF_KEY_IDC_A T2
                                                                                                                                                ON T2.BAS_YM = T1.BAS_YM
                                                                                                                                               AND T2.PCF_ID = T1.PCF_ID
                                                                                                                                               AND T2.TOT_AST_AMT IS NOT NULL
                                                                                                     WHERE  T1.STFND_USE_MTLTLD_RTO IS NOT NULL
                                                                                                    )
                                                                                             GROUP BY BAS_YM, RANK_GRP
                                                                                            ) YY
                                                                                         ON YY.BAS_YM   = XX.BAS_YM
                                                                                        AND YY.RANK_GRP = XX.RANK_GRP
                                                                    ) T2
                                                                 ON T2.BAS_YM   >= TO_CHAR(ADD_MONTHS(TO_DATE(T1.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                                AND T2.BAS_YM   <= T1.BAS_YM
                                                                AND T2.PCF_ID    = T1.PCF_ID
                                                         INNER JOIN FROM_TM24_MMLY_FUND_SRC_USG_A T3
                                                                 ON T3.BAS_YM   = T1.BAS_YM
                                                                AND T3.PCF_ID   = T1.PCF_ID
                                                                AND T3.STFND_USE_MTLTLD_RTO IS NOT NULL
                  WHERE T1.TOT_AST_AMT IS NOT NULL
                  GROUP BY T1.BAS_YM, T1.PCF_ID
                 )

           UNION ALL

           SELECT BAS_YM,
                  PCF_ID,
                  '066'    AS KPI_TYP_CD,
                  '7'      AS AGT_METH_CD,
                  '066011' AS REF_IDC_TYP_CD,
                  RANK_GRP,
                  AGT_VAL
           FROM  (
                  SELECT T1.BAS_YM,
                         T1.PCF_ID,
                         MAX(T2.RANK_GRP) AS RANK_GRP,
                         ROUND(AVG(T2.AGT_VAL), 2) AS AGT_VAL
                  FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN (SELECT XX.BAS_YM,
                                                                            XX.PCF_ID,
                                                                            XX.RANK_GRP,
                                                                            YY.AGT_VAL
                                                                     FROM   (SELECT T1.BAS_YM,
                                                                                    T1.PCF_ID,
                                                                                    CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                                                                              THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                                                                                         ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                                                                                         END AS RANK_GRP
                                                                             FROM   TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN MED_AND_LONG_TERM_LENDING T2
                                                                                                                       ON T2.BAS_YM = T1.BAS_YM
                                                                                                                      AND T2.PCF_ID = T1.PCF_ID
                                                                             WHERE  T1.TOT_AST_AMT IS NOT NULL
                                                                            ) XX INNER JOIN (SELECT BAS_YM,
                                                                                                    RANK_GRP,
                                                                                                    ROUND(SUM(STFND_USE_MTLTLD_RTO), 2) AS AGT_VAL
                                                                                             FROM   (SELECT T1.BAS_YM,
                                                                                                            T1.PCF_ID,
                                                                                                            CASE WHEN T1.MTLTLD_AMT - T1.MED_AND_LONG_TERM_CAPITAL =  0 THEN 0
                                                                                                                 WHEN T1.MTLTLD_AMT - T1.MED_AND_LONG_TERM_CAPITAL <> 0 AND T1.SHORT_TERM_CAPITAL = 0 THEN NULL
                                                                                                                 ELSE CASE WHEN T1.SHORT_TERM_CAPITAL = 0 THEN 0
                                                                                                                           ELSE ROUND((T1.MTLTLD_AMT - T1.MED_AND_LONG_TERM_CAPITAL) / T1.SHORT_TERM_CAPITAL * 100, 2)
                                                                                                                           END
                                                                                                                 END * T3.ASSET_WEIGHTED_AVG AS STFND_USE_MTLTLD_RTO,
                                                                                                            T2.TOT_AST_AMT,
                                                                                                            RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T2.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) AS RNK_NUM,
                                                                                                            CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T2.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                                                                                                      THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T2.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                                                                                                                 ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T2.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                                                                                                                 END AS RANK_GRP
                                                                                                     FROM   MED_AND_LONG_TERM_LENDING  T1 INNER JOIN TM27_MMLY_PCF_KEY_IDC_A T2
                                                                                                                                                  ON T2.BAS_YM = T1.BAS_YM
                                                                                                                                                 AND T2.PCF_ID = T1.PCF_ID
                                                                                                                                                 AND T2.TOT_AST_AMT IS NOT NULL
                                                                                                                                          INNER JOIN (SELECT A.BAS_YM,
                                                                                                                                                             A.PCF_ID,
                                                                                                                                                             CASE WHEN B.TOT_AST_AMT = 0 THEN 0
                                                                                                                                                                  ELSE A.TOT_AST_AMT / B.TOT_AST_AMT
                                                                                                                                                                  END AS ASSET_WEIGHTED_AVG
                                                                                                                                                      FROM   (SELECT T1.BAS_YM,
                                                                                                                                                                     CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                                                                                                                                                               THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                                                                                                                                                                          ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                                                                                                                                                                          END AS RANK_GRP,
                                                                                                                                                                     T1.PCF_ID,
                                                                                                                                                                     T1.TOT_AST_AMT
                                                                                                                                                              FROM   TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN MED_AND_LONG_TERM_LENDING T2
                                                                                                                                                                                                        ON T2.BAS_YM = T1.BAS_YM
                                                                                                                                                                                                       AND T2.PCF_ID = T1.PCF_ID
                                                                                                                                                              WHERE  T1.TOT_AST_AMT IS NOT NULL
                                                                                                                                                             ) A INNER JOIN (SELECT BAS_YM,
                                                                                                                                                                                    RANK_GRP,
                                                                                                                                                                                    SUM(TOT_AST_AMT) AS TOT_AST_AMT
                                                                                                                                                                             FROM   (SELECT T1.BAS_YM,
                                                                                                                                                                                            CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                                                                                                                                                                                      THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                                                                                                                                                                                                 ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                                                                                                                                                                                                 END AS RANK_GRP,
                                                                                                                                                                                            T1.TOT_AST_AMT
                                                                                                                                                                                     FROM   TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN MED_AND_LONG_TERM_LENDING T2
                                                                                                                                                                                                                               ON T2.BAS_YM = T1.BAS_YM
                                                                                                                                                                                                                              AND T2.PCF_ID = T1.PCF_ID
                                                                                                                                                                                     WHERE  T1.TOT_AST_AMT IS NOT NULL
                                                                                                                                                                                    )
                                                                                                                                                                             GROUP BY BAS_YM, RANK_GRP
                                                                                                                                                                            ) B
                                                                                                                                                                         ON B.BAS_YM   = A.BAS_YM
                                                                                                                                                                        AND B.RANK_GRP = A.RANK_GRP
                                                                                                                                                     ) T3
                                                                                                                                                  ON T3.BAS_YM = T1.BAS_YM
                                                                                                                                                 AND T3.PCF_ID = T1.PCF_ID
                                                                                                    )
                                                                                             GROUP BY BAS_YM, RANK_GRP
                                                                                            ) YY
                                                                                         ON YY.BAS_YM   = XX.BAS_YM
                                                                                        AND YY.RANK_GRP = XX.RANK_GRP
                                                                    ) T2
                                                                 ON T2.BAS_YM   >= TO_CHAR(ADD_MONTHS(TO_DATE(T1.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                                AND T2.BAS_YM   <= T1.BAS_YM
                                                                AND T2.PCF_ID   = T1.PCF_ID
                                                         INNER JOIN MED_AND_LONG_TERM_LENDING T3
                                                                 ON T3.BAS_YM   = T1.BAS_YM
                                                                AND T3.PCF_ID   = T1.PCF_ID
                  GROUP BY T1.BAS_YM, T1.PCF_ID
                 )
          ),
          FROM_KPI_TYP_069 AS
          (SELECT XX.BAS_YM,
                  XX.PCF_ID,
                  '069'    AS KPI_TYP_CD,
                  '2'      AS AGT_METH_CD,
                  '069006' AS REF_IDC_TYP_CD,
                  XX.RANK_GRP,
                  YY.AGT_VAL
           FROM   (SELECT T1.BAS_YM,
                          T1.PCF_ID,
                          CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                    THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                               ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                               END AS RANK_GRP
                   FROM FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN FROM_TM26_MMLY_AST_QAL_A T2
                                                                ON T2.BAS_YM = T1.BAS_YM
                                                               AND T2.PCF_ID = T1.PCF_ID
                                                               AND T2.BAD_DBT_AMT IS NOT NULL
                                                               AND T2.TOT_LN_BAL  IS NOT NULL
                    WHERE T1.EQT_AMT IS NOT NULL
                  ) XX INNER JOIN (SELECT BAS_YM,
                                          RANK_GRP,
                                          ROUND(AVG(BAD_DBT_RTO), 2) AS AGT_VAL
                                   FROM   (SELECT T1.BAS_YM,
                                                  T1.PCF_ID,
                                                  (CASE WHEN T1.TOT_LN_BAL = 0 AND T1.BAD_DBT_AMT = 0 THEN 0
                                                        ELSE CASE WHEN T1.TOT_LN_BAL = 0 THEN 0
                                                                  ELSE T1.BAD_DBT_AMT / T1.TOT_LN_BAL * 100
                                                                  END
                                                        END) AS BAD_DBT_RTO,
                                                  T2.EQT_AMT,
                                                  RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T2.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) AS RNK_NUM,
                                                  CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T2.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                                            THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T2.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                                                       ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T2.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                                                       END AS RANK_GRP
                                             FROM FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN FROM_TM27_MMLY_PCF_KEY_IDC_A T2
                                                                                      ON T2.BAS_YM = T1.BAS_YM
                                                                                     AND T2.PCF_ID = T1.PCF_ID
                                                                                     AND T2.EQT_AMT IS NOT NULL
                                            WHERE T1.TOT_LN_BAL  IS NOT NULL
                                              AND T1.BAD_DBT_AMT IS NOT NULL
                                          )
                                   GROUP BY BAS_YM, RANK_GRP
                                  ) YY
                               ON YY.BAS_YM   = XX.BAS_YM
                              AND YY.RANK_GRP = XX.RANK_GRP

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.PCF_ID,
                  '069'    AS KPI_TYP_CD,
                  '4'      AS AGT_METH_CD,
                  '069006' AS REF_IDC_TYP_CD,
                  XX.RANK_GRP,
                  YY.AGT_VAL
           FROM   (SELECT T1.BAS_YM,
                          T1.PCF_ID,
                          CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                    THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                               ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                               END AS RANK_GRP
                   FROM FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN FROM_TM26_MMLY_AST_QAL_A T2
                                                                ON T2.BAS_YM = T1.BAS_YM
                                                               AND T2.PCF_ID = T1.PCF_ID
                                                               AND T2.TOT_LN_BAL  IS NOT NULL
                                                               AND T2.BAD_DBT_AMT IS NOT NULL
                    WHERE T1.EQT_AMT IS NOT NULL
                  ) XX INNER JOIN (SELECT BAS_YM,
                                          RANK_GRP,
                                          ROUND(SUM(BAD_DBT_RTO), 2) AS AGT_VAL
                                     FROM (SELECT T1.BAS_YM,
                                                  T1.PCF_ID,
                                                  CASE WHEN T1.TOT_LN_BAL = 0 AND T1.BAD_DBT_AMT = 0 THEN 0
                                                       ELSE CASE WHEN T1.TOT_LN_BAL = 0 THEN 0
                                                                 ELSE T1.BAD_DBT_AMT / T1.TOT_LN_BAL * 100 * T3.ASSET_WEIGHTED_AVG
                                                                 END
                                                       END AS BAD_DBT_RTO,
                                                  T2.EQT_AMT,
                                                  RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T2.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) AS RNK_NUM,
                                                  CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T2.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                                            THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T2.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                                                       ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T2.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                                                       END AS RANK_GRP
                                             FROM FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN FROM_TM27_MMLY_PCF_KEY_IDC_A T2
                                                                                      ON T2.BAS_YM = T1.BAS_YM
                                                                                     AND T2.PCF_ID = T1.PCF_ID
                                                                                     AND T2.EQT_AMT IS NOT NULL
                                                                              INNER JOIN (SELECT A.BAS_YM,
                                                                                                 A.PCF_ID,
                                                                                                 CASE WHEN B.TOT_AST_AMT = 0 THEN 0
                                                                                                      ELSE A.TOT_AST_AMT / B.TOT_AST_AMT
                                                                                                      END AS ASSET_WEIGHTED_AVG
                                                                                          FROM   (SELECT T1.BAS_YM,
                                                                                                         CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                                                                                                   THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                                                                                                              ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                                                                                                              END AS RANK_GRP,
                                                                                                         T1.PCF_ID,
                                                                                                         T1.TOT_AST_AMT
                                                                                                  FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN FROM_TM26_MMLY_AST_QAL_A T2
                                                                                                                                                 ON T2.BAS_YM = T1.BAS_YM
                                                                                                                                                AND T2.PCF_ID = T1.PCF_ID
                                                                                                                                                AND T2.TOT_LN_BAL  IS NOT NULL
                                                                                                                                                AND T2.BAD_DBT_AMT IS NOT NULL
                                                                                                  WHERE  T1.TOT_AST_AMT IS NOT NULL
                                                                                                  AND    T1.EQT_AMT IS NOT NULL
                                                                                                 ) A INNER JOIN (SELECT BAS_YM,
                                                                                                                        RANK_GRP,
                                                                                                                        SUM(TOT_AST_AMT) AS TOT_AST_AMT
                                                                                                                 FROM  (SELECT T1.BAS_YM,
                                                                                                                               CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                                                                                                                         THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                                                                                                                                    ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                                                                                                                                    END AS RANK_GRP,
                                                                                                                               T1.TOT_AST_AMT
                                                                                                                        FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN FROM_TM26_MMLY_AST_QAL_A T2
                                                                                                                                                                       ON T2.BAS_YM = T1.BAS_YM
                                                                                                                                                                      AND T2.PCF_ID = T1.PCF_ID
                                                                                                                                                                      AND T2.TOT_LN_BAL  IS NOT NULL
                                                                                                                                                                      AND T2.BAD_DBT_AMT IS NOT NULL
                                                                                                                        WHERE  T1.TOT_AST_AMT IS NOT NULL
                                                                                                                        AND    T1.EQT_AMT IS NOT NULL
                                                                                                                       )
                                                                                                                 GROUP BY BAS_YM, RANK_GRP
                                                                                                                ) B
                                                                                                             ON B.BAS_YM = A.BAS_YM
                                                                                                            AND B.RANK_GRP = A.RANK_GRP
                                                                                         ) T3
                                                                                      ON T3.BAS_YM = T1.BAS_YM
                                                                                     AND T3.PCF_ID = T1.PCF_ID
                                            WHERE T1.TOT_LN_BAL  IS NOT NULL
                                              AND T1.BAD_DBT_AMT IS NOT NULL
                                          )
                                   GROUP BY BAS_YM, RANK_GRP
                                  ) YY
                               ON YY.BAS_YM   = XX.BAS_YM
                              AND YY.RANK_GRP = XX.RANK_GRP
          ),
          FROM_KPI_TYP_070 AS
          (SELECT XX.BAS_YM,
                  XX.PCF_ID,
                  '070'    AS KPI_TYP_CD,
                  '2'      AS AGT_METH_CD,
                  '070006' AS REF_IDC_TYP_CD,
                  XX.RANK_GRP,
                  YY.AGT_VAL
           FROM   (SELECT T1.BAS_YM,
                          T1.PCF_ID,
                          CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                    THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                               ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                               END AS RANK_GRP
                   FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN FROM_TM26_MMLY_AST_QAL_A T2
                                                                  ON T2.BAS_YM = T1.BAS_YM
                                                                 AND T2.PCF_ID = T1.PCF_ID
                                                                 AND T2.TOT_LN_BAL   IS NOT NULL
                                                                 AND T2.TOT_COLL_VAL IS NOT NULL
                    WHERE T1.EQT_AMT IS NOT NULL
                  ) XX INNER JOIN (SELECT BAS_YM,
                                          RANK_GRP,
                                          ROUND(AVG(COLL_RTO), 2) AS AGT_VAL
                                   FROM   (SELECT T1.BAS_YM,
                                                  T1.PCF_ID,
                                                  (CASE WHEN T1.TOT_LN_BAL = 0 THEN 0
                                                        ELSE T1.TOT_COLL_VAL / T1.TOT_LN_BAL * 100
                                                        END) AS COLL_RTO,
                                                  T2.EQT_AMT,
                                                  RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T2.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) AS RNK_NUM,
                                                  CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T2.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                                            THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T2.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                                                       ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T2.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                                                       END AS RANK_GRP
                                           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN FROM_TM27_MMLY_PCF_KEY_IDC_A T2
                                                                                      ON T2.BAS_YM = T1.BAS_YM
                                                                                     AND T2.PCF_ID = T1.PCF_ID
                                                                                     AND T2.EQT_AMT IS NOT NULL
                                           WHERE  T1.TOT_LN_BAL   IS NOT NULL
                                           AND    T1.TOT_COLL_VAL IS NOT NULL
                                          )
                                   GROUP BY BAS_YM, RANK_GRP
                                  ) YY
                               ON YY.BAS_YM   = XX.BAS_YM
                              AND YY.RANK_GRP = XX.RANK_GRP

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.PCF_ID,
                  '070'    AS KPI_TYP_CD,
                  '4'      AS AGT_METH_CD,
                  '070006' AS REF_IDC_TYP_CD,
                  XX.RANK_GRP,
                  YY.AGT_VAL
           FROM   (SELECT T1.BAS_YM,
                          T1.PCF_ID,
                          CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                    THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                               ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                               END AS RANK_GRP
                   FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN FROM_TM26_MMLY_AST_QAL_A T2
                                                                  ON T2.BAS_YM = T1.BAS_YM
                                                                 AND T2.PCF_ID = T1.PCF_ID
                                                                 AND T2.TOT_LN_BAL   IS NOT NULL
                                                                 AND T2.TOT_COLL_VAL IS NOT NULL
                    WHERE T1.EQT_AMT IS NOT NULL
                  ) XX INNER JOIN (SELECT BAS_YM,
                                          RANK_GRP,
                                          ROUND(SUM(COLL_RTO), 2) AS AGT_VAL
                                   FROM   (SELECT T1.BAS_YM,
                                                  T1.PCF_ID,
                                                  CASE WHEN T1.TOT_LN_BAL = 0 THEN 0
                                                       ELSE T1.TOT_COLL_VAL / T1.TOT_LN_BAL * 100 * T3.ASSET_WEIGHTED_AVG
                                                       END AS COLL_RTO,
                                                  T2.EQT_AMT,
                                                  RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T2.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) AS RNK_NUM,
                                                  CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T2.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                                            THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T2.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                                                       ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T2.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                                                       END AS RANK_GRP
                                           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN FROM_TM27_MMLY_PCF_KEY_IDC_A T2
                                                                                      ON T2.BAS_YM = T1.BAS_YM
                                                                                     AND T2.PCF_ID = T1.PCF_ID
                                                                                     AND T2.EQT_AMT IS NOT NULL
                                                                              INNER JOIN (SELECT A.BAS_YM,
                                                                                                 A.PCF_ID,
                                                                                                 CASE WHEN B.TOT_AST_AMT = 0 THEN 0
                                                                                                      ELSE A.TOT_AST_AMT / B.TOT_AST_AMT
                                                                                                      END AS ASSET_WEIGHTED_AVG
                                                                                          FROM   (SELECT T1.BAS_YM,
                                                                                                         CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                                                                                                   THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                                                                                                              ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                                                                                                              END AS RANK_GRP,
                                                                                                         T1.PCF_ID,
                                                                                                         T1.TOT_AST_AMT
                                                                                                  FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN FROM_TM26_MMLY_AST_QAL_A T2
                                                                                                                                                ON T2.BAS_YM = T1.BAS_YM
                                                                                                                                               AND T2.PCF_ID = T1.PCF_ID
                                                                                                                                               AND T2.TOT_LN_BAL   IS NOT NULL
                                                                                                                                               AND T2.TOT_COLL_VAL IS NOT NULL
                                                                                                  WHERE  T1.TOT_AST_AMT IS NOT NULL
                                                                                                  AND    T1.EQT_AMT IS NOT NULL
                                                                                                 ) A INNER JOIN (SELECT BAS_YM,
                                                                                                                        RANK_GRP,
                                                                                                                        SUM(TOT_AST_AMT) AS TOT_AST_AMT
                                                                                                                 FROM  (SELECT T1.BAS_YM,
                                                                                                                               CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                                                                                                                         THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                                                                                                                                    ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                                                                                                                                    END AS RANK_GRP,
                                                                                                                               T1.TOT_AST_AMT
                                                                                                                        FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN FROM_TM26_MMLY_AST_QAL_A T2
                                                                                                                                                                       ON T2.BAS_YM = T1.BAS_YM
                                                                                                                                                                      AND T2.PCF_ID = T1.PCF_ID
                                                                                                                                                                      AND T2.TOT_LN_BAL   IS NOT NULL
                                                                                                                                                                      AND T2.TOT_COLL_VAL IS NOT NULL
                                                                                                                        WHERE  T1.TOT_AST_AMT IS NOT NULL
                                                                                                                        AND    T1.EQT_AMT IS NOT NULL
                                                                                                                       )
                                                                                                                 GROUP BY BAS_YM, RANK_GRP
                                                                                                                ) B
                                                                                                             ON B.BAS_YM = A.BAS_YM
                                                                                                            AND B.RANK_GRP = A.RANK_GRP
                                                                                         ) T3
                                                                                      ON T3.BAS_YM = T1.BAS_YM
                                                                                     AND T3.PCF_ID = T1.PCF_ID
                                            WHERE T1.TOT_LN_BAL   IS NOT NULL
                                              AND T1.TOT_COLL_VAL IS NOT NULL
                                          )
                                   GROUP BY BAS_YM, RANK_GRP
                                  ) YY
                               ON YY.BAS_YM   = XX.BAS_YM
                              AND YY.RANK_GRP = XX.RANK_GRP

          ),
          FROM_KPI_TYP_071 AS
          (SELECT XX.BAS_YM,
                  XX.PCF_ID,
                  '071'    AS KPI_TYP_CD,
                  '2'      AS AGT_METH_CD,
                  '071006' AS REF_IDC_TYP_CD,
                  XX.RANK_GRP,
                  YY.AGT_VAL
           FROM   (SELECT T1.BAS_YM,
                          T1.PCF_ID,
                          CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                    THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                               ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                               END AS RANK_GRP
                   FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN FROM_TM26_MMLY_AST_QAL_A T2
                                                                  ON T2.BAS_YM = T1.BAS_YM
                                                                 AND T2.PCF_ID = T1.PCF_ID
                                                                 AND T2.TOT_LN_BAL   IS NOT NULL
                                                                 AND T2.TOT_PRVS_AMT IS NOT NULL
                    WHERE T1.EQT_AMT IS NOT NULL
                  ) XX INNER JOIN (SELECT BAS_YM,
                                          RANK_GRP,
                                          ROUND(AVG(PRVS_RTO), 2) AS AGT_VAL
                                   FROM   (SELECT T1.BAS_YM,
                                                  T1.PCF_ID,
                                                  (CASE WHEN T1.TOT_LN_BAL = 0 THEN 0
                                                        ELSE T1.TOT_PRVS_AMT / T1.TOT_LN_BAL * 100
                                                        END) AS PRVS_RTO,
                                                  T2.EQT_AMT,
                                                  RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T2.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) AS RNK_NUM,
                                                  CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T2.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                                            THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T2.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                                                       ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T2.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                                                       END AS RANK_GRP
                                           FROM   FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN FROM_TM27_MMLY_PCF_KEY_IDC_A T2
                                                                                      ON T2.BAS_YM = T1.BAS_YM
                                                                                     AND T2.PCF_ID = T1.PCF_ID
                                                                                     AND T2.EQT_AMT IS NOT NULL
                                           WHERE T1.TOT_LN_BAL IS NOT NULL
                                           AND T1.TOT_PRVS_AMT IS NOT NULL
                                          )
                                   GROUP BY BAS_YM, RANK_GRP
                                  ) YY
                               ON YY.BAS_YM   = XX.BAS_YM
                              AND YY.RANK_GRP = XX.RANK_GRP

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.PCF_ID,
                  '071'    AS KPI_TYP_CD,
                  '4'      AS AGT_METH_CD,
                  '071006' AS REF_IDC_TYP_CD,
                  XX.RANK_GRP,
                  YY.AGT_VAL
           FROM   (SELECT T1.BAS_YM,
                          T1.PCF_ID,
                          CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                    THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                               ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                               END AS RANK_GRP
                   FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN FROM_TM26_MMLY_AST_QAL_A T2
                                                                  ON T2.BAS_YM = T1.BAS_YM
                                                                 AND T2.PCF_ID = T1.PCF_ID
                                                                 AND T2.TOT_LN_BAL   IS NOT NULL
                                                                 AND T2.TOT_PRVS_AMT IS NOT NULL
                   WHERE T1.EQT_AMT IS NOT NULL
                  ) XX INNER JOIN (SELECT BAS_YM,
                                          RANK_GRP,
                                          ROUND(SUM(PRVS_RTO), 2) AS AGT_VAL
                                   FROM   (SELECT T1.BAS_YM,
                                                  T1.PCF_ID,
                                                  CASE WHEN T1.TOT_LN_BAL = 0 THEN 0
                                                       ELSE T1.TOT_PRVS_AMT / T1.TOT_LN_BAL * 100 * T3.ASSET_WEIGHTED_AVG
                                                       END AS PRVS_RTO,
                                                  T2.EQT_AMT,
                                                  RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T2.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) AS RNK_NUM,
                                                  CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T2.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                                            THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T2.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                                                       ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T2.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                                                       END AS RANK_GRP
                                           FROM FROM_TM26_MMLY_AST_QAL_A T1 INNER JOIN FROM_TM27_MMLY_PCF_KEY_IDC_A T2
                                                                                    ON T2.BAS_YM = T1.BAS_YM
                                                                                   AND T2.PCF_ID = T1.PCF_ID
                                                                                   AND T2.EQT_AMT IS NOT NULL
                                                                            INNER JOIN (SELECT A.BAS_YM,
                                                                                               A.PCF_ID,
                                                                                               CASE WHEN B.TOT_AST_AMT = 0 THEN 0
                                                                                                    ELSE A.TOT_AST_AMT / B.TOT_AST_AMT
                                                                                                    END AS ASSET_WEIGHTED_AVG
                                                                                        FROM   (SELECT T1.BAS_YM,
                                                                                                       CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                                                                                                 THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                                                                                                            ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                                                                                                            END AS RANK_GRP,
                                                                                                       T1.PCF_ID,
                                                                                                       T1.TOT_AST_AMT
                                                                                                FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN FROM_TM26_MMLY_AST_QAL_A T2
                                                                                                                                               ON T2.BAS_YM = T1.BAS_YM
                                                                                                                                              AND T2.PCF_ID = T1.PCF_ID
                                                                                                                                              AND T2.TOT_LN_BAL   IS NOT NULL
                                                                                                                                              AND T2.TOT_PRVS_AMT IS NOT NULL
                                                                                                WHERE  T1.TOT_AST_AMT IS NOT NULL
                                                                                                AND    T1.EQT_AMT IS NOT NULL
                                                                                               ) A INNER JOIN (SELECT BAS_YM,
                                                                                                                      RANK_GRP,
                                                                                                                      SUM(TOT_AST_AMT) AS TOT_AST_AMT
                                                                                                               FROM  (SELECT T1.BAS_YM,
                                                                                                                             CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                                                                                                                       THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                                                                                                                                  ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                                                                                                                                  END AS RANK_GRP,
                                                                                                                             T1.TOT_AST_AMT
                                                                                                                      FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN FROM_TM26_MMLY_AST_QAL_A T2
                                                                                                                                                                     ON T2.BAS_YM = T1.BAS_YM
                                                                                                                                                                    AND T2.PCF_ID = T1.PCF_ID
                                                                                                                                                                    AND T2.TOT_LN_BAL   IS NOT NULL
                                                                                                                                                                    AND T2.TOT_PRVS_AMT IS NOT NULL
                                                                                                                      WHERE  T1.TOT_AST_AMT IS NOT NULL
                                                                                                                      AND    T1.EQT_AMT IS NOT NULL
                                                                                                                     )
                                                                                                               GROUP BY BAS_YM, RANK_GRP
                                                                                                              ) B
                                                                                                           ON B.BAS_YM = A.BAS_YM
                                                                                                          AND B.RANK_GRP = A.RANK_GRP
                                                                                       ) T3
                                                                                    ON T3.BAS_YM = T1.BAS_YM
                                                                                   AND T3.PCF_ID = T1.PCF_ID
                                            WHERE T1.TOT_LN_BAL   IS NOT NULL
                                              AND T1.TOT_PRVS_AMT IS NOT NULL
                                          )
                                   GROUP BY BAS_YM, RANK_GRP
                                  ) YY
                               ON YY.BAS_YM   = XX.BAS_YM
                              AND YY.RANK_GRP = XX.RANK_GRP

          ),
          FROM_KPI_TYP_088 AS
          (SELECT XX.BAS_YM,
                  XX.PCF_ID,
                  '088'    AS KPI_TYP_CD,
                  '2'      AS AGT_METH_CD,
                  '088011' AS REF_IDC_TYP_CD,
                  XX.RANK_GRP,
                  YY.AGT_VAL
           FROM   (SELECT BAS_YM,
                          PCF_ID,
                          CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                    THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100
                               ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                               END AS RANK_GRP
                   FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A
                   WHERE  TOT_AST_AMT IS NOT NULL
                   AND    PRFBL_AST_VS_TOT_AST_RTO IS NOT NULL
                  ) XX INNER JOIN (SELECT BAS_YM,
                                          RANK_GRP,
                                          ROUND(AVG(PRFBL_AST_VS_TOT_AST_RTO), 2) AS AGT_VAL
                                   FROM   (SELECT BAS_YM,
                                                  PCF_ID,
                                                  PRFBL_AST_VS_TOT_AST_RTO,
                                                  TOT_AST_AMT,
                                                  RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) AS RNK_NUM,
                                                  CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                                            THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100
                                                       ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                                                       END AS RANK_GRP
                                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A
                                           WHERE  TOT_AST_AMT IS NOT NULL
                                           AND    PRFBL_AST_VS_TOT_AST_RTO IS NOT NULL
                                          )
                                   GROUP BY BAS_YM, RANK_GRP
                                  ) YY
                               ON YY.BAS_YM   = XX.BAS_YM
                              AND YY.RANK_GRP = XX.RANK_GRP

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.PCF_ID,
                  '088'    AS KPI_TYP_CD,
                  '4'      AS AGT_METH_CD,
                  '088011' AS REF_IDC_TYP_CD,
                  XX.RANK_GRP,
                  YY.AGT_VAL
           FROM   (SELECT BAS_YM,
                          PCF_ID,
                          CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                    THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100
                               ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                               END AS RANK_GRP
                   FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A
                   WHERE  TOT_AST_AMT IS NOT NULL
                   AND    PRFBL_AST_AMT IS NOT NULL
                  ) XX INNER JOIN (SELECT BAS_YM,
                                          RANK_GRP,
                                          ROUND(SUM(PRFBL_AST_VS_TOT_AST_RTO), 2) AS AGT_VAL
                                   FROM   (SELECT T1.BAS_YM,
                                                  T1.PCF_ID,
                                                  CASE WHEN T1.TOT_AST_AMT = 0 THEN 0
                                                       ELSE T1.PRFBL_AST_AMT / T1.TOT_AST_AMT * 100 * T2.ASSET_WEIGHTED_AVG
                                                       END AS PRFBL_AST_VS_TOT_AST_RTO,
                                                  T1.TOT_AST_AMT,
                                                  RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) AS RNK_NUM,
                                                  CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                                            THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                                                       ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                                                       END AS RANK_GRP
                                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN (SELECT A.BAS_YM,
                                                                                                     A.PCF_ID,
                                                                                                     CASE WHEN B.TOT_AST_AMT = 0 THEN 0
                                                                                                          ELSE A.TOT_AST_AMT / B.TOT_AST_AMT
                                                                                                          END AS ASSET_WEIGHTED_AVG
                                                                                              FROM   (SELECT T1.BAS_YM,
                                                                                                             CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                                                                                                       THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                                                                                                                  ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                                                                                                                  END AS RANK_GRP,
                                                                                                             T1.PCF_ID,
                                                                                                             T1.TOT_AST_AMT
                                                                                                      FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1
                                                                                                      WHERE  T1.TOT_AST_AMT IS NOT NULL
                                                                                                      AND    T1.PRFBL_AST_AMT IS NOT NULL
                                                                                                     ) A INNER JOIN (SELECT BAS_YM,
                                                                                                                            RANK_GRP,
                                                                                                                            SUM(TOT_AST_AMT) AS TOT_AST_AMT
                                                                                                                     FROM   (SELECT T1.BAS_YM,
                                                                                                                                    CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                                                                                                                              THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                                                                                                                                         ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                                                                                                                                         END AS RANK_GRP,
                                                                                                                                    T1.TOT_AST_AMT
                                                                                                                             FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1
                                                                                                                             WHERE  T1.TOT_AST_AMT IS NOT NULL
                                                                                                                             AND    T1.PRFBL_AST_AMT IS NOT NULL
                                                                                                                            )
                                                                                                                     GROUP BY BAS_YM, RANK_GRP
                                                                                                                    ) B
                                                                                                                 ON B.BAS_YM = A.BAS_YM
                                                                                                                AND B.RANK_GRP = A.RANK_GRP
                                                                                             ) T2
                                                                                          ON T2.BAS_YM = T1.BAS_YM
                                                                                         AND T2.PCF_ID = T1.PCF_ID
                                            WHERE T1.TOT_AST_AMT IS NOT NULL
                                              AND T1.PRFBL_AST_AMT IS NOT NULL
                                          )
                                   GROUP BY BAS_YM, RANK_GRP
                                  ) YY
                               ON YY.BAS_YM   = XX.BAS_YM
                              AND YY.RANK_GRP = XX.RANK_GRP

           UNION ALL

           SELECT BAS_YM,
                  PCF_ID,
                  '088'    AS KPI_TYP_CD,
                  '5'      AS AGT_METH_CD,
                  '088011' AS REF_IDC_TYP_CD,
                  RANK_GRP,
                  AGT_VAL
           FROM  (
                  SELECT T1.BAS_YM,
                         T1.PCF_ID,
                         MAX(T2.RANK_GRP) AS RANK_GRP,
                         ROUND(AVG(T2.AGT_VAL), 2) AS AGT_VAL
                    FROM FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN (SELECT XX.BAS_YM,
                                                                            XX.PCF_ID,
                                                                            XX.RANK_GRP,
                                                                            YY.AGT_VAL
                                                                     FROM   (SELECT BAS_YM,
                                                                                    PCF_ID,
                                                                                    CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                                                                              THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100
                                                                                         ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                                                                                         END AS RANK_GRP
                                                                             FROM   TM27_MMLY_PCF_KEY_IDC_A
                                                                             WHERE  TOT_AST_AMT IS NOT NULL
                                                                             AND    PRFBL_AST_VS_TOT_AST_RTO IS NOT NULL
                                                                            ) XX INNER JOIN (SELECT BAS_YM,
                                                                                                    RANK_GRP,
                                                                                                    ROUND(AVG(PRFBL_AST_VS_TOT_AST_RTO), 2) AS AGT_VAL
                                                                                             FROM   (SELECT BAS_YM,
                                                                                                            PCF_ID,
                                                                                                            PRFBL_AST_VS_TOT_AST_RTO,
                                                                                                            TOT_AST_AMT,
                                                                                                            RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) AS RNK_NUM,
                                                                                                            CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                                                                                                      THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100
                                                                                                                 ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                                                                                                                 END AS RANK_GRP
                                                                                                     FROM   TM27_MMLY_PCF_KEY_IDC_A
                                                                                                     WHERE  TOT_AST_AMT IS NOT NULL
                                                                                                     AND    PRFBL_AST_VS_TOT_AST_RTO IS NOT NULL
                                                                                                    )
                                                                                             GROUP BY BAS_YM, RANK_GRP
                                                                                            ) YY
                                                                                         ON YY.BAS_YM   = XX.BAS_YM
                                                                                        AND YY.RANK_GRP = XX.RANK_GRP
                                                                    ) T2
                                                                 ON T2.BAS_YM   >= TO_CHAR(ADD_MONTHS(TO_DATE(T1.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                                AND T2.BAS_YM   <= T1.BAS_YM
                                                                AND T2.PCF_ID   = T1.PCF_ID
                   WHERE T1.TOT_AST_AMT IS NOT NULL
                     AND T1.PRFBL_AST_VS_TOT_AST_RTO IS NOT NULL
                   GROUP BY T1.BAS_YM, T1.PCF_ID
                 )

           UNION ALL

           SELECT BAS_YM,
                  PCF_ID,
                  '088'    AS KPI_TYP_CD,
                  '7'      AS AGT_METH_CD,
                  '088011' AS REF_IDC_TYP_CD,
                  RANK_GRP,
                  AGT_VAL
           FROM  (
                  SELECT T1.BAS_YM,
                         T1.PCF_ID,
                         MAX(T2.RANK_GRP) AS RANK_GRP,
                         ROUND(AVG(T2.AGT_VAL), 2) AS AGT_VAL
                  FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN (SELECT XX.BAS_YM,
                                                                            XX.PCF_ID,
                                                                            XX.RANK_GRP,
                                                                            YY.AGT_VAL
                                                                     FROM   (SELECT BAS_YM,
                                                                                    PCF_ID,
                                                                                    CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                                                                              THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100
                                                                                         ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                                                                                         END AS RANK_GRP
                                                                             FROM   TM27_MMLY_PCF_KEY_IDC_A
                                                                             WHERE  TOT_AST_AMT IS NOT NULL
                                                                             AND    PRFBL_AST_AMT IS NOT NULL
                                                                            ) XX INNER JOIN (SELECT BAS_YM,
                                                                                                    RANK_GRP,
                                                                                                    ROUND(SUM(PRFBL_AST_VS_TOT_AST_RTO), 2) AS AGT_VAL
                                                                                             FROM   (SELECT T1.BAS_YM,
                                                                                                            T1.PCF_ID,
                                                                                                            CASE WHEN T1.TOT_AST_AMT = 0 THEN 0
                                                                                                                 ELSE T1.PRFBL_AST_AMT / T1.TOT_AST_AMT * 100 * T2.ASSET_WEIGHTED_AVG
                                                                                                                 END AS PRFBL_AST_VS_TOT_AST_RTO,
                                                                                                            T1.TOT_AST_AMT,
                                                                                                            RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) AS RNK_NUM,
                                                                                                            CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                                                                                                      THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                                                                                                                 ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                                                                                                                 END AS RANK_GRP
                                                                                                       FROM TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN (SELECT A.BAS_YM,
                                                                                                                                                          A.PCF_ID,
                                                                                                                                                          CASE WHEN B.TOT_AST_AMT = 0 THEN 0
                                                                                                                                                               ELSE A.TOT_AST_AMT / B.TOT_AST_AMT
                                                                                                                                                               END AS ASSET_WEIGHTED_AVG
                                                                                                                                                   FROM   (SELECT T1.BAS_YM,
                                                                                                                                                                  CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                                                                                                                                                            THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                                                                                                                                                                       ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                                                                                                                                                                       END AS RANK_GRP,
                                                                                                                                                                  T1.PCF_ID,
                                                                                                                                                                  T1.TOT_AST_AMT
                                                                                                                                                           FROM   TM27_MMLY_PCF_KEY_IDC_A T1
                                                                                                                                                           WHERE  T1.TOT_AST_AMT IS NOT NULL
                                                                                                                                                           AND    T1.PRFBL_AST_AMT IS NOT NULL
                                                                                                                                                          ) A INNER JOIN (SELECT BAS_YM,
                                                                                                                                                                                 RANK_GRP,
                                                                                                                                                                                 SUM(TOT_AST_AMT) AS TOT_AST_AMT
                                                                                                                                                                          FROM   (SELECT T1.BAS_YM,
                                                                                                                                                                                         CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                                                                                                                                                                                   THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                                                                                                                                                                                              ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                                                                                                                                                                                              END AS RANK_GRP,
                                                                                                                                                                                         T1.TOT_AST_AMT
                                                                                                                                                                                  FROM   TM27_MMLY_PCF_KEY_IDC_A T1
                                                                                                                                                                                  WHERE  T1.TOT_AST_AMT IS NOT NULL
                                                                                                                                                                                  AND    T1.PRFBL_AST_AMT IS NOT NULL
                                                                                                                                                                                 )
                                                                                                                                                                          GROUP BY BAS_YM, RANK_GRP
                                                                                                                                                                         ) B
                                                                                                                                                                      ON B.BAS_YM = A.BAS_YM
                                                                                                                                                                     AND B.RANK_GRP = A.RANK_GRP
                                                                                                                                                  ) T2
                                                                                                                                               ON T2.BAS_YM = T1.BAS_YM
                                                                                                                                              AND T2.PCF_ID = T1.PCF_ID
                                                                                                      WHERE T1.TOT_AST_AMT IS NOT NULL
                                                                                                        AND T1.PRFBL_AST_AMT IS NOT NULL
                                                                                                    )
                                                                                             GROUP BY BAS_YM, RANK_GRP
                                                                                            ) YY
                                                                                         ON YY.BAS_YM   = XX.BAS_YM
                                                                                        AND YY.RANK_GRP = XX.RANK_GRP
                                                                    ) T2
                                                                 ON T2.BAS_YM   >= TO_CHAR(ADD_MONTHS(TO_DATE(T1.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                                AND T2.BAS_YM   <= T1.BAS_YM
                                                                AND T2.PCF_ID   = T1.PCF_ID
                  GROUP BY T1.BAS_YM, T1.PCF_ID
                 )
          ),
          FROM_KPI_TYP_090 AS
          (SELECT XX.BAS_YM,
                  XX.PCF_ID,
                  '090'    AS KPI_TYP_CD,
                  '2'      AS AGT_METH_CD,
                  '090011' AS REF_IDC_TYP_CD,
                  XX.RANK_GRP,
                  YY.AGT_VAL
           FROM   (SELECT BAS_YM,
                          PCF_ID,
                          CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                    THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100
                               ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                               END AS RANK_GRP
                   FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A
                   WHERE  TOT_AST_AMT IS  NOT NULL
                   AND    TOT_INCM_AMT IS NOT NULL
                  ) XX INNER JOIN (SELECT BAS_YM,
                                          RANK_GRP,
                                          ROUND(AVG(NET_PROFIT), 0) AS AGT_VAL
                                   FROM   (SELECT BAS_YM,
                                                  PCF_ID,
                                                  TOT_INCM_AMT - TOT_EXP_AMT AS NET_PROFIT,
                                                  TOT_AST_AMT,
                                                  RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) AS RNK_NUM,
                                                  CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                                            THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100
                                                       ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                                                       END AS RANK_GRP
                                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A
                                           WHERE  TOT_AST_AMT IS  NOT NULL
                                           AND    TOT_INCM_AMT IS NOT NULL
                                          )
                                   GROUP BY BAS_YM, RANK_GRP
                                  ) YY
                               ON YY.BAS_YM   = XX.BAS_YM
                              AND YY.RANK_GRP = XX.RANK_GRP

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.PCF_ID,
                  '090'    AS KPI_TYP_CD,
                  '4'      AS AGT_METH_CD,
                  '090011' AS REF_IDC_TYP_CD,
                  XX.RANK_GRP,
                  YY.AGT_VAL
           FROM   (SELECT BAS_YM,
                          PCF_ID,
                          CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                    THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100
                               ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                               END AS RANK_GRP
                   FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A
                   WHERE  TOT_AST_AMT  IS NOT NULL
                   AND    TOT_INCM_AMT IS NOT NULL
                  ) XX INNER JOIN (SELECT BAS_YM,
                                          RANK_GRP,
                                          ROUND(SUM(NET_PROFIT), 0) AS AGT_VAL
                                   FROM   (SELECT T1.BAS_YM,
                                                  T1.PCF_ID,
                                                  T1.TOT_INCM_AMT - T1.TOT_EXP_AMT * T2.ASSET_WEIGHTED_AVG AS NET_PROFIT,
                                                  T1.TOT_AST_AMT,
                                                  RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) AS RNK_NUM,
                                                  CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                                            THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                                                       ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                                                       END AS RANK_GRP
                                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN (SELECT A.BAS_YM,
                                                                                                     A.PCF_ID,
                                                                                                     CASE WHEN B.TOT_AST_AMT = 0 THEN 0
                                                                                                          ELSE A.TOT_AST_AMT / B.TOT_AST_AMT
                                                                                                          END AS ASSET_WEIGHTED_AVG
                                                                                              FROM   (SELECT T1.BAS_YM,
                                                                                                             CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                                                                                                       THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                                                                                                                  ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                                                                                                                  END AS RANK_GRP,
                                                                                                             T1.PCF_ID,
                                                                                                             T1.TOT_AST_AMT
                                                                                                      FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1
                                                                                                      WHERE  T1.TOT_AST_AMT IS NOT NULL
                                                                                                      AND    T1.TOT_INCM_AMT IS NOT NULL
                                                                                                     ) A INNER JOIN (SELECT BAS_YM,
                                                                                                                            RANK_GRP,
                                                                                                                            SUM(TOT_AST_AMT) AS TOT_AST_AMT
                                                                                                                     FROM   (SELECT T1.BAS_YM,
                                                                                                                                    CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                                                                                                                              THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                                                                                                                                         ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                                                                                                                                         END AS RANK_GRP,
                                                                                                                                    T1.TOT_AST_AMT
                                                                                                                             FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1
                                                                                                                             WHERE  T1.TOT_AST_AMT IS NOT NULL
                                                                                                                             AND    T1.TOT_INCM_AMT IS NOT NULL
                                                                                                                            )
                                                                                                                     GROUP BY BAS_YM, RANK_GRP
                                                                                                                    ) B
                                                                                                                 ON B.BAS_YM = A.BAS_YM
                                                                                                                AND B.RANK_GRP = A.RANK_GRP
                                                                                             ) T2
                                                                                          ON T2.BAS_YM = T1.BAS_YM
                                                                                         AND T2.PCF_ID = T1.PCF_ID
                                            WHERE T1.TOT_AST_AMT  IS NOT NULL
                                              AND T1.TOT_INCM_AMT IS NOT NULL
                                          )
                                   GROUP BY BAS_YM, RANK_GRP
                                  ) YY
                               ON YY.BAS_YM   = XX.BAS_YM
                              AND YY.RANK_GRP = XX.RANK_GRP

           UNION ALL

           SELECT BAS_YM,
                  PCF_ID,
                  '090'    AS KPI_TYP_CD,
                  '5'      AS AGT_METH_CD,
                  '090011' AS REF_IDC_TYP_CD,
                  RANK_GRP,
                  AGT_VAL
           FROM  (
                  SELECT T1.BAS_YM,
                         T1.PCF_ID,
                         MAX(T2.RANK_GRP) AS RANK_GRP,
                         ROUND(AVG(T2.AGT_VAL), 0) AS AGT_VAL
                  FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN (SELECT XX.BAS_YM,
                                                                            XX.PCF_ID,
                                                                            XX.RANK_GRP,
                                                                            YY.AGT_VAL
                                                                     FROM   (SELECT BAS_YM,
                                                                                    PCF_ID,
                                                                                    CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                                                                              THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100
                                                                                         ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                                                                                         END AS RANK_GRP
                                                                               FROM TM27_MMLY_PCF_KEY_IDC_A
                                                                              WHERE TOT_AST_AMT  IS NOT NULL
                                                                                AND TOT_INCM_AMT IS NOT NULL
                                                                            ) XX INNER JOIN (SELECT BAS_YM,
                                                                                                    RANK_GRP,
                                                                                                    ROUND(AVG(NET_PROFIT), 0) AS AGT_VAL
                                                                                             FROM   (SELECT BAS_YM,
                                                                                                            PCF_ID,
                                                                                                            TOT_INCM_AMT - TOT_EXP_AMT AS NET_PROFIT,
                                                                                                            TOT_AST_AMT,
                                                                                                            RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) AS RNK_NUM,
                                                                                                            CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                                                                                                      THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100
                                                                                                                 ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                                                                                                                 END AS RANK_GRP
                                                                                                       FROM TM27_MMLY_PCF_KEY_IDC_A
                                                                                                      WHERE TOT_AST_AMT  IS NOT NULL
                                                                                                        AND TOT_INCM_AMT IS NOT NULL
                                                                                                    )
                                                                                             GROUP BY BAS_YM, RANK_GRP
                                                                                            ) YY
                                                                                         ON YY.BAS_YM   = XX.BAS_YM
                                                                                        AND YY.RANK_GRP = XX.RANK_GRP
                                                                    ) T2
                                                                 ON T2.BAS_YM   >= TO_CHAR(ADD_MONTHS(TO_DATE(T1.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                                AND T2.BAS_YM   <= T1.BAS_YM
                                                                AND T2.PCF_ID   = T1.PCF_ID
                  WHERE  T1.TOT_AST_AMT  IS NOT NULL
                  AND    T1.TOT_INCM_AMT IS NOT NULL
                  GROUP BY T1.BAS_YM, T1.PCF_ID
                 )

           UNION ALL

           SELECT BAS_YM,
                  PCF_ID,
                  '090'    AS KPI_TYP_CD,
                  '7'      AS AGT_METH_CD,
                  '090011' AS REF_IDC_TYP_CD,
                  RANK_GRP,
                  AGT_VAL
           FROM  (
                  SELECT T1.BAS_YM,
                         T1.PCF_ID,
                         MAX(T2.RANK_GRP) AS RANK_GRP,
                         ROUND(AVG(T2.AGT_VAL), 0) AS AGT_VAL
                  FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN (SELECT XX.BAS_YM,
                                                                            XX.PCF_ID,
                                                                            XX.RANK_GRP,
                                                                            YY.AGT_VAL
                                                                     FROM   (SELECT BAS_YM,
                                                                                    PCF_ID,
                                                                                    CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                                                                              THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100
                                                                                         ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                                                                                         END AS RANK_GRP
                                                                             FROM   TM27_MMLY_PCF_KEY_IDC_A
                                                                             WHERE  TOT_AST_AMT  IS NOT NULL
                                                                             AND    TOT_INCM_AMT IS NOT NULL
                                                                            ) XX INNER JOIN (SELECT BAS_YM,
                                                                                                    RANK_GRP,
                                                                                                    ROUND(SUM(NET_PROFIT), 0) AS AGT_VAL
                                                                                             FROM   (SELECT T1.BAS_YM,
                                                                                                            T1.PCF_ID,
                                                                                                            T1.TOT_INCM_AMT - T1.TOT_EXP_AMT * T2.ASSET_WEIGHTED_AVG AS NET_PROFIT,
                                                                                                            T1.TOT_AST_AMT,
                                                                                                            RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) AS RNK_NUM,
                                                                                                            CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                                                                                                      THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                                                                                                                 ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                                                                                                                 END AS RANK_GRP
                                                                                                       FROM TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN (SELECT A.BAS_YM,
                                                                                                                                                          A.PCF_ID,
                                                                                                                                                          CASE WHEN B.TOT_AST_AMT = 0 THEN 0
                                                                                                                                                               ELSE A.TOT_AST_AMT / B.TOT_AST_AMT
                                                                                                                                                               END AS ASSET_WEIGHTED_AVG
                                                                                                                                                     FROM (SELECT T1.BAS_YM,
                                                                                                                                                                  CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                                                                                                                                                            THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                                                                                                                                                                       ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                                                                                                                                                                       END AS RANK_GRP,
                                                                                                                                                                  T1.PCF_ID,
                                                                                                                                                                  T1.TOT_AST_AMT
                                                                                                                                                           FROM   TM27_MMLY_PCF_KEY_IDC_A T1
                                                                                                                                                           WHERE  T1.TOT_AST_AMT IS NOT NULL
                                                                                                                                                           AND    T1.TOT_INCM_AMT IS NOT NULL
                                                                                                                                                          ) A INNER JOIN (SELECT BAS_YM,
                                                                                                                                                                                 RANK_GRP,
                                                                                                                                                                                 SUM(TOT_AST_AMT) AS TOT_AST_AMT
                                                                                                                                                                          FROM   (SELECT T1.BAS_YM,
                                                                                                                                                                                         CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                                                                                                                                                                                   THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                                                                                                                                                                                              ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                                                                                                                                                                                              END AS RANK_GRP,
                                                                                                                                                                                         T1.TOT_AST_AMT
                                                                                                                                                                                  FROM   TM27_MMLY_PCF_KEY_IDC_A T1
                                                                                                                                                                                  WHERE  T1.TOT_AST_AMT IS NOT NULL
                                                                                                                                                                                  AND    T1.TOT_INCM_AMT IS NOT NULL
                                                                                                                                                                                 )
                                                                                                                                                                          GROUP BY BAS_YM, RANK_GRP
                                                                                                                                                                         ) B
                                                                                                                                                                      ON B.BAS_YM = A.BAS_YM
                                                                                                                                                                     AND B.RANK_GRP = A.RANK_GRP
                                                                                                                                                  ) T2
                                                                                                                                               ON T2.BAS_YM = T1.BAS_YM
                                                                                                                                              AND T2.PCF_ID = T1.PCF_ID
                                                                                                      WHERE T1.TOT_AST_AMT  IS NOT NULL
                                                                                                        AND T1.TOT_INCM_AMT IS NOT NULL
                                                                                                    )
                                                                                             GROUP BY BAS_YM, RANK_GRP
                                                                                            ) YY
                                                                                         ON YY.BAS_YM   = XX.BAS_YM
                                                                                        AND YY.RANK_GRP = XX.RANK_GRP
                                                                    ) T2
                                                                 ON T2.BAS_YM   >= TO_CHAR(ADD_MONTHS(TO_DATE(T1.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                                AND T2.BAS_YM   <= T1.BAS_YM
                                                                AND T2.PCF_ID   = T1.PCF_ID
                  WHERE  T1.TOT_AST_AMT  IS NOT NULL
                  AND    T1.TOT_INCM_AMT IS NOT NULL
                  GROUP BY T1.BAS_YM, T1.PCF_ID
                 )
          ),
          FROM_KPI_TYP_091 AS
          (SELECT XX.BAS_YM,
                  XX.PCF_ID,
                  '091'    AS KPI_TYP_CD,
                  '2'      AS AGT_METH_CD,
                  '091011' AS REF_IDC_TYP_CD,
                  XX.RANK_GRP,
                  YY.AGT_VAL
           FROM   (SELECT BAS_YM,
                          PCF_ID,
                          CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                    THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100
                               ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                               END AS RANK_GRP
                   FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A
                   WHERE  TOT_AST_AMT  IS NOT NULL
                   AND    TOT_INCM_AMT IS NOT NULL
                  ) XX INNER JOIN (SELECT BAS_YM,
                                          RANK_GRP,
                                          ROUND(AVG(PROF_VS_TOT_INCM_RTO), 2) AS AGT_VAL
                                   FROM   (SELECT BAS_YM,
                                                  PCF_ID,
                                                  CASE WHEN TOT_INCM_AMT - TOT_EXP_AMT = 0 THEN 0
                                                       WHEN TOT_INCM_AMT - TOT_EXP_AMT <> 0 AND TOT_INCM_AMT = 0 THEN NULL
                                                       ELSE CASE WHEN TOT_INCM_AMT = 0 THEN 0
                                                                 ELSE ( TOT_INCM_AMT - TOT_EXP_AMT ) / ( TOT_INCM_AMT ) * 100
                                                                 END
                                                       END AS PROF_VS_TOT_INCM_RTO,
                                                  TOT_AST_AMT,
                                                  RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) AS RNK_NUM,
                                                  CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                                            THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100
                                                       ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                                                       END AS RANK_GRP
                                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A
                                           WHERE  TOT_AST_AMT  IS NOT NULL
                                           AND    TOT_INCM_AMT IS NOT NULL
                                          )
                                   GROUP BY BAS_YM, RANK_GRP
                                  ) YY
                               ON YY.BAS_YM   = XX.BAS_YM
                              AND YY.RANK_GRP = XX.RANK_GRP

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.PCF_ID,
                  '091'    AS KPI_TYP_CD,
                  '4'      AS AGT_METH_CD,
                  '091011' AS REF_IDC_TYP_CD,
                  XX.RANK_GRP,
                  YY.AGT_VAL
           FROM   (SELECT BAS_YM,
                          PCF_ID,
                          CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                    THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100
                               ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                               END AS RANK_GRP
                   FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A
                   WHERE  TOT_AST_AMT  IS NOT NULL
                   AND    TOT_INCM_AMT IS NOT NULL
                  ) XX INNER JOIN (SELECT BAS_YM,
                                          RANK_GRP,
                                          ROUND(SUM(PROF_VS_TOT_INCM_RTO), 2) AS AGT_VAL
                                   FROM   (SELECT T1.BAS_YM,
                                                  T1.PCF_ID,
                                                  CASE WHEN T1.TOT_INCM_AMT - T1.TOT_EXP_AMT = 0 THEN 0
                                                       WHEN T1.TOT_INCM_AMT - T1.TOT_EXP_AMT <> 0 AND T1.TOT_INCM_AMT = 0 THEN NULL
                                                       ELSE CASE WHEN T1.TOT_INCM_AMT = 0 THEN 0
                                                                 ELSE ( T1.TOT_INCM_AMT - T1.TOT_EXP_AMT ) / ( T1.TOT_INCM_AMT ) * 100
                                                                 END
                                                       END * T2.ASSET_WEIGHTED_AVG AS PROF_VS_TOT_INCM_RTO,
                                                  T1.TOT_AST_AMT,
                                                  RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) AS RNK_NUM,
                                                  CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                                            THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                                                       ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                                                       END AS RANK_GRP
                                           FROM FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN (SELECT A.BAS_YM,
                                                                                                   A.PCF_ID,
                                                                                                   CASE WHEN B.TOT_AST_AMT = 0 THEN 0
                                                                                                        ELSE A.TOT_AST_AMT / B.TOT_AST_AMT
                                                                                                        END AS ASSET_WEIGHTED_AVG
                                                                                            FROM   (SELECT T1.BAS_YM,
                                                                                                           CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                                                                                                     THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                                                                                                                ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                                                                                                                END AS RANK_GRP,
                                                                                                           T1.PCF_ID,
                                                                                                           T1.TOT_AST_AMT
                                                                                                    FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1
                                                                                                    WHERE  T1.TOT_AST_AMT IS NOT NULL
                                                                                                    AND    T1.TOT_INCM_AMT IS NOT NULL
                                                                                                   ) A INNER JOIN (SELECT BAS_YM,
                                                                                                                          RANK_GRP,
                                                                                                                          SUM(TOT_AST_AMT) AS TOT_AST_AMT
                                                                                                                   FROM   (SELECT T1.BAS_YM,
                                                                                                                                  CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                                                                                                                            THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                                                                                                                                       ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                                                                                                                                       END AS RANK_GRP,
                                                                                                                                  T1.TOT_AST_AMT
                                                                                                                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1
                                                                                                                           WHERE  T1.TOT_AST_AMT IS NOT NULL
                                                                                                                           AND    T1.TOT_INCM_AMT IS NOT NULL
                                                                                                                          )
                                                                                                                   GROUP BY BAS_YM, RANK_GRP
                                                                                                                  ) B
                                                                                                               ON B.BAS_YM = A.BAS_YM
                                                                                                              AND B.RANK_GRP = A.RANK_GRP
                                                                                           ) T2
                                                                                        ON T2.BAS_YM = T1.BAS_YM
                                                                                       AND T2.PCF_ID = T1.PCF_ID
                                            WHERE T1.TOT_AST_AMT  IS NOT NULL
                                              AND T1.TOT_INCM_AMT IS NOT NULL
                                          )
                                   GROUP BY BAS_YM, RANK_GRP
                                  ) YY
                               ON YY.BAS_YM   = XX.BAS_YM
                              AND YY.RANK_GRP = XX.RANK_GRP

           UNION ALL

           SELECT BAS_YM,
                  PCF_ID,
                  '091'    AS KPI_TYP_CD,
                  '5'      AS AGT_METH_CD,
                  '091011' AS REF_IDC_TYP_CD,
                  RANK_GRP,
                  AGT_VAL
           FROM  (
                  SELECT T1.BAS_YM,
                         T1.PCF_ID,
                         MAX(T2.RANK_GRP) AS RANK_GRP,
                         ROUND(AVG(T2.AGT_VAL), 2) AS AGT_VAL
                  FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN (SELECT XX.BAS_YM,
                                                                            XX.PCF_ID,
                                                                            XX.RANK_GRP,
                                                                            YY.AGT_VAL
                                                                     FROM   (SELECT BAS_YM,
                                                                                    PCF_ID,
                                                                                    CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                                                                              THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100
                                                                                         ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                                                                                         END AS RANK_GRP
                                                                               FROM TM27_MMLY_PCF_KEY_IDC_A
                                                                              WHERE TOT_AST_AMT  IS NOT NULL
                                                                                AND TOT_INCM_AMT IS NOT NULL
                                                                            ) XX INNER JOIN (SELECT BAS_YM,
                                                                                                    RANK_GRP,
                                                                                                    AVG(PROF_VS_TOT_INCM_RTO) AS AGT_VAL
                                                                                             FROM   (SELECT BAS_YM,
                                                                                                            PCF_ID,
                                                                                                            CASE WHEN TOT_INCM_AMT - TOT_EXP_AMT = 0 THEN 0
                                                                                                                 WHEN TOT_INCM_AMT - TOT_EXP_AMT <> 0 AND TOT_INCM_AMT = 0 THEN NULL
                                                                                                                 ELSE CASE WHEN TOT_INCM_AMT = 0 THEN 0
                                                                                                                           ELSE ( TOT_INCM_AMT - TOT_EXP_AMT ) / ( TOT_INCM_AMT ) * 100
                                                                                                                           END
                                                                                                                 END AS PROF_VS_TOT_INCM_RTO,
                                                                                                            TOT_AST_AMT,
                                                                                                            RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) AS RNK_NUM,
                                                                                                            CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                                                                                                      THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100
                                                                                                                 ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                                                                                                                 END AS RANK_GRP
                                                                                                       FROM TM27_MMLY_PCF_KEY_IDC_A
                                                                                                      WHERE TOT_AST_AMT  IS NOT NULL
                                                                                                        AND TOT_INCM_AMT IS NOT NULL
                                                                                                    )
                                                                                             GROUP BY BAS_YM, RANK_GRP
                                                                                            ) YY
                                                                                         ON YY.BAS_YM   = XX.BAS_YM
                                                                                        AND YY.RANK_GRP = XX.RANK_GRP
                                                                    ) T2
                                                                 ON T2.BAS_YM   >= TO_CHAR(ADD_MONTHS(TO_DATE(T1.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                                AND T2.BAS_YM   <= T1.BAS_YM
                                                                AND T2.PCF_ID   = T1.PCF_ID
                  WHERE T1.TOT_AST_AMT  IS NOT NULL
                  AND T1.TOT_INCM_AMT IS NOT NULL
                  GROUP BY T1.BAS_YM, T1.PCF_ID
                 )

           UNION ALL

           SELECT BAS_YM,
                  PCF_ID,
                  '091'    AS KPI_TYP_CD,
                  '7'      AS AGT_METH_CD,
                  '091011' AS REF_IDC_TYP_CD,
                  RANK_GRP,
                  AGT_VAL
           FROM  (
                  SELECT T1.BAS_YM,
                         T1.PCF_ID,
                         MAX(T2.RANK_GRP) AS RANK_GRP,
                         ROUND(AVG(T2.AGT_VAL), 2) AS AGT_VAL
                  FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN (SELECT XX.BAS_YM,
                                                                            XX.PCF_ID,
                                                                            XX.RANK_GRP,
                                                                            YY.AGT_VAL
                                                                     FROM   (SELECT BAS_YM,
                                                                                    PCF_ID,
                                                                                    CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                                                                              THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100
                                                                                         ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                                                                                         END AS RANK_GRP
                                                                             FROM   TM27_MMLY_PCF_KEY_IDC_A
                                                                             WHERE  TOT_AST_AMT  IS NOT NULL
                                                                             AND    TOT_INCM_AMT IS NOT NULL
                                                                            ) XX INNER JOIN (SELECT BAS_YM,
                                                                                                    RANK_GRP,
                                                                                                    SUM(PROF_VS_TOT_INCM_RTO) AS AGT_VAL
                                                                                             FROM   (SELECT T1.BAS_YM,
                                                                                                            T1.PCF_ID,
                                                                                                            CASE WHEN T1.TOT_INCM_AMT - T1.TOT_EXP_AMT = 0 THEN 0
                                                                                                                 WHEN T1.TOT_INCM_AMT - T1.TOT_EXP_AMT <> 0 AND T1.TOT_INCM_AMT = 0 THEN NULL
                                                                                                                 ELSE CASE WHEN T1.TOT_INCM_AMT = 0 THEN 0
                                                                                                                           ELSE ( T1.TOT_INCM_AMT - T1.TOT_EXP_AMT ) / ( T1.TOT_INCM_AMT ) * 100
                                                                                                                           END
                                                                                                                 END * T2.ASSET_WEIGHTED_AVG AS PROF_VS_TOT_INCM_RTO,
                                                                                                            T1.TOT_AST_AMT,
                                                                                                            RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) AS RNK_NUM,
                                                                                                            CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                                                                                                      THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                                                                                                                 ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                                                                                                                 END AS RANK_GRP
                                                                                                       FROM TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN (SELECT A.BAS_YM,
                                                                                                                                                          A.PCF_ID,
                                                                                                                                                          CASE WHEN B.TOT_AST_AMT = 0 THEN 0
                                                                                                                                                               ELSE A.TOT_AST_AMT / B.TOT_AST_AMT
                                                                                                                                                               END AS ASSET_WEIGHTED_AVG
                                                                                                                                                   FROM   (SELECT T1.BAS_YM,
                                                                                                                                                                  CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                                                                                                                                                            THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                                                                                                                                                                       ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                                                                                                                                                                       END AS RANK_GRP,
                                                                                                                                                                  T1.PCF_ID,
                                                                                                                                                                  T1.TOT_AST_AMT
                                                                                                                                                           FROM   TM27_MMLY_PCF_KEY_IDC_A T1
                                                                                                                                                           WHERE  T1.TOT_AST_AMT IS NOT NULL
                                                                                                                                                           AND    T1.TOT_INCM_AMT IS NOT NULL
                                                                                                                                                          ) A INNER JOIN (SELECT BAS_YM,
                                                                                                                                                                                 RANK_GRP,
                                                                                                                                                                                 SUM(TOT_AST_AMT) AS TOT_AST_AMT
                                                                                                                                                                          FROM   (SELECT T1.BAS_YM,
                                                                                                                                                                                         CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                                                                                                                                                                                   THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                                                                                                                                                                                              ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                                                                                                                                                                                              END AS RANK_GRP,
                                                                                                                                                                                         T1.TOT_AST_AMT
                                                                                                                                                                                  FROM   TM27_MMLY_PCF_KEY_IDC_A T1
                                                                                                                                                                                  WHERE  T1.TOT_AST_AMT IS NOT NULL
                                                                                                                                                                                  AND    T1.TOT_INCM_AMT IS NOT NULL
                                                                                                                                                                                 )
                                                                                                                                                                          GROUP BY BAS_YM, RANK_GRP
                                                                                                                                                                         ) B
                                                                                                                                                                      ON B.BAS_YM = A.BAS_YM
                                                                                                                                                                     AND B.RANK_GRP = A.RANK_GRP
                                                                                                                                                  ) T2
                                                                                                                                               ON T2.BAS_YM = T1.BAS_YM
                                                                                                                                              AND T2.PCF_ID = T1.PCF_ID
                                                                                                      WHERE T1.TOT_AST_AMT  IS NOT NULL
                                                                                                        AND T1.TOT_INCM_AMT IS NOT NULL
                                                                                                    )
                                                                                             GROUP BY BAS_YM, RANK_GRP
                                                                                            ) YY
                                                                                         ON YY.BAS_YM   = XX.BAS_YM
                                                                                        AND YY.RANK_GRP = XX.RANK_GRP
                                                                    ) T2
                                                                 ON T2.BAS_YM   >= TO_CHAR(ADD_MONTHS(TO_DATE(T1.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                                AND T2.BAS_YM   <= T1.BAS_YM
                                                                AND T2.PCF_ID   = T1.PCF_ID
                  WHERE  T1.TOT_AST_AMT  IS NOT NULL
                  AND    T1.TOT_INCM_AMT IS NOT NULL
                  GROUP BY T1.BAS_YM, T1.PCF_ID
                 )
          ),
          FROM_KPI_TYP_092 AS
          (SELECT XX.BAS_YM,
                  XX.PCF_ID,
                  '092'    AS KPI_TYP_CD,
                  '2'      AS AGT_METH_CD,
                  '092011' AS REF_IDC_TYP_CD,
                  XX.RANK_GRP,
                  YY.AGT_VAL
           FROM   (SELECT BAS_YM,
                          PCF_ID,
                          CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                    THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100
                               ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                               END AS RANK_GRP
                   FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A
                   WHERE  TOT_AST_AMT  IS NOT NULL
                   AND    TOT_INCM_AMT IS NOT NULL
                  ) XX INNER JOIN (SELECT BAS_YM,
                                          RANK_GRP,
                                          ROUND(AVG(PROF_VS_LST_12_MM_AVG_AST), 2) AS AGT_VAL
                                   FROM   (SELECT BAS_YM,
                                                  PCF_ID,
                                                  CASE WHEN L12M_AST_AMT = 0 THEN 0
                                                       ELSE (TOT_INCM_AMT - TOT_EXP_AMT) * 12 /  L12M_AST_AMT * 100
                                                       END AS PROF_VS_LST_12_MM_AVG_AST,
                                                  TOT_AST_AMT,
                                                  RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) AS RNK_NUM,
                                                  CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                                            THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100
                                                       ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                                                       END AS RANK_GRP
                                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A
                                           WHERE  TOT_AST_AMT  IS NOT NULL
                                           AND    TOT_INCM_AMT IS NOT NULL
                                          )
                                   GROUP BY BAS_YM, RANK_GRP
                                  ) YY
                               ON YY.BAS_YM   = XX.BAS_YM
                              AND YY.RANK_GRP = XX.RANK_GRP

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.PCF_ID,
                  '092'    AS KPI_TYP_CD,
                  '4'      AS AGT_METH_CD,
                  '092011' AS REF_IDC_TYP_CD,
                  XX.RANK_GRP,
                  YY.AGT_VAL
           FROM   (SELECT BAS_YM,
                          PCF_ID,
                          CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                    THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100
                               ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                               END AS RANK_GRP
                   FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A
                   WHERE  TOT_AST_AMT  IS NOT NULL
                   AND    TOT_INCM_AMT IS NOT NULL
                  ) XX INNER JOIN (SELECT BAS_YM,
                                          RANK_GRP,
                                          ROUND(SUM(PROF_VS_LST_12_MM_AVG_AST), 2) AS AGT_VAL
                                   FROM   (SELECT T1.BAS_YM,
                                                  T1.PCF_ID,
                                                  CASE WHEN T1.L12M_AST_AMT = 0 THEN 0
                                                       ELSE ( T1.TOT_INCM_AMT - T1.TOT_EXP_AMT ) * 12 / ( T1.L12M_AST_AMT ) * 100 * T2.ASSET_WEIGHTED_AVG
                                                       END AS PROF_VS_LST_12_MM_AVG_AST,
                                                  T1.TOT_AST_AMT,
                                                  RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) AS RNK_NUM,
                                                  CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                                            THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                                                       ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                                                       END AS RANK_GRP
                                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN (SELECT A.BAS_YM,
                                                                                                     A.PCF_ID,
                                                                                                     CASE WHEN B.TOT_AST_AMT = 0 THEN 0
                                                                                                          ELSE A.TOT_AST_AMT / B.TOT_AST_AMT
                                                                                                          END AS ASSET_WEIGHTED_AVG
                                                                                              FROM   (SELECT T1.BAS_YM,
                                                                                                             CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                                                                                                       THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                                                                                                                  ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                                                                                                                  END AS RANK_GRP,
                                                                                                             T1.PCF_ID,
                                                                                                             T1.TOT_AST_AMT
                                                                                                      FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1
                                                                                                      WHERE  T1.TOT_AST_AMT IS NOT NULL
                                                                                                      AND    T1.TOT_INCM_AMT IS NOT NULL
                                                                                                     ) A INNER JOIN (SELECT BAS_YM,
                                                                                                                            RANK_GRP,
                                                                                                                            SUM(TOT_AST_AMT) AS TOT_AST_AMT
                                                                                                                     FROM   (SELECT T1.BAS_YM,
                                                                                                                                    CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                                                                                                                              THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                                                                                                                                         ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                                                                                                                                         END AS RANK_GRP,
                                                                                                                                    T1.TOT_AST_AMT
                                                                                                                             FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1
                                                                                                                             WHERE  T1.TOT_AST_AMT IS NOT NULL
                                                                                                                             AND    T1.TOT_INCM_AMT IS NOT NULL
                                                                                                                            )
                                                                                                                     GROUP BY BAS_YM, RANK_GRP
                                                                                                                    ) B
                                                                                                                 ON B.BAS_YM = A.BAS_YM
                                                                                                                AND B.RANK_GRP = A.RANK_GRP
                                                                                             ) T2
                                                                                          ON T2.BAS_YM = T1.BAS_YM
                                                                                         AND T2.PCF_ID = T1.PCF_ID
                                            WHERE T1.TOT_AST_AMT  IS NOT NULL
                                              AND T1.TOT_INCM_AMT IS NOT NULL
                                          )
                                   GROUP BY BAS_YM, RANK_GRP
                                  ) YY
                               ON YY.BAS_YM   = XX.BAS_YM
                              AND YY.RANK_GRP = XX.RANK_GRP

           UNION ALL

           SELECT BAS_YM,
                  PCF_ID,
                  '092'    AS KPI_TYP_CD,
                  '5'      AS AGT_METH_CD,
                  '092011' AS REF_IDC_TYP_CD,
                  RANK_GRP,
                  AGT_VAL
           FROM  (
                  SELECT T1.BAS_YM,
                         T1.PCF_ID,
                         MAX(T2.RANK_GRP) AS RANK_GRP,
                         ROUND(AVG(T2.AGT_VAL), 2) AS AGT_VAL
                  FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN (SELECT XX.BAS_YM,
                                                                            XX.PCF_ID,
                                                                            XX.RANK_GRP,
                                                                            YY.AGT_VAL
                                                                     FROM   (SELECT BAS_YM,
                                                                                    PCF_ID,
                                                                                    CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                                                                              THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100
                                                                                         ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                                                                                         END AS RANK_GRP
                                                                               FROM TM27_MMLY_PCF_KEY_IDC_A
                                                                              WHERE TOT_AST_AMT  IS NOT NULL
                                                                                AND TOT_INCM_AMT IS NOT NULL
                                                                            ) XX INNER JOIN (SELECT BAS_YM,
                                                                                                    RANK_GRP,
                                                                                                    AVG(PROF_VS_LST_12_MM_AVG_AST) AS AGT_VAL
                                                                                             FROM   (SELECT BAS_YM,
                                                                                                            PCF_ID,
                                                                                                            CASE WHEN L12M_AST_AMT = 0 THEN 0
                                                                                                                 ELSE (TOT_INCM_AMT - TOT_EXP_AMT) * 12 /  L12M_AST_AMT * 100
                                                                                                                 END AS PROF_VS_LST_12_MM_AVG_AST,
                                                                                                            TOT_AST_AMT,
                                                                                                            RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) AS RNK_NUM,
                                                                                                            CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                                                                                                      THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100
                                                                                                                 ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                                                                                                                 END AS RANK_GRP
                                                                                                       FROM TM27_MMLY_PCF_KEY_IDC_A
                                                                                                      WHERE TOT_AST_AMT  IS NOT NULL
                                                                                                        AND TOT_INCM_AMT IS NOT NULL
                                                                                                    )
                                                                                             GROUP BY BAS_YM, RANK_GRP
                                                                                            ) YY
                                                                                         ON YY.BAS_YM   = XX.BAS_YM
                                                                                        AND YY.RANK_GRP = XX.RANK_GRP
                                                                    ) T2
                                                                 ON T2.BAS_YM   >= TO_CHAR(ADD_MONTHS(TO_DATE(T1.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                                AND T2.BAS_YM   <= T1.BAS_YM
                                                                AND T2.PCF_ID   = T1.PCF_ID
                  WHERE T1.TOT_AST_AMT  IS NOT NULL
                  AND   T1.TOT_INCM_AMT IS NOT NULL
                  GROUP BY T1.BAS_YM, T1.PCF_ID
                 )

           UNION ALL

           SELECT BAS_YM,
                  PCF_ID,
                  '092'    AS KPI_TYP_CD,
                  '7'      AS AGT_METH_CD,
                  '092011' AS REF_IDC_TYP_CD,
                  RANK_GRP,
                  AGT_VAL
           FROM  (
                  SELECT T1.BAS_YM,
                         T1.PCF_ID,
                         MAX(T2.RANK_GRP) AS RANK_GRP,
                         ROUND(AVG(T2.AGT_VAL), 2) AS AGT_VAL
                  FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN (SELECT XX.BAS_YM,
                                                                            XX.PCF_ID,
                                                                            XX.RANK_GRP,
                                                                            YY.AGT_VAL
                                                                     FROM   (SELECT BAS_YM,
                                                                                    PCF_ID,
                                                                                    CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                                                                              THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100
                                                                                         ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                                                                                         END AS RANK_GRP
                                                                             FROM   TM27_MMLY_PCF_KEY_IDC_A
                                                                             WHERE  TOT_AST_AMT  IS NOT NULL
                                                                             AND    TOT_INCM_AMT IS NOT NULL
                                                                            ) XX INNER JOIN (SELECT BAS_YM,
                                                                                                    RANK_GRP,
                                                                                                    SUM(PROF_VS_LST_12_MM_AVG_AST) AS AGT_VAL
                                                                                             FROM   (SELECT T1.BAS_YM,
                                                                                                            T1.PCF_ID,
                                                                                                            CASE WHEN T1.L12M_AST_AMT = 0 THEN 0
                                                                                                                 ELSE ( T1.TOT_INCM_AMT - T1.TOT_EXP_AMT ) * 12 / ( T1.L12M_AST_AMT ) * 100 * T2.ASSET_WEIGHTED_AVG
                                                                                                                 END AS PROF_VS_LST_12_MM_AVG_AST,
                                                                                                            T1.TOT_AST_AMT,
                                                                                                            RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) AS RNK_NUM,
                                                                                                            CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                                                                                                      THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                                                                                                                 ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                                                                                                                 END AS RANK_GRP
                                                                                                       FROM TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN (SELECT A.BAS_YM,
                                                                                                                                                          A.PCF_ID,
                                                                                                                                                          CASE WHEN B.TOT_AST_AMT = 0 THEN 0
                                                                                                                                                               ELSE A.TOT_AST_AMT / B.TOT_AST_AMT
                                                                                                                                                               END AS ASSET_WEIGHTED_AVG
                                                                                                                                                   FROM   (SELECT T1.BAS_YM,
                                                                                                                                                                  CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                                                                                                                                                            THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                                                                                                                                                                       ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                                                                                                                                                                       END AS RANK_GRP,
                                                                                                                                                                  T1.PCF_ID,
                                                                                                                                                                  T1.TOT_AST_AMT
                                                                                                                                                           FROM   TM27_MMLY_PCF_KEY_IDC_A T1
                                                                                                                                                           WHERE  T1.TOT_AST_AMT IS NOT NULL
                                                                                                                                                           AND    T1.TOT_INCM_AMT IS NOT NULL
                                                                                                                                                          ) A INNER JOIN (SELECT BAS_YM,
                                                                                                                                                                                 RANK_GRP,
                                                                                                                                                                                 SUM(TOT_AST_AMT) AS TOT_AST_AMT
                                                                                                                                                                          FROM   (SELECT T1.BAS_YM,
                                                                                                                                                                                         CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                                                                                                                                                                                   THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                                                                                                                                                                                              ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                                                                                                                                                                                              END AS RANK_GRP,
                                                                                                                                                                                         T1.TOT_AST_AMT
                                                                                                                                                                                  FROM   TM27_MMLY_PCF_KEY_IDC_A T1
                                                                                                                                                                                  WHERE  T1.TOT_AST_AMT IS NOT NULL
                                                                                                                                                                                  AND    T1.TOT_INCM_AMT IS NOT NULL
                                                                                                                                                                                 )
                                                                                                                                                                          GROUP BY BAS_YM, RANK_GRP
                                                                                                                                                                         ) B
                                                                                                                                                                      ON B.BAS_YM = A.BAS_YM
                                                                                                                                                                     AND B.RANK_GRP = A.RANK_GRP
                                                                                                                                                  ) T2
                                                                                                                                               ON T2.BAS_YM = T1.BAS_YM
                                                                                                                                              AND T2.PCF_ID = T1.PCF_ID
                                                                                                      WHERE T1.TOT_AST_AMT  IS NOT NULL
                                                                                                        AND T1.TOT_INCM_AMT IS NOT NULL
                                                                                                    )
                                                                                             GROUP BY BAS_YM, RANK_GRP
                                                                                            ) YY
                                                                                         ON YY.BAS_YM   = XX.BAS_YM
                                                                                        AND YY.RANK_GRP = XX.RANK_GRP
                                                                    ) T2
                                                                 ON T2.BAS_YM   >= TO_CHAR(ADD_MONTHS(TO_DATE(T1.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                                AND T2.BAS_YM   <= T1.BAS_YM
                                                                AND T2.PCF_ID   = T1.PCF_ID
                  WHERE  T1.TOT_AST_AMT  IS NOT NULL
                  AND    T1.TOT_INCM_AMT IS NOT NULL
                  GROUP BY T1.BAS_YM, T1.PCF_ID
                 )
          ),
          FROM_KPI_TYP_093 AS
          (SELECT XX.BAS_YM,
                  XX.PCF_ID,
                  '093'    AS KPI_TYP_CD,
                  '2'      AS AGT_METH_CD,
                  '093011' AS REF_IDC_TYP_CD,
                  XX.RANK_GRP,
                  YY.AGT_VAL
           FROM   (SELECT BAS_YM,
                          PCF_ID,
                          CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                    THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100
                               ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                               END AS RANK_GRP
                   FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A
                   WHERE  TOT_AST_AMT  IS NOT NULL
                   AND    TOT_INCM_AMT IS NOT NULL
                  ) XX INNER JOIN (SELECT BAS_YM,
                                          RANK_GRP,
                                          ROUND(AVG(PROF_VS_LST_12_MM_AVG_LN_AMT), 2) AS AGT_VAL
                                   FROM   (SELECT BAS_YM,
                                                  PCF_ID,
                                                  CASE WHEN L12M_LN_AMT = 0 THEN 0
                                                       ELSE (TOT_INCM_AMT - TOT_EXP_AMT) * 12 /  L12M_LN_AMT * 100
                                                       END AS PROF_VS_LST_12_MM_AVG_LN_AMT,
                                                  TOT_AST_AMT,
                                                  RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) AS RNK_NUM,
                                                  CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                                            THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100
                                                       ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                                                       END AS RANK_GRP
                                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A
                                           WHERE  TOT_AST_AMT  IS NOT NULL
                                           AND    TOT_INCM_AMT IS NOT NULL
                                          )
                                   GROUP BY BAS_YM, RANK_GRP
                                  ) YY
                               ON YY.BAS_YM   = XX.BAS_YM
                              AND YY.RANK_GRP = XX.RANK_GRP

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.PCF_ID,
                  '093'    AS KPI_TYP_CD,
                  '4'      AS AGT_METH_CD,
                  '093011' AS REF_IDC_TYP_CD,
                  XX.RANK_GRP,
                  YY.AGT_VAL
           FROM   (SELECT BAS_YM,
                          PCF_ID,
                          CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                    THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100
                               ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                               END AS RANK_GRP
                   FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A
                   WHERE  TOT_AST_AMT  IS NOT NULL
                   AND    TOT_INCM_AMT IS NOT NULL
                  ) XX INNER JOIN (SELECT BAS_YM,
                                          RANK_GRP,
                                          ROUND(SUM(PROF_VS_LST_12_MM_AVG_LN_AMT), 2) AS AGT_VAL
                                   FROM   (SELECT T1.BAS_YM,
                                                  T1.PCF_ID,
                                                  CASE WHEN L12M_LN_AMT = 0 THEN 0
                                                       ELSE ( T1.TOT_INCM_AMT - T1.TOT_EXP_AMT ) * 12 / ( T1.L12M_LN_AMT ) * 100 * T2.ASSET_WEIGHTED_AVG
                                                       END AS PROF_VS_LST_12_MM_AVG_LN_AMT,
                                                  T1.TOT_AST_AMT,
                                                  RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) AS RNK_NUM,
                                                  CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                                            THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                                                       ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                                                       END AS RANK_GRP
                                             FROM FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN (SELECT A.BAS_YM,
                                                                                                     A.PCF_ID,
                                                                                                     CASE WHEN B.TOT_AST_AMT = 0 THEN 0
                                                                                                          ELSE A.TOT_AST_AMT / B.TOT_AST_AMT
                                                                                                          END AS ASSET_WEIGHTED_AVG
                                                                                              FROM   (SELECT T1.BAS_YM,
                                                                                                             CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                                                                                                       THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                                                                                                                  ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                                                                                                                  END AS RANK_GRP,
                                                                                                             T1.PCF_ID,
                                                                                                             T1.TOT_AST_AMT
                                                                                                      FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1
                                                                                                      WHERE  T1.TOT_AST_AMT IS NOT NULL
                                                                                                      AND    T1.TOT_INCM_AMT IS NOT NULL
                                                                                                     ) A INNER JOIN (SELECT BAS_YM,
                                                                                                                            RANK_GRP,
                                                                                                                            SUM(TOT_AST_AMT) AS TOT_AST_AMT
                                                                                                                     FROM   (SELECT T1.BAS_YM,
                                                                                                                                    CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                                                                                                                              THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                                                                                                                                         ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                                                                                                                                         END AS RANK_GRP,
                                                                                                                                    T1.TOT_AST_AMT
                                                                                                                             FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1
                                                                                                                             WHERE  T1.TOT_AST_AMT IS NOT NULL
                                                                                                                             AND    T1.TOT_INCM_AMT IS NOT NULL
                                                                                                                            )
                                                                                                                     GROUP BY BAS_YM, RANK_GRP
                                                                                                                    ) B
                                                                                                                 ON B.BAS_YM = A.BAS_YM
                                                                                                                AND B.RANK_GRP = A.RANK_GRP
                                                                                             ) T2
                                                                                          ON T2.BAS_YM = T1.BAS_YM
                                                                                         AND T2.PCF_ID = T1.PCF_ID
                                            WHERE T1.TOT_AST_AMT  IS NOT NULL
                                              AND T1.TOT_INCM_AMT IS NOT NULL
                                          )
                                   GROUP BY BAS_YM, RANK_GRP
                                  ) YY
                               ON YY.BAS_YM   = XX.BAS_YM
                              AND YY.RANK_GRP = XX.RANK_GRP

           UNION ALL

           SELECT BAS_YM,
                  PCF_ID,
                  '093'    AS KPI_TYP_CD,
                  '5'      AS AGT_METH_CD,
                  '093011' AS REF_IDC_TYP_CD,
                  RANK_GRP,
                  AGT_VAL
           FROM  (
                  SELECT T1.BAS_YM,
                         T1.PCF_ID,
                         MAX(T2.RANK_GRP) AS RANK_GRP,
                         ROUND(AVG(T2.AGT_VAL), 2) AS AGT_VAL
                  FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN (SELECT XX.BAS_YM,
                                                                            XX.PCF_ID,
                                                                            XX.RANK_GRP,
                                                                            YY.AGT_VAL
                                                                     FROM   (SELECT BAS_YM,
                                                                                    PCF_ID,
                                                                                    CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                                                                              THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100
                                                                                         ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                                                                                         END AS RANK_GRP
                                                                             FROM   TM27_MMLY_PCF_KEY_IDC_A
                                                                             WHERE  TOT_AST_AMT  IS NOT NULL
                                                                             AND    TOT_INCM_AMT IS NOT NULL
                                                                            ) XX INNER JOIN (SELECT BAS_YM,
                                                                                                    RANK_GRP,
                                                                                                    AVG(PROF_VS_LST_12_MM_AVG_LN_AMT) AS AGT_VAL
                                                                                             FROM   (SELECT BAS_YM,
                                                                                                            PCF_ID,
                                                                                                            CASE WHEN L12M_LN_AMT = 0 THEN 0
                                                                                                                 ELSE (TOT_INCM_AMT - TOT_EXP_AMT) * 12 /  L12M_LN_AMT * 100
                                                                                                                 END AS PROF_VS_LST_12_MM_AVG_LN_AMT,
                                                                                                            TOT_AST_AMT,
                                                                                                            RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) AS RNK_NUM,
                                                                                                            CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                                                                                                      THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100
                                                                                                                 ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                                                                                                                 END AS RANK_GRP
                                                                                                     FROM   TM27_MMLY_PCF_KEY_IDC_A
                                                                                                     WHERE  TOT_AST_AMT  IS NOT NULL
                                                                                                     AND    TOT_INCM_AMT IS NOT NULL
                                                                                                    )
                                                                                             GROUP BY BAS_YM, RANK_GRP
                                                                                            ) YY
                                                                                         ON YY.BAS_YM   = XX.BAS_YM
                                                                                        AND YY.RANK_GRP = XX.RANK_GRP
                                                                    ) T2
                                                                 ON T2.BAS_YM   >= TO_CHAR(ADD_MONTHS(TO_DATE(T1.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                                AND T2.BAS_YM   <= T1.BAS_YM
                                                                AND T2.PCF_ID   = T1.PCF_ID
                  WHERE T1.TOT_AST_AMT  IS NOT NULL
                  AND   T1.TOT_INCM_AMT IS NOT NULL
                  GROUP BY T1.BAS_YM, T1.PCF_ID
                 )

           UNION ALL

           SELECT BAS_YM,
                  PCF_ID,
                  '093'    AS KPI_TYP_CD,
                  '7'      AS AGT_METH_CD,
                  '093011' AS REF_IDC_TYP_CD,
                  RANK_GRP,
                  AGT_VAL
           FROM  (
                  SELECT T1.BAS_YM,
                         T1.PCF_ID,
                         MAX(T2.RANK_GRP) AS RANK_GRP,
                         ROUND(AVG(T2.AGT_VAL), 2) AS AGT_VAL
                  FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN (SELECT XX.BAS_YM,
                                                                            XX.PCF_ID,
                                                                            XX.RANK_GRP,
                                                                            YY.AGT_VAL
                                                                     FROM   (SELECT BAS_YM,
                                                                                    PCF_ID,
                                                                                    CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                                                                              THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100
                                                                                         ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                                                                                         END AS RANK_GRP
                                                                             FROM   TM27_MMLY_PCF_KEY_IDC_A
                                                                             WHERE  TOT_AST_AMT  IS NOT NULL
                                                                             AND   TOT_INCM_AMT IS NOT NULL
                                                                            ) XX INNER JOIN (SELECT BAS_YM,
                                                                                                    RANK_GRP,
                                                                                                    SUM(PROF_VS_LST_12_MM_AVG_LN_AMT) AS AGT_VAL
                                                                                             FROM   (SELECT T1.BAS_YM,
                                                                                                            T1.PCF_ID,
                                                                                                            CASE WHEN T1.L12M_LN_AMT = 0 THEN 0
                                                                                                                 ELSE ( T1.TOT_INCM_AMT - T1.TOT_EXP_AMT ) * 12 / ( T1.L12M_LN_AMT ) * 100 * T2.ASSET_WEIGHTED_AVG
                                                                                                                 END AS PROF_VS_LST_12_MM_AVG_LN_AMT,
                                                                                                            T1.TOT_AST_AMT,
                                                                                                            RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) AS RNK_NUM,
                                                                                                            CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                                                                                                      THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                                                                                                                 ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                                                                                                                 END AS RANK_GRP
                                                                                                       FROM TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN (SELECT A.BAS_YM,
                                                                                                                                                          A.PCF_ID,
                                                                                                                                                          CASE WHEN B.TOT_AST_AMT = 0 THEN 0
                                                                                                                                                               ELSE A.TOT_AST_AMT / B.TOT_AST_AMT
                                                                                                                                                               END AS ASSET_WEIGHTED_AVG
                                                                                                                                                   FROM   (SELECT T1.BAS_YM,
                                                                                                                                                                  CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                                                                                                                                                            THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                                                                                                                                                                       ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                                                                                                                                                                       END AS RANK_GRP,
                                                                                                                                                                  T1.PCF_ID,
                                                                                                                                                                  T1.TOT_AST_AMT
                                                                                                                                                           FROM   TM27_MMLY_PCF_KEY_IDC_A T1
                                                                                                                                                           WHERE  T1.TOT_AST_AMT IS NOT NULL
                                                                                                                                                           AND    T1.TOT_INCM_AMT IS NOT NULL
                                                                                                                                                          ) A INNER JOIN (SELECT BAS_YM,
                                                                                                                                                                                 RANK_GRP,
                                                                                                                                                                                 SUM(TOT_AST_AMT) AS TOT_AST_AMT
                                                                                                                                                                          FROM   (SELECT T1.BAS_YM,
                                                                                                                                                                                         CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                                                                                                                                                                                   THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                                                                                                                                                                                              ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.TOT_AST_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                                                                                                                                                                                              END AS RANK_GRP,
                                                                                                                                                                                         T1.TOT_AST_AMT
                                                                                                                                                                                  FROM   TM27_MMLY_PCF_KEY_IDC_A T1
                                                                                                                                                                                  WHERE  T1.TOT_AST_AMT IS NOT NULL
                                                                                                                                                                                  AND    T1.TOT_INCM_AMT IS NOT NULL
                                                                                                                                                                                 )
                                                                                                                                                                          GROUP BY BAS_YM, RANK_GRP
                                                                                                                                                                         ) B
                                                                                                                                                                      ON B.BAS_YM = A.BAS_YM
                                                                                                                                                                     AND B.RANK_GRP = A.RANK_GRP
                                                                                                                                                  ) T2
                                                                                                                                               ON T2.BAS_YM = T1.BAS_YM
                                                                                                                                              AND T2.PCF_ID = T1.PCF_ID
                                                                                                      WHERE T1.TOT_AST_AMT  IS NOT NULL
                                                                                                        AND T1.TOT_INCM_AMT IS NOT NULL
                                                                                                    )
                                                                                             GROUP BY BAS_YM, RANK_GRP
                                                                                            ) YY
                                                                                         ON YY.BAS_YM   = XX.BAS_YM
                                                                                        AND YY.RANK_GRP = XX.RANK_GRP
                                                                    ) T2
                                                                 ON T2.BAS_YM   >= TO_CHAR(ADD_MONTHS(TO_DATE(T1.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                                AND T2.BAS_YM   <= T1.BAS_YM
                                                                AND T2.PCF_ID   = T1.PCF_ID
                  WHERE  T1.TOT_AST_AMT  IS NOT NULL
                  AND    T1.TOT_INCM_AMT IS NOT NULL
                  GROUP BY T1.BAS_YM, T1.PCF_ID
                 )
          ),
          FROM_KPI_TYP_094 AS
          (SELECT XX.BAS_YM,
                  XX.PCF_ID,
                  '094'    AS KPI_TYP_CD,
                  '2'      AS AGT_METH_CD,
                  '094006' AS REF_IDC_TYP_CD,
                  XX.RANK_GRP,
                  YY.AGT_VAL
           FROM   (SELECT BAS_YM,
                          PCF_ID,
                          CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY EQT_AMT ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                    THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY EQT_AMT ASC NULLS LAST, PCF_ID ASC) / 100
                               ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY EQT_AMT ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                               END AS RANK_GRP
                   FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A
                   WHERE  EQT_AMT IS NOT NULL
                   AND    TOT_INCM_AMT IS NOT NULL
                  ) XX INNER JOIN (SELECT BAS_YM,
                                          RANK_GRP,
                                          ROUND(AVG(PROF_VS_LST_12_MM_AVG_EQT_AMT), 2) AS AGT_VAL
                                   FROM   (SELECT BAS_YM,
                                                  PCF_ID,
                                                  CASE WHEN L12M_EQT_AMT = 0 THEN 0
                                                       ELSE (TOT_INCM_AMT - TOT_EXP_AMT) * 12 /  L12M_EQT_AMT * 100
                                                       END AS PROF_VS_LST_12_MM_AVG_EQT_AMT,
                                                  EQT_AMT,
                                                  RANK() OVER(PARTITION BY BAS_YM ORDER BY EQT_AMT ASC NULLS LAST, PCF_ID ASC) AS RNK_NUM,
                                                  CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY EQT_AMT ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                                            THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY EQT_AMT ASC NULLS LAST, PCF_ID ASC) / 100
                                                       ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY EQT_AMT ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                                                       END AS RANK_GRP
                                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A
                                           WHERE  EQT_AMT IS NOT NULL
                                           AND    TOT_INCM_AMT IS NOT NULL
                                          )
                                   GROUP BY BAS_YM, RANK_GRP
                                  ) YY
                               ON YY.BAS_YM   = XX.BAS_YM
                              AND YY.RANK_GRP = XX.RANK_GRP

           UNION ALL

           SELECT XX.BAS_YM,
                  XX.PCF_ID,
                  '094'    AS KPI_TYP_CD,
                  '4'      AS AGT_METH_CD,
                  '094006' AS REF_IDC_TYP_CD,
                  XX.RANK_GRP,
                  YY.AGT_VAL
           FROM   (SELECT BAS_YM,
                          PCF_ID,
                          CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY EQT_AMT ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                    THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY EQT_AMT ASC NULLS LAST, PCF_ID ASC) / 100
                               ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY EQT_AMT ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                               END AS RANK_GRP
                   FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A
                   WHERE  TOT_AST_AMT  IS NOT NULL
                   AND    EQT_AMT      IS NOT NULL
                   AND    TOT_INCM_AMT IS NOT NULL
                  ) XX INNER JOIN (SELECT BAS_YM,
                                          RANK_GRP,
                                          ROUND(SUM(PROF_VS_LST_12_MM_AVG_EQT_AMT), 2) AS AGT_VAL
                                   FROM   (SELECT T1.BAS_YM,
                                                  T1.PCF_ID,
                                                  CASE WHEN T1.L12M_EQT_AMT = 0 THEN 0
                                                       ELSE ( T1.TOT_INCM_AMT - T1.TOT_EXP_AMT ) * 12 / ( T1.L12M_EQT_AMT ) * 100 * T2.ASSET_WEIGHTED_AVG
                                                       END AS PROF_VS_LST_12_MM_AVG_EQT_AMT,
                                                  T1.EQT_AMT,
                                                  RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) AS RNK_NUM,
                                                  CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                                            THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                                                       ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                                                       END AS RANK_GRP
                                             FROM FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN (SELECT A.BAS_YM,
                                                                                                     A.PCF_ID,
                                                                                                     CASE WHEN B.TOT_AST_AMT = 0 THEN 0
                                                                                                          ELSE A.TOT_AST_AMT / B.TOT_AST_AMT
                                                                                                          END AS ASSET_WEIGHTED_AVG
                                                                                              FROM   (SELECT T1.BAS_YM,
                                                                                                             CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                                                                                                       THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                                                                                                                  ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                                                                                                                  END AS RANK_GRP,
                                                                                                             T1.PCF_ID,
                                                                                                             T1.TOT_AST_AMT
                                                                                                      FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1
                                                                                                      WHERE  T1.TOT_AST_AMT IS NOT NULL
                                                                                                      AND    T1.TOT_INCM_AMT IS NOT NULL
                                                                                                      AND    T1.EQT_AMT IS NOT NULL
                                                                                                     ) A INNER JOIN (SELECT BAS_YM,
                                                                                                                            RANK_GRP,
                                                                                                                            SUM(TOT_AST_AMT) AS TOT_AST_AMT
                                                                                                                     FROM   (SELECT T1.BAS_YM,
                                                                                                                                    CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                                                                                                                              THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                                                                                                                                         ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                                                                                                                                         END AS RANK_GRP,
                                                                                                                                    T1.TOT_AST_AMT
                                                                                                                             FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1
                                                                                                                             WHERE  T1.TOT_AST_AMT IS NOT NULL
                                                                                                                             AND    T1.TOT_INCM_AMT IS NOT NULL
                                                                                                                             AND    T1.EQT_AMT IS NOT NULL
                                                                                                                            )
                                                                                                                     GROUP BY BAS_YM, RANK_GRP
                                                                                                                    ) B
                                                                                                                 ON B.BAS_YM = A.BAS_YM
                                                                                                                AND B.RANK_GRP = A.RANK_GRP
                                                                                             ) T2
                                                                                          ON T2.BAS_YM = T1.BAS_YM
                                                                                         AND T2.PCF_ID = T1.PCF_ID
                                             WHERE T1.TOT_AST_AMT  IS NOT NULL
                                               AND T1.EQT_AMT      IS NOT NULL
                                               AND T1.TOT_INCM_AMT IS NOT NULL
                                          )
                                   GROUP BY BAS_YM, RANK_GRP
                                  ) YY
                               ON YY.BAS_YM   = XX.BAS_YM
                              AND YY.RANK_GRP = XX.RANK_GRP

           UNION ALL

           SELECT BAS_YM,
                  PCF_ID,
                  '094'    AS KPI_TYP_CD,
                  '5'      AS AGT_METH_CD,
                  '094006' AS REF_IDC_TYP_CD,
                  RANK_GRP,
                  AGT_VAL
           FROM  (
                  SELECT T1.BAS_YM,
                         T1.PCF_ID,
                         MAX(T2.RANK_GRP) AS RANK_GRP,
                         ROUND(AVG(T2.AGT_VAL), 2) AS AGT_VAL
                  FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN (SELECT XX.BAS_YM,
                                                                            XX.PCF_ID,
                                                                            XX.RANK_GRP,
                                                                            YY.AGT_VAL
                                                                     FROM   (SELECT BAS_YM,
                                                                                    PCF_ID,
                                                                                    CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY EQT_AMT ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                                                                              THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY EQT_AMT ASC NULLS LAST, PCF_ID ASC) / 100
                                                                                         ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY EQT_AMT ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                                                                                         END AS RANK_GRP
                                                                             FROM   TM27_MMLY_PCF_KEY_IDC_A
                                                                             WHERE  EQT_AMT IS NOT NULL
                                                                             AND    TOT_INCM_AMT IS NOT NULL
                                                                            ) XX INNER JOIN (SELECT BAS_YM,
                                                                                                    RANK_GRP,
                                                                                                    AVG(PROF_VS_LST_12_MM_AVG_EQT_AMT) AS AGT_VAL
                                                                                             FROM   (SELECT BAS_YM,
                                                                                                            PCF_ID,
                                                                                                            CASE WHEN L12M_EQT_AMT = 0 THEN 0
                                                                                                                 ELSE (TOT_INCM_AMT - TOT_EXP_AMT) * 12 /  L12M_EQT_AMT * 100
                                                                                                                 END AS PROF_VS_LST_12_MM_AVG_EQT_AMT,
                                                                                                            EQT_AMT,
                                                                                                            RANK() OVER(PARTITION BY BAS_YM ORDER BY EQT_AMT ASC NULLS LAST, PCF_ID ASC) AS RNK_NUM,
                                                                                                            CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY EQT_AMT ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                                                                                                      THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY EQT_AMT ASC NULLS LAST, PCF_ID ASC) / 100
                                                                                                                 ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY EQT_AMT ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                                                                                                                 END AS RANK_GRP
                                                                                                     FROM   TM27_MMLY_PCF_KEY_IDC_A
                                                                                                     WHERE  EQT_AMT IS NOT NULL
                                                                                                     AND TOT_INCM_AMT IS NOT NULL
                                                                                                    )
                                                                                             GROUP BY BAS_YM, RANK_GRP
                                                                                            ) YY
                                                                                         ON YY.BAS_YM   = XX.BAS_YM
                                                                                        AND YY.RANK_GRP = XX.RANK_GRP
                                                                    ) T2
                                                                 ON T2.BAS_YM   >= TO_CHAR(ADD_MONTHS(TO_DATE(T1.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                                AND T2.BAS_YM   <= T1.BAS_YM
                                                                AND T2.PCF_ID   = T1.PCF_ID
                   WHERE T1.EQT_AMT IS NOT NULL
                     AND T1.TOT_INCM_AMT IS NOT NULL
                   GROUP BY T1.BAS_YM, T1.PCF_ID
                 )

           UNION ALL

           SELECT BAS_YM,
                  PCF_ID,
                  '094'    AS KPI_TYP_CD,
                  '7'      AS AGT_METH_CD,
                  '094006' AS REF_IDC_TYP_CD,
                  RANK_GRP,
                  AGT_VAL
           FROM  (
                  SELECT T1.BAS_YM,
                         T1.PCF_ID,
                         MAX(T2.RANK_GRP) AS RANK_GRP,
                         ROUND(AVG(T2.AGT_VAL), 2) AS AGT_VAL
                  FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN (SELECT XX.BAS_YM,
                                                                            XX.PCF_ID,
                                                                            XX.RANK_GRP,
                                                                            YY.AGT_VAL
                                                                     FROM   (SELECT BAS_YM,
                                                                                    PCF_ID,
                                                                                    CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY EQT_AMT ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                                                                              THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY EQT_AMT ASC NULLS LAST, PCF_ID ASC) / 100
                                                                                         ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY EQT_AMT ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                                                                                         END AS RANK_GRP
                                                                             FROM   TM27_MMLY_PCF_KEY_IDC_A
                                                                             WHERE  TOT_AST_AMT  IS NOT NULL
                                                                             AND    EQT_AMT      IS NOT NULL
                                                                             AND    TOT_INCM_AMT IS NOT NULL
                                                                            ) XX INNER JOIN (SELECT BAS_YM,
                                                                                                    RANK_GRP,
                                                                                                    SUM(PROF_VS_LST_12_MM_AVG_EQT_AMT) AS AGT_VAL
                                                                                             FROM   (SELECT T1.BAS_YM,
                                                                                                            T1.PCF_ID,
                                                                                                            CASE WHEN T1.L12M_EQT_AMT = 0 THEN 0
                                                                                                                 ELSE ( T1.TOT_INCM_AMT - T1.TOT_EXP_AMT ) * 12 / ( T1.L12M_EQT_AMT ) * 100 * T2.ASSET_WEIGHTED_AVG
                                                                                                                 END AS PROF_VS_LST_12_MM_AVG_EQT_AMT,
                                                                                                            T1.EQT_AMT,
                                                                                                            RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) AS RNK_NUM,
                                                                                                            CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                                                                                                      THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                                                                                                                 ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                                                                                                                 END AS RANK_GRP
                                                                                                       FROM TM27_MMLY_PCF_KEY_IDC_A T1 INNER JOIN (SELECT A.BAS_YM,
                                                                                                                                                          A.PCF_ID,
                                                                                                                                                          CASE WHEN B.TOT_AST_AMT = 0 THEN 0
                                                                                                                                                               ELSE A.TOT_AST_AMT / B.TOT_AST_AMT
                                                                                                                                                               END AS ASSET_WEIGHTED_AVG
                                                                                                                                                   FROM   (SELECT T1.BAS_YM,
                                                                                                                                                                  CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                                                                                                                                                            THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                                                                                                                                                                       ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                                                                                                                                                                       END AS RANK_GRP,
                                                                                                                                                                  T1.PCF_ID,
                                                                                                                                                                  T1.TOT_AST_AMT
                                                                                                                                                           FROM   TM27_MMLY_PCF_KEY_IDC_A T1
                                                                                                                                                           WHERE  T1.TOT_AST_AMT IS NOT NULL
                                                                                                                                                           AND    T1.TOT_INCM_AMT IS NOT NULL
                                                                                                                                                           AND    T1.EQT_AMT IS NOT NULL
                                                                                                                                                          ) A INNER JOIN (SELECT BAS_YM,
                                                                                                                                                                                 RANK_GRP,
                                                                                                                                                                                 SUM(TOT_AST_AMT) AS TOT_AST_AMT
                                                                                                                                                                          FROM   (SELECT T1.BAS_YM,
                                                                                                                                                                                         CASE WHEN MOD(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100, 1) = 0
                                                                                                                                                                                                   THEN RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100
                                                                                                                                                                                              ELSE TRUNC(RANK() OVER(PARTITION BY T1.BAS_YM ORDER BY T1.EQT_AMT ASC NULLS LAST, T1.PCF_ID ASC) / 100) + 1
                                                                                                                                                                                              END AS RANK_GRP,
                                                                                                                                                                                         T1.TOT_AST_AMT
                                                                                                                                                                                  FROM   TM27_MMLY_PCF_KEY_IDC_A T1
                                                                                                                                                                                  WHERE  T1.TOT_AST_AMT IS NOT NULL
                                                                                                                                                                                  AND    T1.TOT_INCM_AMT IS NOT NULL
                                                                                                                                                                                  AND    T1.EQT_AMT IS NOT NULL
                                                                                                                                                                                 )
                                                                                                                                                                          GROUP BY BAS_YM, RANK_GRP
                                                                                                                                                                         ) B
                                                                                                                                                                      ON B.BAS_YM = A.BAS_YM
                                                                                                                                                                     AND B.RANK_GRP = A.RANK_GRP
                                                                                                                                                  ) T2
                                                                                                                                               ON T2.BAS_YM = T1.BAS_YM
                                                                                                                                              AND T2.PCF_ID = T1.PCF_ID
                                                                                                      WHERE T1.TOT_AST_AMT  IS NOT NULL
                                                                                                        AND T1.EQT_AMT      IS NOT NULL
                                                                                                        AND T1.TOT_INCM_AMT IS NOT NULL
                                                                                                    )
                                                                                             GROUP BY BAS_YM, RANK_GRP
                                                                                            ) YY
                                                                                         ON YY.BAS_YM   = XX.BAS_YM
                                                                                        AND YY.RANK_GRP = XX.RANK_GRP
                                                                    ) T2
                                                                 ON T2.BAS_YM   >= TO_CHAR(ADD_MONTHS(TO_DATE(T1.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                                                AND T2.BAS_YM   <= T1.BAS_YM
                                                                AND T2.PCF_ID   = T1.PCF_ID
                  WHERE  T1.TOT_AST_AMT  IS NOT NULL
                  AND    T1.EQT_AMT      IS NOT NULL
                  AND    T1.TOT_INCM_AMT IS NOT NULL
                  GROUP BY T1.BAS_YM, T1.PCF_ID
                 )
          ),
          FROM_KPI_TYP_133 AS
          (SELECT XX.BAS_YM,
                  XX.PCF_ID,
                  '133'    AS KPI_TYP_CD,
                  '2'      AS AGT_METH_CD,
                  '133011' AS REF_IDC_TYP_CD,
                  XX.RANK_GRP,
                  YY.AGT_VAL
           FROM   (SELECT BAS_YM,
                          PCF_ID,
                          CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                    THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100
                               ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                               END AS RANK_GRP
                   FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A
                   WHERE  TOT_AST_AMT IS NOT NULL
                   AND    WIT_COLL_LN_BAL IS NOT NULL
                  ) XX INNER JOIN (SELECT BAS_YM,
                                          RANK_GRP,
                                          ROUND(AVG(WIT_COLL_LN_BAL), 0) AS AGT_VAL
                                   FROM   (SELECT BAS_YM,
                                                  PCF_ID,
                                                  WIT_COLL_LN_BAL,
                                                  TOT_AST_AMT,
                                                  RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) AS RNK_NUM,
                                                  CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                                            THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100
                                                       ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                                                       END AS RANK_GRP
                                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A
                                           WHERE  TOT_AST_AMT IS NOT NULL
                                           AND    WIT_COLL_LN_BAL IS NOT NULL
                                          )
                                   GROUP BY BAS_YM, RANK_GRP
                                  ) YY
                               ON YY.BAS_YM   = XX.BAS_YM
                              AND YY.RANK_GRP = XX.RANK_GRP
          ),
          FROM_KPI_TYP_143 AS
          (SELECT XX.BAS_YM,
                  XX.PCF_ID,
                  '143'    AS KPI_TYP_CD,
                  '2'      AS AGT_METH_CD,
                  '143011' AS REF_IDC_TYP_CD,
                  XX.RANK_GRP,
                  YY.AGT_VAL
           FROM   (SELECT BAS_YM,
                          PCF_ID,
                          CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                    THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100
                               ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                               END AS RANK_GRP
                   FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A
                   WHERE  TOT_AST_AMT IS NOT NULL
                   AND    CUR_TOT_INCM_AMT IS NOT NULL
                  ) XX INNER JOIN (SELECT BAS_YM,
                                          RANK_GRP,
                                          ROUND(AVG(CUR_NET_PROFIT), 0) AS AGT_VAL
                                   FROM   (SELECT BAS_YM,
                                                  PCF_ID,
                                                  CUR_TOT_INCM_AMT - CUR_TOT_EXP_AMT AS CUR_NET_PROFIT,
                                                  TOT_AST_AMT,
                                                  RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) AS RNK_NUM,
                                                  CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                                            THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100
                                                       ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                                                       END AS RANK_GRP
                                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A
                                           WHERE  TOT_AST_AMT IS NOT NULL
                                           AND    CUR_TOT_INCM_AMT IS NOT NULL
                                          )
                                   GROUP BY BAS_YM, RANK_GRP
                                  ) YY
                               ON YY.BAS_YM   = XX.BAS_YM
                              AND YY.RANK_GRP = XX.RANK_GRP
          ),
          FROM_KPI_TYP_144 AS
          (SELECT XX.BAS_YM,
                  XX.PCF_ID,
                  '144'    AS KPI_TYP_CD,
                  '2'      AS AGT_METH_CD,
                  '144011' AS REF_IDC_TYP_CD,
                  XX.RANK_GRP,
                  YY.AGT_VAL
           FROM   (SELECT BAS_YM,
                          PCF_ID,
                          CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                    THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100
                               ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                               END AS RANK_GRP
                   FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A
                   WHERE  TOT_AST_AMT IS NOT NULL
                   AND    INCL_ALOSS_INCM_MNS_EXP_AMT IS NOT NULL
                  ) XX INNER JOIN (SELECT BAS_YM,
                                          RANK_GRP,
                                          ROUND(AVG(INCL_ALOSS_INCM_MNS_EXP_AMT), 0) AS AGT_VAL
                                   FROM   (SELECT BAS_YM,
                                                  PCF_ID,
                                                  INCL_ALOSS_INCM_MNS_EXP_AMT,
                                                  TOT_AST_AMT,
                                                  RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) AS RNK_NUM,
                                                  CASE WHEN MOD(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100, 1) = 0
                                                            THEN RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100
                                                       ELSE TRUNC(RANK() OVER(PARTITION BY BAS_YM ORDER BY TOT_AST_AMT ASC NULLS LAST, PCF_ID ASC) / 100) + 1
                                                       END AS RANK_GRP
                                           FROM   FROM_TM27_MMLY_PCF_KEY_IDC_A
                                           WHERE  TOT_AST_AMT IS NOT NULL
                                           AND    INCL_ALOSS_INCM_MNS_EXP_AMT IS NOT NULL
                                          )
                                   GROUP BY BAS_YM, RANK_GRP
                                  ) YY
                               ON YY.BAS_YM   = XX.BAS_YM
                              AND YY.RANK_GRP = XX.RANK_GRP
          )
          SELECT BAS_YM, PCF_ID, KPI_TYP_CD, AGT_METH_CD, REF_IDC_TYP_CD, RANK_GRP, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_001
          UNION ALL
          SELECT BAS_YM, PCF_ID, KPI_TYP_CD, AGT_METH_CD, REF_IDC_TYP_CD, RANK_GRP, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_002
          UNION ALL
          SELECT BAS_YM, PCF_ID, KPI_TYP_CD, AGT_METH_CD, REF_IDC_TYP_CD, RANK_GRP, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_003
          UNION ALL
          SELECT BAS_YM, PCF_ID, KPI_TYP_CD, AGT_METH_CD, REF_IDC_TYP_CD, RANK_GRP, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_004
          UNION ALL
          SELECT BAS_YM, PCF_ID, KPI_TYP_CD, AGT_METH_CD, REF_IDC_TYP_CD, RANK_GRP, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_005
          UNION ALL
          SELECT BAS_YM, PCF_ID, KPI_TYP_CD, AGT_METH_CD, REF_IDC_TYP_CD, RANK_GRP, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_006
          UNION ALL
          SELECT BAS_YM, PCF_ID, KPI_TYP_CD, AGT_METH_CD, REF_IDC_TYP_CD, RANK_GRP, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_007
          UNION ALL
          SELECT BAS_YM, PCF_ID, KPI_TYP_CD, AGT_METH_CD, REF_IDC_TYP_CD, RANK_GRP, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_008
          UNION ALL
          SELECT BAS_YM, PCF_ID, KPI_TYP_CD, AGT_METH_CD, REF_IDC_TYP_CD, RANK_GRP, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_009
          UNION ALL
          SELECT BAS_YM, PCF_ID, KPI_TYP_CD, AGT_METH_CD, REF_IDC_TYP_CD, RANK_GRP, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_010
          UNION ALL
          SELECT BAS_YM, PCF_ID, KPI_TYP_CD, AGT_METH_CD, REF_IDC_TYP_CD, RANK_GRP, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_058
          UNION ALL
          SELECT BAS_YM, PCF_ID, KPI_TYP_CD, AGT_METH_CD, REF_IDC_TYP_CD, RANK_GRP, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_066
          UNION ALL
          SELECT BAS_YM, PCF_ID, KPI_TYP_CD, AGT_METH_CD, REF_IDC_TYP_CD, RANK_GRP, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_069
          UNION ALL
          SELECT BAS_YM, PCF_ID, KPI_TYP_CD, AGT_METH_CD, REF_IDC_TYP_CD, RANK_GRP, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_070
          UNION ALL
          SELECT BAS_YM, PCF_ID, KPI_TYP_CD, AGT_METH_CD, REF_IDC_TYP_CD, RANK_GRP, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_071
          UNION ALL
          SELECT BAS_YM, PCF_ID, KPI_TYP_CD, AGT_METH_CD, REF_IDC_TYP_CD, RANK_GRP, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_088
          UNION ALL
          SELECT BAS_YM, PCF_ID, KPI_TYP_CD, AGT_METH_CD, REF_IDC_TYP_CD, RANK_GRP, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_090
          UNION ALL
          SELECT BAS_YM, PCF_ID, KPI_TYP_CD, AGT_METH_CD, REF_IDC_TYP_CD, RANK_GRP, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_091
          UNION ALL
          SELECT BAS_YM, PCF_ID, KPI_TYP_CD, AGT_METH_CD, REF_IDC_TYP_CD, RANK_GRP, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_092
          UNION ALL
          SELECT BAS_YM, PCF_ID, KPI_TYP_CD, AGT_METH_CD, REF_IDC_TYP_CD, RANK_GRP, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_093
          UNION ALL
          SELECT BAS_YM, PCF_ID, KPI_TYP_CD, AGT_METH_CD, REF_IDC_TYP_CD, RANK_GRP, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_094
          UNION ALL
          SELECT BAS_YM, PCF_ID, KPI_TYP_CD, AGT_METH_CD, REF_IDC_TYP_CD, RANK_GRP, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_133
          UNION ALL
          SELECT BAS_YM, PCF_ID, KPI_TYP_CD, AGT_METH_CD, REF_IDC_TYP_CD, RANK_GRP, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_143
          UNION ALL
          SELECT BAS_YM, PCF_ID, KPI_TYP_CD, AGT_METH_CD, REF_IDC_TYP_CD, RANK_GRP, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_144
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

END;
/