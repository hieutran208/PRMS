create or replace PROCEDURE            "P4_M27_MMLY_PCF_KEY_IDC_1718"
                 ( p_wkdate    IN  CHAR DEFAULT NULL
                 , p_stdate_01 IN  CHAR DEFAULT NULL
                 , p_eddate_01 IN  CHAR DEFAULT NULL
                 )
IS
    /*--------------------------------------------------------------------------
     * PROGRAM ID    : P4_M27_MMLY_PCF_KEY_IDC_1718
     * PROGRAM NAME  : A program for insert data to TM27_MMLY_PCF_KEY_IDC_A
     * SOURCE TABLE  : TB01_G32_016_TTGS_A
                       TB01_G32_018_TTGS_A
                       TM21_MMLY_CUST_CR_TRANS_A
                       TM21_DDLY_CUST_CR_TRANS_A
                       TM21_MMLY_PCF_CR_TRANS_A
                       TM22_MMLY_CUST_DPST_TRANS_A 
                       TM22_DDLY_CUST_DPST_TRANS_A
                       TM22_MMLY_PCF_DPST_TRANS_A
                       TM23_MMLY_BAL_SHET_A
                       TM23_MMLY_INCM_EXP_A
                       TM24_MMLY_IDC_CALC_CAR_A
                       TM26_MMLY_AST_QAL_A
     * TARGET TABLE  : TM27_MMLY_PCF_KEY_IDC_A
     * PROGRAMER     : HIEU
     * LAST MODIFICATION DATE : 2025-12-25
     * UNIQUENESS    : N/A
     * COMMENTS      : N/A
    ----------------------------------------------------------------------------
     * Revision History : 2025-12-25 : Create
     * Revision History :
    --------------------------------------------------------------------------*/
    ----------------------------------------------------------------------------
    --  Declare Local Variables
    ----------------------------------------------------------------------------
    v_wk_date                 CHAR(8) DEFAULT NULL ;
    v_st_date_01              CHAR(8) DEFAULT NULL ;
    v_end_date_01             CHAR(8) DEFAULT NULL ;

    v_program_id              VARCHAR2(100) DEFAULT 'P4_M27_MMLY_PCF_KEY_IDC_1718' ;
    v_program_type_name       VARCHAR2(100) DEFAULT 'Stored Procedure' ;
    v_step_code               VARCHAR2(3) ;
    v_step_desc               VARCHAR2(100) ;
    v_time                    DATE ;
    v_cnt                     NUMBER DEFAULT 0 ;
    v_seq                     CHAR(14) DEFAULT ' '||to_char(SEQ_UID.NEXTVAL,'000000000000');
    ----------------------------------------------------------------------------
    --  Declare Cursor
    ----------------------------------------------------------------------------

    CURSOR v_sbmt_day(v_dt1 VARCHAR2, v_dt2 VARCHAR2, v_dt3 VARCHAR2) IS

    /*SELECT BAS_YM
    FROM   TM00_MMLY_CAL_D
    WHERE  BAS_YM BETWEEN '202401' AND '202410'
    ORDER BY 1;*/

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
            FROM TM27_MMLY_PCF_KEY_IDC_A T1
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

          INSERT INTO TM27_MMLY_PCF_KEY_IDC_A
          (   BAS_YM
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
             ,OTHR_INST_TRM_DPST_AMT
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
             ,TRGT_DATA_LST_MOD_TM
          )
          WITH
          FULL_SET AS
          (SELECT  T1.BAS_YM, T1.PCF_ID
           FROM   (SELECT BAS_YM, PCF_ID FROM TM23_MMLY_BAL_SHET_A WHERE BAS_YM = loop_bas_day.BAS_YM GROUP BY BAS_YM, PCF_ID
                  ) T1
          ),
          FROM_G32_016_TTGS AS
          (SELECT B.BAS_YM,
                  A.PCF_ID,
                  A.TOTAL_CURRENT_MEMBERS AS TOT_PCF_MBR_NUM_CNT,
                  A.TOTAL_NEW_MEMBERS  AS NEW_JON_PCF_MBR_NUM_CNT,
                  A.TOTAL_TERMINATED_MEMBERS AS LV_PCF_MBR_NUM_CNT
           FROM   TB01_G32_016_TTGS_A A INNER JOIN TM00_MMLY_CAL_D B
                                           ON B.BAS_YM = A.BAS_YM

           UNION ALL

           SELECT TO_CHAR(ADD_MONTHS(TO_DATE(B.BAS_YM, 'YYYYMM'), 1), 'YYYYMM') AS BAS_YM,
                  A.PCF_ID,
                  A.TOTAL_CURRENT_MEMBERS AS TOT_PCF_MBR_NUM_CNT,
                  NULL AS NEW_JON_PCF_MBR_NUM_CNT,
                  NULL AS LV_PCF_MBR_NUM_CNT
           FROM   TB01_G32_016_TTGS_A A INNER JOIN TM00_MMLY_CAL_D B
                                           ON B.BAS_YM = A.BAS_YM

           UNION ALL

           SELECT TO_CHAR(ADD_MONTHS(TO_DATE(B.BAS_YM, 'YYYYMM'), 2), 'YYYYMM') AS BAS_YM,
                  A.PCF_ID,
                  A.TOTAL_CURRENT_MEMBERS AS TOT_PCF_MBR_NUM_CNT,
                  NULL AS NEW_JON_PCF_MBR_NUM_CNT,
                  NULL AS LV_PCF_MBR_NUM_CNT
           FROM   TB01_G32_016_TTGS_A A INNER JOIN TM00_MMLY_CAL_D B
                                           ON B.BAS_YM = A.BAS_YM
          ),
          FROM_G32_016_TTGS_L3M AS
          (SELECT T1.BAS_YM,
                  T1.PCF_ID,
                  ROUND(AVG(T2.TOT_PCF_MBR_NUM_CNT), 0) AS L3M_TOT_PCF_MBR_CNT
           FROM   (SELECT BAS_YM,
                          PCF_ID
                   FROM   FULL_SET A
                  ) T1 LEFT OUTER JOIN FROM_G32_016_TTGS T2
                                    ON T2.BAS_YM    >= TO_CHAR(ADD_MONTHS(TO_DATE(T1.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                   AND T2.BAS_YM    <= T1.BAS_YM
                                   AND T2.PCF_ID     = T1.PCF_ID
           GROUP BY T1.BAS_YM, T1.PCF_ID
          ),
          FROM_TB01_G32_018_TTGS_A_BASE AS
          (SELECT T1.BAS_YM,
                  T1.PCF_ID,
                  NVL(T2.PCF_CR_OFCR_NUM_CNT, 0) AS PCF_CR_OFCR_NUM_CNT
           FROM   (SELECT B.BAS_YM, A.PCF_ID
                   FROM   TB01_G32_018_TTGS_A A INNER JOIN TM00_MMLY_CAL_D B
                                                   ON B.BAS_YM = A.BAS_YM
                   GROUP BY B.BAS_YM, A.PCF_ID
                  ) T1 LEFT OUTER JOIN (SELECT B.BAS_YM,
                                               A.PCF_ID,
                                               COUNT(DISTINCT A.ID_CARD) AS PCF_CR_OFCR_NUM_CNT
                                        FROM   TB01_G32_018_TTGS_A A INNER JOIN TM00_MMLY_CAL_D B
                                                                        ON B.BAS_YM = A.BAS_YM
                                                              /*  INNER JOIN TM00_PCF_POSITION_D C
                                                                        ON C.IS_CREDIT_OFFICER = '1'
                                                                       AND UPPER(TRIM(A.ID_CARD)) = UPPER(TRIM(C.PCF_POSITION_NAME)) */
                                        WHERE  A.ID_CARD IS NOT NULL
                                        GROUP BY B.BAS_YM, A.PCF_ID
                                       ) T2
                                    ON T2.BAS_YM = T1.BAS_YM
                                   AND T2.PCF_ID = T1.PCF_ID
          ),
          FROM_TB01_G32_018_TTGS_A AS
          (SELECT T3.BAS_YM,
                  T1.PCF_ID,
                  T2.PCF_CR_OFCR_NUM_CNT
           FROM   (SELECT PCF_ID,
                          MAX(BAS_YM) AS MAX_BAS_YM
                   FROM   FROM_TB01_G32_018_TTGS_A_BASE
                   GROUP BY PCF_ID
                  ) T1 INNER JOIN FROM_TB01_G32_018_TTGS_A_BASE T2
                               ON T2.BAS_YM = T1.MAX_BAS_YM
                              AND T2.PCF_ID = T1.PCF_ID
                       INNER JOIN TM00_MMLY_CAL_D T3
                               ON T3.BAS_YM > T1.MAX_BAS_YM
                              AND T3.BAS_YM < TO_CHAR(SYSTIMESTAMP,'YYYYMM')
           UNION ALL

           SELECT BAS_YM,
                  PCF_ID,
                  PCF_CR_OFCR_NUM_CNT
           FROM   FROM_TB01_G32_018_TTGS_A_BASE
          ),
          FROM_MMLY_IDC_CALC_CAR AS
          (SELECT BAS_YM,
                  PCF_ID,
                  EQT_AMT,
                  CNSLDT_EQT_AMT,
                  CCAP_AMT,
                  TOT_AST_AMT,
                  CAR
           FROM   TM24_MMLY_IDC_CALC_CAR_A
           WHERE  BAS_YM = loop_bas_day.BAS_YM
          ),
          FROM_MMLY_IDC_CALC_CAR_L3M AS
          (SELECT T1.BAS_YM,
                  T1.PCF_ID,
                  ROUND(AVG(T2.EQT_AMT), 0) AS L3M_EQT_AMT,
                  ROUND(AVG(T2.CCAP_AMT), 0) AS L3M_CCAP_AMT
           FROM   FULL_SET T1 LEFT OUTER JOIN TM24_MMLY_IDC_CALC_CAR_A T2
                                           ON T2.BAS_YM    >= TO_CHAR(ADD_MONTHS(TO_DATE(T1.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                          AND T2.BAS_YM    <= T1.BAS_YM
                                          AND T2.PCF_ID     = T1.PCF_ID
           WHERE  T1.BAS_YM = loop_bas_day.BAS_YM
           GROUP BY T1.BAS_YM, T1.PCF_ID
          ),
          FROM_MMLY_IDC_CALC_CAR_L12M AS
          (SELECT T1.BAS_YM,
                  T1.PCF_ID,
                  ROUND(AVG(T2.EQT_AMT), 0) AS L12M_EQT_AMT,
                  ROUND(AVG(T2.TOT_AST_AMT), 0) AS L12M_AST_AMT
           FROM   FULL_SET T1 LEFT OUTER JOIN TM24_MMLY_IDC_CALC_CAR_A T2
                                           ON T2.BAS_YM    >= TO_CHAR(ADD_MONTHS(TO_DATE(T1.BAS_YM, 'YYYYMM'), -11), 'YYYYMM')
                                          AND T2.BAS_YM    <= T1.BAS_YM
                                          AND T2.PCF_ID     = T1.PCF_ID
           WHERE  T1.BAS_YM = loop_bas_day.BAS_YM
           GROUP BY T1.BAS_YM, T1.PCF_ID
          ),
          FROM_MMLY_BAL_SHET AS
          (SELECT BAS_YM,
                  PCF_ID,
                  SUM(CASE WHEN PCF_COA_ID IN ('41592', '41593') THEN CLO_CR_BAL ELSE 0 END) AS CBV_OVD_BOR_AMT,
                  SUM(CASE WHEN PCF_COA_ID IN ('41512', '41513', '41592', '41593') THEN CLO_CR_BAL ELSE 0 END) AS CBV_BOR_EXCL_SFTY_FUND_AMT,
                  SUM(CASE WHEN PCF_COA_ID IN ('41511', '41591') THEN CLO_CR_BAL ELSE 0 END) AS SFTY_FUND_BOR_AMT,
                  SUM(CASE WHEN PCF_COA_ID IN ('41519', '41599') THEN CLO_CR_BAL ELSE 0 END) AS OTHR_INST_BOR_AMT,
                  SUM(CASE WHEN PCF_COA_ID IN ('13111', '13121') THEN CLO_DR_BAL ELSE 0 END) AS CBV_SVG_AMT,

                  SUM(CASE WHEN PCF_COA_ID IN ('13119', '13129') THEN CLO_DR_BAL ELSE 0 END) AS OTHR_INST_SVG_AMT,
                  SUM(CASE WHEN PCF_COA_ID = '13129' THEN CLO_DR_BAL ELSE 0 END) AS OTHR_INST_TRM_DPST_AMT, /* 2022:06:07 Add Column */

                  SUM(CASE WHEN PCF_COA_ID = '10' THEN CLO_DR_BAL ELSE 0 END) AS IN_HAND_CSH_AMT,
                  SUM(CASE WHEN PCF_COA_ID = '34401' THEN CLO_DR_BAL ELSE 0 END) AS CNTRBT_CBV_LT_CAP_AMT,
                  SUM(CASE WHEN PCF_COA_ID IN ('7', '69') THEN CLO_CR_BAL ELSE 0 END) - SUM(CASE WHEN PCF_COA_ID IN ('8', '69') THEN CLO_DR_BAL ELSE 0 END) AS INCL_ALOSS_INCM_MNS_EXP_AMT,
                  SUM(CASE WHEN PCF_COA_ID IN ('601', '611') THEN CLO_CR_BAL ELSE 0 END) AS CCAP_RSRV_FUND_SUPL_CCAP_AMT,
                  SUM(CASE WHEN PCF_COA_ID = '42' THEN CLO_CR_BAL ELSE 0 END) AS TDP_BAL,
                  SUM(CASE WHEN PCF_COA_ID = '211' THEN CLO_DR_BAL ELSE 0 END) AS BAL_SHET_BAS_SHRT_TRM_LN_BAL,
                  SUM(CASE WHEN PCF_COA_ID IN ('212', '213', '25') THEN CLO_DR_BAL ELSE 0 END) AS BAL_SHET_BAS_MED_LT_LN_BAL,
                  SUM(CASE WHEN PCF_COA_ID IN ('219', '259', '289', '299', '209') THEN CLO_CR_BAL ELSE 0 END) AS RISK_PRVS_FUND_AMT
           FROM   TM23_MMLY_BAL_SHET_A
           WHERE  BAS_YM = loop_bas_day.BAS_YM
           GROUP BY BAS_YM, PCF_ID
          ),
          FROM_MMLY_BAL_SHET_L3M AS
          (SELECT T1.BAS_YM,
                  T1.PCF_ID,
                  ROUND(AVG(T2.CBV_BOR_EXCL_SFTY_FUND_AMT), 0) AS L3M_CBV_BOR_EXCL_SFTY_FUND_AMT,
                  ROUND(AVG(T2.CBV_SVG_AMT), 0) AS L3M_CBV_SVG_AMT,
                  ROUND(AVG(T2.OTHR_INST_SVG_AMT), 0) AS L3M_OTHR_INST_SVG_AMT,
                  ROUND(AVG(T2.TDP_BAL), 0) AS L3M_TDP_BAL,
                  ROUND(AVG(T2.TDP_BAL), 0) AS L3M_INCL_ALOS_INCM_MNS_EXP_AMT
           FROM   FULL_SET T1 LEFT OUTER JOIN (SELECT BAS_YM,
                                                      PCF_ID,
                                                      SUM(CASE WHEN PCF_COA_ID IN ('41512', '41513', '41592', '41593') THEN CLO_CR_BAL ELSE 0 END) AS CBV_BOR_EXCL_SFTY_FUND_AMT,
                                                      SUM(CASE WHEN PCF_COA_ID IN ('13111', '13121') THEN CLO_DR_BAL ELSE 0 END) AS CBV_SVG_AMT,
                                                      SUM(CASE WHEN PCF_COA_ID IN ('13119', '13129') THEN CLO_DR_BAL ELSE 0 END) AS OTHR_INST_SVG_AMT,
                                                      SUM(CASE WHEN PCF_COA_ID = '42' THEN CLO_CR_BAL ELSE 0 END) AS TDP_BAL,
                                                      SUM(CASE WHEN PCF_COA_ID IN ('7', '69') THEN CLO_CR_BAL ELSE 0 END) - SUM(CASE WHEN PCF_COA_ID IN ('8', '69') THEN CLO_DR_BAL ELSE 0 END) AS INCL_ALOSS_INCM_MNS_EXP_AMT
                                               FROM   TM23_MMLY_BAL_SHET_A
                                               GROUP BY BAS_YM, PCF_ID
                                              ) T2
                                           ON T2.BAS_YM    >= TO_CHAR(ADD_MONTHS(TO_DATE(T1.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                          AND T2.BAS_YM    <= T1.BAS_YM
                                          AND T2.PCF_ID     = T1.PCF_ID
           WHERE  T1.BAS_YM = loop_bas_day.BAS_YM
           GROUP BY T1.BAS_YM, T1.PCF_ID
          ),
          FROM_MMLY_BAL_SHET_L12M AS
          (SELECT T1.BAS_YM,
                  T1.PCF_ID,
                  ROUND(AVG(T2.TDP_BAL), 0) AS L12M_DPST_AMT
           FROM   FULL_SET T1 LEFT OUTER JOIN (SELECT BAS_YM,
                                                      PCF_ID,
                                                      SUM(CASE WHEN PCF_COA_ID = '42' THEN CLO_CR_BAL ELSE 0 END) AS TDP_BAL
                                               FROM   TM23_MMLY_BAL_SHET_A
                                               GROUP BY BAS_YM, PCF_ID
                                              ) T2
                                           ON T2.BAS_YM    >= TO_CHAR(ADD_MONTHS(TO_DATE(T1.BAS_YM, 'YYYYMM'), -11), 'YYYYMM')
                                          AND T2.BAS_YM    <= T1.BAS_YM
                                          AND T2.PCF_ID     = T1.PCF_ID
           WHERE  T1.BAS_YM = loop_bas_day.BAS_YM
           GROUP BY T1.BAS_YM, T1.PCF_ID
          ),
          FROM_MMLY_AST_QAL AS
          (SELECT BAS_YM,
                  PCF_ID,
                  TOT_LN_BAL,
                  BAD_DBT_AMT,
                  GENL_PRVS_AMT,
                  DBT_GRP_1_SPEC_PRVS_AMT + DBT_GRP_2_SPEC_PRVS_AMT + DBT_GRP_3_SPEC_PRVS_AMT + DBT_GRP_4_SPEC_PRVS_AMT + DBT_GRP_5_SPEC_PRVS_AMT AS SPEC_PRVS_AMT,
                  TOT_COLL_VAL AS COLL_VAL
           FROM   TM26_MMLY_AST_QAL_A
           WHERE  BAS_YM = loop_bas_day.BAS_YM
          ),
          FROM_MMLY_AST_QAL_L3M AS
          (SELECT T1.BAS_YM,
                  T1.PCF_ID,
                  ROUND(AVG(T2.TOT_LN_BAL), 0) AS L3M_TOT_LN_BAL,
                  ROUND(AVG(T2.BAD_DBT_AMT), 0) AS L3M_BAD_DBT_AMT
           FROM   FULL_SET T1 LEFT OUTER JOIN TM26_MMLY_AST_QAL_A T2
                                           ON T2.BAS_YM    >= TO_CHAR(ADD_MONTHS(TO_DATE(T1.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                          AND T2.BAS_YM    <= T1.BAS_YM
                                          AND T2.PCF_ID     = T1.PCF_ID
           WHERE  T1.BAS_YM = loop_bas_day.BAS_YM
           GROUP BY T1.BAS_YM, T1.PCF_ID
          ),
          FROM_MMLY_AST_QAL_L12M AS
          (SELECT T1.BAS_YM,
                  T1.PCF_ID,
                  ROUND(AVG(T2.TOT_LN_BAL), 0) AS L12M_LN_AMT
           FROM   FULL_SET T1 LEFT OUTER JOIN TM26_MMLY_AST_QAL_A T2
                                           ON T2.BAS_YM    >= TO_CHAR(ADD_MONTHS(TO_DATE(T1.BAS_YM, 'YYYYMM'), -11), 'YYYYMM')
                                          AND T2.BAS_YM    <= T1.BAS_YM
                                          AND T2.PCF_ID     = T1.PCF_ID
           WHERE  T1.BAS_YM = loop_bas_day.BAS_YM
           GROUP BY T1.BAS_YM, T1.PCF_ID
          ),
          FROM_MMLY_CUST_CR_TRANS AS
          (SELECT BAS_YM,
                  PCF_ID,
                  SUM(CASE WHEN RMTRT_LN_TRM_CD BETWEEN '001' AND '012' THEN TOT_LN_BAL
                           WHEN RMTRT_LN_TRM_CD = 'ZZZ' THEN TOT_LN_BAL
                           ELSE 0
                           END) AS SHRT_TRM_LN_BAL,
                  SUM(CASE WHEN RMTRT_LN_TRM_CD BETWEEN '013' AND '999' THEN TOT_LN_BAL
                           WHEN RMTRT_LN_TRM_CD = 'XXX' THEN TOT_LN_BAL
                           ELSE 0
                           END) AS MED_LT_LN_BAL,
                  SUM(CASE WHEN COLL_TYP_CD  = '00' THEN TOT_LN_BAL ELSE 0 END) AS WO_COLL_LN_BAL,
                  SUM(CASE WHEN COLL_TYP_CD <> '00' THEN TOT_LN_BAL ELSE 0 END) AS WIT_COLL_LN_BAL,
                  SUM(NEW_ARISN_LN_AMT) AS NEW_ARISN_LN_AMT,
                  SUM(NEW_ARISN_LN_CNTR_NUM_CNT) AS NEW_ARISN_LN_CNTR_NUM_CNT,
                  SUM(NEW_BRWR_NUM_CNT) AS NEW_BRWR_NUM_CNT
           FROM   TM21_MMLY_CUST_CR_TRANS_A
           WHERE  BAS_YM = loop_bas_day.BAS_YM
           GROUP BY BAS_YM, PCF_ID
          ),
          FROM_MMLY_CUST_CR_TRANS_L3M AS
          (SELECT T1.BAS_YM,
                  T1.PCF_ID,
                  ROUND(AVG(WO_COLL_LN_BAL), 0)  AS L3M_WO_COLL_LN_BAL,
                  ROUND(AVG(WIT_COLL_LN_BAL), 0) AS L3M_WIT_COLL_LN_BAL
           FROM   FULL_SET T1 LEFT OUTER JOIN (SELECT BAS_YM,
                                                      PCF_ID,
                                                      SUM(CASE WHEN COLL_TYP_CD  = '00' THEN TOT_LN_BAL ELSE 0 END) AS WO_COLL_LN_BAL,
                                                      SUM(CASE WHEN COLL_TYP_CD <> '00' THEN TOT_LN_BAL ELSE 0 END) AS WIT_COLL_LN_BAL
                                               FROM   TM21_MMLY_CUST_CR_TRANS_A
                                               GROUP BY BAS_YM, PCF_ID
                                              ) T2
                                           ON T2.BAS_YM    >= TO_CHAR(ADD_MONTHS(TO_DATE(T1.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                          AND T2.BAS_YM    <= T1.BAS_YM
                                          AND T2.PCF_ID     = T1.PCF_ID
           WHERE  T1.BAS_YM = loop_bas_day.BAS_YM
           GROUP BY T1.BAS_YM, T1.PCF_ID
          ),
          FROM_MMLY_CUST_DPST_TRANS AS
          (SELECT BAS_YM,
                  PCF_ID,
                  SUM(LSTHN_1_YR_TRM_DPST_AMT) AS SHRT_TRM_DPST_BAL,
                  SUM(OVER_1_YR_TRM_DPST_AMT) AS MED_LT_DPST_BAL,
                  SUM(CASE WHEN SUBSTR(CUST_TYP_CD,1,1) = '1' THEN TDP_BAL ELSE 0 END) AS MBR_DPST_BAL,
                  SUM(CASE WHEN SUBSTR(CUST_TYP_CD,1,1) = '2' THEN TDP_BAL ELSE 0 END) AS NON_MBR_DPST_BAL
           FROM   TM22_MMLY_CUST_DPST_TRANS_A
           WHERE  BAS_YM = loop_bas_day.BAS_YM
           GROUP BY BAS_YM, PCF_ID
          ),
          FROM_DDLY_CUST_DPST_TRANS AS
          (SELECT loop_bas_day.BAS_YM AS BAS_YM,
                  A.PCF_ID,
                  COUNT(DISTINCT CASE WHEN A.DPST_BAL > 0 THEN A.CUST_ID ELSE NULL END) AS TOT_DPSTR_NUM_CNT,
                  COUNT(DISTINCT CASE WHEN SUBSTR(A.CUST_TYP_CD,1,1) = '1' AND A.DPST_BAL > 0 THEN A.CUST_ID ELSE NULL END) AS MBR_DPSTR_NUM_CNT,
                  COUNT(DISTINCT CASE WHEN SUBSTR(A.CUST_TYP_CD,1,1) = '2' AND A.DPST_BAL > 0 THEN A.CUST_ID ELSE NULL END) AS NON_MBR_DPSTR_NUM_CNT
           FROM   TM22_DDLY_CUST_DPST_TRANS_A A INNER JOIN (SELECT PCF_ID, MAX(BAS_DAY) AS BAS_DAY
                                                            FROM   TM22_DDLY_CUST_DPST_TRANS_A
                                                            WHERE  BAS_DAY BETWEEN loop_bas_day.BAS_YM||'01' AND loop_bas_day.BAS_YM||'31'
                                                            GROUP BY PCF_ID
                                                           ) B
                                                        ON B.PCF_ID  = A.PCF_ID
                                                       AND B.BAS_DAY = A.BAS_DAY
           GROUP BY A.PCF_ID
          ),
          FROM_DDLY_CUST_CR_TRANS AS
          (SELECT loop_bas_day.BAS_YM AS BAS_YM,
                  A.PCF_ID,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 THEN
                                                             CASE WHEN CUST_ID IS NULL THEN LN_CNTR_NUM_INFO
                                                                  ELSE CUST_ID
                                                                  END
                                      ELSE NULL
                                      END) AS TOT_BRWR_NUM_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS TOT_LN_CNTR_NUM_CNT
           FROM   TM21_DDLY_CUST_CR_TRANS_A A INNER JOIN (SELECT PCF_ID, MAX(BAS_DAY) AS BAS_DAY
                                                          FROM   TM21_DDLY_CUST_CR_TRANS_A
                                                          WHERE  BAS_DAY BETWEEN loop_bas_day.BAS_YM||'01' AND loop_bas_day.BAS_YM||'31'
                                                          GROUP BY PCF_ID
                                                         ) B
                                                      ON B.PCF_ID  = A.PCF_ID
                                                     AND B.BAS_DAY = A.BAS_DAY
           GROUP BY A.PCF_ID
          ),
          FROM_MMLY_PCF_CR_TRANS AS
          (SELECT BAS_YM,
                  PCF_ID,
                  SUM(CASE WHEN PCF_VS_CI_BOR_TYP_CD IN ('1','2','3','4','5')
                                THEN CASE WHEN LN_TRM_CD BETWEEN '000' AND '012' OR LN_TRM_CD = 'ZZZ'
                                               THEN BOR_AMT
                                          ELSE 0
                                          END
                           ELSE 0
                           END) AS CBV_STBOR_AMT,
                  SUM(CASE WHEN PCF_VS_CI_BOR_TYP_CD IN ('1','2','3','4','5')
                                THEN CASE WHEN LN_TRM_CD BETWEEN '013' AND '999' OR LN_TRM_CD = 'XXX'
                                               THEN BOR_AMT
                                          ELSE 0
                                          END
                           ELSE 0
                           END) AS CBV_MED_LT_BOR_AMT,
                  SUM(CASE WHEN PCF_VS_CI_BOR_TYP_CD IN ('1','2','3','4','5') THEN BOR_AMT
                           ELSE 0
                           END) AS CBV_BOR_AMT
           FROM   TM21_MMLY_PCF_CR_TRANS_A
           WHERE  BAS_YM = loop_bas_day.BAS_YM
           GROUP BY BAS_YM, PCF_ID
          ),
          FROM_MMLY_PCF_DPST_TRANS AS
          (SELECT BAS_YM,
                  PCF_ID,
                  SUM(CASE WHEN PCF_VS_CI_DPST_TYP_CD IN ('1','2','3')
                                THEN CASE WHEN DPST_TRM_CD BETWEEN '000' AND '012' OR DPST_TRM_CD = 'ZZZ'
                                               THEN DPST_BAL
                                          ELSE 0
                                          END
                           ELSE 0
                           END) AS CBV_SHRT_TRM_SVG_AMT,
                  SUM(CASE WHEN PCF_VS_CI_DPST_TYP_CD IN ('2','3')
                                THEN CASE WHEN DPST_TRM_CD BETWEEN '013' AND '999' OR DPST_TRM_CD = 'XXX'
                                               THEN DPST_BAL
                                          ELSE 0
                                          END
                           ELSE 0
                           END) AS CBV_MED_LT_SVG_AMT
           FROM   TM22_MMLY_PCF_DPST_TRANS_A
           WHERE  BAS_YM = loop_bas_day.BAS_YM
           GROUP BY BAS_YM, PCF_ID
          ),
          FROM_MMLY_INCM_EXP AS
          (SELECT BAS_YM,
                  PCF_ID,
                  TOT_INCM_AMT,
                  TOT_EXP_AMT,
                  NET_PNL_AMT,
                  CUR_TOT_INCM_AMT,
                  CUR_TOT_EXP_AMT,
                  CUR_NET_PNL_AMT
           FROM   TM23_MMLY_INCM_EXP_A
           WHERE  BAS_YM = loop_bas_day.BAS_YM
          ),
          FROM_MMLY_INCM_EXP_L3M AS
          (SELECT T1.BAS_YM,
                  T1.PCF_ID,
                  ROUND(AVG(T2.NET_PNL_AMT), 0) AS L3M_NET_PNL_AMT,
                  ROUND(AVG(T2.CUR_NET_PNL_AMT), 0) AS L3M_CUR_NET_PNL_AMT
           FROM   FULL_SET T1 LEFT OUTER JOIN TM23_MMLY_INCM_EXP_A T2
                                           ON T2.BAS_YM    >= TO_CHAR(ADD_MONTHS(TO_DATE(T1.BAS_YM, 'YYYYMM'), -2), 'YYYYMM')
                                          AND T2.BAS_YM    <= T1.BAS_YM
                                          AND T2.PCF_ID     = T1.PCF_ID
           WHERE  T1.BAS_YM = loop_bas_day.BAS_YM
           GROUP BY T1.BAS_YM, T1.PCF_ID
          )
          SELECT A.BAS_YM
                ,A.PCF_ID
                ,B.TOT_PCF_MBR_NUM_CNT
                ,B.NEW_JON_PCF_MBR_NUM_CNT
                ,B.LV_PCF_MBR_NUM_CNT
                ,C.PCF_CR_OFCR_NUM_CNT
                ,D.EQT_AMT
                ,D.CNSLDT_EQT_AMT
                ,D.CCAP_AMT
                ,E.CCAP_RSRV_FUND_SUPL_CCAP_AMT
                ,D.TOT_AST_AMT
                ,F.TOT_LN_BAL
                ,G.SHRT_TRM_LN_BAL
                ,G.MED_LT_LN_BAL
                ,G.WO_COLL_LN_BAL
                ,G.WIT_COLL_LN_BAL
                ,W.TOT_LN_CNTR_NUM_CNT
                ,W.TOT_BRWR_NUM_CNT
                ,G.NEW_ARISN_LN_AMT
                ,G.NEW_ARISN_LN_CNTR_NUM_CNT
                ,G.NEW_BRWR_NUM_CNT
                ,F.BAD_DBT_AMT
                ,F.GENL_PRVS_AMT
                ,F.SPEC_PRVS_AMT
                ,F.COLL_VAL
                ,E.TDP_BAL
                ,H.MBR_DPST_BAL
                ,H.NON_MBR_DPST_BAL
                ,H.SHRT_TRM_DPST_BAL
                ,H.MED_LT_DPST_BAL
                ,V.TOT_DPSTR_NUM_CNT
                ,V.MBR_DPSTR_NUM_CNT
                ,V.NON_MBR_DPSTR_NUM_CNT
                ,ROUND(H.MBR_DPST_BAL / (H.SHRT_TRM_DPST_BAL + H.MED_LT_DPST_BAL) * 100, 2) AS MBR_DPST_VS_TDP_RTO
                ,E.CBV_BOR_EXCL_SFTY_FUND_AMT
                ,I.CBV_BOR_AMT
                ,I.CBV_STBOR_AMT
                ,I.CBV_MED_LT_BOR_AMT
                ,E.CBV_OVD_BOR_AMT
                ,E.SFTY_FUND_BOR_AMT
                ,E.OTHR_INST_BOR_AMT
                ,E.CBV_SVG_AMT
                ,J.CBV_SHRT_TRM_SVG_AMT
                ,J.CBV_MED_LT_SVG_AMT
                ,E.OTHR_INST_SVG_AMT
                ,E.OTHR_INST_TRM_DPST_AMT
                ,E.IN_HAND_CSH_AMT
                ,D.CAR
                ,E.CNTRBT_CBV_LT_CAP_AMT
                ,K.TOT_INCM_AMT
                ,K.CUR_TOT_INCM_AMT
                ,K.TOT_EXP_AMT
                ,K.CUR_TOT_EXP_AMT
                ,K.NET_PNL_AMT
                ,K.CUR_NET_PNL_AMT
                ,E.INCL_ALOSS_INCM_MNS_EXP_AMT
                ,F.TOT_LN_BAL + E.CBV_SVG_AMT + E.OTHR_INST_SVG_AMT + E.CNTRBT_CBV_LT_CAP_AMT AS PRFBL_AST_AMT
                ,CASE WHEN (F.TOT_LN_BAL + E.CBV_SVG_AMT + E.OTHR_INST_SVG_AMT + E.CNTRBT_CBV_LT_CAP_AMT) = 0 AND D.TOT_AST_AMT = 0
                           THEN 0
                      ELSE ROUND((F.TOT_LN_BAL + E.CBV_SVG_AMT + E.OTHR_INST_SVG_AMT + E.CNTRBT_CBV_LT_CAP_AMT) / D.TOT_AST_AMT * 100, 2)
                      END AS PRFBL_AST_VS_TOT_AST_RTO
                ,U.L3M_TOT_PCF_MBR_CNT
                ,L.L3M_TDP_BAL
                ,M.L3M_TOT_LN_BAL
                ,O.L3M_WO_COLL_LN_BAL
                ,O.L3M_WIT_COLL_LN_BAL
                ,M.L3M_BAD_DBT_AMT
                ,N.L3M_EQT_AMT
                ,N.L3M_CCAP_AMT
                ,P.L3M_NET_PNL_AMT
                ,P.L3M_CUR_NET_PNL_AMT
                ,L.L3M_CBV_BOR_EXCL_SFTY_FUND_AMT
                ,L.L3M_CBV_SVG_AMT
                ,L.L3M_OTHR_INST_SVG_AMT
                ,L.L3M_INCL_ALOS_INCM_MNS_EXP_AMT
                ,R.L12M_AST_AMT
                ,R.L12M_EQT_AMT
                ,S.L12M_LN_AMT
                ,T.L12M_DPST_AMT
                ,E.BAL_SHET_BAS_SHRT_TRM_LN_BAL
                ,E.BAL_SHET_BAS_MED_LT_LN_BAL
                ,E.RISK_PRVS_FUND_AMT
                ,SYSTIMESTAMP
          FROM   FULL_SET A LEFT OUTER JOIN FROM_G32_016_TTGS B
                                         ON A.BAS_YM = B.BAS_YM
                                        AND A.PCF_ID = B.PCF_ID
                            LEFT OUTER JOIN FROM_TB01_G32_018_TTGS_A C
                                         ON A.BAS_YM = C.BAS_YM
                                        AND A.PCF_ID = C.PCF_ID
                            LEFT OUTER JOIN FROM_MMLY_IDC_CALC_CAR D
                                         ON A.BAS_YM = D.BAS_YM
                                        AND A.PCF_ID = D.PCF_ID
                            LEFT OUTER JOIN FROM_MMLY_BAL_SHET E
                                         ON A.BAS_YM = E.BAS_YM
                                        AND A.PCF_ID = E.PCF_ID
                            LEFT OUTER JOIN FROM_MMLY_AST_QAL F
                                         ON A.BAS_YM = F.BAS_YM
                                        AND A.PCF_ID = F.PCF_ID
                            LEFT OUTER JOIN FROM_MMLY_CUST_CR_TRANS G
                                         ON A.BAS_YM = G.BAS_YM
                                        AND A.PCF_ID = G.PCF_ID
                            LEFT OUTER JOIN FROM_MMLY_CUST_DPST_TRANS H
                                         ON A.BAS_YM = H.BAS_YM
                                        AND A.PCF_ID = H.PCF_ID
                            LEFT OUTER JOIN FROM_MMLY_PCF_CR_TRANS I
                                         ON A.BAS_YM = I.BAS_YM
                                        AND A.PCF_ID = I.PCF_ID
                            LEFT OUTER JOIN FROM_MMLY_PCF_DPST_TRANS J
                                         ON A.BAS_YM = J.BAS_YM
                                        AND A.PCF_ID = J.PCF_ID
                            LEFT OUTER JOIN FROM_MMLY_INCM_EXP K
                                         ON A.BAS_YM = K.BAS_YM
                                        AND A.PCF_ID = K.PCF_ID
                            LEFT OUTER JOIN FROM_MMLY_BAL_SHET_L3M L
                                         ON A.BAS_YM = L.BAS_YM
                                        AND A.PCF_ID = L.PCF_ID
                            LEFT OUTER JOIN FROM_MMLY_AST_QAL_L3M M
                                         ON A.BAS_YM = M.BAS_YM
                                        AND A.PCF_ID = M.PCF_ID
                            LEFT OUTER JOIN FROM_MMLY_IDC_CALC_CAR_L3M N
                                         ON A.BAS_YM = N.BAS_YM
                                        AND A.PCF_ID = N.PCF_ID
                            LEFT OUTER JOIN FROM_MMLY_CUST_CR_TRANS_L3M O
                                         ON A.BAS_YM = O.BAS_YM
                                        AND A.PCF_ID = O.PCF_ID
                            LEFT OUTER JOIN FROM_MMLY_INCM_EXP_L3M P
                                         ON A.BAS_YM = P.BAS_YM
                                        AND A.PCF_ID = P.PCF_ID
                            LEFT OUTER JOIN FROM_MMLY_IDC_CALC_CAR_L12M R
                                         ON A.BAS_YM = R.BAS_YM
                                        AND A.PCF_ID = R.PCF_ID
                            LEFT OUTER JOIN FROM_MMLY_AST_QAL_L12M S
                                         ON A.BAS_YM = S.BAS_YM
                                        AND A.PCF_ID = S.PCF_ID
                            LEFT OUTER JOIN FROM_MMLY_BAL_SHET_L12M T
                                         ON A.BAS_YM = T.BAS_YM
                                        AND A.PCF_ID = T.PCF_ID
                            LEFT OUTER JOIN FROM_G32_016_TTGS_L3M U
                                         ON A.BAS_YM = U.BAS_YM
                                        AND A.PCF_ID = U.PCF_ID
                            LEFT OUTER JOIN FROM_DDLY_CUST_DPST_TRANS V
                                         ON A.BAS_YM = V.BAS_YM
                                        AND A.PCF_ID = V.PCF_ID
                            LEFT OUTER JOIN FROM_DDLY_CUST_CR_TRANS W
                                         ON A.BAS_YM = W.BAS_YM
                                        AND A.PCF_ID = W.PCF_ID;

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