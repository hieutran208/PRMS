create or replace PROCEDURE "P1_M27_DDLY_PCF_KEY_IDC_1718"
                 ( p_wkdate    IN  CHAR DEFAULT NULL
                 , p_stdate_01 IN  CHAR DEFAULT NULL
                 , p_eddate_01 IN  CHAR DEFAULT NULL
                 )
IS
    /*--------------------------------------------------------------------------
     * PROGRAM ID    : P1_M27_DDLY_PCF_KEY_IDC_1718
     * PROGRAM NAME  : A program for insert data to TM27_DDLY_PCF_KEY_IDC_A
     * SOURCE TABLE  : TM23_DDLY_BAL_SHET_A
                       TM22_DDLY_CUST_DPST_TRANS_A
                       TM21_DDLY_CUST_CR_TRANS_A
                       TB07_G32_005_TTGS_A
     * TARGET TABLE  : TM27_DDLY_PCF_KEY_IDC_A
     * PROGRAMER     : HIEU
     * LAST MODIFICATION DATE : 2025-12-18
     * UNIQUENESS    : N/A
     * COMMENTS      : N/A
    ----------------------------------------------------------------------------
     * Revision History : 2025-12-18 : Create
     * Revision History :
    --------------------------------------------------------------------------*/
    ----------------------------------------------------------------------------
    --  Declare Local Variables
    ----------------------------------------------------------------------------
    v_wk_date                 CHAR(8) DEFAULT NULL ;
    v_st_date_01              CHAR(8) DEFAULT NULL ;
    v_end_date_01             CHAR(8) DEFAULT NULL ;

    v_program_id              VARCHAR2(100) DEFAULT 'P1_M27_DDLY_PCF_KEY_IDC_1718' ;
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
    SELECT W2.BAS_DAY
    FROM   (
            SELECT NVL(MIN(BAS_DAY), TO_CHAR(TO_DATE(v_dt2, 'YYYYMMDD') -2, 'YYYYMMDD')) AS MIN_DAY, TO_CHAR(TO_DATE(v_dt2, 'YYYYMMDD') -2, 'YYYYMMDD') AS MAX_DAY
            FROM   (
/*
                    SELECT T1.BAS_DAY
                    FROM   TM00_DDLY_CAL_D T1
                    WHERE  T1.BAS_DAY BETWEEN '20201212' AND '20201218'
*/
                    SELECT MAX(BAS_DAY) AS BAS_DAY
                    FROM   TM23_DDLY_BAL_SHET_A

                    UNION ALL

                    SELECT T1.BAS_DAY
                    FROM   TM00_DDLY_CAL_D T1 INNER JOIN (SELECT MIN(DATA_BAS_DAY) AS MIN_DAY, MAX(DATA_BAS_DAY) AS MAX_DAY
                                                          FROM   TBSM_INPT_RPT_SUBMIT_L
                                                          WHERE  BTCH_BAS_DAY = v_dt2
                                                          AND    INPT_RPT_ID  = 'G32_012_TTGS'
                                                         ) T2
                                                      ON T1.BAS_DAY BETWEEN T2.MIN_DAY AND T2.MAX_DAY
                    UNION ALL

                    SELECT T1.BAS_DAY
                    FROM   TM00_DDLY_CAL_D T1 INNER JOIN (SELECT MIN(DATA_BAS_DAY) AS MIN_DAY, MAX(DATA_BAS_DAY) AS MAX_DAY
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
                    FROM   TM00_DDLY_CAL_D T1 INNER JOIN (SELECT MIN(DATA_BAS_DAY) AS MIN_DAY, MAX(DATA_BAS_DAY) AS MAX_DAY
                                                          FROM   TBSM_INPT_RPT_SUBMIT_L
                                                          WHERE  BTCH_BAS_DAY = v_dt2
                                                          AND    INPT_RPT_ID  = 'G32_005_TTGS'
                                                         ) T2
                                                      ON T1.BAS_DAY BETWEEN T2.MIN_DAY AND T2.MAX_DAY

                   )
           ) W1 INNER JOIN TM00_DDLY_CAL_D W2
                        ON W2.BAS_DAY BETWEEN W1.MIN_DAY AND W1.MAX_DAY
    WHERE W2.BAS_DAY >= '20210102'
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
    --  1.1 Delete data from two days ago on TM27_DDLY_PCF_KEY_IDC_A
    ----------------------------------------------------------------------------

    DELETE
    FROM   TM27_DDLY_PCF_KEY_IDC_A
    WHERE  BAS_DAY = TO_CHAR(TO_DATE(v_st_date_01, 'YYYYMMDD') -2, 'YYYYMMDD')
    ;

    ----------------------------------------------------------------------------
    --  Transaction Log
    ----------------------------------------------------------------------------
    v_step_code    := '010' ;
    v_step_desc    := v_wk_date || v_seq || ' : Delete data from two days ago on TM27_DDLY_PCF_KEY_IDC_A ' ;
    v_time         := SYSDATE ;

    P1_BSM_PROG_EXEC_LOG(v_program_id, v_program_type_name, v_step_code, v_step_desc, v_time, sql%rowcount, NULL, NULL) ;

    ----------------------------------------------------------------------------
    --  1.2 Replicate data from three days ago to two days ago
    ----------------------------------------------------------------------------

    INSERT INTO TM27_DDLY_PCF_KEY_IDC_A
    (   BAS_DAY
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
       ,TRGT_DATA_LST_MOD_TM
    )
    WITH
    FULL_SET AS
    (SELECT  T1.BAS_DAY, T1.PCF_ID
     FROM   (SELECT A.BAS_DAY, A.PCF_ID
             FROM   TM22_DDLY_CUST_DPST_TRANS_A A
             WHERE  A.BAS_DAY = TO_CHAR(TO_DATE(v_st_date_01, 'YYYYMMDD') -2, 'YYYYMMDD')
             GROUP BY A.BAS_DAY, A.PCF_ID

             UNION ALL

             SELECT A.BAS_DAY, A.PCF_ID
             FROM   TM21_DDLY_CUST_CR_TRANS_A A
             WHERE  A.BAS_DAY = TO_CHAR(TO_DATE(v_st_date_01, 'YYYYMMDD') -2, 'YYYYMMDD')
             GROUP BY A.BAS_DAY, A.PCF_ID

             UNION ALL

             SELECT A.BAS_DAY, A.PCF_ID
             FROM   TM23_DDLY_BAL_SHET_A A
             WHERE  A.BAS_DAY = TO_CHAR(TO_DATE(v_st_date_01, 'YYYYMMDD') -2, 'YYYYMMDD')
             GROUP BY A.BAS_DAY, A.PCF_ID

             UNION ALL

             SELECT A.BAS_YMD, A.PCF_ID
             FROM   TB07_G32_005_TTGS_A A
             WHERE  A.BAS_YMD = TO_CHAR(TO_DATE(v_st_date_01, 'YYYYMMDD') -2, 'YYYYMMDD')
             GROUP BY A.BAS_YMD, A.PCF_ID

            ) T1
    GROUP BY T1.BAS_DAY, T1.PCF_ID
    ),
     TM21_DDLY_CUST_CR_TRANS_V AS
    (SELECT  A.BAS_DAY
            ,A.PCF_ID
            ,A.RPLC_DATA_YN
            ,A.LN_CNTR_NUM_INFO
            ,MAX(A.CUST_ID             ) AS CUST_ID
            ,MAX(A.CUST_NM             ) AS CUST_NM
            ,MAX(A.CUST_TYP_CD         ) AS CUST_TYP_CD
            ,MAX(A.INIT_LN_TRM_CD      ) AS INIT_LN_TRM_CD
            ,MAX(A.RMTRT_LN_TRM_CD     ) AS RMTRT_LN_TRM_CD
            ,MAX(A.ES_CD               ) AS ES_CD
            ,MAX(A.COLL_TYP_INFO       ) AS COLL_TYP_INFO
            ,MAX(A.COLL_TYP_CD         ) AS COLL_TYP_CD
            ,MAX(A.LN_TRANS_TYP_CD     ) AS LN_TRANS_TYP_CD
            ,MAX(A.OLD_NORML_DBT_GRP_CD) AS OLD_NORML_DBT_GRP_CD
            ,MAX(A.OLD_OVD_DBT_GRP_CD  ) AS OLD_OVD_DBT_GRP_CD
            ,MAX(A.NEW_NORML_DBT_GRP_CD) AS NEW_NORML_DBT_GRP_CD
            ,MAX(A.NEW_OVD_DBT_GRP_CD  ) AS NEW_OVD_DBT_GRP_CD
            ,MAX(A.LN_CNTR_AMT         ) AS LN_CNTR_AMT
            ,MAX(A.LN_BAL              ) AS LN_BAL
            ,SUM(A.TRANS_AMT           ) AS TRANS_AMT
            ,MAX(A.COLL_VAL_AMT        ) AS COLL_VAL_AMT
            ,MAX(A.SPEC_PRVS_AMT       ) AS SPEC_PRVS_AMT
            ,MAX(A.OPN_DAY             ) AS OPN_DAY
            ,MAX(A.MTRT_DAY            ) AS MTRT_DAY
            ,MAX(A.ACTL_IR             ) AS ACTL_IR
     FROM   TM21_DDLY_CUST_CR_TRANS_A A
     WHERE  A.BAS_DAY = TO_CHAR(TO_DATE(v_st_date_01, 'YYYYMMDD') -2, 'YYYYMMDD')
     GROUP BY A.BAS_DAY
             ,A.PCF_ID
             ,A.RPLC_DATA_YN
             ,A.LN_CNTR_NUM_INFO
    ),
    TM21_DDLY_CUST_CR_TRANS_W AS
    (SELECT A.BAS_DAY,
            A.PCF_ID,
            SUM(CASE WHEN A.RPLC_DATA_YN = 'N' AND A.LN_TRANS_TYP_CD = '1'
                          THEN A.TRANS_AMT
                     ELSE 0
                     END
               ) AS NEW_ARISN_LN_AMT,
            COUNT(DISTINCT CASE WHEN A.RPLC_DATA_YN = 'N' AND A.LN_TRANS_TYP_CD = '1' AND A.TRANS_AMT > 0
                                     THEN A.LN_CNTR_NUM_INFO
                                ELSE NULL
                                END
                 ) AS NEW_ARISN_LN_CNTR_NUM_CNT,
            COUNT(DISTINCT CASE WHEN A.RPLC_DATA_YN = 'N' AND A.LN_TRANS_TYP_CD = '1' AND A.TRANS_AMT > 0
                                     THEN A.CUST_ID
                                ELSE NULL
                                END
                 ) AS NEW_BRWR_NUM_CNT,
            COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 THEN A.LN_CNTR_NUM_INFO
                                ELSE NULL
                                END) AS TOT_LN_CNTR_NUM_CNT,
            COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 THEN
                                                     CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO
                                                          ELSE A.CUST_ID
                                                          END
                                ELSE NULL
                                END) AS TOT_BRWR_NUM_CNT
     FROM   TM21_DDLY_CUST_CR_TRANS_A A
     WHERE  A.BAS_DAY = TO_CHAR(TO_DATE(v_st_date_01, 'YYYYMMDD') -2, 'YYYYMMDD')
     AND    A.LN_BAL > 0
     GROUP BY A.BAS_DAY,
              A.PCF_ID
    ),
    FROM_DDLY_CUST_CR_TRANS AS
    (SELECT BAS_DAY,
            PCF_ID,
            SUM(CASE WHEN RMTRT_LN_TRM_CD BETWEEN '001' AND '012' THEN LN_BAL
                     WHEN RMTRT_LN_TRM_CD = 'ZZZ' THEN LN_BAL
                     ELSE 0
                     END) AS SHRT_TRM_LN_BAL,
            SUM(CASE WHEN RMTRT_LN_TRM_CD BETWEEN '013' AND '999' THEN LN_BAL
                     WHEN RMTRT_LN_TRM_CD = 'XXX' THEN LN_BAL
                     ELSE 0
                     END) AS MED_LT_LN_BAL,
            SUM(CASE WHEN COLL_TYP_CD =  '00' THEN LN_BAL ELSE 0 END) AS WO_COLL_LN_BAL,
            SUM(CASE WHEN COLL_TYP_CD <> '00' THEN LN_BAL ELSE 0 END) AS WIT_COLL_LN_BAL,
            ROUND((SUM(CASE WHEN COLL_TYP_CD = '01' THEN LN_BAL ELSE 0 END) * 0.5), 0) AS FULL_GTD_RL_EST_LN_BAL_50P,
            ROUND((SUM(CASE WHEN COLL_TYP_CD = '02' THEN LN_BAL ELSE 0 END) * 0.2), 0) AS FULL_GTD_VP_LN_BAL_20P,
            SUM(CASE WHEN COLL_TYP_CD = '01' THEN LN_BAL ELSE 0 END) AS FULL_GTD_RL_EST_LN_BAL,
            SUM(CASE WHEN COLL_TYP_CD = '02' THEN LN_BAL ELSE 0 END) AS FULL_GTD_VP_LN_BAL
     FROM   TM21_DDLY_CUST_CR_TRANS_V
     WHERE  LN_BAL > 0
     GROUP BY BAS_DAY, PCF_ID
    ),
    FROM_DDLY_CUST_DPST_TRANS AS
    (SELECT A.BAS_DAY,
            A.PCF_ID,
            SUM(CASE WHEN A.INIT_DPST_TRM_CD <> 'XXX' AND (A.INIT_DPST_TRM_CD <= '012' OR A.INIT_DPST_TRM_CD = 'ZZZ')
                          THEN A.DPST_BAL
                     ELSE 0
                     END
               ) AS SHRT_TRM_DPST_BAL,
            SUM(CASE WHEN A.INIT_DPST_TRM_CD <> 'ZZZ' AND (A.INIT_DPST_TRM_CD > '012' OR A.INIT_DPST_TRM_CD = 'XXX')
                          THEN A.DPST_BAL
                     ELSE 0
                     END) AS MED_LT_DPST_BAL,
            COUNT(DISTINCT A.CUST_ID) AS TOT_DPSTR_NUM_CNT,
            SUM(CASE WHEN SUBSTR(A.CUST_TYP_CD,1,1) = '1' THEN A.DPST_BAL ELSE 0 END) AS MBR_DPST_BAL,
            SUM(CASE WHEN SUBSTR(A.CUST_TYP_CD,1,1) = '2' THEN A.DPST_BAL ELSE 0 END) AS NON_MBR_DPST_BAL,
            COUNT(DISTINCT CASE WHEN SUBSTR(A.CUST_TYP_CD,1,1) = '1' THEN A.CUST_ID ELSE NULL END) AS MBR_DPSTR_NUM_CNT,
            COUNT(DISTINCT CASE WHEN SUBSTR(A.CUST_TYP_CD,1,1) = '2' THEN A.CUST_ID ELSE NULL END) AS NON_MBR_DPSTR_NUM_CNT
     FROM   TM22_DDLY_CUST_DPST_TRANS_A A
     WHERE  A.BAS_DAY = TO_CHAR(TO_DATE(v_st_date_01, 'YYYYMMDD') -2, 'YYYYMMDD')
     AND    A.DPST_BAL > 0
     GROUP BY A.BAS_DAY, A.PCF_ID
    ),
    FROM_TB07_G32_005_TTGS_A AS
    (SELECT A.BAS_YMD AS BAS_DAY,
            A.PCF_ID,
            SUM(CASE WHEN INDC_CODE = 'G32005TTGS_018' THEN A.CALC_VALUE_NEXT_DAY ELSE 0 END) AS NXT_WRK_DAY_AST_AMT,
            SUM(CASE WHEN INDC_CODE = 'G32005TTGS_001' THEN A.CALC_VALUE_NEXT_DAY ELSE 0 END)  AS NXT_WRK_DAY_PAY_LBLTY_AMT,
            CASE WHEN SUM(CASE WHEN INDC_CODE = 'G32005TTGS_001' THEN A.CALC_VALUE_NEXT_DAY ELSE 0 END) = 0 THEN 0
                 WHEN SUM(CASE WHEN INDC_CODE = 'G32005TTGS_018' THEN A.CALC_VALUE_NEXT_DAY ELSE 0 END)  = 0 THEN 99999.99
                 ELSE ROUND(SUM(CASE WHEN INDC_CODE = 'G32005TTGS_001' THEN A.CALC_VALUE_NEXT_DAY ELSE 0 END) / SUM(CASE WHEN INDC_CODE = 'G32005TTGS_018' THEN A.CALC_VALUE_NEXT_DAY ELSE 0 END), 3)
                 END AS NXT_WRK_DAY_LQDTY_RTO,
            SUM(CASE WHEN INDC_CODE = 'G32005TTGS_018' THEN A.CALC_VALUE_NEXT_DAY ELSE 0 END) +
            SUM(CASE WHEN INDC_CODE = 'G32005TTGS_018' THEN A.CALC_VALUE_2TO7_DAY ELSE 0 END) AS NXT_7_WRK_DAY_AST_AMT,
            SUM(CASE WHEN INDC_CODE = 'G32005TTGS_001' THEN A.CALC_VALUE_NEXT_DAY ELSE 0 END) +
            SUM(CASE WHEN INDC_CODE = 'G32005TTGS_001' THEN A.CALC_VALUE_2TO7_DAY ELSE 0 END) AS NXT_7_WRK_DAY_PAY_LBLTY_AMT,
            CASE WHEN SUM(CASE WHEN INDC_CODE = 'G32005TTGS_001' THEN A.CALC_VALUE_NEXT_DAY + A.CALC_VALUE_2TO7_DAY ELSE 0 END) = 0 THEN 0
                 WHEN SUM(CASE WHEN INDC_CODE = 'G32005TTGS_018' THEN A.BOOK_VALUE_NEXT_DAY + A.BOOK_VALUE_2TO7_DAY ELSE 0 END) = 0 THEN 99999.99
                 ELSE ROUND(SUM(CASE WHEN INDC_CODE = 'G32005TTGS_001' THEN A.CALC_VALUE_NEXT_DAY + A.CALC_VALUE_2TO7_DAY ELSE 0 END) / SUM(CASE WHEN INDC_CODE = 'G32005TTGS_018' THEN A.BOOK_VALUE_NEXT_DAY + A.BOOK_VALUE_2TO7_DAY ELSE 0 END), 3)
                 END AS NXT_7_WRK_DAY_LQDTY_RTO
     FROM   TB07_G32_005_TTGS_A A
     WHERE  A.BAS_YMD <= TO_CHAR(TO_DATE(v_st_date_01, 'YYYYMMDD') -2, 'YYYYMMDD')
    ),
    FROM_TB07_G32_005_TTGS_A_3M_SET AS
    (SELECT T1.PCF_ID,
            T1.BAS_DAY AS BAS_DAY1,
            MAX(T2.BAS_DAY) AS BAS_DAY2,
            MAX(T3.BAS_DAY) AS BAS_DAY3
     FROM   FROM_TB07_G32_005_TTGS_A T1 LEFT OUTER JOIN FROM_TB07_G32_005_TTGS_A T2
                                                ON T2.BAS_DAY <= TO_CHAR(ADD_MONTHS(TO_DATE(T1.BAS_DAY, 'YYYYMMDD'), -1), 'YYYYMMDD')
                                               AND T2.PCF_ID = T1.PCF_ID
                                               AND T2.NXT_WRK_DAY_LQDTY_RTO IS NOT NULL
                                   LEFT OUTER JOIN FROM_TB07_G32_005_TTGS_A T3
                                                ON T3.BAS_DAY <= TO_CHAR(ADD_MONTHS(TO_DATE(T1.BAS_DAY, 'YYYYMMDD'), -2), 'YYYYMMDD')
                                               AND T3.PCF_ID = T1.PCF_ID
                                               AND T3.NXT_WRK_DAY_LQDTY_RTO IS NOT NULL
     WHERE  T1.BAS_DAY = TO_CHAR(TO_DATE(v_st_date_01, 'YYYYMMDD') -2, 'YYYYMMDD')
     GROUP BY T1.PCF_ID, T1.BAS_DAY
    ),
    FROM_TB07_G32_005_TTGS_A_3M AS
    (SELECT T1.PCF_ID,
            T1.BAS_DAY1 AS BAS_DAY,
            CASE WHEN NVL(T2.NXT_WRK_DAY_LQDTY_RTO, 0) + NVL(T3.NXT_WRK_DAY_LQDTY_RTO, 0) + NVL(T4.NXT_WRK_DAY_LQDTY_RTO, 0) = 0 THEN 0
                 WHEN CASE WHEN T2.NXT_WRK_DAY_LQDTY_RTO IS NOT NULL THEN 1 ELSE 0 END +
                      CASE WHEN T3.NXT_WRK_DAY_LQDTY_RTO IS NOT NULL THEN 1 ELSE 0 END +
                      CASE WHEN T4.NXT_WRK_DAY_LQDTY_RTO IS NOT NULL THEN 1 ELSE 0 END = 0 THEN 99999.99
                 ELSE
                      ROUND((NVL(T2.NXT_WRK_DAY_LQDTY_RTO, 0) +
                             NVL(T3.NXT_WRK_DAY_LQDTY_RTO, 0) +
                             NVL(T4.NXT_WRK_DAY_LQDTY_RTO, 0)) / (CASE WHEN T2.NXT_WRK_DAY_LQDTY_RTO IS NOT NULL THEN 1 ELSE 0 END +
                                                                  CASE WHEN T3.NXT_WRK_DAY_LQDTY_RTO IS NOT NULL THEN 1 ELSE 0 END +
                                                                  CASE WHEN T4.NXT_WRK_DAY_LQDTY_RTO IS NOT NULL THEN 1 ELSE 0 END
                                                                 ), 3)
                 END AS L3M_NXT_WRK_DAY_LQDTY_RTO,
           CASE WHEN NVL(T2.NXT_7_WRK_DAY_LQDTY_RTO, 0) + NVL(T3.NXT_7_WRK_DAY_LQDTY_RTO, 0) + NVL(T4.NXT_7_WRK_DAY_LQDTY_RTO, 0) = 0 THEN 0
                WHEN CASE WHEN T2.NXT_7_WRK_DAY_LQDTY_RTO IS NOT NULL THEN 1 ELSE 0 END +
                     CASE WHEN T3.NXT_7_WRK_DAY_LQDTY_RTO IS NOT NULL THEN 1 ELSE 0 END +
                     CASE WHEN T4.NXT_7_WRK_DAY_LQDTY_RTO IS NOT NULL THEN 1 ELSE 0 END = 0 THEN 99999.99
                ELSE
                     ROUND((NVL(T2.NXT_7_WRK_DAY_LQDTY_RTO, 0) +
                            NVL(T3.NXT_7_WRK_DAY_LQDTY_RTO, 0) +
                            NVL(T4.NXT_7_WRK_DAY_LQDTY_RTO, 0)) / (CASE WHEN T2.NXT_7_WRK_DAY_LQDTY_RTO IS NOT NULL THEN 1 ELSE 0 END +
                                                                   CASE WHEN T3.NXT_7_WRK_DAY_LQDTY_RTO IS NOT NULL THEN 1 ELSE 0 END +
                                                                   CASE WHEN T4.NXT_7_WRK_DAY_LQDTY_RTO IS NOT NULL THEN 1 ELSE 0 END
                                                                  ), 3)
                END AS L3M_NXT_7_WRK_DAY_LQDTY_RTO
     FROM   FROM_TB07_G32_005_TTGS_A_3M_SET T1 LEFT OUTER JOIN FROM_TB07_G32_005_TTGS_A T2
                                                  ON T2.BAS_DAY = T1.BAS_DAY1
                                                 AND T2.PCF_ID  = T1.PCF_ID
                                          LEFT OUTER JOIN FROM_TB07_G32_005_TTGS_A T3
                                                  ON T3.BAS_DAY = T1.BAS_DAY2
                                                 AND T3.PCF_ID  = T1.PCF_ID
                                          LEFT OUTER JOIN FROM_TB07_G32_005_TTGS_A T4
                                                  ON T4.BAS_DAY = T1.BAS_DAY3
                                                 AND T4.PCF_ID  = T1.PCF_ID
    ),
    FROM_BAL_SHEET AS
    (SELECT A.BAS_DAY,
            A.PCF_ID,
            SUM(CASE WHEN A.PCF_COA_ID = '601'
                          THEN A.CLO_CR_BAL
                     ELSE 0
                     END) AS CCAP_AMT,
            SUM(CASE WHEN A.PCF_COA_ID = '602'
                          THEN A.CLO_CR_BAL
                     ELSE 0
                     END) AS INVST_INFRA_FIX_AST_FUND_AMT,
            SUM(CASE WHEN A.PCF_COA_ID = '611'
                          THEN A.CLO_CR_BAL
                     ELSE 0
                     END) AS FOR_INCR_CCAP_RSRV_FUND_AMT,
            SUM(CASE WHEN A.PCF_COA_ID = '612'
                          THEN A.CLO_CR_BAL
                     ELSE 0
                     END) AS PRFS_DEV_INVST_FUND_AMT,
            SUM(CASE WHEN A.PCF_COA_ID = '609'
                          THEN A.CLO_CR_BAL
                     ELSE 0
                     END) AS CNTRBT_PCF_NON_RFNDBL_CAP_AMT,
            SUM(CASE WHEN A.PCF_COA_ID = '34401'
                          THEN A.CLO_DR_BAL
                     ELSE 0
                     END) AS CNTRBT_CBV_CAP_AMT,
            SUM(CASE WHEN A.PCF_COA_ID = '613'
                          THEN A.CLO_CR_BAL
                     ELSE 0
                     END) AS FIN_RSRV_FUND_AMT,
            SUM(CASE WHEN A.PCF_COA_ID = '642'
                          THEN A.CLO_DR_BAL
                     ELSE 0
                     END) AS DUTO_FXASTRV_100_PCT_DCR_AMT,
            SUM(CASE WHEN A.PCF_COA_ID = '7'
                          THEN A.CLO_CR_BAL
                     ELSE 0
                     END) AS TOTAL_INCOMES,
            SUM(CASE WHEN A.PCF_COA_ID = '8'
                          THEN A.CLO_DR_BAL
                     ELSE 0
                     END) AS TOTAL_EXPENSES,
            SUM(CASE WHEN A.PCF_COA_ID = '69'
                          THEN A.CLO_CR_BAL - A.CLO_DR_BAL
                     ELSE 0
                     END) AS INIT_RTAIN_ERN_AMT,
            ROUND(SUM(CASE WHEN A.PCF_COA_ID IN ('13119', '13129')
                                THEN A.CLO_DR_BAL
                           ELSE 0
                           END) * 0.2, 0) AS AT_CBNK_PAY_DPST_AMT,
            SUM(CASE WHEN A.PCF_COA_ID = '30'
                          THEN A.CLO_DR_BAL
                     ELSE 0
                     END) AS FIX_AST_AMT,
            SUM(CASE WHEN A.PCF_COA_ID = '2192'
                          THEN A.CLO_CR_BAL
                     ELSE 0
                     END) AS GENERAL_PROVISION,
            SUM(CASE WHEN A.PCF_COA_ID IN ('10','11','13','20','211','212','213','251','252','253','281','282','283','284','285','291','292','293','344','301',
                                           '302','303','31','32','351','352','353','3592','361','369','381','387','388','389','39','453')
                          THEN A.CLO_DR_BAL
                     ELSE 0
                     END) AS EE,
            SUM(CASE WHEN A.PCF_COA_ID IN ('139','209','219','259','289','299','349','305','3599','386','4892','4899')
                          THEN A.CLO_CR_BAL
                     ELSE 0
                     END) AS FF,
            SUM(CASE WHEN A.PCF_COA_ID = '5'
                          THEN A.CLO_DR_BAL - A.CLO_CR_BAL
                     ELSE 0
                     END) AS GG,
            SUM(CASE WHEN A.PCF_COA_ID IN ('10','11','13111','13121','25')
                          THEN A.CLO_DR_BAL
                     ELSE 0
                     END) AS HH,
            SUM(CASE WHEN A.PCF_COA_ID IN ('13119', '13129')
                          THEN A.CLO_DR_BAL
                     ELSE 0
                     END) AS II,
          --SUM(CASE WHEN A.PCF_COA_ID IN ('601', '61') THEN A.CLO_CR_BAL ELSE 0 END) - SUM(CASE WHEN A.PCF_COA_ID = '34401' THEN A.CLO_DR_BAL ELSE 0 END) AS CCAP_RSRV_FUND_SUPL_CCAP_AMT,
            SUM(CASE WHEN A.PCF_COA_ID IN ('601', '611') THEN A.CLO_CR_BAL ELSE 0 END) AS CCAP_RSRV_FUND_SUPL_CCAP_AMT,
            SUM(CASE WHEN A.PCF_COA_ID = '2' THEN A.CLO_DR_BAL ELSE 0 END) AS TOT_LN_BAL,
            SUM(CASE WHEN A.PCF_COA_ID IN ('20113', '20123', '21113', '21123', '21213', '21223', '21313', '21323', '25113', '25123', '25213', '25223', '25313', '25323',
                                           '20114', '20124', '21114', '21124', '21214', '21224', '21314', '21324', '25114', '25124', '25214', '25224', '25314', '25324',
                                           '20115', '20125', '21115', '21125', '21215', '21225', '21315', '21325', '25115', '25125', '25215', '25225', '25315', '25325')
                          THEN A.CLO_DR_BAL
                     ELSE 0
                     END) AS BAD_DBT_AMT,
            SUM(CASE WHEN A.PCF_COA_ID IN ('2192', '2592', '2892', '2992') THEN A.CLO_CR_BAL ELSE 0 END) AS GENL_PRVS_AMT,
            SUM(CASE WHEN A.PCF_COA_ID IN ('2191', '2591', '2891', '2991') THEN A.CLO_CR_BAL ELSE 0 END) AS SPEC_PRVS_AMT,
            SUM(CASE WHEN A.PCF_COA_ID = '42' THEN A.CLO_CR_BAL ELSE 0 END) AS TDP_BAL,
            SUM(CASE WHEN A.PCF_COA_ID IN ('41592', '41593') THEN A.CLO_CR_BAL ELSE 0 END) AS CBV_OVD_BOR_AMT,
            SUM(CASE WHEN A.PCF_COA_ID IN ('41512', '41513', '41592', '41593') THEN A.CLO_CR_BAL ELSE 0 END) AS CBV_BOR_EXCL_SFTY_FUND_AMT,
            SUM(CASE WHEN A.PCF_COA_ID IN ('41511', '41591') THEN A.CLO_CR_BAL ELSE 0 END) AS SFTY_FUND_BOR_AMT,
            SUM(CASE WHEN A.PCF_COA_ID IN ('41519', '41599') THEN A.CLO_CR_BAL ELSE 0 END) AS OTHR_INST_BOR_AMT,
            SUM(CASE WHEN A.PCF_COA_ID IN ('13111', '13121') THEN A.CLO_DR_BAL ELSE 0 END) AS CBV_SVG_AMT,
            SUM(CASE WHEN A.PCF_COA_ID IN ('13119', '13129') THEN A.CLO_DR_BAL ELSE 0 END) AS OTHR_INST_SVG_AMT,
            SUM(CASE WHEN A.PCF_COA_ID = '10' THEN A.CLO_DR_BAL ELSE 0 END) AS IN_HAND_CSH_AMT,
            SUM(CASE WHEN A.PCF_COA_ID IN ('7', '69') THEN A.CLO_CR_BAL ELSE 0 END) - SUM(CASE WHEN A.PCF_COA_ID IN ('8', '69') THEN A.CLO_DR_BAL ELSE 0 END) AS INCL_ALOSS_INCM_MNS_EXP_AMT,
            SUM(CASE WHEN A.PCF_COA_ID = '7' THEN A.CLO_CR_BAL ELSE 0 END) AS TOT_INCM_AMT,
            SUM(CASE WHEN A.PCF_COA_ID = '8' THEN A.CLO_DR_BAL ELSE 0 END) AS TOT_EXP_AM,
            SUM(CASE WHEN A.PCF_COA_ID = '211' THEN A.CLO_DR_BAL ELSE 0 END) AS BAL_SHET_BAS_SHRT_TRM_LN_BAL,
            SUM(CASE WHEN A.PCF_COA_ID IN ('212', '213', '25') THEN A.CLO_DR_BAL ELSE 0 END) AS BAL_SHET_BAS_MED_LT_LN_BAL,
            SUM(CASE WHEN A.PCF_COA_ID IN ('219', '259', '289', '299', '209') THEN A.CLO_CR_BAL ELSE 0 END) AS RISK_PRVS_FUND_AMT
     FROM   TM23_DDLY_BAL_SHET_A A
     WHERE  A.BAS_DAY = TO_CHAR(TO_DATE(v_st_date_01, 'YYYYMMDD') -2, 'YYYYMMDD')
     GROUP BY A.BAS_DAY, A.PCF_ID
    ),
    FROM_ASSET_EQUITY AS
    (SELECT A.BAS_DAY,
            A.PCF_ID,
            CASE WHEN A.GG > 0
                      THEN A.EE - A.FF + A.GG
                 ELSE A.EE - A.FF
                 END AS TOT_AST_AMT,
            ROUND(CASE WHEN A.GENERAL_PROVISION <= 0.0125 * (A.AT_CBNK_PAY_DPST_AMT + B.FULL_GTD_RL_EST_LN_BAL_50P + A.FIX_AST_AMT
                                                                                    + A.EE - A.FF + CASE WHEN A.GG > 0 THEN A.GG ELSE 0 END
                                                                                     - (A.HH + A.II + B.FULL_GTD_RL_EST_LN_BAL + B.FULL_GTD_VP_LN_BAL)
                                                            )
                            THEN A.GENERAL_PROVISION
                       ELSE 0.0125 * (A.AT_CBNK_PAY_DPST_AMT + B.FULL_GTD_RL_EST_LN_BAL_50P + A.FIX_AST_AMT
                                                             + A.EE - A.FF + CASE WHEN A.GG > 0 THEN A.GG ELSE 0 END
                                                             - (A.HH + A.II + B.FULL_GTD_RL_EST_LN_BAL + B.FULL_GTD_VP_LN_BAL)
                                     )
                       END, 0) AS PCT_125_VS_RWA_GENL_PRVS_AMT,
            CASE WHEN (A.TOTAL_INCOMES - A.TOTAL_EXPENSES + A.INIT_RTAIN_ERN_AMT >= 0 AND A.INIT_RTAIN_ERN_AMT <  0)
                       THEN 0
                 WHEN (A.TOTAL_INCOMES - A.TOTAL_EXPENSES + A.INIT_RTAIN_ERN_AMT >= 0 AND A.INIT_RTAIN_ERN_AMT >= 0)
                       THEN A.INIT_RTAIN_ERN_AMT
                 WHEN (A.TOTAL_INCOMES - A.TOTAL_EXPENSES + A.INIT_RTAIN_ERN_AMT <  0)
                       THEN -(A.TOTAL_INCOMES - A.TOTAL_EXPENSES + A.INIT_RTAIN_ERN_AMT)
                 END AS ZZZ
     FROM   FROM_BAL_SHEET A LEFT OUTER JOIN FROM_DDLY_CUST_CR_TRANS B
                                          ON B.BAS_DAY = A.BAS_DAY
                                         AND B.PCF_ID  = A.PCF_ID
    )
    SELECT A.BAS_DAY
          ,A.PCF_ID
          ,E.CCAP_AMT + E.INVST_INFRA_FIX_AST_FUND_AMT + E.CNTRBT_PCF_NON_RFNDBL_CAP_AMT + E.FOR_INCR_CCAP_RSRV_FUND_AMT
                      + E.PRFS_DEV_INVST_FUND_AMT + E.FIN_RSRV_FUND_AMT
                      - E.CNTRBT_CBV_CAP_AMT - E.DUTO_FXASTRV_100_PCT_DCR_AMT
                      + F.PCT_125_VS_RWA_GENL_PRVS_AMT
                      + F.ZZZ AS EQT_AMT
          ,E.CCAP_AMT + E.INVST_INFRA_FIX_AST_FUND_AMT + E.CNTRBT_PCF_NON_RFNDBL_CAP_AMT + E.FOR_INCR_CCAP_RSRV_FUND_AMT
                      + E.PRFS_DEV_INVST_FUND_AMT + E.FIN_RSRV_FUND_AMT
                      - E.CNTRBT_CBV_CAP_AMT - E.DUTO_FXASTRV_100_PCT_DCR_AMT
                      + F.PCT_125_VS_RWA_GENL_PRVS_AMT
                      + E.INIT_RTAIN_ERN_AMT AS CNSLDT_EQT_AMT
          ,E.CCAP_AMT
          ,E.CCAP_RSRV_FUND_SUPL_CCAP_AMT
          ,F.TOT_AST_AMT
          ,E.TOT_LN_BAL
          ,B.SHRT_TRM_LN_BAL
          ,B.MED_LT_LN_BAL
          ,B.WO_COLL_LN_BAL
          ,B.WIT_COLL_LN_BAL
          ,G.TOT_LN_CNTR_NUM_CNT
          ,G.TOT_BRWR_NUM_CNT
          ,G.NEW_ARISN_LN_AMT
          ,G.NEW_ARISN_LN_CNTR_NUM_CNT
          ,G.NEW_BRWR_NUM_CNT
          ,E.BAD_DBT_AMT
          ,E.GENL_PRVS_AMT
          ,E.SPEC_PRVS_AMT
          ,E.TDP_BAL
          ,C.MBR_DPST_BAL
          ,C.NON_MBR_DPST_BAL
          ,C.SHRT_TRM_DPST_BAL
          ,C.MED_LT_DPST_BAL
          ,C.TOT_DPSTR_NUM_CNT
          ,C.MBR_DPSTR_NUM_CNT
          ,C.NON_MBR_DPSTR_NUM_CNT
          ,ROUND(CASE WHEN (C.SHRT_TRM_DPST_BAL IS NULL AND C.MED_LT_DPST_BAL IS NULL) OR C.SHRT_TRM_DPST_BAL + C.MED_LT_DPST_BAL = 0
                           THEN 99999.99
                      WHEN C.MBR_DPST_BAL = 0 THEN 0
                      ELSE C.MBR_DPST_BAL / (C.SHRT_TRM_DPST_BAL + C.MED_LT_DPST_BAL) * 100
                      END, 2) AS MBR_DPST_VS_TDP_RTO
          ,E.CBV_BOR_EXCL_SFTY_FUND_AMT
          ,E.CBV_OVD_BOR_AMT
          ,E.SFTY_FUND_BOR_AMT
          ,E.OTHR_INST_BOR_AMT
          ,E.CBV_SVG_AMT
          ,E.OTHR_INST_SVG_AMT
          ,E.IN_HAND_CSH_AMT
          ,E.TOTAL_INCOMES
          ,E.TOTAL_EXPENSES
          ,E.TOTAL_INCOMES - E.TOTAL_EXPENSES AS NET_PNL_AMT
          ,E.INCL_ALOSS_INCM_MNS_EXP_AMT
          ,D.NXT_WRK_DAY_AST_AMT
          ,D.NXT_WRK_DAY_PAY_LBLTY_AMT
          ,CASE WHEN D.NXT_WRK_DAY_LQDTY_RTO > 99999.999 THEN 99999.99 ELSE D.NXT_WRK_DAY_LQDTY_RTO END
          ,D.NXT_7_WRK_DAY_AST_AMT
          ,D.NXT_7_WRK_DAY_PAY_LBLTY_AMT
          ,CASE WHEN D.NXT_7_WRK_DAY_LQDTY_RTO > 99999.999 THEN 99999.99 ELSE D.NXT_7_WRK_DAY_LQDTY_RTO END
          ,H.L3M_NXT_WRK_DAY_LQDTY_RTO
          ,H.L3M_NXT_7_WRK_DAY_LQDTY_RTO
          ,E.BAL_SHET_BAS_SHRT_TRM_LN_BAL
          ,E.BAL_SHET_BAS_MED_LT_LN_BAL
          ,E.RISK_PRVS_FUND_AMT
          ,SYSTIMESTAMP
    FROM   FULL_SET A LEFT OUTER JOIN FROM_DDLY_CUST_CR_TRANS B
                                   ON A.BAS_DAY = B.BAS_DAY
                                  AND A.PCF_ID  = B.PCF_ID
                      LEFT OUTER JOIN FROM_DDLY_CUST_DPST_TRANS C
                                   ON A.BAS_DAY = C.BAS_DAY
                                  AND A.PCF_ID  = C.PCF_ID
                      LEFT OUTER JOIN FROM_TB07_G32_005_TTGS_A D
                                   ON A.BAS_DAY = D.BAS_DAY
                                  AND A.PCF_ID  = D.PCF_ID
                      LEFT OUTER JOIN FROM_BAL_SHEET E
                                   ON A.BAS_DAY = E.BAS_DAY
                                  AND A.PCF_ID  = E.PCF_ID
                      LEFT OUTER JOIN FROM_ASSET_EQUITY F
                                   ON A.BAS_DAY = F.BAS_DAY
                                  AND A.PCF_ID  = F.PCF_ID
                      LEFT OUTER JOIN TM21_DDLY_CUST_CR_TRANS_W G
                                   ON A.BAS_DAY = G.BAS_DAY
                                  AND A.PCF_ID  = G.PCF_ID
                      LEFT OUTER JOIN FROM_TB07_G32_005_TTGS_A_3M H
                                   ON A.BAS_DAY = H.BAS_DAY
                                  AND A.PCF_ID  = H.PCF_ID;

    ----------------------------------------------------------------------------
    --  Transaction Log
    ----------------------------------------------------------------------------
    v_step_code    := '020' ;
    v_step_desc    := v_wk_date || v_seq || ' : Replicate data from three days ago to two days ago ' ;
    v_time         := SYSDATE ;

    P1_BSM_PROG_EXEC_LOG(v_program_id, v_program_type_name, v_step_code, v_step_desc, v_time, sql%rowcount, NULL, NULL) ;

    COMMIT;

    ----------------------------------------------------------------------------
    --  1.3 Delete Historical Data
    ----------------------------------------------------------------------------

    FOR loop_bas_day in v_sbmt_day(v_wk_date,v_st_date_01,v_end_date_01)
    LOOP
    BEGIN

          DELETE
            FROM TM27_DDLY_PCF_KEY_IDC_A T1
           WHERE T1.BAS_DAY = loop_bas_day.BAS_DAY
           AND   EXISTS (SELECT *
                         FROM   (SELECT PCF_ID
                                 FROM   TBSM_INPT_RPT_SUBMIT_L
                                 WHERE  BTCH_BAS_DAY     = v_st_date_01
                                 AND    INPT_RPT_ID      IN ('G32_012_TTGS', 'G32_003_TTGS', 'G32_020_TTGS_01', 'G32_005_TTGS')
                                 AND    DATA_BAS_DAY    <= loop_bas_day.BAS_DAY
                                 GROUP BY PCF_ID
                                ) T2
                         WHERE  T2.PCF_ID     = T1.PCF_ID
                        );

          ----------------------------------------------------------------------------
          --  Transaction Log
          ----------------------------------------------------------------------------
          v_step_code    := '030' ;
          v_step_desc    := v_wk_date || v_seq || ' ' || loop_bas_day.BAS_DAY ||' : Deleting Data Result';
          v_time         := SYSDATE ;

          P1_BSM_PROG_EXEC_LOG(v_program_id, v_program_type_name, v_step_code, v_step_desc, v_time, sql%rowcount, NULL, NULL) ;

          ----------------------------------------------------------------------------

          v_cnt := v_cnt+sql%rowcount;

          EXCEPTION
          WHEN OTHERS THEN
               begin
                     v_step_code    := '031' ;
                     v_step_desc    := v_wk_date || v_seq || ' : Deleting Data Error with BAS_DAY = ' ;
                     v_time         := SYSDATE ;
                     P1_BSM_PROG_EXEC_LOG(v_program_id, v_program_type_name, v_step_code, v_step_desc||loop_bas_day.BAS_DAY, v_time, v_cnt, TO_CHAR(SQLCODE), SQLERRM) ;
                     CONTINUE;
               end;
    End;
    END LOOP;

    ----------------------------------------------------------------------------
    --  1.4 Inserting Data verification results
    ----------------------------------------------------------------------------
    FOR loop_bas_day in v_sbmt_day(v_wk_date,v_st_date_01,v_end_date_01)
    LOOP
    BEGIN

          INSERT INTO TM27_DDLY_PCF_KEY_IDC_A
          (   BAS_DAY
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
             ,TRGT_DATA_LST_MOD_TM
          )
          WITH
          FULL_SET AS
          (SELECT  T1.BAS_DAY, T1.PCF_ID
           FROM   (SELECT A.BAS_DAY, A.PCF_ID
                   FROM   TM22_DDLY_CUST_DPST_TRANS_A A INNER JOIN (SELECT PCF_ID
                                                                    FROM   TBSM_INPT_RPT_SUBMIT_L
                                                                    WHERE  BTCH_BAS_DAY     = v_st_date_01
                                                                    AND    INPT_RPT_ID      IN ('G32_012_TTGS', 'G32_003_TTGS', 'G32_020_TTGS_01', 'G32_005_TTGS')
                                                                    AND    DATA_BAS_DAY    <= loop_bas_day.BAS_DAY
                                                                    GROUP BY PCF_ID
                                                                   ) B
                                                                ON B.PCF_ID = A.PCF_ID
                   WHERE  A.BAS_DAY = loop_bas_day.BAS_DAY
                   GROUP BY A.BAS_DAY, A.PCF_ID

                   UNION ALL

                   SELECT A.BAS_DAY, A.PCF_ID
                   FROM   TM21_DDLY_CUST_CR_TRANS_A A INNER JOIN (SELECT PCF_ID
                                                                  FROM   TBSM_INPT_RPT_SUBMIT_L
                                                                  WHERE  BTCH_BAS_DAY     = v_st_date_01
                                                                  AND    INPT_RPT_ID      IN ('G32_012_TTGS', 'G32_003_TTGS', 'G32_020_TTGS_01', 'G32_005_TTGS')
                                                                  AND    DATA_BAS_DAY    <= loop_bas_day.BAS_DAY
                                                                  GROUP BY PCF_ID
                                                                 ) B
                                                              ON B.PCF_ID = A.PCF_ID
                   WHERE  A.BAS_DAY = loop_bas_day.BAS_DAY
                   GROUP BY A.BAS_DAY, A.PCF_ID

                   UNION ALL

                   SELECT A.BAS_DAY, A.PCF_ID
                   FROM   TM23_DDLY_BAL_SHET_A A INNER JOIN (SELECT PCF_ID
                                                             FROM   TBSM_INPT_RPT_SUBMIT_L
                                                             WHERE  BTCH_BAS_DAY     = v_st_date_01
                                                             AND    INPT_RPT_ID      IN ('G32_012_TTGS', 'G32_003_TTGS', 'G32_020_TTGS_01', 'G32_005_TTGS')
                                                             AND    DATA_BAS_DAY    <= loop_bas_day.BAS_DAY
                                                             GROUP BY PCF_ID
                                                            ) B
                                                         ON B.PCF_ID = A.PCF_ID
                   WHERE  A.BAS_DAY = loop_bas_day.BAS_DAY
                   GROUP BY A.BAS_DAY, A.PCF_ID

                   UNION ALL

                   SELECT A.BAS_YMD, A.PCF_ID
                   FROM   TB07_G32_005_TTGS_A A INNER JOIN (SELECT PCF_ID
                                                       FROM   TBSM_INPT_RPT_SUBMIT_L
                                                       WHERE  BTCH_BAS_DAY     = v_st_date_01
                                                       AND    INPT_RPT_ID      IN ('G32_012_TTGS', 'G32_003_TTGS', 'G32_020_TTGS_01', 'G32_005_TTGS')
                                                       AND    DATA_BAS_DAY    <= loop_bas_day.BAS_DAY
                                                       GROUP BY PCF_ID
                                                      ) B
                                                   ON B.PCF_ID = A.PCF_ID
                   WHERE  A.BAS_YMD = loop_bas_day.BAS_DAY
                   GROUP BY A.BAS_YMD, A.PCF_ID

                  ) T1
          GROUP BY T1.BAS_DAY, T1.PCF_ID
          ),
           TM21_DDLY_CUST_CR_TRANS_V AS
          (SELECT  A.BAS_DAY
                  ,A.PCF_ID
                  ,A.RPLC_DATA_YN
                  ,A.LN_CNTR_NUM_INFO
                  ,MAX(A.CUST_ID             ) AS CUST_ID
                  ,MAX(A.CUST_NM             ) AS CUST_NM
                  ,MAX(A.CUST_TYP_CD         ) AS CUST_TYP_CD
                  ,MAX(A.INIT_LN_TRM_CD      ) AS INIT_LN_TRM_CD
                  ,MAX(A.RMTRT_LN_TRM_CD     ) AS RMTRT_LN_TRM_CD
                  ,MAX(A.ES_CD               ) AS ES_CD
                  ,MAX(A.COLL_TYP_INFO       ) AS COLL_TYP_INFO
                  ,MAX(A.COLL_TYP_CD         ) AS COLL_TYP_CD
                  ,MAX(A.LN_TRANS_TYP_CD     ) AS LN_TRANS_TYP_CD
                  ,MAX(A.OLD_NORML_DBT_GRP_CD) AS OLD_NORML_DBT_GRP_CD
                  ,MAX(A.OLD_OVD_DBT_GRP_CD  ) AS OLD_OVD_DBT_GRP_CD
                  ,MAX(A.NEW_NORML_DBT_GRP_CD) AS NEW_NORML_DBT_GRP_CD
                  ,MAX(A.NEW_OVD_DBT_GRP_CD  ) AS NEW_OVD_DBT_GRP_CD
                  ,MAX(A.LN_CNTR_AMT         ) AS LN_CNTR_AMT
                  ,MAX(A.LN_BAL              ) AS LN_BAL
                  ,SUM(A.TRANS_AMT           ) AS TRANS_AMT
                  ,MAX(A.COLL_VAL_AMT        ) AS COLL_VAL_AMT
                  ,MAX(A.SPEC_PRVS_AMT       ) AS SPEC_PRVS_AMT
                  ,MAX(A.OPN_DAY             ) AS OPN_DAY
                  ,MAX(A.MTRT_DAY            ) AS MTRT_DAY
                  ,MAX(A.ACTL_IR             ) AS ACTL_IR
           FROM   TM21_DDLY_CUST_CR_TRANS_A A INNER JOIN (SELECT PCF_ID
                                                          FROM   TBSM_INPT_RPT_SUBMIT_L
                                                          WHERE  BTCH_BAS_DAY     = v_st_date_01
                                                          AND    INPT_RPT_ID      IN ('G32_012_TTGS', 'G32_003_TTGS', 'G32_020_TTGS_01', 'G32_005_TTGS')
                                                          AND    DATA_BAS_DAY    <= loop_bas_day.BAS_DAY
                                                          GROUP BY PCF_ID
                                                         ) B
                                                      ON B.PCF_ID = A.PCF_ID
           WHERE  A.BAS_DAY = loop_bas_day.BAS_DAY
           GROUP BY A.BAS_DAY
                   ,A.PCF_ID
                   ,A.RPLC_DATA_YN
                   ,A.LN_CNTR_NUM_INFO
          ),
          TM21_DDLY_CUST_CR_TRANS_W AS
          (SELECT A.BAS_DAY,
                  A.PCF_ID,
                  SUM(CASE WHEN A.RPLC_DATA_YN = 'N' AND A.LN_TRANS_TYP_CD = '1'
                                THEN A.TRANS_AMT
                           ELSE 0
                           END
                     ) AS NEW_ARISN_LN_AMT,
                  COUNT(DISTINCT CASE WHEN A.RPLC_DATA_YN = 'N' AND A.LN_TRANS_TYP_CD = '1' AND A.TRANS_AMT > 0
                                           THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END
                       ) AS NEW_ARISN_LN_CNTR_NUM_CNT,
                  COUNT(DISTINCT CASE WHEN A.RPLC_DATA_YN = 'N' AND A.LN_TRANS_TYP_CD = '1' AND A.TRANS_AMT > 0
                                           THEN A.CUST_ID
                                      ELSE NULL
                                      END
                       ) AS NEW_BRWR_NUM_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS TOT_LN_CNTR_NUM_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 THEN
                                                           CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO
                                                                ELSE A.CUST_ID
                                                                END
                                      ELSE NULL
                                      END) AS TOT_BRWR_NUM_CNT
           FROM   TM21_DDLY_CUST_CR_TRANS_A A INNER JOIN (SELECT PCF_ID
                                                          FROM   TBSM_INPT_RPT_SUBMIT_L
                                                          WHERE  BTCH_BAS_DAY     = v_st_date_01
                                                          AND    INPT_RPT_ID      IN ('G32_012_TTGS', 'G32_003_TTGS', 'G32_020_TTGS_01', 'G32_005_TTGS')
                                                          AND    DATA_BAS_DAY    <= loop_bas_day.BAS_DAY
                                                          GROUP BY PCF_ID
                                                         ) B
                                                      ON B.PCF_ID = A.PCF_ID
           WHERE  A.BAS_DAY = loop_bas_day.BAS_DAY
           AND    A.LN_BAL > 0
           GROUP BY A.BAS_DAY,
                    A.PCF_ID
          ),
          FROM_DDLY_CUST_CR_TRANS AS
          (SELECT BAS_DAY,
                  PCF_ID,
                  SUM(CASE WHEN RMTRT_LN_TRM_CD BETWEEN '001' AND '012' THEN LN_BAL
                           WHEN RMTRT_LN_TRM_CD = 'ZZZ' THEN LN_BAL
                           ELSE 0
                           END) AS SHRT_TRM_LN_BAL,
                  SUM(CASE WHEN RMTRT_LN_TRM_CD BETWEEN '013' AND '999' THEN LN_BAL
                           WHEN RMTRT_LN_TRM_CD = 'XXX' THEN LN_BAL
                           ELSE 0
                           END) AS MED_LT_LN_BAL,
                  SUM(CASE WHEN COLL_TYP_CD =  '00' THEN LN_BAL ELSE 0 END) AS WO_COLL_LN_BAL,
                  SUM(CASE WHEN COLL_TYP_CD <> '00' THEN LN_BAL ELSE 0 END) AS WIT_COLL_LN_BAL,
                  ROUND((SUM(CASE WHEN COLL_TYP_CD = '01' THEN LN_BAL ELSE 0 END) * 0.5), 0) AS FULL_GTD_RL_EST_LN_BAL_50P,
                  ROUND((SUM(CASE WHEN COLL_TYP_CD = '02' THEN LN_BAL ELSE 0 END) * 0.2), 0) AS FULL_GTD_VP_LN_BAL_20P,
                  SUM(CASE WHEN COLL_TYP_CD = '01' THEN LN_BAL ELSE 0 END) AS FULL_GTD_RL_EST_LN_BAL,
                  SUM(CASE WHEN COLL_TYP_CD = '02' THEN LN_BAL ELSE 0 END) AS FULL_GTD_VP_LN_BAL
           FROM   TM21_DDLY_CUST_CR_TRANS_V
           WHERE  LN_BAL > 0
           GROUP BY BAS_DAY, PCF_ID
          ),
          FROM_DDLY_CUST_DPST_TRANS AS
          (SELECT A.BAS_DAY,
                  A.PCF_ID,
                  SUM(CASE WHEN A.INIT_DPST_TRM_CD <> 'XXX' AND (A.INIT_DPST_TRM_CD <= '012' OR A.INIT_DPST_TRM_CD = 'ZZZ')
                                THEN A.DPST_BAL
                           ELSE 0
                           END
                     ) AS SHRT_TRM_DPST_BAL,
                  SUM(CASE WHEN A.INIT_DPST_TRM_CD <> 'ZZZ' AND (A.INIT_DPST_TRM_CD > '012' OR A.INIT_DPST_TRM_CD = 'XXX')
                                THEN A.DPST_BAL
                           ELSE 0
                           END) AS MED_LT_DPST_BAL,
                  COUNT(DISTINCT A.CUST_ID) AS TOT_DPSTR_NUM_CNT,
                  SUM(CASE WHEN SUBSTR(A.CUST_TYP_CD,1,1) = '1' THEN A.DPST_BAL ELSE 0 END) AS MBR_DPST_BAL,
                  SUM(CASE WHEN SUBSTR(A.CUST_TYP_CD,1,1) = '2' THEN A.DPST_BAL ELSE 0 END) AS NON_MBR_DPST_BAL,
                  COUNT(DISTINCT CASE WHEN SUBSTR(A.CUST_TYP_CD,1,1) = '1' THEN A.CUST_ID ELSE NULL END) AS MBR_DPSTR_NUM_CNT,
                  COUNT(DISTINCT CASE WHEN SUBSTR(A.CUST_TYP_CD,1,1) = '2' THEN A.CUST_ID ELSE NULL END) AS NON_MBR_DPSTR_NUM_CNT
           FROM   TM22_DDLY_CUST_DPST_TRANS_A A INNER JOIN (SELECT PCF_ID
                                                            FROM   TBSM_INPT_RPT_SUBMIT_L
                                                            WHERE  BTCH_BAS_DAY     = v_st_date_01
                                                            AND    INPT_RPT_ID      IN ('G32_012_TTGS', 'G32_003_TTGS', 'G32_020_TTGS_01')
                                                            AND    DATA_BAS_DAY    <= loop_bas_day.BAS_DAY
                                                            GROUP BY PCF_ID
                                                           ) B
                                                        ON B.PCF_ID = A.PCF_ID
           WHERE  A.BAS_DAY = loop_bas_day.BAS_DAY
           AND    A.DPST_BAL > 0
           GROUP BY A.BAS_DAY, A.PCF_ID
          ),
          FROM_TB07_G32_005_TTGS_A AS
          (SELECT A.BAS_YMD AS BAS_DAY,
                  A.PCF_ID,
                  SUM(CASE WHEN INDC_CODE = 'G32005TTGS_018' THEN A.CALC_VALUE_NEXT_DAY ELSE 0 END) AS NXT_WRK_DAY_AST_AMT,
                  SUM(CASE WHEN INDC_CODE = 'G32005TTGS_001' THEN A.CALC_VALUE_NEXT_DAY ELSE 0 END) AS NXT_WRK_DAY_PAY_LBLTY_AMT,
                  CASE WHEN SUM(CASE WHEN INDC_CODE = 'G32005TTGS_001' THEN A.CALC_VALUE_NEXT_DAY ELSE 0 END) = 0 THEN 0
                       WHEN SUM(CASE WHEN INDC_CODE = 'G32005TTGS_018' THEN A.CALC_VALUE_NEXT_DAY ELSE 0 END)  = 0 THEN 99999.99
                       ELSE ROUND(SUM(CASE WHEN INDC_CODE = 'G32005TTGS_001' THEN CALC_VALUE_NEXT_DAY ELSE 0 END) / SUM(CASE WHEN INDC_CODE = 'G32005TTGS_018' THEN A.CALC_VALUE_NEXT_DAY ELSE 0 END), 3)
                       END AS NXT_WRK_DAY_LQDTY_RTO,
                  SUM(CASE WHEN INDC_CODE = 'G32005TTGS_018' THEN A.CALC_VALUE_NEXT_DAY ELSE 0 END) +  
                  SUM(CASE WHEN INDC_CODE = 'G32005TTGS_018' THEN A.CALC_VALUE_2TO7_DAY ELSE 0 END) AS NXT_7_WRK_DAY_AST_AMT,
                  SUM(CASE WHEN INDC_CODE = 'G32005TTGS_001' THEN A.CALC_VALUE_NEXT_DAY ELSE 0 END) +
                  SUM(CASE WHEN INDC_CODE = 'G32005TTGS_001' THEN A.CALC_VALUE_2TO7_DAY ELSE 0 END) AS NXT_7_WRK_DAY_PAY_LBLTY_AMT,
                  CASE WHEN SUM(CASE WHEN INDC_CODE = 'G32005TTGS_001' THEN A.CALC_VALUE_NEXT_DAY + A.CALC_VALUE_2TO7_DAY ELSE 0 END) = 0 THEN 0
                       WHEN SUM(CASE WHEN INDC_CODE = 'G32005TTGS_018' THEN A.BOOK_VALUE_NEXT_DAY + A.BOOK_VALUE_2TO7_DAY ELSE 0 END) = 0 THEN 99999.99
                       ELSE ROUND(SUM(CASE WHEN INDC_CODE = 'G32005TTGS_001' THEN A.CALC_VALUE_NEXT_DAY + A.CALC_VALUE_2TO7_DAY ELSE 0 END) / SUM(CASE WHEN INDC_CODE = 'G32005TTGS_018' THEN A.BOOK_VALUE_NEXT_DAY + A.BOOK_VALUE_2TO7_DAY ELSE 0 END), 3)
                       END AS NXT_7_WRK_DAY_LQDTY_RTO
           FROM   TB07_G32_005_TTGS_A A INNER JOIN (SELECT PCF_ID
                                               FROM   TBSM_INPT_RPT_SUBMIT_L
                                               WHERE  BTCH_BAS_DAY     = v_st_date_01
                                               AND    INPT_RPT_ID      IN ('G32_012_TTGS', 'G32_003_TTGS', 'G32_020_TTGS_01', 'G32_005_TTGS')
                                               AND    DATA_BAS_DAY    <= loop_bas_day.BAS_DAY
                                               GROUP BY PCF_ID
                                              ) B
                                           ON B.PCF_ID = A.PCF_ID
           WHERE  A.BAS_YMD <= loop_bas_day.BAS_DAY
          ),
          FROM_TB07_G32_005_TTGS_A_3M_SET AS
          (SELECT T1.PCF_ID,
                  T1.BAS_DAY AS BAS_DAY1,
                  MAX(T2.BAS_DAY) AS BAS_DAY2,
                  MAX(T3.BAS_DAY) AS BAS_DAY3
           FROM   FROM_TB07_G32_005_TTGS_A T1 LEFT OUTER JOIN FROM_TB07_G32_005_TTGS_A T2
                                                      ON T2.BAS_DAY <= TO_CHAR(ADD_MONTHS(TO_DATE(T1.BAS_DAY, 'YYYYMMDD'), -1), 'YYYYMMDD')
                                                     AND T2.PCF_ID = T1.PCF_ID
                                                     AND T2.NXT_WRK_DAY_LQDTY_RTO IS NOT NULL
                                         LEFT OUTER JOIN FROM_TB07_G32_005_TTGS_A T3
                                                      ON T3.BAS_DAY <= TO_CHAR(ADD_MONTHS(TO_DATE(T1.BAS_DAY, 'YYYYMMDD'), -2), 'YYYYMMDD')
                                                     AND T3.PCF_ID = T1.PCF_ID
                                                     AND T3.NXT_WRK_DAY_LQDTY_RTO IS NOT NULL
           WHERE  T1.BAS_DAY = loop_bas_day.BAS_DAY
           GROUP BY T1.PCF_ID, T1.BAS_DAY
          ),
          FROM_TB07_G32_005_TTGS_A_3M AS
          (SELECT T1.PCF_ID,
                  T1.BAS_DAY1 AS BAS_DAY,
                  CASE WHEN NVL(T2.NXT_WRK_DAY_LQDTY_RTO, 0) + NVL(T3.NXT_WRK_DAY_LQDTY_RTO, 0) + NVL(T4.NXT_WRK_DAY_LQDTY_RTO, 0) = 0 THEN 0
                       WHEN CASE WHEN T2.NXT_WRK_DAY_LQDTY_RTO IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN T3.NXT_WRK_DAY_LQDTY_RTO IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN T4.NXT_WRK_DAY_LQDTY_RTO IS NOT NULL THEN 1 ELSE 0 END = 0 THEN 99999.99
                       ELSE
                            ROUND((NVL(T2.NXT_WRK_DAY_LQDTY_RTO, 0) +
                                   NVL(T3.NXT_WRK_DAY_LQDTY_RTO, 0) +
                                   NVL(T4.NXT_WRK_DAY_LQDTY_RTO, 0)) / (CASE WHEN T2.NXT_WRK_DAY_LQDTY_RTO IS NOT NULL THEN 1 ELSE 0 END +
                                                                        CASE WHEN T3.NXT_WRK_DAY_LQDTY_RTO IS NOT NULL THEN 1 ELSE 0 END +
                                                                        CASE WHEN T4.NXT_WRK_DAY_LQDTY_RTO IS NOT NULL THEN 1 ELSE 0 END
                                                                       ), 3)
                       END AS L3M_NXT_WRK_DAY_LQDTY_RTO,
                 CASE WHEN NVL(T2.NXT_7_WRK_DAY_LQDTY_RTO, 0) + NVL(T3.NXT_7_WRK_DAY_LQDTY_RTO, 0) + NVL(T4.NXT_7_WRK_DAY_LQDTY_RTO, 0) = 0 THEN 0
                      WHEN CASE WHEN T2.NXT_7_WRK_DAY_LQDTY_RTO IS NOT NULL THEN 1 ELSE 0 END +
                           CASE WHEN T3.NXT_7_WRK_DAY_LQDTY_RTO IS NOT NULL THEN 1 ELSE 0 END +
                           CASE WHEN T4.NXT_7_WRK_DAY_LQDTY_RTO IS NOT NULL THEN 1 ELSE 0 END = 0 THEN 99999.99
                      ELSE
                           ROUND((NVL(T2.NXT_7_WRK_DAY_LQDTY_RTO, 0) +
                                  NVL(T3.NXT_7_WRK_DAY_LQDTY_RTO, 0) +
                                  NVL(T4.NXT_7_WRK_DAY_LQDTY_RTO, 0)) / (CASE WHEN T2.NXT_7_WRK_DAY_LQDTY_RTO IS NOT NULL THEN 1 ELSE 0 END +
                                                                         CASE WHEN T3.NXT_7_WRK_DAY_LQDTY_RTO IS NOT NULL THEN 1 ELSE 0 END +
                                                                         CASE WHEN T4.NXT_7_WRK_DAY_LQDTY_RTO IS NOT NULL THEN 1 ELSE 0 END
                                                                        ), 3)
                      END AS L3M_NXT_7_WRK_DAY_LQDTY_RTO
           FROM   FROM_TB07_G32_005_TTGS_A_3M_SET T1 LEFT OUTER JOIN FROM_TB07_G32_005_TTGS_A T2
                                                        ON T2.BAS_DAY = T1.BAS_DAY1
                                                       AND T2.PCF_ID  = T1.PCF_ID
                                                LEFT OUTER JOIN FROM_TB07_G32_005_TTGS_A T3
                                                        ON T3.BAS_DAY = T1.BAS_DAY2
                                                       AND T3.PCF_ID  = T1.PCF_ID
                                                LEFT OUTER JOIN FROM_TB07_G32_005_TTGS_A T4
                                                        ON T4.BAS_DAY = T1.BAS_DAY3
                                                       AND T4.PCF_ID  = T1.PCF_ID
          ),
          FROM_BAL_SHEET AS
          (SELECT A.BAS_DAY,
                  A.PCF_ID,
                  SUM(CASE WHEN A.PCF_COA_ID = '601'
                                THEN A.CLO_CR_BAL
                           ELSE 0
                           END) AS CCAP_AMT,
                  SUM(CASE WHEN A.PCF_COA_ID = '602'
                                THEN A.CLO_CR_BAL
                           ELSE 0
                           END) AS INVST_INFRA_FIX_AST_FUND_AMT,
                  SUM(CASE WHEN A.PCF_COA_ID = '611'
                                THEN A.CLO_CR_BAL
                           ELSE 0
                           END) AS FOR_INCR_CCAP_RSRV_FUND_AMT,
                  SUM(CASE WHEN A.PCF_COA_ID = '612'
                                THEN A.CLO_CR_BAL
                           ELSE 0
                           END) AS PRFS_DEV_INVST_FUND_AMT,
                  SUM(CASE WHEN A.PCF_COA_ID = '609'
                                THEN A.CLO_CR_BAL
                           ELSE 0
                           END) AS CNTRBT_PCF_NON_RFNDBL_CAP_AMT,
                  SUM(CASE WHEN A.PCF_COA_ID = '34401'
                                THEN A.CLO_DR_BAL
                           ELSE 0
                           END) AS CNTRBT_CBV_CAP_AMT,
                  SUM(CASE WHEN A.PCF_COA_ID = '613'
                                THEN A.CLO_CR_BAL
                           ELSE 0
                           END) AS FIN_RSRV_FUND_AMT,
                  SUM(CASE WHEN A.PCF_COA_ID = '642'
                                THEN A.CLO_DR_BAL
                           ELSE 0
                           END) AS DUTO_FXASTRV_100_PCT_DCR_AMT,
                  SUM(CASE WHEN A.PCF_COA_ID = '7'
                                THEN A.CLO_CR_BAL
                           ELSE 0
                           END) AS TOTAL_INCOMES,
                  SUM(CASE WHEN A.PCF_COA_ID = '8'
                                THEN A.CLO_DR_BAL
                           ELSE 0
                           END) AS TOTAL_EXPENSES,
                  SUM(CASE WHEN A.PCF_COA_ID = '69'
                                THEN A.CLO_CR_BAL - A.CLO_DR_BAL
                           ELSE 0
                           END) AS INIT_RTAIN_ERN_AMT,
                  ROUND(SUM(CASE WHEN A.PCF_COA_ID IN ('13119', '13129')
                                      THEN A.CLO_DR_BAL
                                 ELSE 0
                                 END) * 0.2, 0) AS AT_CBNK_PAY_DPST_AMT,
                  SUM(CASE WHEN A.PCF_COA_ID = '30'
                                THEN A.CLO_DR_BAL
                           ELSE 0
                           END) AS FIX_AST_AMT,
                  SUM(CASE WHEN A.PCF_COA_ID = '2192'
                                THEN A.CLO_CR_BAL
                           ELSE 0
                           END) AS GENERAL_PROVISION,
                  SUM(CASE WHEN A.PCF_COA_ID IN ('10','11','13','20','211','212','213','251','252','253','281','282','283','284','285','291','292','293','344','301',
                                                 '302','303','31','32','351','352','353','3592','361','369','381','387','388','389','39','453')
                                THEN A.CLO_DR_BAL
                           ELSE 0
                           END) AS EE,
                  SUM(CASE WHEN A.PCF_COA_ID IN ('139','209','219','259','289','299','349','305','3599','386','4892','4899')
                                THEN A.CLO_CR_BAL
                           ELSE 0
                           END) AS FF,
                  SUM(CASE WHEN A.PCF_COA_ID = '5'
                                THEN A.CLO_DR_BAL - A.CLO_CR_BAL
                           ELSE 0
                           END) AS GG,
                  SUM(CASE WHEN A.PCF_COA_ID IN ('10','11','13111','13121','25')
                                THEN A.CLO_DR_BAL
                           ELSE 0
                           END) AS HH,
                  SUM(CASE WHEN A.PCF_COA_ID IN ('13119', '13129')
                                THEN A.CLO_DR_BAL
                           ELSE 0
                           END) AS II,
                --SUM(CASE WHEN A.PCF_COA_ID IN ('601', '61') THEN A.CLO_CR_BAL ELSE 0 END) - SUM(CASE WHEN A.PCF_COA_ID = '34401' THEN A.CLO_DR_BAL ELSE 0 END) AS CCAP_RSRV_FUND_SUPL_CCAP_AMT,
                  SUM(CASE WHEN A.PCF_COA_ID IN ('601', '611') THEN A.CLO_CR_BAL ELSE 0 END) AS CCAP_RSRV_FUND_SUPL_CCAP_AMT,
                  SUM(CASE WHEN A.PCF_COA_ID = '2' THEN A.CLO_DR_BAL ELSE 0 END) AS TOT_LN_BAL,
                  SUM(CASE WHEN A.PCF_COA_ID IN ('20113', '20123', '21113', '21123', '21213', '21223', '21313', '21323', '25113', '25123', '25213', '25223', '25313', '25323',
                                                 '20114', '20124', '21114', '21124', '21214', '21224', '21314', '21324', '25114', '25124', '25214', '25224', '25314', '25324',
                                                 '20115', '20125', '21115', '21125', '21215', '21225', '21315', '21325', '25115', '25125', '25215', '25225', '25315', '25325')
                                THEN A.CLO_DR_BAL
                           ELSE 0
                           END) AS BAD_DBT_AMT,
                  SUM(CASE WHEN A.PCF_COA_ID IN ('2192', '2592', '2892', '2992') THEN A.CLO_CR_BAL ELSE 0 END) AS GENL_PRVS_AMT,
                  SUM(CASE WHEN A.PCF_COA_ID IN ('2191', '2591', '2891', '2991') THEN A.CLO_CR_BAL ELSE 0 END) AS SPEC_PRVS_AMT,
                  SUM(CASE WHEN A.PCF_COA_ID = '42' THEN A.CLO_CR_BAL ELSE 0 END) AS TDP_BAL,
                  SUM(CASE WHEN A.PCF_COA_ID IN ('41592', '41593') THEN A.CLO_CR_BAL ELSE 0 END) AS CBV_OVD_BOR_AMT,
                  SUM(CASE WHEN A.PCF_COA_ID IN ('41512', '41513', '41592', '41593') THEN A.CLO_CR_BAL ELSE 0 END) AS CBV_BOR_EXCL_SFTY_FUND_AMT,
                  SUM(CASE WHEN A.PCF_COA_ID IN ('41511', '41591') THEN A.CLO_CR_BAL ELSE 0 END) AS SFTY_FUND_BOR_AMT,
                  SUM(CASE WHEN A.PCF_COA_ID IN ('41519', '41599') THEN A.CLO_CR_BAL ELSE 0 END) AS OTHR_INST_BOR_AMT,
                  SUM(CASE WHEN A.PCF_COA_ID IN ('13111', '13121') THEN A.CLO_DR_BAL ELSE 0 END) AS CBV_SVG_AMT,
                  SUM(CASE WHEN A.PCF_COA_ID IN ('13119', '13129') THEN A.CLO_DR_BAL ELSE 0 END) AS OTHR_INST_SVG_AMT,
                  SUM(CASE WHEN A.PCF_COA_ID = '10' THEN A.CLO_DR_BAL ELSE 0 END) AS IN_HAND_CSH_AMT,
                  SUM(CASE WHEN A.PCF_COA_ID IN ('7', '69') THEN A.CLO_CR_BAL ELSE 0 END) - SUM(CASE WHEN A.PCF_COA_ID IN ('8', '69') THEN A.CLO_DR_BAL ELSE 0 END) AS INCL_ALOSS_INCM_MNS_EXP_AMT,
                  SUM(CASE WHEN A.PCF_COA_ID = '7' THEN A.CLO_CR_BAL ELSE 0 END) AS TOT_INCM_AMT,
                  SUM(CASE WHEN A.PCF_COA_ID = '8' THEN A.CLO_DR_BAL ELSE 0 END) AS TOT_EXP_AM,
                  SUM(CASE WHEN A.PCF_COA_ID = '211' THEN A.CLO_DR_BAL ELSE 0 END) AS BAL_SHET_BAS_SHRT_TRM_LN_BAL,
                  SUM(CASE WHEN A.PCF_COA_ID IN ('212', '213', '25') THEN A.CLO_DR_BAL ELSE 0 END) AS BAL_SHET_BAS_MED_LT_LN_BAL,
                  SUM(CASE WHEN A.PCF_COA_ID IN ('219', '259', '289', '299', '209') THEN A.CLO_CR_BAL ELSE 0 END) AS RISK_PRVS_FUND_AMT
           FROM   TM23_DDLY_BAL_SHET_A A INNER JOIN (SELECT PCF_ID
                                                     FROM   TBSM_INPT_RPT_SUBMIT_L
                                                     WHERE  BTCH_BAS_DAY     = v_st_date_01
                                                     AND    INPT_RPT_ID      IN ('G32_012_TTGS', 'G32_003_TTGS', 'G32_020_TTGS_01', 'G32_005_TTGS')
                                                     AND    DATA_BAS_DAY    <= loop_bas_day.BAS_DAY
                                                     GROUP BY PCF_ID
                                                    ) B
                                                 ON B.PCF_ID = A.PCF_ID
           WHERE  A.BAS_DAY = loop_bas_day.BAS_DAY
           GROUP BY A.BAS_DAY, A.PCF_ID
          ),
          FROM_ASSET_EQUITY AS
          (SELECT A.BAS_DAY,
                  A.PCF_ID,
                  CASE WHEN A.GG > 0
                            THEN A.EE - A.FF + A.GG
                       ELSE A.EE - A.FF
                       END AS TOT_AST_AMT,
                  ROUND(CASE WHEN A.GENERAL_PROVISION <= 0.0125 * (A.AT_CBNK_PAY_DPST_AMT + B.FULL_GTD_RL_EST_LN_BAL_50P + A.FIX_AST_AMT
                                                                                          + A.EE - A.FF + CASE WHEN A.GG > 0 THEN A.GG ELSE 0 END
                                                                                           - (A.HH + A.II + B.FULL_GTD_RL_EST_LN_BAL + B.FULL_GTD_VP_LN_BAL)
                                                                  )
                                  THEN A.GENERAL_PROVISION
                             ELSE 0.0125 * (A.AT_CBNK_PAY_DPST_AMT + B.FULL_GTD_RL_EST_LN_BAL_50P + A.FIX_AST_AMT
                                                                   + A.EE - A.FF + CASE WHEN A.GG > 0 THEN A.GG ELSE 0 END
                                                                   - (A.HH + A.II + B.FULL_GTD_RL_EST_LN_BAL + B.FULL_GTD_VP_LN_BAL)
                                           )
                             END, 0) AS PCT_125_VS_RWA_GENL_PRVS_AMT,
                  CASE WHEN (A.TOTAL_INCOMES - A.TOTAL_EXPENSES + A.INIT_RTAIN_ERN_AMT >= 0 AND A.INIT_RTAIN_ERN_AMT <  0)
                             THEN 0
                       WHEN (A.TOTAL_INCOMES - A.TOTAL_EXPENSES + A.INIT_RTAIN_ERN_AMT >= 0 AND A.INIT_RTAIN_ERN_AMT >= 0)
                             THEN A.INIT_RTAIN_ERN_AMT
                       WHEN (A.TOTAL_INCOMES - A.TOTAL_EXPENSES + A.INIT_RTAIN_ERN_AMT <  0)
                             THEN -(A.TOTAL_INCOMES - A.TOTAL_EXPENSES + A.INIT_RTAIN_ERN_AMT)
                       END AS ZZZ
           FROM   FROM_BAL_SHEET A LEFT OUTER JOIN FROM_DDLY_CUST_CR_TRANS B
                                                ON B.BAS_DAY = A.BAS_DAY
                                               AND B.PCF_ID  = A.PCF_ID
          )
          SELECT A.BAS_DAY
                ,A.PCF_ID
                ,E.CCAP_AMT + E.INVST_INFRA_FIX_AST_FUND_AMT + E.CNTRBT_PCF_NON_RFNDBL_CAP_AMT + E.FOR_INCR_CCAP_RSRV_FUND_AMT
                            + E.PRFS_DEV_INVST_FUND_AMT + E.FIN_RSRV_FUND_AMT
                            - E.CNTRBT_CBV_CAP_AMT - E.DUTO_FXASTRV_100_PCT_DCR_AMT
                            + F.PCT_125_VS_RWA_GENL_PRVS_AMT
                            + F.ZZZ AS EQT_AMT
                ,E.CCAP_AMT + E.INVST_INFRA_FIX_AST_FUND_AMT + E.CNTRBT_PCF_NON_RFNDBL_CAP_AMT + E.FOR_INCR_CCAP_RSRV_FUND_AMT
                            + E.PRFS_DEV_INVST_FUND_AMT + E.FIN_RSRV_FUND_AMT
                            - E.CNTRBT_CBV_CAP_AMT - E.DUTO_FXASTRV_100_PCT_DCR_AMT
                            + F.PCT_125_VS_RWA_GENL_PRVS_AMT
                            + E.INIT_RTAIN_ERN_AMT AS CNSLDT_EQT_AMT
                ,E.CCAP_AMT
                ,E.CCAP_RSRV_FUND_SUPL_CCAP_AMT
                ,F.TOT_AST_AMT
                ,E.TOT_LN_BAL
                ,B.SHRT_TRM_LN_BAL
                ,B.MED_LT_LN_BAL
                ,B.WO_COLL_LN_BAL
                ,B.WIT_COLL_LN_BAL
                ,G.TOT_LN_CNTR_NUM_CNT
                ,G.TOT_BRWR_NUM_CNT
                ,G.NEW_ARISN_LN_AMT
                ,G.NEW_ARISN_LN_CNTR_NUM_CNT
                ,G.NEW_BRWR_NUM_CNT
                ,E.BAD_DBT_AMT
                ,E.GENL_PRVS_AMT
                ,E.SPEC_PRVS_AMT
                ,E.TDP_BAL
                ,C.MBR_DPST_BAL
                ,C.NON_MBR_DPST_BAL
                ,C.SHRT_TRM_DPST_BAL
                ,C.MED_LT_DPST_BAL
                ,C.TOT_DPSTR_NUM_CNT
                ,C.MBR_DPSTR_NUM_CNT
                ,C.NON_MBR_DPSTR_NUM_CNT
                ,ROUND(CASE WHEN (C.SHRT_TRM_DPST_BAL IS NULL AND C.MED_LT_DPST_BAL IS NULL) OR C.SHRT_TRM_DPST_BAL + C.MED_LT_DPST_BAL = 0
                                 THEN 99999.99
                            WHEN C.MBR_DPST_BAL = 0 THEN 0
                            ELSE C.MBR_DPST_BAL / (C.SHRT_TRM_DPST_BAL + C.MED_LT_DPST_BAL) * 100
                            END, 2) AS MBR_DPST_VS_TDP_RTO
                ,E.CBV_BOR_EXCL_SFTY_FUND_AMT
                ,E.CBV_OVD_BOR_AMT
                ,E.SFTY_FUND_BOR_AMT
                ,E.OTHR_INST_BOR_AMT
                ,E.CBV_SVG_AMT
                ,E.OTHR_INST_SVG_AMT
                ,E.IN_HAND_CSH_AMT
                ,E.TOTAL_INCOMES
                ,E.TOTAL_EXPENSES
                ,E.TOTAL_INCOMES - E.TOTAL_EXPENSES AS NET_PNL_AMT
                ,E.INCL_ALOSS_INCM_MNS_EXP_AMT
                ,D.NXT_WRK_DAY_AST_AMT
                ,D.NXT_WRK_DAY_PAY_LBLTY_AMT
                ,CASE WHEN D.NXT_WRK_DAY_LQDTY_RTO > 99999.999 THEN 99999.99 ELSE D.NXT_WRK_DAY_LQDTY_RTO END
                ,D.NXT_7_WRK_DAY_AST_AMT
                ,D.NXT_7_WRK_DAY_PAY_LBLTY_AMT
                ,CASE WHEN D.NXT_7_WRK_DAY_LQDTY_RTO > 99999.999 THEN 99999.99 ELSE D.NXT_7_WRK_DAY_LQDTY_RTO END
                ,CASE WHEN H.L3M_NXT_WRK_DAY_LQDTY_RTO > 99999.999 THEN 99999.99 ELSE H.L3M_NXT_WRK_DAY_LQDTY_RTO END
                ,CASE WHEN H.L3M_NXT_7_WRK_DAY_LQDTY_RTO > 99999.999 THEN 99999.99 ELSE H.L3M_NXT_7_WRK_DAY_LQDTY_RTO END
                ,E.BAL_SHET_BAS_SHRT_TRM_LN_BAL
                ,E.BAL_SHET_BAS_MED_LT_LN_BAL
                ,E.RISK_PRVS_FUND_AMT
                ,SYSTIMESTAMP
          FROM   FULL_SET A LEFT OUTER JOIN FROM_DDLY_CUST_CR_TRANS B
                                         ON A.BAS_DAY = B.BAS_DAY
                                        AND A.PCF_ID  = B.PCF_ID
                            LEFT OUTER JOIN FROM_DDLY_CUST_DPST_TRANS C
                                         ON A.BAS_DAY = C.BAS_DAY
                                        AND A.PCF_ID  = C.PCF_ID
                            LEFT OUTER JOIN FROM_TB07_G32_005_TTGS_A D
                                         ON A.BAS_DAY = D.BAS_DAY
                                        AND A.PCF_ID  = D.PCF_ID
                            LEFT OUTER JOIN FROM_BAL_SHEET E
                                         ON A.BAS_DAY = E.BAS_DAY
                                        AND A.PCF_ID  = E.PCF_ID
                            LEFT OUTER JOIN FROM_ASSET_EQUITY F
                                         ON A.BAS_DAY = F.BAS_DAY
                                        AND A.PCF_ID  = F.PCF_ID
                            LEFT OUTER JOIN TM21_DDLY_CUST_CR_TRANS_W G
                                         ON A.BAS_DAY = G.BAS_DAY
                                        AND A.PCF_ID  = G.PCF_ID
                            LEFT OUTER JOIN FROM_TB07_G32_005_TTGS_A_3M H
                                         ON A.BAS_DAY = H.BAS_DAY
                                        AND A.PCF_ID  = H.PCF_ID;

          ----------------------------------------------------------------------------
          --  Transaction Log
          ----------------------------------------------------------------------------
          v_step_code    := '040' ;
          v_step_desc    := v_wk_date || v_seq || ' ' || loop_bas_day.BAS_DAY ||' : Inserting Data Result';
          v_time         := SYSDATE ;

          P1_BSM_PROG_EXEC_LOG(v_program_id, v_program_type_name, v_step_code, v_step_desc, v_time, sql%rowcount, NULL, NULL) ;

          COMMIT;

          ----------------------------------------------------------------------------

          v_cnt := v_cnt+sql%rowcount;

          EXCEPTION
          WHEN OTHERS THEN
               begin
                     v_step_code    := '041' ;
                     v_step_desc    := v_wk_date || v_seq || ' : Inserting Data Error with BAS_DAY = ' ;
                     v_time         := SYSDATE ;
                     P1_BSM_PROG_EXEC_LOG(v_program_id, v_program_type_name, v_step_code, v_step_desc||loop_bas_day.BAS_DAY, v_time, v_cnt, TO_CHAR(SQLCODE), SQLERRM) ;
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