create or replace PROCEDURE            "P4_M27_MMLY_PCF_RM_DTL_IDC_1718"
                 ( p_wkdate    IN  CHAR DEFAULT NULL
                 , p_stdate_01 IN  CHAR DEFAULT NULL
                 , p_eddate_01 IN  CHAR DEFAULT NULL
                 )
IS
    /*--------------------------------------------------------------------------
     * PROGRAM ID    : P4_M27_MMLY_PCF_RM_DTL_IDC_1718
     * PROGRAM NAME  : A program for insert data to TM27_MMLY_PCF_RM_DTL_IDC_A
     * SOURCE TABLE  : TM24_MMLY_IDC_CALC_CAR_A
                       TM24_MMLY_OTHR_IDC_A
                       TM24_MMLY_FUND_SRC_USG_A
                       TM27_DDLY_PCF_KEY_IDC_A
                       TM23_MMLY_BAL_SHET_A
                       TM00_MMLY_CAL_D
                       TM00_DDLY_CAL_D
                       TM00_PCF_D
                       TBSM_INPT_RPT_SUBMIT_L
     * TARGET TABLE  : TM27_MMLY_PCF_RM_DTL_IDC_A
     * PROGRAMER     : HIEU
     * LAST MODIFICATION DATE : 2025-12-24
     * UNIQUENESS    : N/A
     * COMMENTS      : N/A
    ----------------------------------------------------------------------------
     * Revision History : 2025-12-24 : Create
     * Revision History :

    --------------------------------------------------------------------------*/
    ----------------------------------------------------------------------------
    --  Declare Local Variables
    ----------------------------------------------------------------------------
    v_wk_date                 CHAR(8) DEFAULT NULL ;
    v_st_date_01              CHAR(8) DEFAULT NULL ;
    v_end_date_01             CHAR(8) DEFAULT NULL ;

    v_program_id              VARCHAR2(100) DEFAULT 'P4_M27_MMLY_PCF_RM_DTL_IDC_1718' ;
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
    WHERE  BAS_YM BETWEEN '202410' AND '202411'
    ORDER BY 1;*/

    SELECT T2.BAS_YM
    FROM  (SELECT MIN(BAS_YM) MIN_BAS_YM, MAX(BAS_YM) MAX_BAS_YM
           FROM   (SELECT DISTINCT TRIM(DATA_BAS_DAY) AS BAS_YM
                   FROM   TBSM_INPT_RPT_SUBMIT_L
                   WHERE  BTCH_BAS_DAY >= v_st_date_01
                   AND    INPT_RPT_ID IN ('G32_020_TTGS_02','G32_007_TTGS','G32_006_TTGS')

                   UNION

                   SELECT T1.BAS_YM
                   FROM   TM00_MMLY_CAL_D T1 INNER JOIN (SELECT SUBSTR(MIN(DATA_BAS_DAY), 1, 6) AS MIN_YM, SUBSTR(MAX(DATA_BAS_DAY), 1, 6) AS MAX_YM
                                                         FROM   TBSM_INPT_RPT_SUBMIT_L
                                                         WHERE  BTCH_BAS_DAY >= v_st_date_01
                                                         AND    INPT_RPT_ID  IN ('G32_012_TTGS','G32_005_TTGS')
                                                        ) T2
                                                     ON T1.BAS_YM BETWEEN T2.MIN_YM AND T2.MAX_YM
                  )
           WHERE  BAS_YM <= TO_CHAR(SYSDATE - 35, 'YYYYMM')
          ) T1 INNER JOIN TM00_MMLY_CAL_D T2
                       ON T2.BAS_YM BETWEEN T1.MIN_BAS_YM AND T1.MAX_BAS_YM
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
            FROM TM27_MMLY_PCF_RM_DTL_IDC_A T1
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
    --  1.2 Inserting Data
    ----------------------------------------------------------------------------
    FOR loop_bas_day in v_sbmt_day(v_wk_date,v_st_date_01,v_end_date_01)
    LOOP
    BEGIN

          INSERT INTO TM27_MMLY_PCF_RM_DTL_IDC_A
          (   BAS_YM
             ,PCF_ID
             ,PCF_CD
             ,CAR
             ,TOT_AST_AMT
             ,OWNR_EQT_AMT
             ,CCAP_AMT
             ,OWNR_EQT_VS_CCAP_RTO
             ,TDP_BAL
             ,TDP_VS_OWNR_EQT_RTO
             ,G35_BAD_DBT_VS_TOT_RISK_LN_RTO
             ,G35_BAD_DBT_AMT
             ,TOT_RISK_LN_BAL
             ,TOT_RISK_PRVS_AMT
             ,BAD_DBT_PRVS_AMT
             ,TOT_RISK_VS_BAD_DBT_PRVS_RTO
             ,SNGL_CUST_BIG_LN_VS_EQT_RTO
             ,RLT_GRP_BIG_LN_VS_EQT_RTO
             ,ACRD_INT_AMT
             ,TOT_RISK_G1_LN_BAL
             ,ACRD_INT_VS_TOT_RISK_G1_LN_RTO
             ,NXT_WRK_DAY_LQDTY_RTO
             ,NXT_7_WRK_DAY_LQDTY_RTO
             ,L12M_NWD_LQDTY_OVR100P_CNT
             ,L12M_N7WD_LQDTY_OVR100P_CNT
             ,STFND_USE_MTLTLD_RTO
             ,TOT_CBV_BOR_AMT
             ,TOT_CBV_BOR_VS_TOT_AST_RTO
             ,TOT_IHAND_CSH_AMT
             ,TOT_IHAND_CSH_VS_TOT_AST_RTO
             ,RISK_FIX_AST_AMT
             ,FOR_INCR_CCAP_RSRV_FUND_AMT
             ,CCAP_RSRV_FUND_SUPL_CCAP_AMT
             ,RISK_FIX_AST_RTO
             ,RISK_OTHR_INST_SVG_AMT
             ,TRGT_DATA_LST_MOD_TM
          )
          WITH
          FULL_SET AS
          (SELECT T1.BAS_YM, T2.PCF_ID
           FROM   TM00_MMLY_CAL_D T1 INNER JOIN (SELECT PCF_ID
                                                 FROM   TM00_PCF_D
                                               --WHERE  IN_OPRT_YN = 'Y'
                                                ) T2
                                             ON 1=1
           WHERE  T1.BAS_YM = loop_bas_day.BAS_YM
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
          FROM_MMLY_OTHR_IDC AS
          (SELECT BAS_YM,
                  PCF_ID,
                  CASE WHEN SNGL_CUST_BIG_LN_VS_EQT_RTO >= 99999.99 THEN 99999.99
                       WHEN SNGL_CUST_BIG_LN_VS_EQT_RTO <= -99999.99 THEN -99999.99
                       ELSE SNGL_CUST_BIG_LN_VS_EQT_RTO
                       END AS SNGL_CUST_BIG_LN_VS_EQT_RTO,
                  RLT_GRP_BIG_LN_VS_EQT_RTO
           FROM   TM24_MMLY_OTHR_IDC_A
           WHERE  BAS_YM = loop_bas_day.BAS_YM
          ),
          FROM_MMLY_FUND_SRC_USG AS
          (SELECT BAS_YM,
                  PCF_ID,
                  STFND_USE_MTLTLD_RTO
           FROM   TM24_MMLY_FUND_SRC_USG_A
           WHERE  BAS_YM = loop_bas_day.BAS_YM
          ),
          FROM_DDLY_PCF_KEY AS
          (SELECT loop_bas_day.BAS_YM AS BAS_YM,
                  T1.PCF_ID,
                  SUM(CASE WHEN T1.NXT_WRK_DAY_LQDTY_RTO   > 1 THEN 1 ELSE 0 END) AS L12M_NWD_LQDTY_OVR100P_CNT,
                  SUM(CASE WHEN T1.NXT_7_WRK_DAY_LQDTY_RTO > 1 THEN 1 ELSE 0 END) AS L12M_N7WD_LQDTY_OVR100P_CNT
           FROM   TM27_DDLY_PCF_KEY_IDC_A T1 INNER JOIN (SELECT DISTINCT THIS_MM_LST_WRK_DAY AS BAS_DAY
                                                         FROM   TM00_DDLY_CAL_D
                                                         WHERE  BAS_YM BETWEEN TO_CHAR(ADD_MONTHS(TO_DATE(loop_bas_day.BAS_YM, 'YYYYMM'), -11), 'YYYYMM') AND loop_bas_day.BAS_YM
                                                        ) T2
                                                     ON T2.BAS_DAY = T1.BAS_DAY
           WHERE  T1.NXT_WRK_DAY_LQDTY_RTO   > 1
           OR     T1.NXT_7_WRK_DAY_LQDTY_RTO > 1
           GROUP BY T1.PCF_ID
          ),
          NULL_NXT_WRK_DAY_LQDTY_RTO AS
          (SELECT loop_bas_day.BAS_YM AS BAS_YM,
                  T1.PCF_ID
           FROM   TM27_DDLY_PCF_KEY_IDC_A T1 INNER JOIN (SELECT DISTINCT THIS_MM_LST_WRK_DAY AS BAS_DAY
                                                         FROM   TM00_DDLY_CAL_D
                                                         WHERE  BAS_YM = loop_bas_day.BAS_YM
                                                        ) T2
                                                     ON T2.BAS_DAY = T1.BAS_DAY
           WHERE  T1.NXT_WRK_DAY_LQDTY_RTO IS NULL
          ),
          NULL_NXT_7_WRK_DAY_LQDTY_RTO AS
          (SELECT loop_bas_day.BAS_YM AS BAS_YM,
                  T1.PCF_ID
           FROM   TM27_DDLY_PCF_KEY_IDC_A T1 INNER JOIN (SELECT DISTINCT THIS_MM_LST_WRK_DAY AS BAS_DAY
                                                         FROM   TM00_DDLY_CAL_D
                                                         WHERE  BAS_YM = loop_bas_day.BAS_YM
                                                        ) T2
                                                     ON T2.BAS_DAY = T1.BAS_DAY
           WHERE  T1.NXT_7_WRK_DAY_LQDTY_RTO IS NULL
          ),
          THIS_MONTH_LQDTY_RTO AS
          (SELECT loop_bas_day.BAS_YM AS BAS_YM,
                  T1.PCF_ID,
                  T1.NXT_WRK_DAY_LQDTY_RTO,
                  T1.NXT_7_WRK_DAY_LQDTY_RTO
           FROM   TM27_DDLY_PCF_KEY_IDC_A T1 INNER JOIN (SELECT DISTINCT THIS_MM_LST_WRK_DAY AS BAS_DAY
                                                         FROM   TM00_DDLY_CAL_D
                                                         WHERE  BAS_YM = loop_bas_day.BAS_YM
                                                        ) T2
                                                     ON T2.BAS_DAY = T1.BAS_DAY
          ),
          FROM_MMLY_BAL_SHET AS
          (SELECT BAS_YM,
                  PCF_ID,
                  SUM(CASE WHEN PCF_COA_ID = '6' THEN NVL(CLO_CR_BAL,0) - NVL(CLO_DR_BAL,0) ELSE 0 END
                      +
                      CASE WHEN PCF_COA_ID = '7' THEN NVL(CLO_CR_BAL,0) ELSE 0 END
                      -
                      CASE WHEN PCF_COA_ID = '8' THEN NVL(CLO_DR_BAL,0) ELSE 0 END
                     ) AS OWNR_EQT_AMT,
                  SUM(CASE WHEN PCF_COA_ID = '42' THEN CLO_CR_BAL ELSE 0 END) AS TDP_BAL,
                  SUM(CASE WHEN PCF_COA_ID IN ('21113', '21114', '21115', '21123', '21124', '21125', '21213', '21214','21215',
                                               '21223', '21224', '21225', '21313', '21314', '21315', '21323', '21324',
                                               '21325', '25113', '25114', '25115', '25123', '25124', '25125', '25213',
                                               '25214', '25215', '25223', '25224', '25225', '25313', '25314', '25315',
                                               '25323', '25324', '25325', '28', '352','3614','3615','387')
                           THEN CLO_DR_BAL ELSE 0 END) AS G345_BAD_DBT_AMT,
                  SUM(CASE WHEN PCF_COA_ID IN ('2','352','3614','3615','387') THEN CLO_DR_BAL ELSE 0 END) AS TOT_RISK_LN_BAL,
                  SUM(CASE WHEN PCF_COA_ID IN ('219','259','289','299') THEN CLO_CR_BAL ELSE 0 END) AS TOT_RISK_PRVS_AMT,
                  SUM(CASE WHEN PCF_COA_ID IN ('21113', '21114', '21115', '21123', '21124', '21125', '21213', '21214', '21215',
                                               '21223', '21224', '21225', '21313', '21314', '21315', '21323', '21324',
                                               '21325', '25113', '25114', '25115', '25123', '25124', '25125', '25213',
                                               '25214', '25215', '25223', '25224', '25225', '25313', '25314', '25315',
                                               '25323', '25324', '25325')
                           THEN CLO_DR_BAL ELSE 0 END) AS BAD_DBT_PRVS_AMT,
                  SUM(CASE WHEN PCF_COA_ID = '394' THEN CLO_DR_BAL ELSE 0 END) AS ACRD_INT_AMT,
                  /* Change Logic : '2111' => '21111' : 2023/06/28 */
                  SUM(CASE WHEN PCF_COA_ID IN ('21111', '21121', '21211', '21221', '21311', '21321',
                                                        '25111', '25121', '25211', '25221', '25311', '25321')
                           THEN CLO_DR_BAL ELSE 0 END) AS TOT_RISK_G1_LN_BAL,
                  SUM(CASE WHEN PCF_COA_ID IN ('41511','41512','41513','41591','41592','41593')
                           THEN CLO_CR_BAL ELSE 0 END) AS TOT_CBV_BOR_AMT,
                  SUM(CASE WHEN PCF_COA_ID IN ('101', '103') THEN CLO_DR_BAL ELSE 0 END) AS TOT_IHAND_CSH_AMT,
                  SUM(CASE WHEN PCF_COA_ID = '30' THEN CLO_DR_BAL - CLO_CR_BAL ELSE 0 END) AS RISK_FIX_AST_AMT,
                  SUM(CASE WHEN PCF_COA_ID = '611' THEN CLO_CR_BAL ELSE 0 END) AS FOR_INCR_CCAP_RSRV_FUND_AMT,
                  SUM(CASE WHEN PCF_COA_ID IN ('601', '611') THEN CLO_CR_BAL ELSE 0 END) AS CCAP_RSRV_FUND_SUPL_CCAP_AMT,
                  SUM(CASE WHEN PCF_COA_ID = '13129' THEN CLO_DR_BAL ELSE 0 END) AS RISK_OTHR_INST_SVG_AMT
           FROM   TM23_MMLY_BAL_SHET_A
           WHERE  BAS_YM = loop_bas_day.BAS_YM
           GROUP BY BAS_YM, PCF_ID
          )
          SELECT A.BAS_YM
                ,A.PCF_ID
                ,G.PCF_CD
                ,B.CAR
                ,B.TOT_AST_AMT
                ,F.OWNR_EQT_AMT
                ,B.CCAP_AMT
                ,CASE WHEN F.OWNR_EQT_AMT IS NULL OR B.CCAP_AMT IS NULL THEN NULL
                      WHEN NVL(F.OWNR_EQT_AMT, 0) = 0 OR NVL(B.CCAP_AMT, 0) = 0 THEN 0
                      ELSE ROUND(F.OWNR_EQT_AMT / B.CCAP_AMT, 2)
                      END  AS OWNR_EQT_VS_CCAP_RTO
                ,F.TDP_BAL
                ,CASE WHEN F.TDP_BAL IS NULL OR F.OWNR_EQT_AMT IS NULL THEN NULL
                      WHEN (CASE WHEN NVL(F.TDP_BAL, 0) = 0 OR NVL(F.OWNR_EQT_AMT, 0) = 0  THEN 0
                                 ELSE ROUND(F.TDP_BAL / F.OWNR_EQT_AMT, 2)
                                 END) < -99999.99
                      THEN  -99999.99
                      WHEN (CASE WHEN NVL(F.TDP_BAL, 0) = 0 OR NVL(F.OWNR_EQT_AMT, 0) = 0  THEN 0
                                 ELSE ROUND(F.TDP_BAL / F.OWNR_EQT_AMT, 2)
                                 END) > 99999.99
                      THEN  99999.99
                      ELSE (CASE WHEN NVL(F.TDP_BAL, 0) = 0 OR NVL(F.OWNR_EQT_AMT, 0) = 0  THEN 0
                                 ELSE ROUND(F.TDP_BAL / F.OWNR_EQT_AMT, 2)
                                 END)
                      END AS TDP_VS_OWNR_EQT_RTO
                ,CASE WHEN F.G345_BAD_DBT_AMT IS NULL OR  F.TOT_RISK_LN_BAL IS NULL THEN NULL
                      WHEN F.G345_BAD_DBT_AMT = 0     AND F.TOT_RISK_LN_BAL <> 0    THEN 0
                      WHEN F.G345_BAD_DBT_AMT <> 0    AND F.TOT_RISK_LN_BAL = 0     THEN NULL
                      WHEN F.G345_BAD_DBT_AMT = 0     AND F.TOT_RISK_LN_BAL = 0     THEN NULL
                      ELSE ROUND(F.G345_BAD_DBT_AMT / F.TOT_RISK_LN_BAL * 100, 2)
                      END  AS G35_BAD_DBT_VS_TOT_RISK_LN_RTO
                ,F.G345_BAD_DBT_AMT
                ,F.TOT_RISK_LN_BAL
                ,F.TOT_RISK_PRVS_AMT
                ,F.BAD_DBT_PRVS_AMT
                ,CASE WHEN F.TOT_RISK_PRVS_AMT IS NULL OR F.BAD_DBT_PRVS_AMT IS NULL THEN NULL
                      WHEN (CASE WHEN NVL(F.TOT_RISK_PRVS_AMT, 0) = 0 OR NVL(F.BAD_DBT_PRVS_AMT, 0) = 0  THEN 0
                                 ELSE ROUND(F.TOT_RISK_PRVS_AMT / F.BAD_DBT_PRVS_AMT * 100, 2)
                                 END) < -99999.99
                      THEN -99999.99
                      WHEN (CASE WHEN NVL(F.TOT_RISK_PRVS_AMT, 0) = 0 OR NVL(F.BAD_DBT_PRVS_AMT, 0) = 0  THEN 0
                                 ELSE ROUND(F.TOT_RISK_PRVS_AMT / F.BAD_DBT_PRVS_AMT * 100, 2)
                                 END) > 99999.99
                      THEN 99999.99
                      WHEN NVL(F.TOT_RISK_PRVS_AMT, 0) > 0 AND NVL(F.BAD_DBT_PRVS_AMT, 0) = 0
                      THEN 100.00
                      --------------------------------------------------------------------------
                      ELSE (CASE WHEN NVL(F.TOT_RISK_PRVS_AMT, 0) = 0 OR NVL(F.BAD_DBT_PRVS_AMT, 0) = 0  THEN 0
                                 ELSE ROUND(F.TOT_RISK_PRVS_AMT / F.BAD_DBT_PRVS_AMT * 100, 2)
                                 END)
                      END   AS TOT_RISK_VS_BAD_DBT_PRVS_RTO
                ,C.SNGL_CUST_BIG_LN_VS_EQT_RTO
                ,C.RLT_GRP_BIG_LN_VS_EQT_RTO
                ,F.ACRD_INT_AMT
                ,F.TOT_RISK_G1_LN_BAL
                ,CASE WHEN F.ACRD_INT_AMT IS NULL OR  F.TOT_RISK_G1_LN_BAL IS NULL THEN NULL
                      WHEN F.ACRD_INT_AMT = 0     AND F.TOT_RISK_G1_LN_BAL <> 0    THEN 0
                      WHEN F.ACRD_INT_AMT <> 0    AND F.TOT_RISK_G1_LN_BAL = 0     THEN NULL
                      WHEN F.ACRD_INT_AMT = 0     AND F.TOT_RISK_G1_LN_BAL = 0     THEN NULL
                      ELSE ROUND(F.ACRD_INT_AMT / F.TOT_RISK_G1_LN_BAL * 100, 2)
                      END  AS ACRD_INT_VS_TOT_RISK_G1_LN_RTO
                ,J.NXT_WRK_DAY_LQDTY_RTO
                ,J.NXT_7_WRK_DAY_LQDTY_RTO
                ,CASE WHEN H.PCF_ID IS NOT NULL THEN NULL
                      ELSE E.L12M_NWD_LQDTY_OVR100P_CNT
                      END  AS L12M_NWD_LQDTY_OVR100P_CNT
                ,CASE WHEN I.PCF_ID IS NOT NULL THEN NULL
                      ELSE E.L12M_N7WD_LQDTY_OVR100P_CNT
                      END  AS L12M_N7WD_LQDTY_OVR100P_CNT
                --------------------------------------------------------
                ,D.STFND_USE_MTLTLD_RTO
                ,F.TOT_CBV_BOR_AMT
                ,CASE WHEN F.TOT_CBV_BOR_AMT IS NULL OR B.TOT_AST_AMT IS NULL THEN NULL
                      WHEN NVL(F.TOT_CBV_BOR_AMT, 0) = 0 OR NVL(B.TOT_AST_AMT, 0) = 0  THEN 0
                      ELSE ROUND(F.TOT_CBV_BOR_AMT / B.TOT_AST_AMT * 100, 2)
                      END  AS TOT_CBV_BOR_VS_TOT_AST_RTO
                ,F.TOT_IHAND_CSH_AMT
                ,CASE WHEN F.TOT_IHAND_CSH_AMT IS NULL OR B.TOT_AST_AMT IS NULL THEN NULL
                      WHEN NVL(F.TOT_IHAND_CSH_AMT, 0) = 0 OR NVL(B.TOT_AST_AMT, 0) = 0  THEN 0
                      ELSE ROUND(F.TOT_IHAND_CSH_AMT / B.TOT_AST_AMT * 100, 2)
                      END  AS TOT_IHAND_CSH_VS_TOT_AST_RTO
                ,F.RISK_FIX_AST_AMT
                ,F.FOR_INCR_CCAP_RSRV_FUND_AMT
                ,F.CCAP_RSRV_FUND_SUPL_CCAP_AMT
                ,CASE WHEN F.RISK_FIX_AST_AMT IS NULL OR F.CCAP_RSRV_FUND_SUPL_CCAP_AMT IS NULL THEN NULL
                      WHEN NVL(F.RISK_FIX_AST_AMT, 0) = 0 OR NVL(F.CCAP_RSRV_FUND_SUPL_CCAP_AMT, 0) = 0  THEN 0
                      ELSE ROUND(F.RISK_FIX_AST_AMT / F.CCAP_RSRV_FUND_SUPL_CCAP_AMT * 100, 2)
                      END  AS RISK_FIX_AST_RTO
                ,F.RISK_OTHR_INST_SVG_AMT
                ,SYSTIMESTAMP
          FROM   FULL_SET A LEFT OUTER JOIN FROM_MMLY_IDC_CALC_CAR B
                                         ON A.BAS_YM = B.BAS_YM
                                        AND A.PCF_ID = B.PCF_ID
                            LEFT OUTER JOIN FROM_MMLY_OTHR_IDC C
                                         ON A.BAS_YM = C.BAS_YM
                                        AND A.PCF_ID = C.PCF_ID
                            LEFT OUTER JOIN FROM_MMLY_FUND_SRC_USG D
                                         ON A.BAS_YM = D.BAS_YM
                                        AND A.PCF_ID = D.PCF_ID
                            LEFT OUTER JOIN FROM_DDLY_PCF_KEY E
                                         ON A.BAS_YM = E.BAS_YM
                                        AND A.PCF_ID = E.PCF_ID
                            LEFT OUTER JOIN FROM_MMLY_BAL_SHET F
                                         ON A.BAS_YM = F.BAS_YM
                                        AND A.PCF_ID = F.PCF_ID
                            LEFT OUTER JOIN TM00_PCF_D G
                                         ON A.PCF_ID = G.PCF_ID
                            LEFT OUTER JOIN NULL_NXT_WRK_DAY_LQDTY_RTO H
                                         ON A.BAS_YM = H.BAS_YM
                                        AND A.PCF_ID = H.PCF_ID
                            LEFT OUTER JOIN NULL_NXT_7_WRK_DAY_LQDTY_RTO I
                                         ON A.BAS_YM = I.BAS_YM
                                        AND A.PCF_ID = I.PCF_ID
                            LEFT OUTER JOIN THIS_MONTH_LQDTY_RTO J
                                         ON A.BAS_YM = J.BAS_YM
                                        AND A.PCF_ID = J.PCF_ID
                            ;

          ----------------------------------------------------------------------------
          --  Transaction Log
          ----------------------------------------------------------------------------
          v_step_code    := '020' ;
          v_step_desc    := v_wk_date || v_seq || ' ' || loop_bas_day.BAS_YM ||' : Inserting Data Result';
          v_time         := SYSDATE ;

          P1_BSM_PROG_EXEC_LOG(v_program_id, v_program_type_name, v_step_code, v_step_desc, v_time, sql%rowcount, NULL, NULL) ;

          COMMIT;
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