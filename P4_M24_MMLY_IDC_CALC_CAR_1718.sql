create or replace PROCEDURE            "P4_M24_MMLY_IDC_CALC_CAR_1718"
                 ( p_wkdate    IN  CHAR DEFAULT NULL
                 , p_stdate_01 IN  CHAR DEFAULT NULL
                 , p_eddate_01 IN  CHAR DEFAULT NULL
                 )
IS
    /*--------------------------------------------------------------------------
     * PROGRAM ID    : P4_M24_MMLY_IDC_CALC_CAR_1718
     * PROGRAM NAME  : A program for insert data to TM24_MMLY_IDC_CALC_CAR_A
     * SOURCE TABLE  : TM23_MMLY_BAL_SHET_A
                       TM21_MMLY_CUST_CR_TRANS_A
     * TARGET TABLE  : TM24_MMLY_IDC_CALC_CAR_A
     * PROGRAMER     : HIEU
     * LAST MODIFICATION DATE : 2025-12-23
     * UNIQUENESS    : N/A
     * COMMENTS      : N/A   ----------------------------------------------------------------------------
     * Revision History : 2025-12-23 : Create
     * Revision History :
    --------------------------------------------------------------------------*/
    ----------------------------------------------------------------------------
    --  Declare Local Variables
    ----------------------------------------------------------------------------
    v_wk_date                 CHAR(8) DEFAULT NULL ;
    v_st_date_01              CHAR(8) DEFAULT NULL ;
    v_end_date_01             CHAR(8) DEFAULT NULL ;

    v_program_id              VARCHAR2(100) DEFAULT 'P4_M24_MMLY_IDC_CALC_CAR_1718' ;
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
    FROM   (SELECT DISTINCT TRIM(DATA_BAS_DAY) AS BAS_YM
            FROM   TBSM_INPT_RPT_SUBMIT_L
            WHERE  BTCH_BAS_DAY = v_st_date_01
            AND    INPT_RPT_ID  = 'G32_020_TTGS_02'

            UNION

            SELECT T1.BAS_YM
            FROM   TM00_MMLY_CAL_D T1 INNER JOIN (SELECT SUBSTR(MIN(DATA_BAS_DAY), 1, 6) AS MIN_YM, SUBSTR(MAX(DATA_BAS_DAY), 1, 6) AS MAX_YM
                                                  FROM   TBSM_INPT_RPT_SUBMIT_L
                                                  WHERE  BTCH_BAS_DAY = v_st_date_01
                                                  AND    INPT_RPT_ID  = 'G32_012_TTGS'
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
            FROM TM24_MMLY_IDC_CALC_CAR_A T1
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

          INSERT INTO TM24_MMLY_IDC_CALC_CAR_A
          (   BAS_YM
             ,PCF_ID
             ,CCAP_AMT
             ,INVST_INFRA_FIX_AST_FUND_AMT
             ,FOR_INCR_CCAP_RSRV_FUND_AMT
             ,PRFS_DEV_INVST_FUND_AMT
             ,CNTRBT_PCF_NON_RFNDBL_CAP_AMT
             ,RTAIN_ERN_AMT
             ,ACUM_LOSS_AMT
             ,CNTRBT_CBV_CAP_AMT
             ,FIN_RSRV_FUND_AMT
             ,PCT_125_VS_RWA_GENL_PRVS_AMT
             ,DUTO_FXASTRV_100_PCT_DCR_AMT
             ,AT_CBNK_PAY_DPST_AMT
             ,FULL_GTD_VP_LN_BAL
             ,FULL_GTD_RL_EST_LN_BAL
             ,FIX_AST_AMT
             ,OTHR_RMNG_AST_AMT
             ,TOT_AST_AMT
             ,EQT_AMT
             ,CNSLDT_EQT_AMT
             ,RWA_AMT
             ,GENL_PRVS_AMT
             ,CAR
             ,TRGT_DATA_LST_MOD_TM
          )
          WITH
          FULL_SET AS
          (SELECT  T1.BAS_YM, T1.PCF_ID
           FROM   (SELECT BAS_YM, PCF_ID FROM TM23_MMLY_BAL_SHET_A WHERE BAS_YM = loop_bas_day.BAS_YM GROUP BY BAS_YM, PCF_ID
                   UNION
                   SELECT BAS_YM, PCF_ID FROM TM21_MMLY_CUST_CR_TRANS_A WHERE BAS_YM = loop_bas_day.BAS_YM GROUP BY BAS_YM, PCF_ID
                  ) T1
          ),
          FROM_BAL_SHEET AS
          (SELECT BAS_YM,
                  PCF_ID,
                  SUM(CASE WHEN PCF_COA_ID = '601'
                                THEN CLO_CR_BAL
                           ELSE 0
                           END) AS CCAP_AMT,
                  SUM(CASE WHEN PCF_COA_ID = '602'
                                THEN CLO_CR_BAL
                           ELSE 0
                           END) AS INVST_INFRA_FIX_AST_FUND_AMT,
                  SUM(CASE WHEN PCF_COA_ID = '611'
                                THEN CLO_CR_BAL
                           ELSE 0
                           END) AS FOR_INCR_CCAP_RSRV_FUND_AMT,
                  SUM(CASE WHEN PCF_COA_ID = '612'
                                THEN CLO_CR_BAL
                           ELSE 0
                           END) AS PRFS_DEV_INVST_FUND_AMT,
                  SUM(CASE WHEN PCF_COA_ID = '609'
                                THEN CLO_CR_BAL
                           ELSE 0
                           END) AS CNTRBT_PCF_NON_RFNDBL_CAP_AMT,
                  SUM(CASE WHEN PCF_COA_ID = '34401'
                                THEN CLO_DR_BAL
                           ELSE 0
                           END) AS CNTRBT_CBV_CAP_AMT,
                  SUM(CASE WHEN PCF_COA_ID = '613'
                                THEN CLO_CR_BAL
                           ELSE 0
                           END) AS FIN_RSRV_FUND_AMT,
                  SUM(CASE WHEN PCF_COA_ID = '642'
                                THEN CLO_DR_BAL
                           ELSE 0
                           END) AS DUTO_FXASTRV_100_PCT_DCR_AMT,
                  SUM(CASE WHEN PCF_COA_ID = '7'
                                THEN CLO_CR_BAL
                           ELSE 0
                           END) AS TOTAL_INCOMES,
                  SUM(CASE WHEN PCF_COA_ID = '8'
                                THEN CLO_DR_BAL
                           ELSE 0
                           END) AS TOTAL_EXPENSES,
                  SUM(CASE WHEN PCF_COA_ID = '69'
                                THEN CLO_CR_BAL - CLO_DR_BAL
                           ELSE 0
                           END) AS INIT_RTAIN_ERN_AMT,
                  ROUND(SUM(CASE WHEN PCF_COA_ID IN ('13119', '13129')
                                      THEN CLO_DR_BAL
                                 ELSE 0
                                 END) * 0.2, 0) AS AT_CBNK_PAY_DPST_AMT,
                  SUM(CASE WHEN PCF_COA_ID = '30'
                                THEN CLO_DR_BAL
                           ELSE 0
                           END) AS FIX_AST_AMT,
                  SUM(CASE WHEN PCF_COA_ID = '2192'
                                THEN CLO_CR_BAL
                           ELSE 0
                           END) AS GENERAL_PROVISION,
                  SUM(CASE WHEN PCF_COA_ID IN ('10','11','13','20','211','212','213','251','252','253','281','282','283','284','285','291','292','293','344','301',
                                               '302','303','31','32','351','352','353','3592','361','369','381','387','388','389','39','453')
                                THEN CLO_DR_BAL
                           ELSE 0
                           END) AS EE,
                  SUM(CASE WHEN PCF_COA_ID IN ('139','209','219','259','289','299','349','305','3599','386','4892','4899')
                                THEN CLO_CR_BAL
                           ELSE 0
                           END) AS FF,
                  SUM(CASE WHEN PCF_COA_ID = '5'
                                THEN CLO_DR_BAL - CLO_CR_BAL
                           ELSE 0
                           END) AS GG,
                  SUM(CASE WHEN PCF_COA_ID IN ('10','11','13111','13121','25')
                                THEN CLO_DR_BAL
                           ELSE 0
                           END) AS HH,
                  SUM(CASE WHEN PCF_COA_ID IN ('13119', '13129')
                                THEN CLO_DR_BAL
                           ELSE 0
                           END) AS II
           FROM   TM23_MMLY_BAL_SHET_A
           WHERE  BAS_YM = loop_bas_day.BAS_YM
           GROUP BY BAS_YM, PCF_ID
          ),
          FROM_BALANCE AS
          (SELECT BAS_YM,
                  PCF_ID,
                  ROUND((SUM(CASE WHEN COLL_TYP_CD = '01' THEN TOT_LN_BAL ELSE 0 END) * 0.5), 0) AS FULL_GTD_RL_EST_LN_BAL_50P,
                  0 AS FULL_GTD_VP_LN_BAL_20P, --ROUND((SUM(CASE WHEN COLL_TYP_CD = '02' THEN TOT_LN_BAL ELSE 0 END) * 0.2), 0) AS FULL_GTD_VP_LN_BAL_20P,
                  SUM(CASE WHEN COLL_TYP_CD = '01' THEN TOT_LN_BAL ELSE 0 END) AS FULL_GTD_RL_EST_LN_BAL,
                  SUM(CASE WHEN COLL_TYP_CD = '02' THEN TOT_LN_BAL ELSE 0 END) AS FULL_GTD_VP_LN_BAL
           FROM   TM21_MMLY_CUST_CR_TRANS_A
           WHERE  BAS_YM = loop_bas_day.BAS_YM
           GROUP BY BAS_YM, PCF_ID
          )
          SELECT BAS_YM
                ,PCF_ID
                ,CCAP_AMT
                ,INVST_INFRA_FIX_AST_FUND_AMT
                ,FOR_INCR_CCAP_RSRV_FUND_AMT
                ,PRFS_DEV_INVST_FUND_AMT
                ,CNTRBT_PCF_NON_RFNDBL_CAP_AMT
                ,CASE WHEN (TOTAL_INCOMES - TOTAL_EXPENSES + INIT_RTAIN_ERN_AMT < 0)
                            OR
                           (TOTAL_INCOMES - TOTAL_EXPENSES + INIT_RTAIN_ERN_AMT >= 0 AND INIT_RTAIN_ERN_AMT <  0)
                           THEN 0
                      WHEN (TOTAL_INCOMES - TOTAL_EXPENSES + INIT_RTAIN_ERN_AMT >= 0 AND INIT_RTAIN_ERN_AMT >= 0)
                           THEN INIT_RTAIN_ERN_AMT
                      END AS RTAIN_ERN_AMT
                ,CASE WHEN (TOTAL_INCOMES - TOTAL_EXPENSES + INIT_RTAIN_ERN_AMT >= 0)
                           THEN 0
                      WHEN (TOTAL_INCOMES - TOTAL_EXPENSES + INIT_RTAIN_ERN_AMT <  0)
                           THEN -(TOTAL_INCOMES - TOTAL_EXPENSES + INIT_RTAIN_ERN_AMT)
                      END AS ACUM_LOSS_AMT
                ,CNTRBT_CBV_CAP_AMT
                ,FIN_RSRV_FUND_AMT
                ,PCT_125_VS_RWA_GENL_PRVS_AMT
                ,DUTO_FXASTRV_100_PCT_DCR_AMT
                ,AT_CBNK_PAY_DPST_AMT
                ,FULL_GTD_VP_LN_BAL_20P
                ,FULL_GTD_RL_EST_LN_BAL_50P
                ,FIX_AST_AMT
                ,XXX AS OTHR_RMNG_AST_AMT /* XXX- PCT_125_VS_RWA_GENL_PRVS_AMT - ZZZ AS OTHR_RMNG_AST_AMT */
                ,TOT_AST_AMT
                ,CCAP_AMT + INVST_INFRA_FIX_AST_FUND_AMT + FOR_INCR_CCAP_RSRV_FUND_AMT + PRFS_DEV_INVST_FUND_AMT + CNTRBT_PCF_NON_RFNDBL_CAP_AMT
                 +
                 CASE WHEN (TOTAL_INCOMES - TOTAL_EXPENSES + INIT_RTAIN_ERN_AMT < 0)
                            OR
                           (TOTAL_INCOMES - TOTAL_EXPENSES + INIT_RTAIN_ERN_AMT >= 0 AND INIT_RTAIN_ERN_AMT <  0)
                           THEN 0
                      WHEN (TOTAL_INCOMES - TOTAL_EXPENSES + INIT_RTAIN_ERN_AMT >= 0 AND INIT_RTAIN_ERN_AMT >= 0)
                           THEN INIT_RTAIN_ERN_AMT
                      END
                 -
                 CASE WHEN (TOTAL_INCOMES - TOTAL_EXPENSES + INIT_RTAIN_ERN_AMT >= 0)
                           THEN 0
                      WHEN (TOTAL_INCOMES - TOTAL_EXPENSES + INIT_RTAIN_ERN_AMT <  0)
                           THEN -(TOTAL_INCOMES - TOTAL_EXPENSES + INIT_RTAIN_ERN_AMT)
                      END
                 - CNTRBT_CBV_CAP_AMT
                 + FIN_RSRV_FUND_AMT
                 + PCT_125_VS_RWA_GENL_PRVS_AMT
                 - DUTO_FXASTRV_100_PCT_DCR_AMT AS EQT_AMT
                ,CCAP_AMT + INVST_INFRA_FIX_AST_FUND_AMT + CNTRBT_PCF_NON_RFNDBL_CAP_AMT + FOR_INCR_CCAP_RSRV_FUND_AMT + PRFS_DEV_INVST_FUND_AMT + FIN_RSRV_FUND_AMT
                          - CNTRBT_CBV_CAP_AMT - DUTO_FXASTRV_100_PCT_DCR_AMT
                          + PCT_125_VS_RWA_GENL_PRVS_AMT
                          + INIT_RTAIN_ERN_AMT AS CNSLDT_EQT_AMT
/*
                ,CCAP_AMT + INVST_INFRA_FIX_AST_FUND_AMT + CNTRBT_PCF_NON_RFNDBL_CAP_AMT + FOR_INCR_CCAP_RSRV_FUND_AMT + PRFS_DEV_INVST_FUND_AMT + FIN_RSRV_FUND_AMT
                          - CNTRBT_CBV_CAP_AMT - DUTO_FXASTRV_100_PCT_DCR_AMT
                          + PCT_125_VS_RWA_GENL_PRVS_AMT
                          + ZZZ AS EQT_AMT
                ,CCAP_AMT + INVST_INFRA_FIX_AST_FUND_AMT + CNTRBT_PCF_NON_RFNDBL_CAP_AMT + FOR_INCR_CCAP_RSRV_FUND_AMT + PRFS_DEV_INVST_FUND_AMT + FIN_RSRV_FUND_AMT
                          - CNTRBT_CBV_CAP_AMT - DUTO_FXASTRV_100_PCT_DCR_AMT
                          + PCT_125_VS_RWA_GENL_PRVS_AMT
                          + INIT_RTAIN_ERN_AMT AS CNSLDT_EQT_AMT
*/
                ,RWA_AMT
                ,GENERAL_PROVISION
                ,CASE WHEN RWA_AMT = 0 AND
                           (CCAP_AMT + INVST_INFRA_FIX_AST_FUND_AMT + FOR_INCR_CCAP_RSRV_FUND_AMT + PRFS_DEV_INVST_FUND_AMT + CNTRBT_PCF_NON_RFNDBL_CAP_AMT
                            +
                            CASE WHEN (TOTAL_INCOMES - TOTAL_EXPENSES + INIT_RTAIN_ERN_AMT < 0)
                                       OR
                                      (TOTAL_INCOMES - TOTAL_EXPENSES + INIT_RTAIN_ERN_AMT >= 0 AND INIT_RTAIN_ERN_AMT <  0)
                                      THEN 0
                                 WHEN (TOTAL_INCOMES - TOTAL_EXPENSES + INIT_RTAIN_ERN_AMT >= 0 AND INIT_RTAIN_ERN_AMT >= 0)
                                      THEN INIT_RTAIN_ERN_AMT
                                 END
                            -
                            CASE WHEN (TOTAL_INCOMES - TOTAL_EXPENSES + INIT_RTAIN_ERN_AMT >= 0)
                                      THEN 0
                                 WHEN (TOTAL_INCOMES - TOTAL_EXPENSES + INIT_RTAIN_ERN_AMT <  0)
                                      THEN -(TOTAL_INCOMES - TOTAL_EXPENSES + INIT_RTAIN_ERN_AMT)
                                 END
                            - CNTRBT_CBV_CAP_AMT
                            + FIN_RSRV_FUND_AMT
                            + PCT_125_VS_RWA_GENL_PRVS_AMT
                            - DUTO_FXASTRV_100_PCT_DCR_AMT) <> 0
                      THEN 100
                      ELSE CASE WHEN (
                                      ROUND((CCAP_AMT + INVST_INFRA_FIX_AST_FUND_AMT + FOR_INCR_CCAP_RSRV_FUND_AMT + PRFS_DEV_INVST_FUND_AMT + CNTRBT_PCF_NON_RFNDBL_CAP_AMT
                                      +
                                      CASE WHEN (TOTAL_INCOMES - TOTAL_EXPENSES + INIT_RTAIN_ERN_AMT < 0)
                                                 OR
                                                (TOTAL_INCOMES - TOTAL_EXPENSES + INIT_RTAIN_ERN_AMT >= 0 AND INIT_RTAIN_ERN_AMT <  0)
                                                THEN 0
                                           WHEN (TOTAL_INCOMES - TOTAL_EXPENSES + INIT_RTAIN_ERN_AMT >= 0 AND INIT_RTAIN_ERN_AMT >= 0)
                                                THEN INIT_RTAIN_ERN_AMT
                                           END
                                      -
                                      CASE WHEN (TOTAL_INCOMES - TOTAL_EXPENSES + INIT_RTAIN_ERN_AMT >= 0)
                                                THEN 0
                                           WHEN (TOTAL_INCOMES - TOTAL_EXPENSES + INIT_RTAIN_ERN_AMT <  0)
                                                THEN -(TOTAL_INCOMES - TOTAL_EXPENSES + INIT_RTAIN_ERN_AMT)
                                           END
                                      - CNTRBT_CBV_CAP_AMT
                                      + FIN_RSRV_FUND_AMT
                                      + PCT_125_VS_RWA_GENL_PRVS_AMT
                                      - DUTO_FXASTRV_100_PCT_DCR_AMT) / RWA_AMT * 100, 2)
                                     ) > 99999.99 THEN 99999.99
                                WHEN (
                                      ROUND((CCAP_AMT + INVST_INFRA_FIX_AST_FUND_AMT + FOR_INCR_CCAP_RSRV_FUND_AMT + PRFS_DEV_INVST_FUND_AMT + CNTRBT_PCF_NON_RFNDBL_CAP_AMT
                                      +
                                      CASE WHEN (TOTAL_INCOMES - TOTAL_EXPENSES + INIT_RTAIN_ERN_AMT < 0)
                                                 OR
                                                (TOTAL_INCOMES - TOTAL_EXPENSES + INIT_RTAIN_ERN_AMT >= 0 AND INIT_RTAIN_ERN_AMT <  0)
                                                THEN 0
                                           WHEN (TOTAL_INCOMES - TOTAL_EXPENSES + INIT_RTAIN_ERN_AMT >= 0 AND INIT_RTAIN_ERN_AMT >= 0)
                                                THEN INIT_RTAIN_ERN_AMT
                                           END
                                      -
                                      CASE WHEN (TOTAL_INCOMES - TOTAL_EXPENSES + INIT_RTAIN_ERN_AMT >= 0)
                                                THEN 0
                                           WHEN (TOTAL_INCOMES - TOTAL_EXPENSES + INIT_RTAIN_ERN_AMT <  0)
                                                THEN -(TOTAL_INCOMES - TOTAL_EXPENSES + INIT_RTAIN_ERN_AMT)
                                           END
                                      - CNTRBT_CBV_CAP_AMT
                                      + FIN_RSRV_FUND_AMT
                                      + PCT_125_VS_RWA_GENL_PRVS_AMT
                                      - DUTO_FXASTRV_100_PCT_DCR_AMT) / RWA_AMT * 100, 2)
                                     ) < -99999.99 THEN -99999.99
                                ELSE (
                                      ROUND((CCAP_AMT + INVST_INFRA_FIX_AST_FUND_AMT + FOR_INCR_CCAP_RSRV_FUND_AMT + PRFS_DEV_INVST_FUND_AMT + CNTRBT_PCF_NON_RFNDBL_CAP_AMT
                                      +
                                      CASE WHEN (TOTAL_INCOMES - TOTAL_EXPENSES + INIT_RTAIN_ERN_AMT < 0)
                                                 OR
                                                (TOTAL_INCOMES - TOTAL_EXPENSES + INIT_RTAIN_ERN_AMT >= 0 AND INIT_RTAIN_ERN_AMT <  0)
                                                THEN 0
                                           WHEN (TOTAL_INCOMES - TOTAL_EXPENSES + INIT_RTAIN_ERN_AMT >= 0 AND INIT_RTAIN_ERN_AMT >= 0)
                                                THEN INIT_RTAIN_ERN_AMT
                                           END
                                      -
                                      CASE WHEN (TOTAL_INCOMES - TOTAL_EXPENSES + INIT_RTAIN_ERN_AMT >= 0)
                                                THEN 0
                                           WHEN (TOTAL_INCOMES - TOTAL_EXPENSES + INIT_RTAIN_ERN_AMT <  0)
                                                THEN -(TOTAL_INCOMES - TOTAL_EXPENSES + INIT_RTAIN_ERN_AMT)
                                           END
                                      - CNTRBT_CBV_CAP_AMT
                                      + FIN_RSRV_FUND_AMT
                                      + PCT_125_VS_RWA_GENL_PRVS_AMT
                                      - DUTO_FXASTRV_100_PCT_DCR_AMT) / RWA_AMT * 100, 2)
                                     )
                           END
                      END AS CAR
/*
                ,CASE WHEN RWA_AMT = 0 AND
                           (CCAP_AMT + INVST_INFRA_FIX_AST_FUND_AMT + CNTRBT_PCF_NON_RFNDBL_CAP_AMT + FOR_INCR_CCAP_RSRV_FUND_AMT + PRFS_DEV_INVST_FUND_AMT + FIN_RSRV_FUND_AMT
                                       - CNTRBT_CBV_CAP_AMT - DUTO_FXASTRV_100_PCT_DCR_AMT
                                       + PCT_125_VS_RWA_GENL_PRVS_AMT
                                       + ZZZ) <> 0
                           THEN 100
                      ELSE
                           CASE WHEN ROUND((CCAP_AMT + INVST_INFRA_FIX_AST_FUND_AMT + CNTRBT_PCF_NON_RFNDBL_CAP_AMT + FOR_INCR_CCAP_RSRV_FUND_AMT + PRFS_DEV_INVST_FUND_AMT + FIN_RSRV_FUND_AMT
                                                     - CNTRBT_CBV_CAP_AMT - DUTO_FXASTRV_100_PCT_DCR_AMT
                                                     + PCT_125_VS_RWA_GENL_PRVS_AMT
                                                     + ZZZ) / RWA_AMT * 100, 2) > 100
                                     THEN 100
                                ELSE ROUND((CCAP_AMT + INVST_INFRA_FIX_AST_FUND_AMT + CNTRBT_PCF_NON_RFNDBL_CAP_AMT + FOR_INCR_CCAP_RSRV_FUND_AMT + PRFS_DEV_INVST_FUND_AMT + FIN_RSRV_FUND_AMT
                                                     - CNTRBT_CBV_CAP_AMT - DUTO_FXASTRV_100_PCT_DCR_AMT
                                                     + PCT_125_VS_RWA_GENL_PRVS_AMT
                                                     + ZZZ) / RWA_AMT * 100, 2)
                                END
                      END AS CAR
*/
                ,SYSTIMESTAMP
          FROM  (SELECT A.BAS_YM
                       ,A.PCF_ID
                       ,B.CCAP_AMT
                       ,B.INVST_INFRA_FIX_AST_FUND_AMT
                       ,B.FOR_INCR_CCAP_RSRV_FUND_AMT
                       ,B.PRFS_DEV_INVST_FUND_AMT
                       ,B.CNTRBT_PCF_NON_RFNDBL_CAP_AMT
                       ,B.TOTAL_INCOMES
                       ,B.TOTAL_EXPENSES
                       ,B.INIT_RTAIN_ERN_AMT
                       ,B.CNTRBT_CBV_CAP_AMT
                       ,B.FIN_RSRV_FUND_AMT
                       ,B.GENERAL_PROVISION
                       ,B.DUTO_FXASTRV_100_PCT_DCR_AMT
                       ,B.AT_CBNK_PAY_DPST_AMT
                       ,C.FULL_GTD_VP_LN_BAL_20P
                       ,C.FULL_GTD_RL_EST_LN_BAL_50P
                       ,C.FULL_GTD_VP_LN_BAL
                       ,C.FULL_GTD_RL_EST_LN_BAL
                       ,B.FIX_AST_AMT
                       ,CASE WHEN B.GG > 0
                                  THEN B.EE - B.FF + B.GG
                             ELSE B.EE - B.FF
                             END AS TOT_AST_AMT
                       ,B.AT_CBNK_PAY_DPST_AMT + C.FULL_GTD_RL_EST_LN_BAL_50P + B.FIX_AST_AMT
                                               + B.EE - B.FF + CASE WHEN B.GG > 0 THEN B.GG ELSE 0 END
                                               - (B.HH + B.II + C.FULL_GTD_RL_EST_LN_BAL + C.FULL_GTD_VP_LN_BAL) AS RWA_AMT
                       ,ROUND(CASE WHEN B.GENERAL_PROVISION <= 0.0125 * (B.AT_CBNK_PAY_DPST_AMT + C.FULL_GTD_RL_EST_LN_BAL_50P + B.FIX_AST_AMT
                                                                                                + B.EE - B.FF + CASE WHEN B.GG > 0 THEN B.GG ELSE 0 END
                                                                                                - (B.HH + B.II + C.FULL_GTD_RL_EST_LN_BAL + C.FULL_GTD_VP_LN_BAL)
                                                                        )
                                        THEN B.GENERAL_PROVISION
                                   ELSE 0.0125 * (B.AT_CBNK_PAY_DPST_AMT + C.FULL_GTD_RL_EST_LN_BAL_50P + B.FIX_AST_AMT
                                                                         + B.EE - B.FF + CASE WHEN B.GG > 0 THEN B.GG ELSE 0 END
                                                                         - (B.HH + B.II + C.FULL_GTD_RL_EST_LN_BAL + C.FULL_GTD_VP_LN_BAL)
                                                 )
                                   END, 0) AS PCT_125_VS_RWA_GENL_PRVS_AMT
                       ,CASE WHEN (B.TOTAL_INCOMES - B.TOTAL_EXPENSES + B.INIT_RTAIN_ERN_AMT >= 0 AND B.INIT_RTAIN_ERN_AMT <  0)
                                  THEN 0
                             WHEN (B.TOTAL_INCOMES - B.TOTAL_EXPENSES + B.INIT_RTAIN_ERN_AMT >= 0 AND B.INIT_RTAIN_ERN_AMT >= 0)
                                  THEN B.INIT_RTAIN_ERN_AMT
                             WHEN (B.TOTAL_INCOMES - B.TOTAL_EXPENSES + B.INIT_RTAIN_ERN_AMT <  0)
                                  THEN -(B.TOTAL_INCOMES - B.TOTAL_EXPENSES + B.INIT_RTAIN_ERN_AMT)
                             END AS ZZZ
                       ,B.EE - B.FF + CASE WHEN B.GG > 0 THEN B.GG ELSE 0 END
                                    - (B.HH + B.II + C.FULL_GTD_RL_EST_LN_BAL + C.FULL_GTD_VP_LN_BAL) AS XXX
                 FROM   FULL_SET A LEFT OUTER JOIN FROM_BAL_SHEET B
                                                ON A.BAS_YM = B.BAS_YM
                                               AND A.PCF_ID = B.PCF_ID
                                   LEFT OUTER JOIN FROM_BALANCE C
                                                ON A.BAS_YM = C.BAS_YM
                                               AND A.PCF_ID = C.PCF_ID
                );

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