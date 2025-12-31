create or replace PROCEDURE            "P4_M27_MMLY_PCF_RM_SMR_IDC_1718"
                 ( p_wkdate    IN  CHAR DEFAULT NULL
                 , p_stdate_01 IN  CHAR DEFAULT NULL
                 , p_eddate_01 IN  CHAR DEFAULT NULL
                 )
IS
    /*--------------------------------------------------------------------------
     * PROGRAM ID    : P4_M27_MMLY_PCF_RM_SMR_IDC_1718
     * PROGRAM NAME  : A program for insert data to TM27_MMLY_PCF_RM_SMR_IDC_A
     * SOURCE TABLE  : TM27_MMLY_PCF_RM_DTL_IDC_A
     * TARGET TABLE  : TM27_MMLY_PCF_RM_SMR_IDC_A
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

    v_program_id              VARCHAR2(100) DEFAULT 'P4_M27_MMLY_PCF_RM_SMR_IDC_1718' ;
    v_program_type_name       VARCHAR2(100) DEFAULT 'Stored Procedure' ;
    v_step_code               VARCHAR2(3) ;
    v_step_desc               VARCHAR2(100) ;
    v_time                    DATE ;
    v_cnt                     NUMBER DEFAULT 0 ;
    v_seq                     CHAR(14) DEFAULT ' '||to_char(SEQ_UID.NEXTVAL,'000000000000');
    v_risk_rto_1              NUMBER; --TY LE TONG DU NO VOI 1 KH LON NHAT
    v_risk_rto_1n              NUMBER; --TY LE TONG DU NO VOI 1 NHOM KH LON NHAT
    ----------------------------------------------------------------------------
    --  Declare Cursor
    ----------------------------------------------------------------------------

   CURSOR v_sbmt_day(v_dt1 VARCHAR2, v_dt2 VARCHAR2, v_dt3 VARCHAR2) IS

   /* SELECT BAS_YM
    FROM   TM00_MMLY_CAL_D
    WHERE  BAS_YM BETWEEN '202401' AND '202410'
    ORDER BY 1; */

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
            FROM TM27_MMLY_PCF_RM_SMR_IDC_A T1
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
       -- DBMS_OUTPUT.put_line(loop_bas_day.BAS_YM);
        --lay gia tri cho bien v_risk_rto_1 va v_risk_rto_1n
        IF (loop_bas_day.BAS_YM < '202407') THEN
           v_risk_rto_1 := 15;
           v_risk_rto_1n := 25;
        ELSIF (loop_bas_day.BAS_YM >= '202407' AND loop_bas_day.BAS_YM < '202601') THEN
           v_risk_rto_1 := 14;
           v_risk_rto_1n := 23;
        ELSIF (loop_bas_day.BAS_YM >= '202601' AND loop_bas_day.BAS_YM < '202701') THEN
           v_risk_rto_1 := 13;
           v_risk_rto_1n := 21;
        ELSIF (loop_bas_day.BAS_YM >= '202701' AND loop_bas_day.BAS_YM < '202801') THEN
           v_risk_rto_1 := 12;
           v_risk_rto_1n := 19;
        ELSIF (loop_bas_day.BAS_YM >= '202801' AND loop_bas_day.BAS_YM < '202901') THEN
           v_risk_rto_1 := 11;
           v_risk_rto_1n := 17;
        ELSE
           v_risk_rto_1 := 10;
           v_risk_rto_1n := 15;
        END IF;

        --INSERT DU LIEU VAO BANG
          INSERT INTO TM27_MMLY_PCF_RM_SMR_IDC_A
          (   BAS_YM
             ,PCF_ID
             ,TOT_CBV_BOR_AMT
             ,TOT_AST_AMT
             ,CAR_VAL
             ,TDP_VS_OWNR_EQT_VAL
             ,OWNR_EQT_VS_CCAP_VAL
             ,THIS_MM_SLVNCY_RISK_SCR_WVAL
             ,PREV_MM_SLVNCY_RISK_SCR_WVAL
             ,SLVNCY_RISK_MISS_IDC_CNT
             ,G35_BAD_DBT_VS_TOT_RISK_LN_VAL
             ,TOT_RISK_VS_BAD_DBT_PRVS_VAL
             ,SNGL_CUST_BIG_LN_VS_EQT_VAL
             ,RLT_GRP_BIG_LN_VS_EQT_VAL
             ,ACRD_INT_VS_TOT_RISK_G1_LN_VAL
             ,THIS_MM_CR_RISK_SCR_WVAL
             ,PREV_MM_CR_RISK_SCR_WVAL
             ,CR_RISK_MISS_IDC_CNT
             ,L12M_NWD_LQDTY_OVR100P_VAL
             ,L12M_N7WD_LQDTY_OVR100P_VAL
             ,STFND_USE_MTLTLD_VAL
             ,TOT_CBV_BOR_VS_TOT_AST_VAL
             ,THIS_MM_LQDTY_RISK_SCR_WVAL
             ,PREV_MM_LQDTY_RISK_SCR_WVAL
             ,LQDTY_RISK_MISS_IDC_CNT
             ,TOT_IHAND_CSH_VS_TOT_AST_VAL
             ,RISK_FIX_AST_VAL
             ,RISK_OTHR_INST_SVG_VAL
             ,THIS_MM_OPRT_RISK_SCR_WVAL
             ,PREV_MM_OPRT_RISK_SCR_WVAL
             ,OPRT_RISK_MISS_IDC_CNT
             ,THIS_MM_TOT_RISK_SCR_WVAL
             ,PREV_MM_TOT_RISK_SCR_WVAL
             ,TOT_RISK_MISS_IDC_CNT
             ,TRGT_DATA_LST_MOD_TM
          )
          WITH
/*
          FULL_SET AS
          (SELECT T1.BAS_YM, T2.PCF_ID
           FROM   TM00_MMLY_CAL_D T1 INNER JOIN (SELECT PCF_ID
                                                 FROM   TM00_PCF_D
                                               --WHERE IN_OPRT_YN = 'Y'
                                                ) T2
                                             ON 1=1
           WHERE  T1.BAS_YM = loop_bas_day.BAS_YM
          ),

          (SELECT  T1.BAS_YM, T1.PCF_ID
           FROM   (SELECT BAS_YM, PCF_ID
                   FROM   TM23_MMLY_BAL_SHET_A
                   WHERE  BAS_YM = loop_bas_day.BAS_YM
                   GROUP BY BAS_YM, PCF_ID
                  ) T1
          ),
*/
          REPORT_02_DTL AS
          (SELECT BAS_YM
                 ,PCF_ID
                 ,TOT_CBV_BOR_AMT
                 ,TOT_AST_AMT
                 ,CASE WHEN CAR IS NULL THEN 30
                       WHEN CAR >= 10 THEN 100
                       WHEN CAR >=  9 AND CAR < 10 THEN 80
                       WHEN CAR >=  8 AND CAR <  9 THEN 60
                       WHEN CAR <   8 THEN 0
                       END AS CAR_VAL
                 ,CASE WHEN TDP_VS_OWNR_EQT_RTO IS NULL THEN 30
                       WHEN TDP_VS_OWNR_EQT_RTO <   5 THEN 0
                       WHEN TDP_VS_OWNR_EQT_RTO >=  5 AND TDP_VS_OWNR_EQT_RTO < 10 THEN 50
                       WHEN TDP_VS_OWNR_EQT_RTO >= 15 AND TDP_VS_OWNR_EQT_RTO < 20 THEN 50
                       WHEN TDP_VS_OWNR_EQT_RTO >= 10 AND TDP_VS_OWNR_EQT_RTO < 15 THEN 100
                       WHEN TDP_VS_OWNR_EQT_RTO >= 20 THEN 0
                       END AS TDP_VS_OWNR_EQT_VAL
                 ,CASE WHEN OWNR_EQT_VS_CCAP_RTO IS NULL THEN 30
                       WHEN OWNR_EQT_VS_CCAP_RTO <= 0 THEN 0
                       WHEN OWNR_EQT_VS_CCAP_RTO >  0 AND OWNR_EQT_VS_CCAP_RTO <= 1 THEN 50
                       WHEN OWNR_EQT_VS_CCAP_RTO >  1 THEN 100
                       END AS OWNR_EQT_VS_CCAP_VAL
                 ,CASE WHEN G35_BAD_DBT_AMT IS NULL OR TOT_RISK_LN_BAL IS NULL THEN 30
                       WHEN G35_BAD_DBT_AMT = 0 AND TOT_RISK_LN_BAL = 0 THEN 0
                       WHEN G35_BAD_DBT_AMT <> 0 AND TOT_RISK_LN_BAL = 0 THEN 30
                       WHEN G35_BAD_DBT_AMT = 0 AND TOT_RISK_LN_BAL <> 0 THEN 100
                       WHEN G35_BAD_DBT_VS_TOT_RISK_LN_RTO <= 0 THEN 100
                       WHEN G35_BAD_DBT_VS_TOT_RISK_LN_RTO > 0 AND G35_BAD_DBT_VS_TOT_RISK_LN_RTO <= 1 THEN 80
                       WHEN G35_BAD_DBT_VS_TOT_RISK_LN_RTO > 1 AND G35_BAD_DBT_VS_TOT_RISK_LN_RTO <= 2 THEN 60
                       WHEN G35_BAD_DBT_VS_TOT_RISK_LN_RTO > 2 AND G35_BAD_DBT_VS_TOT_RISK_LN_RTO <= 3 THEN 10
                       WHEN G35_BAD_DBT_VS_TOT_RISK_LN_RTO > 3 AND G35_BAD_DBT_VS_TOT_RISK_LN_RTO <= 4 THEN 5
                       WHEN G35_BAD_DBT_VS_TOT_RISK_LN_RTO > 4 THEN 0
                       END AS G35_BAD_DBT_VS_TOT_RISK_LN_VAL
                 ,CASE WHEN TOT_RISK_VS_BAD_DBT_PRVS_RTO IS NULL THEN 30
                       WHEN TOT_RISK_VS_BAD_DBT_PRVS_RTO >= 100 THEN 100
                       WHEN TOT_RISK_VS_BAD_DBT_PRVS_RTO >= 50 AND TOT_RISK_VS_BAD_DBT_PRVS_RTO < 100 THEN 80
                       WHEN TOT_RISK_VS_BAD_DBT_PRVS_RTO >= 20 AND TOT_RISK_VS_BAD_DBT_PRVS_RTO < 50  THEN 60
                       WHEN TOT_RISK_VS_BAD_DBT_PRVS_RTO <  20 THEN 0
                       END AS TOT_RISK_VS_BAD_DBT_PRVS_VAL
                 ,CASE WHEN SNGL_CUST_BIG_LN_VS_EQT_RTO IS NULL THEN 30
                       WHEN SNGL_CUST_BIG_LN_VS_EQT_RTO >  v_risk_rto_1 THEN 0
                       WHEN SNGL_CUST_BIG_LN_VS_EQT_RTO <= v_risk_rto_1 AND SNGL_CUST_BIG_LN_VS_EQT_RTO >= 0 THEN 100
                       WHEN SNGL_CUST_BIG_LN_VS_EQT_RTO <   0 THEN 0
                       END AS SNGL_CUST_BIG_LN_VS_EQT_VAL
                 ,CASE WHEN RLT_GRP_BIG_LN_VS_EQT_RTO IS NULL THEN 30
                       WHEN RLT_GRP_BIG_LN_VS_EQT_RTO >  v_risk_rto_1n THEN 0
                       WHEN RLT_GRP_BIG_LN_VS_EQT_RTO <= v_risk_rto_1n AND RLT_GRP_BIG_LN_VS_EQT_RTO >= 0 THEN 100
                       WHEN RLT_GRP_BIG_LN_VS_EQT_RTO <   0 THEN 0
                       END AS RLT_GRP_BIG_LN_VS_EQT_VAL
                 ,CASE WHEN ACRD_INT_AMT IS NULL OR TOT_RISK_G1_LN_BAL IS NULL THEN 30
                       WHEN ACRD_INT_AMT = 0 AND TOT_RISK_G1_LN_BAL = 0 THEN 0
                       WHEN ACRD_INT_AMT <> 0 AND TOT_RISK_G1_LN_BAL = 0 THEN 30
                       WHEN ACRD_INT_AMT = 0 AND TOT_RISK_G1_LN_BAL <> 0 THEN 100
                       WHEN ACRD_INT_VS_TOT_RISK_G1_LN_RTO >  5 THEN 0
                       WHEN ACRD_INT_VS_TOT_RISK_G1_LN_RTO <= 5 THEN 100
                       END AS ACRD_INT_VS_TOT_RISK_G1_LN_VAL
                 ,CASE WHEN L12M_NWD_LQDTY_OVR100P_CNT = 12
                       THEN 100
                       WHEN L12M_NWD_LQDTY_OVR100P_CNT = 11
                       THEN 80
                       WHEN L12M_NWD_LQDTY_OVR100P_CNT = 10
                       THEN 60
                       WHEN L12M_NWD_LQDTY_OVR100P_CNT <= 9
                       THEN 0
                       WHEN L12M_NWD_LQDTY_OVR100P_CNT IS NULL
                       THEN 30
                       END AS L12M_NWD_LQDTY_OVR100P_VAL
                 ,CASE WHEN L12M_N7WD_LQDTY_OVR100P_CNT = 12
                       THEN 100
                       WHEN L12M_N7WD_LQDTY_OVR100P_CNT = 11
                       THEN 80
                       WHEN L12M_N7WD_LQDTY_OVR100P_CNT = 10
                       THEN 60
                       WHEN L12M_N7WD_LQDTY_OVR100P_CNT <= 9
                       THEN 0
                       WHEN L12M_N7WD_LQDTY_OVR100P_CNT IS NULL
                       THEN 30
                       END AS L12M_N7WD_LQDTY_OVR100P_VAL
                 ,CASE WHEN STFND_USE_MTLTLD_RTO IS NULL THEN 30
                       WHEN STFND_USE_MTLTLD_RTO > 30   THEN 0
                       WHEN STFND_USE_MTLTLD_RTO <= 30 AND STFND_USE_MTLTLD_RTO > 25  THEN 60
                       WHEN STFND_USE_MTLTLD_RTO <= 25 AND STFND_USE_MTLTLD_RTO >= 20  THEN 80
                       WHEN STFND_USE_MTLTLD_RTO < 20 THEN 100
                       END AS STFND_USE_MTLTLD_VAL
                 ,CASE WHEN TOT_CBV_BOR_VS_TOT_AST_RTO IS NULL THEN 30
                       WHEN TOT_CBV_BOR_VS_TOT_AST_RTO >  40   THEN 0
                       WHEN TOT_CBV_BOR_VS_TOT_AST_RTO <= 40 AND TOT_CBV_BOR_VS_TOT_AST_RTO > 30  THEN 60
                       WHEN TOT_CBV_BOR_VS_TOT_AST_RTO <= 30 THEN 100
                       END AS TOT_CBV_BOR_VS_TOT_AST_VAL
                 ,CASE WHEN TOT_IHAND_CSH_VS_TOT_AST_RTO IS NULL THEN 30
                       WHEN TOT_IHAND_CSH_VS_TOT_AST_RTO >  10   THEN 0
                       WHEN TOT_IHAND_CSH_VS_TOT_AST_RTO <= 10 AND TOT_IHAND_CSH_VS_TOT_AST_RTO >= 5  THEN 60
                       WHEN TOT_IHAND_CSH_VS_TOT_AST_RTO <   5 THEN 100
                       END AS TOT_IHAND_CSH_VS_TOT_AST_VAL
                 ,CASE WHEN RISK_FIX_AST_RTO IS NULL THEN 30
                       WHEN RISK_FIX_AST_RTO >  50   THEN 0
                       WHEN RISK_FIX_AST_RTO <= 50   THEN 100
                       END AS RISK_FIX_AST_VAL
                 ,CASE WHEN RISK_OTHR_INST_SVG_AMT IS NULL THEN 30
                       WHEN RISK_OTHR_INST_SVG_AMT <= 0    THEN 100
                       WHEN RISK_OTHR_INST_SVG_AMT >  0    THEN 0
                       END AS RISK_OTHR_INST_SVG_VAL
           FROM   TM27_MMLY_PCF_RM_DTL_IDC_A
           WHERE  BAS_YM = loop_bas_day.BAS_YM
          ),
          REPORT_02_MISS_IDC AS
          (SELECT BAS_YM
                 ,PCF_ID

                 ,CASE WHEN CAR IS NULL THEN 1 ELSE 0 END
                  +
                  CASE WHEN OWNR_EQT_VS_CCAP_RTO IS NULL THEN 1 ELSE 0 END
                  +
                  CASE WHEN TDP_VS_OWNR_EQT_RTO IS NULL THEN 1 ELSE 0 END
                  AS SLVNCY_RISK_MISS_IDC_CNT

                 ,CASE WHEN G35_BAD_DBT_VS_TOT_RISK_LN_RTO IS NULL THEN 1 ELSE 0 END
                  +
                  CASE WHEN TOT_RISK_VS_BAD_DBT_PRVS_RTO IS NULL THEN 1 ELSE 0 END
                  +
                  CASE WHEN SNGL_CUST_BIG_LN_VS_EQT_RTO IS NULL THEN 1 ELSE 0 END
                  +
                  CASE WHEN RLT_GRP_BIG_LN_VS_EQT_RTO IS NULL THEN 1 ELSE 0 END
                  +
                  CASE WHEN ACRD_INT_VS_TOT_RISK_G1_LN_RTO IS NULL THEN 1 ELSE 0 END
                  AS CR_RISK_MISS_IDC_CNT

                 ,CASE WHEN L12M_NWD_LQDTY_OVR100P_CNT IS NULL THEN 1 ELSE 0 END
                  +
                  CASE WHEN L12M_N7WD_LQDTY_OVR100P_CNT IS NULL THEN 1 ELSE 0 END
                  +
                  CASE WHEN STFND_USE_MTLTLD_RTO IS NULL THEN 1 ELSE 0 END
                  +
                  CASE WHEN TOT_CBV_BOR_VS_TOT_AST_RTO IS NULL THEN 1 ELSE 0 END
                  AS LQDTY_RISK_MISS_IDC_CNT

                 ,CASE WHEN TOT_IHAND_CSH_VS_TOT_AST_RTO IS NULL THEN 1 ELSE 0 END
                  +
                  CASE WHEN RISK_FIX_AST_RTO IS NULL THEN 1 ELSE 0 END
                  +
                  CASE WHEN RISK_OTHR_INST_SVG_AMT IS NULL THEN 1 ELSE 0 END
                  AS OPRT_RISK_MISS_IDC_CNT

           FROM   TM27_MMLY_PCF_RM_DTL_IDC_A
           WHERE  BAS_YM = loop_bas_day.BAS_YM
          ),
          EACH_RISK_SCR_WVAL AS
          (SELECT T1.BAS_YM
                 ,T1.PCF_ID
                 ,ROUND(
                        ((T1.CAR_VAL * 0.15)              +
                         (T1.OWNR_EQT_VS_CCAP_VAL * 0.06) +
                         (T1.TDP_VS_OWNR_EQT_VAL * 0.04)
                        ), 2)                                          AS THIS_MM_SLVNCY_RISK_SCR_WVAL
                 ,T2.THIS_MM_SLVNCY_RISK_SCR_WVAL                      AS PREV_MM_SLVNCY_RISK_SCR_WVAL
                 ,ROUND(
                        ((T1.G35_BAD_DBT_VS_TOT_RISK_LN_VAL * 0.135) +
                         (T1.TOT_RISK_VS_BAD_DBT_PRVS_VAL * 0.0675)  +
                         (T1.SNGL_CUST_BIG_LN_VS_EQT_VAL * 0.09)     +
                         (T1.RLT_GRP_BIG_LN_VS_EQT_VAL * 0.0675)     +
                         (T1.ACRD_INT_VS_TOT_RISK_G1_LN_VAL * 0.09)
                        ), 2)                                          AS THIS_MM_CR_RISK_SCR_WVAL
                 ,T2.THIS_MM_CR_RISK_SCR_WVAL                          AS PREV_MM_CR_RISK_SCR_WVAL
                 ,ROUND(
                        ((T1.L12M_NWD_LQDTY_OVR100P_VAL * 0.07)  +
                         (T1.L12M_N7WD_LQDTY_OVR100P_VAL * 0.07) +
                         (T1.STFND_USE_MTLTLD_VAL * 0.04)        +
                         (T1.TOT_CBV_BOR_VS_TOT_AST_VAL * 0.02)
                        ), 2)                                          AS THIS_MM_LQDTY_RISK_SCR_WVAL
                 ,T2.THIS_MM_LQDTY_RISK_SCR_WVAL                       AS PREV_MM_LQDTY_RISK_SCR_WVAL
                 ,ROUND(
                        ((T1.TOT_IHAND_CSH_VS_TOT_AST_VAL * 0.05) +
                         (T1.RISK_FIX_AST_VAL * 0.025)            +
                         (T1.RISK_OTHR_INST_SVG_VAL * 0.025)
                        ), 2)                                          AS THIS_MM_OPRT_RISK_SCR_WVAL
                 ,T2.THIS_MM_OPRT_RISK_SCR_WVAL                        AS PREV_MM_OPRT_RISK_SCR_WVAL
           FROM   REPORT_02_DTL T1 LEFT OUTER JOIN TM27_MMLY_PCF_RM_SMR_IDC_A T2
                                                ON T2.BAS_YM = TO_CHAR(ADD_MONTHS(TO_DATE(T1.BAS_YM, 'YYYYMM'), -1), 'YYYYMM')
                                               AND T2.PCF_ID = T1.PCF_ID
           WHERE  T1.BAS_YM = loop_bas_day.BAS_YM
          ),
          TOT_RISK_SCR AS
          (SELECT T1.BAS_YM
                 ,T1.PCF_ID
                 ,ROUND((T1.THIS_MM_SLVNCY_RISK_SCR_WVAL + T1.THIS_MM_CR_RISK_SCR_WVAL +
                         T1.THIS_MM_LQDTY_RISK_SCR_WVAL  + T1.THIS_MM_OPRT_RISK_SCR_WVAL), 2) AS THIS_MM_TOT_RISK_SCR_WVAL
                 ,T2.THIS_MM_TOT_RISK_SCR_WVAL                             AS PREV_MM_TOT_RISK_SCR_WVAL
                 ,T3.SLVNCY_RISK_MISS_IDC_CNT + T3.CR_RISK_MISS_IDC_CNT +
                  T3.LQDTY_RISK_MISS_IDC_CNT  + T3.OPRT_RISK_MISS_IDC_CNT  AS TOT_RISK_MISS_IDC_CNT
           FROM   EACH_RISK_SCR_WVAL T1 LEFT OUTER JOIN TM27_MMLY_PCF_RM_SMR_IDC_A T2
                                                     ON T2.BAS_YM = TO_CHAR(ADD_MONTHS(TO_DATE(T1.BAS_YM, 'YYYYMM'), -1), 'YYYYMM')
                                                    AND T2.PCF_ID = T1.PCF_ID
                                        LEFT OUTER JOIN REPORT_02_MISS_IDC T3
                                                     ON T3.BAS_YM = T1.BAS_YM
                                                    AND T3.PCF_ID = T1.PCF_ID
           WHERE  T1.BAS_YM = loop_bas_day.BAS_YM
          )
          SELECT T1.BAS_YM
                ,T1.PCF_ID
                ,T1.TOT_CBV_BOR_AMT
                ,T1.TOT_AST_AMT
                ,T1.CAR_VAL
                ,T1.TDP_VS_OWNR_EQT_VAL
                ,T1.OWNR_EQT_VS_CCAP_VAL
                ,T4.THIS_MM_SLVNCY_RISK_SCR_WVAL
                ,T4.PREV_MM_SLVNCY_RISK_SCR_WVAL
                ,T3.SLVNCY_RISK_MISS_IDC_CNT
                ,T1.G35_BAD_DBT_VS_TOT_RISK_LN_VAL
                ,T1.TOT_RISK_VS_BAD_DBT_PRVS_VAL
                ,T1.SNGL_CUST_BIG_LN_VS_EQT_VAL
                ,T1.RLT_GRP_BIG_LN_VS_EQT_VAL
                ,T1.ACRD_INT_VS_TOT_RISK_G1_LN_VAL
                ,T4.THIS_MM_CR_RISK_SCR_WVAL
                ,T4.PREV_MM_CR_RISK_SCR_WVAL
                ,T3.CR_RISK_MISS_IDC_CNT
                ,T1.L12M_NWD_LQDTY_OVR100P_VAL
                ,T1.L12M_N7WD_LQDTY_OVR100P_VAL
                ,T1.STFND_USE_MTLTLD_VAL
                ,T1.TOT_CBV_BOR_VS_TOT_AST_VAL
                ,T4.THIS_MM_LQDTY_RISK_SCR_WVAL
                ,T4.PREV_MM_LQDTY_RISK_SCR_WVAL
                ,T3.LQDTY_RISK_MISS_IDC_CNT
                ,T1.TOT_IHAND_CSH_VS_TOT_AST_VAL
                ,T1.RISK_FIX_AST_VAL
                ,T1.RISK_OTHR_INST_SVG_VAL
                ,T4.THIS_MM_OPRT_RISK_SCR_WVAL
                ,T4.PREV_MM_OPRT_RISK_SCR_WVAL
                ,T3.OPRT_RISK_MISS_IDC_CNT
                ,T5.THIS_MM_TOT_RISK_SCR_WVAL
                ,T5.PREV_MM_TOT_RISK_SCR_WVAL
                ,T5.TOT_RISK_MISS_IDC_CNT
                ,SYSTIMESTAMP
          FROM  REPORT_02_DTL T1 LEFT OUTER JOIN REPORT_02_MISS_IDC T2
                                          ON T2.BAS_YM = T1.BAS_YM
                                         AND T2.PCF_ID = T1.PCF_ID
                             LEFT OUTER JOIN EACH_RISK_SCR_WVAL T3
                                          ON T3.BAS_YM = T1.BAS_YM
                                         AND T3.PCF_ID = T1.PCF_ID
                             LEFT OUTER JOIN TOT_RISK_SCR T4
                                          ON T4.BAS_YM = T1.BAS_YM
                                         AND T4.PCF_ID = T1.PCF_ID;

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