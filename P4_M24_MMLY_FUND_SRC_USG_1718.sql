create or replace PROCEDURE            "P4_M24_MMLY_FUND_SRC_USG_1718"
                 ( p_wkdate    IN  CHAR DEFAULT NULL
                 , p_stdate_01 IN  CHAR DEFAULT NULL
                 , p_eddate_01 IN  CHAR DEFAULT NULL
                 )
IS
    /*--------------------------------------------------------------------------
     * PROGRAM ID    : P4_M24_MMLY_FUND_SRC_USG_1718
     * PROGRAM NAME  : A program for insert data to TM24_MMLY_FUND_SRC_USG_A
     * SOURCE TABLE  : TM23_MMLY_BAL_SHET_A
                       TM24_MMLY_IDC_CALC_CAR_A
                       TM22_MMLY_CUST_DPST_TRANS_A
                       TB03_G32_006_TTGS_A
     * TARGET TABLE  : TM24_MMLY_FUND_SRC_USG_A
     * PROGRAMER     : HIEU
     * LAST MODIFICATION DATE : 2025-12-19
     * UNIQUENESS    : N/A
     * COMMENTS      : N/A
    ----------------------------------------------------------------------------
     * Revision History : 2025-12-19 : Create
     * Revision History :
    --------------------------------------------------------------------------*/
    ----------------------------------------------------------------------------
    --  Declare Local Variables
    ----------------------------------------------------------------------------
    v_wk_date                 CHAR(8) DEFAULT NULL ;
    v_st_date_01              CHAR(8) DEFAULT NULL ;
    v_end_date_01             CHAR(8) DEFAULT NULL ;

    v_program_id              VARCHAR2(100) DEFAULT 'P4_M24_MMLY_FUND_SRC_USG_1718' ;
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
            AND    INPT_RPT_ID IN ('G035841','G031341','G32_006_TTGS')

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
            FROM TM24_MMLY_FUND_SRC_USG_A T1
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

          INSERT INTO TM24_MMLY_FUND_SRC_USG_A
          (   BAS_YM
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
             ,TRGT_DATA_LST_MOD_TM
          )
          WITH
          FULL_SET AS
          (SELECT  T1.BAS_YM, T1.PCF_ID
           FROM   (SELECT BAS_YM, PCF_ID FROM TM23_MMLY_BAL_SHET_A WHERE BAS_YM = loop_bas_day.BAS_YM GROUP BY BAS_YM, PCF_ID
                   UNION
                   SELECT BAS_YM, PCF_ID FROM TM22_MMLY_CUST_DPST_TRANS_A WHERE BAS_YM = loop_bas_day.BAS_YM GROUP BY BAS_YM, PCF_ID
                   UNION
                   SELECT BAS_YM, PCF_ID FROM TM24_MMLY_IDC_CALC_CAR_A WHERE BAS_YM = loop_bas_day.BAS_YM GROUP BY BAS_YM, PCF_ID
                   UNION
                   SELECT BAS_YM, PCF_ID FROM TB03_G32_006_TTGS_A WHERE BAS_YM = loop_bas_day.BAS_YM GROUP BY BAS_YM, PCF_ID
                  ) T1
          ),
          FROM_BAL_SHEET AS
          (SELECT BAS_YM,
                  PCF_ID,
                  SUM(CASE WHEN PCF_COA_ID = '42'
                                THEN CLO_CR_BAL
                           ELSE 0
                           END) AS CUST_DPST_BAL,
                  SUM(CASE WHEN PCF_COA_ID IN ('41512','41513','41592','41593')
                                THEN CLO_CR_BAL
                           ELSE 0
                           END) AS CBV_BOR_EXCL_SFTY_FUND_AMT,
                  SUM(CASE WHEN PCF_COA_ID IN ('41511','41591')
                                THEN CLO_CR_BAL
                           ELSE 0
                           END) AS FR_SFTY_FUND_BOR_AMT,
                  SUM(CASE WHEN PCF_COA_ID IN ('41519','41599')
                                THEN CLO_CR_BAL
                           ELSE 0
                           END) AS FR_OTHR_INST_BOR_AMT,
                  SUM(CASE WHEN PCF_COA_ID = '601'
                                THEN CLO_CR_BAL
                           ELSE 0
                           END) AS CCAP_AMT,
                  SUM(CASE WHEN PCF_COA_ID = '2'
                                THEN CLO_DR_BAL
                           ELSE 0
                           END) AS CUST_LN_BAL,
                  SUM(CASE WHEN PCF_COA_ID = '10'
                                THEN CLO_DR_BAL
                           ELSE 0
                           END) AS IN_HAND_CSH_AMT,
                  SUM(CASE WHEN PCF_COA_ID IN ('13111','13121')
                                THEN CLO_DR_BAL
                           ELSE 0
                           END) AS AT_CBV_SVG_AMT,
                  SUM(CASE WHEN PCF_COA_ID IN ('601','61')
                                THEN CLO_CR_BAL
                           ELSE 0
                           END) AS DD,
                  SUM(CASE WHEN PCF_COA_ID = '34401'
                                THEN CLO_DR_BAL
                           ELSE 0
                           END) AS EE,
                  SUM(CASE WHEN PCF_COA_ID IN ('4211','4231')
                                THEN CLO_CR_BAL
                           ELSE 0
                           END) AS NON_TRM_DPST_AMT
           FROM   TM23_MMLY_BAL_SHET_A
           WHERE  BAS_YM = loop_bas_day.BAS_YM
           GROUP BY BAS_YM, PCF_ID
          ),
          TERM_DEPOSIT AS
          (SELECT BAS_YM,
                  PCF_ID,
                  SUM(OVER_1_YR_TRM_DPST_AMT)  AS OVR_1_YR_TRM_DPST_AMT,
                  SUM(LSTHN_1_YR_TRM_DPST_AMT) AS LSTHN_1_YR_TRM_DPST_AMT
           FROM   TM22_MMLY_CUST_DPST_TRANS_A
           WHERE  BAS_YM = loop_bas_day.BAS_YM
           GROUP BY BAS_YM, PCF_ID
          ),
          MED_AND_LONG_TERM_LENDING AS
          (SELECT BAS_YM,
                  PCF_ID,
                  SUM(CASE WHEN INDC_CODE = 'G32006TTGS_001' THEN INDC_VALUE ELSE 0 END) AS SHORT_TERM_CAPITAL,
                  SUM(CASE WHEN INDC_CODE = 'G32006TTGS_002' THEN INDC_VALUE ELSE 0 END) AS MED_AND_LONG_TERM_CAPITAL,
                  SUM(CASE WHEN INDC_CODE = 'G32006TTGS_003' THEN INDC_VALUE ELSE 0 END) AS MTLTLD_AMT
           FROM   TB03_G32_006_TTGS_A
           WHERE  BAS_YM = loop_bas_day.BAS_YM
           GROUP BY BAS_YM, PCF_ID
          ),
          ASSET_EQUITY AS
          (SELECT BAS_YM,
                  PCF_ID,
                  TOT_AST_AMT,
                  EQT_AMT
           FROM   TM24_MMLY_IDC_CALC_CAR_A
           WHERE  BAS_YM = loop_bas_day.BAS_YM
          )
          SELECT A.BAS_YM
                ,A.PCF_ID
                ,B.CUST_DPST_BAL
                ,B.CBV_BOR_EXCL_SFTY_FUND_AMT
                ,B.FR_SFTY_FUND_BOR_AMT
                ,B.FR_OTHR_INST_BOR_AMT
                ,E.EQT_AMT
                ,B.CCAP_AMT
                ,E.TOT_AST_AMT - B.CUST_DPST_BAL - B.CBV_BOR_EXCL_SFTY_FUND_AMT - B.FR_SFTY_FUND_BOR_AMT - B.FR_OTHR_INST_BOR_AMT - E.EQT_AMT AS OTHR_SRC_FUND_AMT
                ,B.CUST_LN_BAL
                ,B.IN_HAND_CSH_AMT
                ,B.AT_CBV_SVG_AMT
                ,E.TOT_AST_AMT - B.CUST_LN_BAL - B.IN_HAND_CSH_AMT - B.AT_CBV_SVG_AMT AS OTHR_USE_FUND_AMT
                ,B.DD - B.EE AS AFT_DED_CCAP_RSRV_FUND_AMT
                ,C.OVR_1_YR_TRM_DPST_AMT
                ,D.MED_AND_LONG_TERM_CAPITAL - (B.DD - B.EE) - C.OVR_1_YR_TRM_DPST_AMT AS OVR_1_YR_OTHR_CI_BOR_AMT
                ,B.NON_TRM_DPST_AMT
                ,C.LSTHN_1_YR_TRM_DPST_AMT
                ,D.SHORT_TERM_CAPITAL - B.NON_TRM_DPST_AMT - C.LSTHN_1_YR_TRM_DPST_AMT AS LSTHN_1_YR_OTHR_CI_BOR_AMT
                ,D.MTLTLD_AMT
                ,CASE WHEN D.MTLTLD_AMT - D.MED_AND_LONG_TERM_CAPITAL = 0 THEN 0
                      WHEN D.MTLTLD_AMT - D.MED_AND_LONG_TERM_CAPITAL <> 0 AND D.SHORT_TERM_CAPITAL = 0 THEN NULL
                      ELSE ROUND(((D.MTLTLD_AMT - D.MED_AND_LONG_TERM_CAPITAL) / D.SHORT_TERM_CAPITAL) * 100, 2)
                      END  AS STFND_USE_MTLTLD_RTO
                ,SYSTIMESTAMP
          FROM   FULL_SET A LEFT OUTER JOIN FROM_BAL_SHEET B
                                         ON A.BAS_YM = B.BAS_YM
                                        AND A.PCF_ID = B.PCF_ID
                            LEFT OUTER JOIN TERM_DEPOSIT C
                                         ON A.BAS_YM = C.BAS_YM
                                        AND A.PCF_ID = C.PCF_ID
                            LEFT OUTER JOIN MED_AND_LONG_TERM_LENDING D
                                         ON A.BAS_YM = D.BAS_YM
                                        AND A.PCF_ID = D.PCF_ID
                            LEFT OUTER JOIN ASSET_EQUITY E
                                         ON A.BAS_YM = E.BAS_YM
                                        AND A.PCF_ID = E.PCF_ID;

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