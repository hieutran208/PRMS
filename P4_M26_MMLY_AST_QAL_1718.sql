create or replace PROCEDURE            "P4_M26_MMLY_AST_QAL_1718"
                 ( p_wkdate    IN  CHAR DEFAULT NULL
                 , p_stdate_01 IN  CHAR DEFAULT NULL
                 , p_eddate_01 IN  CHAR DEFAULT NULL
                 )
IS
    /*--------------------------------------------------------------------------
     * PROGRAM ID    : P4_M26_MMLY_AST_QAL_1718
     * PROGRAM NAME  : A program for insert data to TM26_MMLY_AST_QAL_A
     * SOURCE TABLE  : TM23_MMLY_BAL_SHET_A
                       TB04_G32_001_TTGS_01_A
                       TB08_G32_002_TTGS_A
     * TARGET TABLE  : TM26_MMLY_AST_QAL_A
     * PROGRAMER     : HIEU
     * LAST MODIFICATION DATE : 2025-12-22
     * UNIQUENESS    : N/A
     * COMMENTS      : N/A
    ----------------------------------------------------------------------------
     * Revision History : 2025-12-22 : Create
     * Revision History :
    --------------------------------------------------------------------------*/
    ----------------------------------------------------------------------------
    --  Declare Local Variables
    ----------------------------------------------------------------------------
    v_wk_date                 CHAR(8) DEFAULT NULL ;
    v_st_date_01              CHAR(8) DEFAULT NULL ;
    v_end_date_01             CHAR(8) DEFAULT NULL ;

    v_program_id              VARCHAR2(100) DEFAULT 'P4_M26_MMLY_AST_QAL_1718' ;
    v_program_type_name       VARCHAR2(100) DEFAULT 'Stored Procedure' ;
    v_step_code               VARCHAR2(3) ;
    v_step_desc               VARCHAR2(100) ;
    v_time                    DATE ;
    v_cnt                     NUMBER DEFAULT 0 ;
    v_seq                     CHAR(14) DEFAULT ' '||to_char(SEQ_UID.NEXTVAL,'000000000000');
    ----------------------------------------------------------------------------
    --  Declare Cursor
    ----------------------------------------------------------------------------

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

    DELETE
      FROM TM26_MMLY_AST_QAL_A
     WHERE BAS_YM IN (SELECT DISTINCT TRIM(DATA_BAS_DAY) AS BAS_YM
                      FROM   TBSM_INPT_RPT_SUBMIT_L
                      WHERE  BTCH_BAS_DAY = v_st_date_01
                      AND    INPT_RPT_ID IN ('G32_020_TTGS_02','G32_001_TTGS_01','G32_001_TTGS_02','G32_002_TTGS')
                      );

    ----------------------------------------------------------------------------
    --  Transaction Log
    ----------------------------------------------------------------------------
    v_step_code    := '010' ;
    v_step_desc    := v_wk_date || v_seq || ' : Delete Historical Time-Series Data' ;
    v_time         := SYSDATE ;

    P1_BSM_PROG_EXEC_LOG(v_program_id, v_program_type_name, v_step_code, v_step_desc, v_time, sql%rowcount, NULL, NULL) ;
    
    COMMIT ;
    ----------------------------------------------------------------------------
    --  1.2 Inserting Data verification results
    ----------------------------------------------------------------------------
    INSERT INTO TM26_MMLY_AST_QAL_A
    (   BAS_YM
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
       ,TRGT_DATA_LST_MOD_TM
    )
    WITH
    FULL_SET AS
    (SELECT  T1.BAS_YM, T1.PCF_ID
     FROM   (SELECT BAS_YM, PCF_ID
             FROM   TM23_MMLY_BAL_SHET_A
             WHERE  BAS_YM IN (SELECT DISTINCT TRIM(DATA_BAS_DAY) AS BAS_YM
                               FROM   TBSM_INPT_RPT_SUBMIT_L
                               WHERE  BTCH_BAS_DAY = v_st_date_01
                               AND    INPT_RPT_ID IN ('G32_020_TTGS_02','G32_001_TTGS_01','G32_001_TTGS_02','G32_002_TTGS')
                              )
             GROUP BY BAS_YM, PCF_ID

             UNION

             SELECT BAS_YM, PCF_ID
             FROM   TB04_G32_001_TTGS_01_A
             WHERE  BAS_YM IN (SELECT DISTINCT TRIM(DATA_BAS_DAY) AS BAS_YM
                               FROM   TBSM_INPT_RPT_SUBMIT_L
                               WHERE  BTCH_BAS_DAY = v_st_date_01
                               AND    INPT_RPT_ID IN ('G32_020_TTGS_02','G32_001_TTGS_01','G32_001_TTGS_02','G32_002_TTGS')
                              )
             GROUP BY BAS_YM, PCF_ID

             UNION

             SELECT BAS_YM, PCF_ID
             FROM   TB08_G32_002_TTGS_A
             WHERE  BAS_YM IN (SELECT DISTINCT TRIM(DATA_BAS_DAY) AS BAS_YM
                               FROM   TBSM_INPT_RPT_SUBMIT_L
                               WHERE  BTCH_BAS_DAY = v_st_date_01
                               AND    INPT_RPT_ID IN ('G32_020_TTGS_02','G32_001_TTGS_01','G32_001_TTGS_02','G32_002_TTGS')
                              )
             GROUP BY BAS_YM, PCF_ID
            ) T1
    ),
    BAD_DEBT AS
    (SELECT BAS_YM,
            PCF_ID,
            SUM(CASE WHEN PCF_COA_ID IN ('20111','20121','21111','21121','21211','21221','21311',
                                         '21321','25111','25121','25211','25221','25311','25321')
                          THEN CLO_DR_BAL
                     ELSE 0
                     END) AS DBT_GRP_1_BAL,
            SUM(CASE WHEN PCF_COA_ID IN ('20112','20122','21112','21122','21212','21222','21312',
                                         '21322','25112','25122','25212','25222','25312','25322')
                          THEN CLO_DR_BAL
                     ELSE 0
                     END) AS DBT_GRP_2_BAL,
            SUM(CASE WHEN PCF_COA_ID IN ('20113','20123','21113','21123','21213','21223','21313',
                                         '21323','25113','25123','25213','25223','25313','25323')
                          THEN CLO_DR_BAL
                     ELSE 0
                     END) AS DBT_GRP_3_BAL,
            SUM(CASE WHEN PCF_COA_ID IN ('20114','20124','21114','21124','21214','21224','21314',
                                         '21324','25114','25124','25214','25224','25314','25324')
                          THEN CLO_DR_BAL
                     ELSE 0
                     END) AS DBT_GRP_4_BAL,
            SUM(CASE WHEN PCF_COA_ID IN ('20115','20125','21115','21125','21215','21225','21315',
                                         '21325','25115','25125','25215','25225','25315','25325')
                          THEN CLO_DR_BAL
                     ELSE 0
                     END) AS DBT_GRP_5_BAL,
            SUM(CASE WHEN PCF_COA_ID = '2'
                          THEN CLO_DR_BAL
                     ELSE 0
                     END) AS TOT_LN_BAL,
            SUM(CASE WHEN PCF_COA_ID IN ('20113','20123','21113','21123','21213','21223','21313','21323','25113','25123','25213','25223','25313','25323',
                                         '20114','20124','21114','21124','21214','21224','21314','21324','25114','25124','25214','25224','25314','25324',
                                         '20115','20125','21115','21125','21215','21225','21315','21325','25115','25125','25215','25225','25315','25325')
                          THEN CLO_DR_BAL
                     ELSE 0
                     END) AS BAD_DBT_AMT,
            SUM(CASE WHEN PCF_COA_ID IN ('219', '259', '289', '299', '209') THEN CLO_CR_BAL ELSE 0 END) AS RISK_PRVS_FUND_AMT
     FROM   TM23_MMLY_BAL_SHET_A
     WHERE  BAS_YM IN (SELECT DISTINCT TRIM(DATA_BAS_DAY) AS BAS_YM
                       FROM   TBSM_INPT_RPT_SUBMIT_L
                       WHERE  BTCH_BAS_DAY = v_st_date_01
                       AND    INPT_RPT_ID IN ('G32_020_TTGS_02','G32_001_TTGS_01','G32_001_TTGS_02','G32_002_TTGS')
                      )
     GROUP BY BAS_YM, PCF_ID
    ),
    COLLATERAL AS
    (SELECT BAS_YM,
            PCF_ID,
            SUM(CASE WHEN INDC_CODE = 'G32001TTGS_017' THEN INDC_VALUE ELSE 0 END) AS TOT_COLL_VAL,
            SUM(CASE WHEN INDC_CODE = 'G32001TTGS_021' THEN INDC_VALUE ELSE 0 END) AS RL_EST_COLL_VAL,
            SUM(CASE WHEN INDC_CODE = 'G32001TTGS_018' THEN INDC_VALUE ELSE 0 END) AS DPST_PPL_CR_FUND_COLL_VAL,
            SUM(CASE WHEN INDC_CODE = 'G32001TTGS_020' THEN INDC_VALUE ELSE 0 END) AS PROD_COLL_VAL,
            SUM(CASE WHEN INDC_CODE = 'G32001TTGS_019' THEN INDC_VALUE ELSE 0 END) AS VP_COLL_VAL,
            SUM(CASE WHEN INDC_CODE = 'G32001TTGS_024' THEN INDC_VALUE ELSE 0 END) AS OTHR_PRPTS_COLL_VAL
     FROM   TB04_G32_001_TTGS_01_A
     WHERE  BAS_YM IN (SELECT DISTINCT TRIM(DATA_BAS_DAY) AS BAS_YM
                       FROM   TBSM_INPT_RPT_SUBMIT_L
                       WHERE  BTCH_BAS_DAY = v_st_date_01
                       AND    INPT_RPT_ID IN ('G32_020_TTGS_02','G32_001_TTGS_01','G32_001_TTGS_02','G32_002_TTGS')
                      )
     GROUP BY BAS_YM, PCF_ID
    ),
    PROVISION AS
    (SELECT BAS_YM,
            PCF_ID,
            SUM(CASE WHEN INDC_CODE = 'G32001TTGS_031' THEN INDC_VALUE ELSE 0 END) AS GENL_PRVS_AMT,
            NULL AS DBT_GRP_1_SPEC_PRVS_AMT,
            SUM(CASE WHEN INDC_CODE = 'G32001TTGS_015' THEN INDC_VALUE ELSE 0 END) AS DBT_GRP_2_SPEC_PRVS_AMT, 
            SUM(CASE WHEN INDC_CODE = 'G32001TTGS_016' THEN INDC_VALUE ELSE 0 END) AS DBT_GRP_3_SPEC_PRVS_AMT,
            SUM(CASE WHEN INDC_CODE = 'G32001TTGS_017' THEN INDC_VALUE ELSE 0 END) AS DBT_GRP_4_SPEC_PRVS_AMT,
            SUM(CASE WHEN INDC_CODE = 'G32001TTGS_018' THEN INDC_VALUE ELSE 0 END) AS DBT_GRP_5_SPEC_PRVS_AMT,
            SUM(CASE WHEN INDC_CODE IN ('G32001TTGS_031', 'G32001TTGS_015', 'G32001TTGS_016', 'G32001TTGS_017', 'G32001TTGS_018') THEN INDC_VALUE ELSE 0 END) AS TOT_PRVS_AMT
     FROM   TB08_G32_002_TTGS_A
     WHERE  BAS_YM IN (SELECT DISTINCT TRIM(DATA_BAS_DAY) AS BAS_YM
                       FROM   TBSM_INPT_RPT_SUBMIT_L
                       WHERE  BTCH_BAS_DAY = v_st_date_01
                       AND    INPT_RPT_ID IN ('G32_020_TTGS_02','G32_001_TTGS_01','G32_001_TTGS_02','G32_002_TTGS')
                      )
     GROUP BY BAS_YM, PCF_ID
    )
    SELECT A.BAS_YM
          ,A.PCF_ID
          ,B.DBT_GRP_1_BAL
          ,B.DBT_GRP_2_BAL
          ,B.DBT_GRP_3_BAL
          ,B.DBT_GRP_4_BAL
          ,B.DBT_GRP_5_BAL
          ,B.TOT_LN_BAL - NVL(B.RISK_PRVS_FUND_AMT,0) AS TOT_LN_BAL
          ,B.BAD_DBT_AMT
          ,C.TOT_COLL_VAL
          ,C.RL_EST_COLL_VAL
          ,C.DPST_PPL_CR_FUND_COLL_VAL
          ,C.PROD_COLL_VAL
          ,C.VP_COLL_VAL
          ,C.OTHR_PRPTS_COLL_VAL
          ,D.GENL_PRVS_AMT
          ,D.DBT_GRP_1_SPEC_PRVS_AMT
          ,D.DBT_GRP_2_SPEC_PRVS_AMT
          ,D.DBT_GRP_3_SPEC_PRVS_AMT
          ,D.DBT_GRP_4_SPEC_PRVS_AMT
          ,D.DBT_GRP_5_SPEC_PRVS_AMT
          ,D.TOT_PRVS_AMT
          ,SYSTIMESTAMP
    FROM   FULL_SET A LEFT OUTER JOIN BAD_DEBT B
                                   ON A.BAS_YM = B.BAS_YM
                                  AND A.PCF_ID = B.PCF_ID
                      LEFT OUTER JOIN COLLATERAL C
                                   ON A.BAS_YM = C.BAS_YM
                                  AND A.PCF_ID = C.PCF_ID
                      LEFT OUTER JOIN PROVISION D
                                   ON A.BAS_YM = D.BAS_YM
                                  AND A.PCF_ID = D.PCF_ID
    ;

    ----------------------------------------------------------------------------
    --  Transaction Log
    ----------------------------------------------------------------------------
    v_step_code    := '020' ;
    v_step_desc    := v_wk_date || v_seq || ' : Inserting Data Result' ;
    v_time         := SYSDATE ;

    P1_BSM_PROG_EXEC_LOG(v_program_id, v_program_type_name, v_step_code, v_step_desc, v_time, sql%rowcount, NULL, NULL) ;

    COMMIT ;
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