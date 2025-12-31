create or replace PROCEDURE            "P4_M21_MMLY_PCF_CR_TRANS_1718"
                 ( p_wkdate    IN  CHAR DEFAULT NULL
                 , p_stdate_01 IN  CHAR DEFAULT NULL
                 , p_eddate_01 IN  CHAR DEFAULT NULL
                 )
IS
    /*--------------------------------------------------------------------------
     * PROGRAM ID    : P4_M21_MMLY_PCF_CR_TRANS_1718
     * PROGRAM NAME  : A program for insert data to TM21_MMLY_PCF_CR_TRANS_A
     * SOURCE TABLE  : TB05_G32_011_TTGS_A
     * TARGET TABLE  : TM21_MMLY_PCF_CR_TRANS_A
     * PROGRAMER     : HIEU
     * LAST MODIFICATION DATE : 2025-12-17
     * UNIQUENESS    : N/A
     * COMMENTS      : N/A
    ----------------------------------------------------------------------------
     * Revision History : 2025-12-17 : Create
     * Revision History :
    --------------------------------------------------------------------------*/
    ----------------------------------------------------------------------------
    --  Declare Local Variables
    ----------------------------------------------------------------------------
    v_wk_date                 CHAR(8) DEFAULT NULL ;
    v_st_date_01              CHAR(8) DEFAULT NULL ;
    v_end_date_01             CHAR(8) DEFAULT NULL ;

    v_program_id              VARCHAR2(100) DEFAULT 'P4_M21_MMLY_PCF_CR_TRANS_1718' ;
    v_program_type_name       VARCHAR2(100) DEFAULT 'Stored Procedure' ;
    v_step_code               VARCHAR2(3) ;
    v_step_desc               VARCHAR2(100) ;
    v_time                    DATE ;
    --v_cnt                     NUMBER DEFAULT 0 ;
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
      FROM TM21_MMLY_PCF_CR_TRANS_A
     WHERE BAS_YM IN (SELECT DISTINCT TRIM(DATA_BAS_DAY) AS BAS_YM
                      FROM   TBSM_INPT_RPT_SUBMIT_L
                      WHERE  BTCH_BAS_DAY = v_st_date_01
                      AND    INPT_RPT_ID = 'G32_011_TTGS' 
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

    INSERT INTO TM21_MMLY_PCF_CR_TRANS_A
    (   BAS_YM
       ,PCF_ID
       ,CI_CD
       ,LN_TRM_CD
       ,PCF_VS_CI_BOR_TYP_CD
       ,BOR_AMT
       ,AVG_ACTL_IR
       ,INT_MULTY_BOR_AMT
       ,TRGT_DATA_LST_MOD_TM
    )
    SELECT BAS_YM,
           PCF_ID,
           '01901001' AS CI_CD,
           'XXX' AS LN_TRM_CD,
           '8' AS PCF_VS_CI_BOR_TYP_CD,
           SUM(TOTAL_LOAN_AMT) AS BOR_AMT,
           NULL AS AVG_ACTL_IR,
           NULL AS INT_MULTY_BOR_AMT,
           SYSTIMESTAMP
    FROM   TB05_G32_011_TTGS_A
    WHERE  BAS_YM IN (SELECT DISTINCT TRIM(DATA_BAS_DAY) AS BAS_YM
                      FROM   TBSM_INPT_RPT_SUBMIT_L
                      WHERE  BTCH_BAS_DAY = v_st_date_01
                      AND    INPT_RPT_ID = 'G32_011_TTGS'
                     )
    AND    TERM_CODE = '0'
    AND    TOTAL_OUTSTND_BAL > 0
    GROUP BY BAS_YM, PCF_ID

    UNION ALL

    SELECT BAS_YM,
           PCF_ID,
           '01901001' AS CI_CD,
           TO_CHAR(TO_NUMBER(REPLACE(TERM_CODE, ',', '.'))) AS LN_TRM_CD,
           '1' AS PCF_VS_CI_BOR_TYP_CD,
           SUM(TOTAL_LOAN_AMT) AS BOR_AMT,
           AVG(INT_RATE)  AS AVG_ACTL_IR,
           SUM(TOTAL_OUTSTND_BAL * TO_NUMBER(REPLACE(INT_RATE, ',', '.'))) AS INT_MULTY_BOR_AMT,
           SYSTIMESTAMP
    FROM   TB05_G32_011_TTGS_A
    WHERE  BAS_YM IN (SELECT DISTINCT TRIM(DATA_BAS_DAY) AS BAS_YM
                      FROM   TBSM_INPT_RPT_SUBMIT_L
                      WHERE  BTCH_BAS_DAY = v_st_date_01
                      AND    INPT_RPT_ID = 'G32_011_TTGS'
                     )
    AND    TERM_CODE = '1'
    AND    TOTAL_OUTSTND_BAL > 0
    GROUP BY BAS_YM,
             PCF_ID,
             TO_CHAR(TO_NUMBER(REPLACE(TERM_CODE, ',', '.')))

    UNION ALL

    SELECT BAS_YM,
           PCF_ID,
           '01901001' AS CI_CD,
           TO_CHAR(TO_NUMBER(REPLACE(TERM_CODE, ',', '.'))) AS LN_TRM_CD,
           '2' AS PCF_VS_CI_BOR_TYP_CD,
           SUM(TOTAL_LOAN_AMT) AS BOR_AMT,
           AVG(INT_RATE)  AS AVG_ACTL_IR,
           SUM(TOTAL_OUTSTND_BAL * TO_NUMBER(REPLACE(INT_RATE, ',', '.'))) AS INT_MULTY_BOR_AMT,
           SYSTIMESTAMP
    FROM   TB05_G32_011_TTGS_A
    WHERE  BAS_YM IN (SELECT DISTINCT TRIM(DATA_BAS_DAY) AS BAS_YM
                      FROM   TBSM_INPT_RPT_SUBMIT_L
                      WHERE  BTCH_BAS_DAY = v_st_date_01
                      AND    INPT_RPT_ID = 'G32_011_TTGS'
                     )
    AND    TERM_CODE = '2'
    AND    TOTAL_OUTSTND_BAL > 0
    GROUP BY BAS_YM,
             PCF_ID,
             TO_CHAR(TO_NUMBER(REPLACE(TERM_CODE, ',', '.')))

    UNION ALL

    SELECT BAS_YM,
           PCF_ID,
           '01901001' AS CI_CD,
           TO_CHAR(TO_NUMBER(REPLACE(TERM_CODE, ',', '.'))) AS LN_TRM_CD,
           '3' AS PCF_VS_CI_BOR_TYP_CD,
           SUM(TOTAL_LOAN_AMT) AS BOR_AMT,
           AVG(INT_RATE)  AS AVG_ACTL_IR,
           SUM(TOTAL_OUTSTND_BAL * TO_NUMBER(REPLACE(INT_RATE, ',', '.'))) AS INT_MULTY_BOR_AMT,
           SYSTIMESTAMP
    FROM   TB05_G32_011_TTGS_A
    WHERE  BAS_YM IN (SELECT DISTINCT TRIM(DATA_BAS_DAY) AS BAS_YM
                      FROM   TBSM_INPT_RPT_SUBMIT_L
                      WHERE  BTCH_BAS_DAY = v_st_date_01
                      AND    INPT_RPT_ID = 'G32_011_TTGS'
                     )
    AND    TERM_CODE = '3'
    AND    TOTAL_OUTSTND_BAL > 0
    GROUP BY BAS_YM,
             PCF_ID,
             TO_CHAR(TO_NUMBER(REPLACE(TERM_CODE, ',', '.')))

    UNION ALL

    SELECT BAS_YM,
           PCF_ID,
           '01901001' AS CI_CD,
           TO_CHAR(TO_NUMBER(REPLACE(TERM_CODE, ',', '.'))) AS LN_TRM_CD,
           '4' AS PCF_VS_CI_BOR_TYP_CD,
           SUM(TOTAL_LOAN_AMT) AS BOR_AMT,
           AVG(INT_RATE)  AS AVG_ACTL_IR,
           SUM(TOTAL_OUTSTND_BAL * TO_NUMBER(REPLACE(INT_RATE, ',', '.'))) AS INT_MULTY_BOR_AMT,
           SYSTIMESTAMP
    FROM   TB05_G32_011_TTGS_A
    WHERE  BAS_YM IN (SELECT DISTINCT TRIM(DATA_BAS_DAY) AS BAS_YM
                      FROM   TBSM_INPT_RPT_SUBMIT_L
                      WHERE  BTCH_BAS_DAY = v_st_date_01
                      AND    INPT_RPT_ID = 'G32_011_TTGS'
                     )
    AND    TERM_CODE = '4'
    AND    TOTAL_OUTSTND_BAL > 0
    GROUP BY BAS_YM,
             PCF_ID,
             TO_CHAR(TO_NUMBER(REPLACE(TERM_CODE, ',', '.')))
    UNION ALL

    SELECT BAS_YM,
           PCF_ID,
           '01901001' AS CI_CD,
           TO_CHAR(TO_NUMBER(REPLACE(TERM_CODE, ',', '.'))) AS LN_TRM_CD,
           '5' AS PCF_VS_CI_BOR_TYP_CD,
           SUM(TOTAL_LOAN_AMT) AS BOR_AMT,
           AVG(INT_RATE)  AS AVG_ACTL_IR,
           SUM(TOTAL_OUTSTND_BAL * TO_NUMBER(REPLACE(INT_RATE, ',', '.'))) AS INT_MULTY_BOR_AMT,
           SYSTIMESTAMP
    FROM   TB05_G32_011_TTGS_A
    WHERE  BAS_YM IN (SELECT DISTINCT TRIM(DATA_BAS_DAY) AS BAS_YM
                      FROM   TBSM_INPT_RPT_SUBMIT_L
                      WHERE  BTCH_BAS_DAY = v_st_date_01
                      AND    INPT_RPT_ID = 'G32_011_TTGS'
                     )
    AND    TERM_CODE = '5'
    AND    TOTAL_OUTSTND_BAL > 0
    GROUP BY BAS_YM,
             PCF_ID,
             TO_CHAR(TO_NUMBER(REPLACE(TERM_CODE, ',', '.')))
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