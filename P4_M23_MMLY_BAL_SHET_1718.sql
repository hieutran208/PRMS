create or replace PROCEDURE            "P4_M23_MMLY_BAL_SHET_1718"
                 ( p_wkdate    IN  CHAR DEFAULT NULL
                 , p_stdate_01 IN  CHAR DEFAULT NULL
                 , p_eddate_01 IN  CHAR DEFAULT NULL
                 )
IS
    /*--------------------------------------------------------------------------
     * PROGRAM ID    : P4_M23_MMLY_BAL_SHET_1718
     * PROGRAM NAME  : A program for insert data to TM23_MMLY_BAL_SHET_A
     * SOURCE TABLE  : TB02_G32_020_TTGS_02_A
     * TARGET TABLE  : TM23_MMLY_BAL_SHET_A
     * PROGRAMER     : HIEU
     * LAST MODIFICATION DATE : 2025-12-23
     * UNIQUENESS    : N/A
     * COMMENTS      : N/A
    ----------------------------------------------------------------------------
     * Revision History : 2025-12-23 : Create
     * Revision History :
    --------------------------------------------------------------------------*/
    ----------------------------------------------------------------------------
    --  Declare Local Variables
    ----------------------------------------------------------------------------
    v_wk_date                 CHAR(8) DEFAULT NULL ;
    v_st_date_01              CHAR(8) DEFAULT NULL ;
    v_end_date_01             CHAR(8) DEFAULT NULL ;

    v_program_id              VARCHAR2(100) DEFAULT 'P4_M23_MMLY_BAL_SHET_1718' ;
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
      FROM TM23_MMLY_BAL_SHET_A
     WHERE BAS_YM IN (SELECT DISTINCT TRIM(DATA_BAS_DAY) AS BAS_YM
                      FROM   TBSM_INPT_RPT_SUBMIT_L
                      WHERE  BTCH_BAS_DAY = v_st_date_01
                      AND    INPT_RPT_ID  = 'G32_020_TTGS_02'
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

    INSERT INTO TM23_MMLY_BAL_SHET_A
    (   BAS_YM
       ,PCF_ID
       ,PCF_COA_ID
       ,ON_OFF_TYP_CD
       ,OPN_DR_BAL
       ,OPN_CR_BAL
       ,INCR_DR_BAL
       ,INCR_CR_BAL
       ,CLO_DR_BAL
       ,CLO_CR_BAL
       ,TRGT_DATA_LST_MOD_TM
    )
    SELECT BAS_YM,
           PCF_ID,
           TO_CHAR(ACCOUNT_LEVEL) AS PCF_COA_CD,
           CASE WHEN SUBSTR(ACCOUNT_CODE,1,1) = '9' THEN '2'
                ELSE '1' 
                END AS ON_OFF_TYP_CD,
           OPENING_DEBIT_BAL,
           OPENING_CREDIT_BAL,
           THIS_PERIOD_DEBIT_BAL,
           THIS_PERIOD_CREDIT_BAL,
           CLOSING_DEBIT_BAL,
           CLOSING_CREDIT_BAL,
           SYSTIMESTAMP
    FROM   TB02_G32_020_TTGS_02_A
    WHERE  BAS_YM IN (SELECT DISTINCT TRIM(DATA_BAS_DAY) AS BAS_YM
                      FROM   TBSM_INPT_RPT_SUBMIT_L
                      WHERE  BTCH_BAS_DAY = v_st_date_01
                      AND    INPT_RPT_ID  = 'G32_020_TTGS_02'
                      )
    AND    (OPENING_DEBIT_BAL <> 0 OR OPENING_CREDIT_BAL <> 0 OR THIS_PERIOD_DEBIT_BAL <> 0 OR THIS_PERIOD_CREDIT_BAL <> 0 OR CLOSING_DEBIT_BAL <> 0 OR CLOSING_CREDIT_BAL <> 0);

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