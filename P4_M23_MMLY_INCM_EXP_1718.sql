create or replace PROCEDURE            "P4_M23_MMLY_INCM_EXP_1718"
                 ( p_wkdate    IN  CHAR DEFAULT NULL
                 , p_stdate_01 IN  CHAR DEFAULT NULL
                 , p_eddate_01 IN  CHAR DEFAULT NULL
                 )
IS
    /*--------------------------------------------------------------------------
     * PROGRAM ID    : P4_M23_MMLY_INCM_EXP_1718
     * PROGRAM NAME  : A program for insert data to TM23_MMLY_INCM_EXP_A
     * SOURCE TABLE  : TM23_MMLY_BAL_SHET_A
     * TARGET TABLE  : TM23_MMLY_INCM_EXP_A
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

    v_program_id              VARCHAR2(100) DEFAULT 'P4_M23_MMLY_INCM_EXP_1718' ;
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
      FROM TM23_MMLY_INCM_EXP_A
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

    INSERT INTO TM23_MMLY_INCM_EXP_A
    (   BAS_YM
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
       ,TRGT_DATA_LST_MOD_TM
    )
    SELECT BAS_YM
          ,PCF_ID
          ,DPST_INT_INCM_AMT + LN_INT_INCM_AMT + OTHR_CR_ACT_INCM_AMT AS CR_ACT_INCM_AMT
          ,DPST_INT_INCM_AMT
          ,LN_INT_INCM_AMT
          ,OTHR_CR_ACT_INCM_AMT
          ,SERV_ACT_INCM_AMT
          ,TOT_INCM_AMT - (DPST_INT_INCM_AMT + LN_INT_INCM_AMT + OTHR_CR_ACT_INCM_AMT + SERV_ACT_INCM_AMT) AS OTHR_INCM_AMT
          ,TOT_INCM_AMT
          ,CR_ACT_EXP_AMT
          ,DPST_INT_EXP_AMT
          ,BOR_INT_EXP_AMT
          ,OTHR_CR_ACT_EXP_AMT
          ,SERV_ACT_EXP_AMT
          ,STF_EXP_AMT
          ,TX_FEE_PAY_EXP_AMT
          ,TOT_EXP_AMT - CR_ACT_EXP_AMT - SERV_ACT_EXP_AMT - STF_EXP_AMT - TX_FEE_PAY_EXP_AMT AS OTHR_EXP_AMT
          ,TOT_EXP_AMT
          ,TOT_INCM_AMT - TOT_EXP_AMT AS NET_PNL_AMT
          ,CUR_TOT_INCM_AMT
          ,CUR_TOT_EXP_AMT
          ,CUR_TOT_INCM_AMT - CUR_TOT_EXP_AMT AS CUR_NET_PNL_AMT
          ,SYSTIMESTAMP
    FROM  (SELECT T1.BAS_YM,
                  T1.PCF_ID,
                  T1.DPST_INT_INCM_AMT,
                  T1.LN_INT_INCM_AMT,
                  T1.OTHR_CR_ACT_INCM_AMT,
                  T1.SERV_ACT_INCM_AMT,
                  T1.TOT_INCM_AMT,
                  T1.CR_ACT_EXP_AMT,
                  T1.DPST_INT_EXP_AMT,
                  T1.BOR_INT_EXP_AMT,
                  T1.OTHR_CR_ACT_EXP_AMT,
                  T1.SERV_ACT_EXP_AMT,
                  T1.STF_EXP_AMT,
                  T1.TX_FEE_PAY_EXP_AMT,
                  T1.TOT_EXP_AMT,
                  T1.CUR_TOT_INCM_AMT,
                  T1.CUR_TOT_EXP_AMT
           FROM   (SELECT BAS_YM,
                          PCF_ID,
                          SUM(CASE WHEN PCF_COA_ID = '701' THEN CLO_CR_BAL - OPN_CR_BAL ELSE 0 END) AS DPST_INT_INCM_AMT,
                          SUM(CASE WHEN PCF_COA_ID = '702' THEN CLO_CR_BAL - OPN_CR_BAL ELSE 0 END) AS LN_INT_INCM_AMT,
                          SUM(CASE WHEN PCF_COA_ID = '703' THEN CLO_CR_BAL - OPN_CR_BAL ELSE 0 END) AS OTHR_CR_ACT_INCM_AMT,
                          SUM(CASE WHEN PCF_COA_ID = '71'  THEN CLO_CR_BAL - OPN_CR_BAL ELSE 0 END) AS SERV_ACT_INCM_AMT,
                          SUM(CASE WHEN PCF_COA_ID = '7'   THEN CLO_CR_BAL - OPN_CR_BAL ELSE 0 END) AS TOT_INCM_AMT,
                          SUM(CASE WHEN PCF_COA_ID = '80'  THEN CLO_DR_BAL - OPN_DR_BAL ELSE 0 END) AS CR_ACT_EXP_AMT,
                          SUM(CASE WHEN PCF_COA_ID = '801' THEN CLO_DR_BAL - OPN_DR_BAL ELSE 0 END) AS DPST_INT_EXP_AMT,
                          SUM(CASE WHEN PCF_COA_ID = '802' THEN CLO_DR_BAL - OPN_DR_BAL ELSE 0 END) AS BOR_INT_EXP_AMT,
                          SUM(CASE WHEN PCF_COA_ID = '809' THEN CLO_DR_BAL - OPN_DR_BAL ELSE 0 END) AS OTHR_CR_ACT_EXP_AMT,
                          SUM(CASE WHEN PCF_COA_ID = '81'  THEN CLO_DR_BAL - OPN_DR_BAL ELSE 0 END) AS SERV_ACT_EXP_AMT,
                          SUM(CASE WHEN PCF_COA_ID = '85'  THEN CLO_DR_BAL - OPN_DR_BAL ELSE 0 END) AS STF_EXP_AMT,
                          SUM(CASE WHEN PCF_COA_ID = '83'  THEN CLO_DR_BAL - OPN_DR_BAL ELSE 0 END) AS TX_FEE_PAY_EXP_AMT,
                          SUM(CASE WHEN PCF_COA_ID = '8'   THEN CLO_DR_BAL - OPN_DR_BAL ELSE 0 END) AS TOT_EXP_AMT,
                          SUM(CASE WHEN PCF_COA_ID = '7'   THEN CLO_CR_BAL ELSE 0 END) AS CUR_TOT_INCM_AMT,
                          SUM(CASE WHEN PCF_COA_ID = '8'   THEN CLO_DR_BAL ELSE 0 END) AS CUR_TOT_EXP_AMT
                   FROM   TM23_MMLY_BAL_SHET_A
                   WHERE  BAS_YM IN (SELECT DISTINCT TRIM(DATA_BAS_DAY) AS BAS_YM
                                     FROM   TBSM_INPT_RPT_SUBMIT_L
                                     WHERE  BTCH_BAS_DAY = v_st_date_01
                                     AND    INPT_RPT_ID  = 'G32_020_TTGS_02'
                                     )
                   GROUP BY BAS_YM, PCF_ID
                  ) T1
          )
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