create or replace PROCEDURE            "P4_M22_MMLY_CUST_DPST_TRANS_1718"
                 ( p_wkdate    IN  CHAR DEFAULT NULL
                 , p_stdate_01 IN  CHAR DEFAULT NULL
                 , p_eddate_01 IN  CHAR DEFAULT NULL
                 )
IS
    /*--------------------------------------------------------------------------
     * PROGRAM ID    : P4_M22_MMLY_CUST_DPST_TRANS_1718
     * PROGRAM NAME  : A program for insert data to TM22_MMLY_CUST_DPST_TRANS_A
     * SOURCE TABLE  : TM22_DDLY_CUST_DPST_TRANS_A
     * TARGET TABLE  : TM22_MMLY_CUST_DPST_TRANS_A
     * PROGRAMER     : HIEU
     * LAST MODIFICATION DATE : 2025-12-16
     * UNIQUENESS    : N/A
     * COMMENTS      : N/A
    ----------------------------------------------------------------------------
     * Revision History : 2025-12-16 : Create
     * Revision History :
    --------------------------------------------------------------------------*/
    ----------------------------------------------------------------------------
    --  Declare Local Variables
    ----------------------------------------------------------------------------
    v_wk_date                 CHAR(8) DEFAULT NULL ;
    v_st_date_01              CHAR(8) DEFAULT NULL ;
    v_end_date_01             CHAR(8) DEFAULT NULL ;

    v_program_id              VARCHAR2(100) DEFAULT 'P4_M22_MMLY_CUST_DPST_TRANS_1718' ;
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
    SELECT T1.BAS_YM
    FROM   TM00_MMLY_CAL_D T1 INNER JOIN (SELECT SUBSTR(MIN(DATA_BAS_DAY), 1, 6) AS MIN_DAY, TO_CHAR(SYSDATE - 35, 'YYYYMM') AS MAX_DAY
                                          FROM   TBSM_INPT_RPT_SUBMIT_L
                                          WHERE  BTCH_BAS_DAY >= v_dt2
                                          AND    INPT_RPT_ID  = 'G32_003_TTGS'
                                         ) T2
                                      ON T1.BAS_YM BETWEEN T2.MIN_DAY AND T2.MAX_DAY
    ORDER BY 1;
/*
    CURSOR v_sbmt_day(v_dt1 VARCHAR2, v_dt2 VARCHAR2, v_dt3 VARCHAR2) IS
    SELECT T1.BAS_YM
    FROM   TM00_MMLY_CAL_D T1
    WHERE  T1.BAS_YM BETWEEN '202108' AND '202108'
    ORDER BY 1;
*/
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
            FROM TM22_MMLY_CUST_DPST_TRANS_A T1
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

          INSERT /*+ APPEND PARALLEL(TM22_MMLY_CUST_DPST_TRANS_A, 2) */ INTO TM22_MMLY_CUST_DPST_TRANS_A
          (   BAS_YM
             ,PCF_ID
             ,CUST_TYP_CD
             ,INIT_DPST_TRM_CD
             ,RMTRT_DPST_TRM_CD
             ,TOT_DPSTR_NUM_CNT
             ,TDP_BAL
             ,AVG_ACTL_IR
             ,OVER_1_YR_TRM_DPST_AMT
             ,LSTHN_1_YR_TRM_DPST_AMT
             ,INT_MULTY_TDP_BAL
             ,TRGT_DATA_LST_MOD_TM
          )
          SELECT SUBSTR(A.BAS_DAY, 1, 6) AS BAS_YM,
                 A.PCF_ID,
                 A.CUST_TYP_CD,
                 NVL(A.INIT_DPST_TRM_CD, 'ZZZ'),
                 A.RMTRT_DPST_TRM_CD,
                 COUNT(DISTINCT CASE WHEN A.DPST_BAL > 0 THEN A.CUST_ID ELSE NULL END) AS TOT_DPSTR_NUM_CNT,
                 SUM(A.DPST_BAL) AS TDP_BAL,
                 ROUND(AVG(A.ACTL_IR), 2) AS AVG_ACTL_IR,
                 SUM(CASE WHEN A.RMTRT_DPST_TRM_CD <> 'ZZZ' AND (A.RMTRT_DPST_TRM_CD > '012' OR A.RMTRT_DPST_TRM_CD = 'XXX')
                               THEN A.DPST_BAL
                          ELSE 0
                          END) AS OVR_1_YR_TRM_DPST_AMT,
                 SUM(CASE WHEN A.RMTRT_DPST_TRM_CD = 'ZZZ' OR A.RMTRT_DPST_TRM_CD <= '012'
                               THEN A.DPST_BAL
                          ELSE 0
                          END) AS LSTHN_1_YR_TRM_DPST_AMT,
                 SUM(A.DPST_BAL * A.ACTL_IR) AS INT_MULTY_TDP_BAL,
                 SYSTIMESTAMP
          FROM   TM22_DDLY_CUST_DPST_TRANS_A A INNER JOIN (SELECT SUBSTR(BAS_DAY, 1, 6) AS BAS_YM, PCF_ID, MAX(BAS_DAY) AS BAS_DAY
                                                           FROM   TM22_DDLY_CUST_DPST_TRANS_A
                                                           WHERE  BAS_DAY BETWEEN loop_bas_day.BAS_YM||'01' AND loop_bas_day.BAS_YM||'31'
                                                           GROUP BY SUBSTR(BAS_DAY, 1, 6), PCF_ID
                                                          ) B
                                                       ON B.BAS_DAY = A.BAS_DAY
                                                      AND B.PCF_ID  = A.PCF_ID
          WHERE  A.BAS_DAY BETWEEN loop_bas_day.BAS_YM||'01' AND loop_bas_day.BAS_YM||'31'
          AND    A.DPST_BAL > 0
          GROUP BY SUBSTR(A.BAS_DAY, 1, 6), A.PCF_ID, A.CUST_TYP_CD, NVL(A.INIT_DPST_TRM_CD, 'ZZZ'), A.RMTRT_DPST_TRM_CD;

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