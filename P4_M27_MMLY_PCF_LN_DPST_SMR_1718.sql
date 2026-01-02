create or replace PROCEDURE            "P4_M27_MMLY_PCF_LN_DPST_SMR_1718"
                 ( p_wkdate    IN  CHAR DEFAULT NULL
                 , p_stdate_01 IN  CHAR DEFAULT NULL
                 , p_eddate_01 IN  CHAR DEFAULT NULL
                 )
IS
    /*--------------------------------------------------------------------------
     * PROGRAM ID    : P4_M27_MMLY_PCF_LN_DPST_SMR_1718
     * PROGRAM NAME  : A program for insert data to TM27_MMLY_PCF_LN_DPST_SMR_A
     * SOURCE TABLE  : TM27_DDLY_PCF_LN_DPST_SMR_A
     * TARGET TABLE  : TM27_MMLY_PCF_LN_DPST_SMR_A
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

    v_program_id              VARCHAR2(100) DEFAULT 'P4_M27_MMLY_PCF_LN_DPST_SMR_1718' ;
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
    SELECT BAS_YM
    FROM  (
           SELECT T1.BAS_YM
           FROM   TM00_MMLY_CAL_D T1 INNER JOIN (SELECT SUBSTR(MIN(DATA_BAS_DAY), 1, 6) AS MIN_DAY, TO_CHAR(TO_DATE(v_dt2, 'YYYYMMDD') -2, 'YYYYMM') AS MAX_DAY
                                                 FROM   TBSM_INPT_RPT_SUBMIT_L
                                                 WHERE  BTCH_BAS_DAY >= v_dt2
                                                 AND    INPT_RPT_ID  = 'G32_012_TTGS'
                                                ) T2
                                             ON T1.BAS_YM BETWEEN T2.MIN_DAY AND T2.MAX_DAY
           UNION

           SELECT T1.BAS_YM
           FROM   TM00_MMLY_CAL_D T1 INNER JOIN (SELECT SUBSTR(MIN(DATA_BAS_DAY), 1, 6) AS MIN_DAY, TO_CHAR(TO_DATE(v_dt2, 'YYYYMMDD') -2, 'YYYYMM') AS MAX_DAY
                                                 FROM   TBSM_INPT_RPT_SUBMIT_L
                                                 WHERE  BTCH_BAS_DAY >= v_dt2
                                                 AND    INPT_RPT_ID  = 'G32_003_TTGS'
                                                ) T2
                                             ON T1.BAS_YM BETWEEN T2.MIN_DAY AND T2.MAX_DAY
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
            FROM TM27_MMLY_PCF_LN_DPST_SMR_A T1
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

          INSERT INTO TM27_MMLY_PCF_LN_DPST_SMR_A
          (   BAS_YM
             ,PCF_ID
             ,INIT_TRM_LSTHN_1_MM_DPST_BAL
             ,INIT_TRM_LSTHN_3_MM_DPST_BAL
             ,INIT_TRM_LSTHN_6_MM_DPST_BAL
             ,INIT_TRM_LSTHN_9_MM_DPST_BAL
             ,INIT_TRM_LSTHN_12_MM_DPST_BAL
             ,INIT_TRM_OVR_12_MM_DPST_BAL
             ,RMTRT_LSTHN_1_MM_DPST_BAL
             ,RMTRT_LSTHN_3_MM_DPST_BAL
             ,RMTRT_LSTHN_6_MM_DPST_BAL
             ,RMTRT_LSTHN_9_MM_DPST_BAL
             ,RMTRT_LSTHN_12_MM_DPST_BAL
             ,RMTRT_OVR_12_MM_DPST_BAL
             ,INIT_TRM_LSTHN_1_MM_DPSTR_CNT
             ,INIT_TRM_LSTHN_3_MM_DPSTR_CNT
             ,INIT_TRM_LSTHN_6_MM_DPSTR_CNT
             ,INIT_TRM_LSTHN_9_MM_DPSTR_CNT
             ,INIT_TRM_LSTHN_12_MM_DPSTR_CNT
             ,INIT_TRM_OVR_12_MM_DPSTR_CNT
             ,RMTRT_LSTHN_1_MM_DPSTR_CNT
             ,RMTRT_LSTHN_3_MM_DPSTR_CNT
             ,RMTRT_LSTHN_6_MM_DPSTR_CNT
             ,RMTRT_LSTHN_9_MM_DPSTR_CNT
             ,RMTRT_LSTHN_12_MM_DPSTR_CNT
             ,RMTRT_OVR_12_MM_DPSTR_CNT
             ,AGRCTR_NASTLN_AMT
             ,AGRCTR_NAMLTLN_AMT
             ,AGRCTR_NASTLN_CCNT
             ,AGRCTR_NAMLTLN_CCNT
             ,NON_AGRCTR_NASTLN_AMT
             ,NON_AGRCTR_NAMLTLN_AMT
             ,NON_AGRCTR_NASTLN_CCNT
             ,NON_AGRCTR_NAMLTLN_CCNT
             ,AGRCTR_SHRT_TRM_LN_BAL
             ,AGRCTR_MED_LT_LN_BAL
             ,AGRCTR_SHRT_TRM_LN_CCNT
             ,AGRCTR_MED_LT_LN_CCNT
             ,NON_AGRCTR_SHRT_TRM_LN_BAL
             ,NON_AGRCTR_MED_LT_LN_BAL
             ,NON_AGRCTR_SHRT_TRM_LN_CCNT
             ,NON_AGRCTR_MED_LT_LN_CCNT
             ,INIT_TRM_LSTHN_3_MM_NALN_AMT
             ,INIT_TRM_LSTHN_3_MM_NALN_CCNT
             ,INIT_TRM_LSTHN_6_MM_NALN_AMT
             ,INIT_TRM_LSTHN_6_MM_NALN_CCNT
             ,INIT_TRM_LSTHN_9_MM_NALN_AMT
             ,INIT_TRM_LSTHN_9_MM_NALN_CCNT
             ,INIT_TRM_LSTHN_12_MM_NALN_AMT
             ,INIT_TRM_LSTHN_12_MM_NALN_CCNT
             ,INIT_TRM_OVR_12_MM_NALN_AMT
             ,INIT_TRM_OVR_12_MM_NALN_CCNT
             ,RMTRT_LSTHN_3_MM_NALN_AMT
             ,RMTRT_LSTHN_3_MM_NALN_CCNT
             ,RMTRT_LSTHN_6_MM_NALN_AMT
             ,RMTRT_LSTHN_6_MM_NALN_CCNT
             ,RMTRT_LSTHN_9_MM_NALN_AMT
             ,RMTRT_LSTHN_9_MM_NALN_CCNT
             ,RMTRT_LSTHN_12_MM_NALN_AMT
             ,RMTRT_LSTHN_12_MM_NALN_CCNT
             ,RMTRT_OVR_12_MM_NALN_AMT
             ,RMTRT_OVR_12_MM_NALN_CCNT
             ,INIT_TRM_LSTHN_3_MM_LN_BAL
             ,INIT_TRM_LSTHN_3_MM_LN_CCNT
             ,INIT_TRM_LSTHN_3_MM_BRWR_CNT
             ,INIT_TRM_LSTHN_6_MM_LN_BAL
             ,INIT_TRM_LSTHN_6_MM_LN_CCNT
             ,INIT_TRM_LSTHN_6_MM_BRWR_CNT
             ,INIT_TRM_LSTHN_9_MM_LN_BAL
             ,INIT_TRM_LSTHN_9_MM_LN_CCNT
             ,INIT_TRM_LSTHN_9_MM_BRWR_CNT
             ,INIT_TRM_LSTHN_12_MM_LN_BAL
             ,INIT_TRM_LSTHN_12_MM_LN_CCNT
             ,INIT_TRM_LSTHN_12_MM_BRWR_CNT
             ,INIT_TRM_OVR_12_MM_LN_BAL
             ,INIT_TRM_OVR_12_MM_LN_CCNT
             ,INIT_TRM_OVR_12_MM_BRWR_CNT
             ,RMTRT_LSTHN_3_MM_LN_BAL
             ,RMTRT_LSTHN_3_MM_LN_CCNT
             ,RMTRT_LSTHN_3_MM_BRWR_CNT
             ,RMTRT_LSTHN_6_MM_LN_BAL
             ,RMTRT_LSTHN_6_MM_LN_CCNT
             ,RMTRT_LSTHN_6_MM_BRWR_CNT
             ,RMTRT_LSTHN_9_MM_LN_BAL
             ,RMTRT_LSTHN_9_MM_LN_CCNT
             ,RMTRT_LSTHN_9_MM_BRWR_CNT
             ,RMTRT_LSTHN_12_MM_LN_BAL
             ,RMTRT_LSTHN_12_MM_LN_CCNT
             ,RMTRT_LSTHN_12_MM_BRWR_CNT
             ,RMTRT_OVR_12_MM_LN_BAL
             ,RMTRT_OVR_12_MM_LN_CCNT
             ,RMTRT_OVR_12_MM_BRWR_CNT
             ,TRGT_DATA_LST_MOD_TM
          )
          WITH
          FULL_SET AS
          (SELECT  T1.BAS_YM, T1.PCF_ID
           FROM   (SELECT SUBSTR(A.BAS_DAY, 1, 6) AS BAS_YM, A.PCF_ID
                   FROM   TM27_DDLY_PCF_LN_DPST_SMR_A A INNER JOIN (SELECT SUBSTR(BAS_DAY, 1, 6) AS BAS_YM, PCF_ID, MAX(BAS_DAY) AS BAS_DAY
                                                                    FROM   TM27_DDLY_PCF_LN_DPST_SMR_A
                                                                    WHERE  BAS_DAY BETWEEN loop_bas_day.BAS_YM||'01' AND loop_bas_day.BAS_YM||'31'
                                                                    GROUP BY SUBSTR(BAS_DAY, 1, 6), PCF_ID
                                                                    ) B
                                                                 ON B.BAS_DAY = A.BAS_DAY
                                                                AND B.PCF_ID  = A.PCF_ID
                   WHERE  A.BAS_DAY BETWEEN loop_bas_day.BAS_YM||'01' AND loop_bas_day.BAS_YM||'31'
                   GROUP BY SUBSTR(A.BAS_DAY, 1, 6), A.PCF_ID
                  ) T1
          GROUP BY T1.BAS_YM, T1.PCF_ID
          ),
          DEPOSIT_LOAN AS
          (SELECT SUBSTR(A.BAS_DAY, 1, 6) AS BAS_YM
                 ,A.PCF_ID
                 ,A.INIT_TRM_LSTHN_1_MM_DPST_BAL
                 ,A.INIT_TRM_LSTHN_3_MM_DPST_BAL
                 ,A.INIT_TRM_LSTHN_6_MM_DPST_BAL
                 ,A.INIT_TRM_LSTHN_9_MM_DPST_BAL
                 ,A.INIT_TRM_LSTHN_12_MM_DPST_BAL
                 ,A.INIT_TRM_OVR_12_MM_DPST_BAL
                 ,A.RMTRT_LSTHN_1_MM_DPST_BAL
                 ,A.RMTRT_LSTHN_3_MM_DPST_BAL
                 ,A.RMTRT_LSTHN_6_MM_DPST_BAL
                 ,A.RMTRT_LSTHN_9_MM_DPST_BAL
                 ,A.RMTRT_LSTHN_12_MM_DPST_BAL
                 ,A.RMTRT_OVR_12_MM_DPST_BAL
                 ,A.INIT_TRM_LSTHN_1_MM_DPSTR_CNT
                 ,A.INIT_TRM_LSTHN_3_MM_DPSTR_CNT
                 ,A.INIT_TRM_LSTHN_6_MM_DPSTR_CNT
                 ,A.INIT_TRM_LSTHN_9_MM_DPSTR_CNT
                 ,A.INIT_TRM_LSTHN_12_MM_DPSTR_CNT
                 ,A.INIT_TRM_OVR_12_MM_DPSTR_CNT
                 ,A.RMTRT_LSTHN_1_MM_DPSTR_CNT
                 ,A.RMTRT_LSTHN_3_MM_DPSTR_CNT
                 ,A.RMTRT_LSTHN_6_MM_DPSTR_CNT
                 ,A.RMTRT_LSTHN_9_MM_DPSTR_CNT
                 ,A.RMTRT_LSTHN_12_MM_DPSTR_CNT
                 ,A.RMTRT_OVR_12_MM_DPSTR_CNT
                 ,A.AGRCTR_SHRT_TRM_LN_BAL
                 ,A.AGRCTR_MED_LT_LN_BAL
                 ,A.AGRCTR_SHRT_TRM_LN_CCNT
                 ,A.AGRCTR_MED_LT_LN_CCNT
                 ,A.NON_AGRCTR_SHRT_TRM_LN_BAL
                 ,A.NON_AGRCTR_MED_LT_LN_BAL
                 ,A.NON_AGRCTR_SHRT_TRM_LN_CCNT
                 ,A.NON_AGRCTR_MED_LT_LN_CCNT
                 ,A.INIT_TRM_LSTHN_3_MM_LN_BAL
                 ,A.INIT_TRM_LSTHN_3_MM_LN_CCNT
                 ,A.INIT_TRM_LSTHN_3_MM_BRWR_CNT
                 ,A.INIT_TRM_LSTHN_6_MM_LN_BAL
                 ,A.INIT_TRM_LSTHN_6_MM_LN_CCNT
                 ,A.INIT_TRM_LSTHN_6_MM_BRWR_CNT
                 ,A.INIT_TRM_LSTHN_9_MM_LN_BAL
                 ,A.INIT_TRM_LSTHN_9_MM_LN_CCNT
                 ,A.INIT_TRM_LSTHN_9_MM_BRWR_CNT
                 ,A.INIT_TRM_LSTHN_12_MM_LN_BAL
                 ,A.INIT_TRM_LSTHN_12_MM_LN_CCNT
                 ,A.INIT_TRM_LSTHN_12_MM_BRWR_CNT
                 ,A.INIT_TRM_OVR_12_MM_LN_BAL
                 ,A.INIT_TRM_OVR_12_MM_LN_CCNT
                 ,A.INIT_TRM_OVR_12_MM_BRWR_CNT
                 ,A.RMTRT_LSTHN_3_MM_LN_BAL
                 ,A.RMTRT_LSTHN_3_MM_LN_CCNT
                 ,A.RMTRT_LSTHN_3_MM_BRWR_CNT
                 ,A.RMTRT_LSTHN_6_MM_LN_BAL
                 ,A.RMTRT_LSTHN_6_MM_LN_CCNT
                 ,A.RMTRT_LSTHN_6_MM_BRWR_CNT
                 ,A.RMTRT_LSTHN_9_MM_LN_BAL
                 ,A.RMTRT_LSTHN_9_MM_LN_CCNT
                 ,A.RMTRT_LSTHN_9_MM_BRWR_CNT
                 ,A.RMTRT_LSTHN_12_MM_LN_BAL
                 ,A.RMTRT_LSTHN_12_MM_LN_CCNT
                 ,A.RMTRT_LSTHN_12_MM_BRWR_CNT
                 ,A.RMTRT_OVR_12_MM_LN_BAL
                 ,A.RMTRT_OVR_12_MM_LN_CCNT
                 ,A.RMTRT_OVR_12_MM_BRWR_CNT
           FROM   TM27_DDLY_PCF_LN_DPST_SMR_A A INNER JOIN (SELECT SUBSTR(BAS_DAY, 1, 6) AS BAS_YM, PCF_ID, MAX(BAS_DAY) AS BAS_DAY
                                                            FROM   TM27_DDLY_PCF_LN_DPST_SMR_A
                                                            WHERE  BAS_DAY BETWEEN loop_bas_day.BAS_YM||'01' AND loop_bas_day.BAS_YM||'31'
                                                            GROUP BY SUBSTR(BAS_DAY, 1, 6), PCF_ID
                                                           ) B
                                                        ON B.BAS_DAY = A.BAS_DAY
                                                       AND B.PCF_ID  = A.PCF_ID
           WHERE  A.BAS_DAY BETWEEN loop_bas_day.BAS_YM||'01' AND loop_bas_day.BAS_YM||'31'
          ),
          LOAN_NEW_ARISEN AS
          (SELECT SUBSTR(A.BAS_DAY, 1, 6) AS BAS_YM,
                  A.PCF_ID,
                  SUM(A.AGRCTR_NASTLN_AMT) AS AGRCTR_NASTLN_AMT,
                  SUM(A.AGRCTR_NAMLTLN_AMT) AS AGRCTR_NAMLTLN_AMT,
                  SUM(A.NON_AGRCTR_NASTLN_AMT) AS NON_AGRCTR_NASTLN_AMT,
                  SUM(A.NON_AGRCTR_NAMLTLN_AMT) AS NON_AGRCTR_NAMLTLN_AMT,
                  SUM(A.INIT_TRM_LSTHN_3_MM_NALN_AMT) AS INIT_TRM_LSTHN_3_MM_NALN_AMT,
                  SUM(A.INIT_TRM_LSTHN_6_MM_NALN_AMT) AS INIT_TRM_LSTHN_6_MM_NALN_AMT,
                  SUM(A.INIT_TRM_LSTHN_9_MM_NALN_AMT) AS INIT_TRM_LSTHN_9_MM_NALN_AMT,
                  SUM(A.INIT_TRM_LSTHN_12_MM_NALN_AMT) AS INIT_TRM_LSTHN_12_MM_NALN_AMT,
                  SUM(A.INIT_TRM_OVR_12_MM_NALN_AMT) AS INIT_TRM_OVR_12_MM_NALN_AMT,
                  SUM(A.RMTRT_LSTHN_3_MM_NALN_AMT) AS RMTRT_LSTHN_3_MM_NALN_AMT,
                  SUM(A.RMTRT_LSTHN_6_MM_NALN_AMT) AS RMTRT_LSTHN_6_MM_NALN_AMT,
                  SUM(A.RMTRT_LSTHN_9_MM_NALN_AMT) AS RMTRT_LSTHN_9_MM_NALN_AMT,
                  SUM(A.RMTRT_LSTHN_12_MM_NALN_AMT) AS RMTRT_LSTHN_12_MM_NALN_AMT,
                  SUM(A.RMTRT_OVR_12_MM_NALN_AMT) AS RMTRT_OVR_12_MM_NALN_AMT,
                  SUM(A.AGRCTR_NASTLN_CCNT) AS AGRCTR_NASTLN_CCNT,
                  SUM(A.AGRCTR_NAMLTLN_CCNT) AS AGRCTR_NAMLTLN_CCNT,
                  SUM(A.NON_AGRCTR_NASTLN_CCNT) AS NON_AGRCTR_NASTLN_CCNT,
                  SUM(A.NON_AGRCTR_NAMLTLN_CCNT) AS NON_AGRCTR_NAMLTLN_CCNT,
                  SUM(A.INIT_TRM_LSTHN_3_MM_NALN_CCNT) AS INIT_TRM_LSTHN_3_MM_NALN_CCNT,
                  SUM(A.INIT_TRM_LSTHN_6_MM_NALN_CCNT) AS INIT_TRM_LSTHN_6_MM_NALN_CCNT,
                  SUM(A.INIT_TRM_LSTHN_9_MM_NALN_CCNT) AS INIT_TRM_LSTHN_9_MM_NALN_CCNT,
                  SUM(A.INIT_TRM_LSTHN_12_MM_NALN_CCNT) AS INIT_TRM_LSTHN_12_MM_NALN_CCNT,
                  SUM(A.INIT_TRM_OVR_12_MM_NALN_CCNT) AS INIT_TRM_OVR_12_MM_NALN_CCNT,
                  SUM(A.RMTRT_LSTHN_3_MM_NALN_CCNT) AS RMTRT_LSTHN_3_MM_NALN_CCNT,
                  SUM(A.RMTRT_LSTHN_6_MM_NALN_CCNT) AS RMTRT_LSTHN_6_MM_NALN_CCNT,
                  SUM(A.RMTRT_LSTHN_9_MM_NALN_CCNT) AS RMTRT_LSTHN_9_MM_NALN_CCNT,
                  SUM(A.RMTRT_LSTHN_12_MM_NALN_CCNT) AS RMTRT_LSTHN_12_MM_NALN_CCNT,
                  SUM(A.RMTRT_OVR_12_MM_NALN_CCNT) AS RMTRT_OVR_12_MM_NALN_CCNT
           FROM   TM27_DDLY_PCF_LN_DPST_SMR_A A
           WHERE  A.BAS_DAY BETWEEN loop_bas_day.BAS_YM||'01' AND loop_bas_day.BAS_YM||'31'
           GROUP BY SUBSTR(A.BAS_DAY, 1, 6), A.PCF_ID
          )
          SELECT A.BAS_YM
                ,A.PCF_ID
                ,B.INIT_TRM_LSTHN_1_MM_DPST_BAL
                ,B.INIT_TRM_LSTHN_3_MM_DPST_BAL
                ,B.INIT_TRM_LSTHN_6_MM_DPST_BAL
                ,B.INIT_TRM_LSTHN_9_MM_DPST_BAL
                ,B.INIT_TRM_LSTHN_12_MM_DPST_BAL
                ,B.INIT_TRM_OVR_12_MM_DPST_BAL
                ,B.RMTRT_LSTHN_1_MM_DPST_BAL
                ,B.RMTRT_LSTHN_3_MM_DPST_BAL
                ,B.RMTRT_LSTHN_6_MM_DPST_BAL
                ,B.RMTRT_LSTHN_9_MM_DPST_BAL
                ,B.RMTRT_LSTHN_12_MM_DPST_BAL
                ,B.RMTRT_OVR_12_MM_DPST_BAL
                ,B.INIT_TRM_LSTHN_1_MM_DPSTR_CNT
                ,B.INIT_TRM_LSTHN_3_MM_DPSTR_CNT
                ,B.INIT_TRM_LSTHN_6_MM_DPSTR_CNT
                ,B.INIT_TRM_LSTHN_9_MM_DPSTR_CNT
                ,B.INIT_TRM_LSTHN_12_MM_DPSTR_CNT
                ,B.INIT_TRM_OVR_12_MM_DPSTR_CNT
                ,B.RMTRT_LSTHN_1_MM_DPSTR_CNT
                ,B.RMTRT_LSTHN_3_MM_DPSTR_CNT
                ,B.RMTRT_LSTHN_6_MM_DPSTR_CNT
                ,B.RMTRT_LSTHN_9_MM_DPSTR_CNT
                ,B.RMTRT_LSTHN_12_MM_DPSTR_CNT
                ,B.RMTRT_OVR_12_MM_DPSTR_CNT
                ,C.AGRCTR_NASTLN_AMT
                ,C.AGRCTR_NAMLTLN_AMT
                ,C.AGRCTR_NASTLN_CCNT
                ,C.AGRCTR_NAMLTLN_CCNT
                ,C.NON_AGRCTR_NASTLN_AMT
                ,C.NON_AGRCTR_NAMLTLN_AMT
                ,C.NON_AGRCTR_NASTLN_CCNT
                ,C.NON_AGRCTR_NAMLTLN_CCNT
                ,B.AGRCTR_SHRT_TRM_LN_BAL
                ,B.AGRCTR_MED_LT_LN_BAL
                ,B.AGRCTR_SHRT_TRM_LN_CCNT
                ,B.AGRCTR_MED_LT_LN_CCNT
                ,B.NON_AGRCTR_SHRT_TRM_LN_BAL
                ,B.NON_AGRCTR_MED_LT_LN_BAL
                ,B.NON_AGRCTR_SHRT_TRM_LN_CCNT
                ,B.NON_AGRCTR_MED_LT_LN_CCNT
                ,C.INIT_TRM_LSTHN_3_MM_NALN_AMT
                ,C.INIT_TRM_LSTHN_3_MM_NALN_CCNT
                ,C.INIT_TRM_LSTHN_6_MM_NALN_AMT
                ,C.INIT_TRM_LSTHN_6_MM_NALN_CCNT
                ,C.INIT_TRM_LSTHN_9_MM_NALN_AMT
                ,C.INIT_TRM_LSTHN_9_MM_NALN_CCNT
                ,C.INIT_TRM_LSTHN_12_MM_NALN_AMT
                ,C.INIT_TRM_LSTHN_12_MM_NALN_CCNT
                ,C.INIT_TRM_OVR_12_MM_NALN_AMT
                ,C.INIT_TRM_OVR_12_MM_NALN_CCNT
                ,C.RMTRT_LSTHN_3_MM_NALN_AMT
                ,C.RMTRT_LSTHN_3_MM_NALN_CCNT
                ,C.RMTRT_LSTHN_6_MM_NALN_AMT
                ,C.RMTRT_LSTHN_6_MM_NALN_CCNT
                ,C.RMTRT_LSTHN_9_MM_NALN_AMT
                ,C.RMTRT_LSTHN_9_MM_NALN_CCNT
                ,C.RMTRT_LSTHN_12_MM_NALN_AMT
                ,C.RMTRT_LSTHN_12_MM_NALN_CCNT
                ,C.RMTRT_OVR_12_MM_NALN_AMT
                ,C.RMTRT_OVR_12_MM_NALN_CCNT
                ,B.INIT_TRM_LSTHN_3_MM_LN_BAL
                ,B.INIT_TRM_LSTHN_3_MM_LN_CCNT
                ,B.INIT_TRM_LSTHN_3_MM_BRWR_CNT
                ,B.INIT_TRM_LSTHN_6_MM_LN_BAL
                ,B.INIT_TRM_LSTHN_6_MM_LN_CCNT
                ,B.INIT_TRM_LSTHN_6_MM_BRWR_CNT
                ,B.INIT_TRM_LSTHN_9_MM_LN_BAL
                ,B.INIT_TRM_LSTHN_9_MM_LN_CCNT
                ,B.INIT_TRM_LSTHN_9_MM_BRWR_CNT
                ,B.INIT_TRM_LSTHN_12_MM_LN_BAL
                ,B.INIT_TRM_LSTHN_12_MM_LN_CCNT
                ,B.INIT_TRM_LSTHN_12_MM_BRWR_CNT
                ,B.INIT_TRM_OVR_12_MM_LN_BAL
                ,B.INIT_TRM_OVR_12_MM_LN_CCNT
                ,B.INIT_TRM_OVR_12_MM_BRWR_CNT
                ,B.RMTRT_LSTHN_3_MM_LN_BAL
                ,B.RMTRT_LSTHN_3_MM_LN_CCNT
                ,B.RMTRT_LSTHN_3_MM_BRWR_CNT
                ,B.RMTRT_LSTHN_6_MM_LN_BAL
                ,B.RMTRT_LSTHN_6_MM_LN_CCNT
                ,B.RMTRT_LSTHN_6_MM_BRWR_CNT
                ,B.RMTRT_LSTHN_9_MM_LN_BAL
                ,B.RMTRT_LSTHN_9_MM_LN_CCNT
                ,B.RMTRT_LSTHN_9_MM_BRWR_CNT
                ,B.RMTRT_LSTHN_12_MM_LN_BAL
                ,B.RMTRT_LSTHN_12_MM_LN_CCNT
                ,B.RMTRT_LSTHN_12_MM_BRWR_CNT
                ,B.RMTRT_OVR_12_MM_LN_BAL
                ,B.RMTRT_OVR_12_MM_LN_CCNT
                ,B.RMTRT_OVR_12_MM_BRWR_CNT
                ,SYSTIMESTAMP
          FROM   FULL_SET A LEFT OUTER JOIN DEPOSIT_LOAN B
                                         ON A.BAS_YM = B.BAS_YM
                                        AND A.PCF_ID = B.PCF_ID
                            LEFT OUTER JOIN LOAN_NEW_ARISEN C
                                         ON A.BAS_YM = C.BAS_YM
                                        AND A.PCF_ID = C.PCF_ID;

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