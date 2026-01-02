create or replace PROCEDURE            "P1_M23_DDLY_BAL_SHET_1718"
                 ( p_wkdate    IN  CHAR DEFAULT NULL
                 , p_stdate_01 IN  CHAR DEFAULT NULL
                 , p_eddate_01 IN  CHAR DEFAULT NULL
                 )
IS
    /*--------------------------------------------------------------------------
     * PROGRAM ID    : P1_M23_DDLY_BAL_SHET_1718
     * PROGRAM NAME  : A program for insert data to TM23_DDLY_BAL_SHET_A
     * SOURCE TABLE  : TB02_G32_020_TTGS_01_A
     * TARGET TABLE  : TM23_DDLY_BAL_SHET_A
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

    v_program_id              VARCHAR2(100) DEFAULT 'P1_M23_DDLY_BAL_SHET_1718' ;
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
    SELECT DISTINCT DATA_BAS_DAY AS BAS_DAY
    FROM   TBSM_INPT_RPT_SUBMIT_L
    WHERE  BTCH_BAS_DAY = v_dt2
    AND    INPT_RPT_ID  = 'G32_020_TTGS_01'
    ORDER BY 1;

/*
    CURSOR v_delete_set(v_dt1 VARCHAR2, v_dt2 VARCHAR2, v_dt3 VARCHAR2) IS
    SELECT K1.BAS_DAY, K2.PCF_ID
    FROM   TM00_DDLY_CAL_D K1 INNER JOIN (SELECT DATA_BAS_DAY, PCF_ID, SUBMIT_INST_CD
                                          FROM   TBSM_INPT_RPT_SUBMIT_L
                                          WHERE  BTCH_BAS_DAY = v_dt2
                                          AND    INPT_RPT_ID  = 'G32_020_TTGS_01'
                                          AND    RSUBMIT_YN = 'Y'
                                          GROUP BY DATA_BAS_DAY, PCF_ID, SUBMIT_INST_CD
                                          ORDER BY 1,2,3
                                         )K2
                                      ON K2.DATA_BAS_DAY = K1.PREV_WRK_DAY
    WHERE  (K1.WEEKEND_YN = 'Y' OR K1.PUB_HLDAY_YN = 'Y')
    GROUP BY K1.BAS_DAY, K2.PCF_ID
    ORDER BY 1,2;

    CURSOR v_holiday(v_dt1 VARCHAR2, v_dt2 VARCHAR2, v_dt3 VARCHAR2) IS
    SELECT T1.BAS_DAY
    FROM   TM00_DDLY_CAL_D T1 INNER JOIN (SELECT MIN(DATA_BAS_DAY)MIN_DAY, MAX(DATA_BAS_DAY) AS MAX_DAY
                                          FROM   TBSM_INPT_RPT_SUBMIT_L
                                          WHERE  BTCH_BAS_DAY = v_dt2
                                          AND    INPT_RPT_ID  = 'G32_020_TTGS_01'
                                         ) T2
                                       ON T1.BAS_DAY BETWEEN T2.MIN_DAY AND T2.MAX_DAY
    WHERE  WEEKEND_YN = 'Y' OR PUB_HLDAY_YN = 'Y'
    GROUP BY T1.BAS_DAY
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
            FROM TM23_DDLY_BAL_SHET_A T1
           WHERE T1.BAS_DAY = loop_bas_day.BAS_DAY
           AND   EXISTS (SELECT *
                         FROM   (SELECT PCF_ID
                                 FROM   TBSM_INPT_RPT_SUBMIT_L
                                 WHERE  BTCH_BAS_DAY     = v_st_date_01
                                 AND    INPT_RPT_ID      = 'G32_020_TTGS_01'
                                 AND    DATA_BAS_DAY     = loop_bas_day.BAS_DAY
                                 GROUP BY PCF_ID
                                ) T2
                         WHERE  T2.PCF_ID     = T1.PCF_ID
                        );

          ----------------------------------------------------------------------------
          --  Transaction Log
          ----------------------------------------------------------------------------
          v_step_code    := '010' ;
          v_step_desc    := v_wk_date || v_seq || ' ' || loop_bas_day.BAS_DAY ||' : Deleting Data Result';
          v_time         := SYSDATE ;

          P1_BSM_PROG_EXEC_LOG(v_program_id, v_program_type_name, v_step_code, v_step_desc, v_time, sql%rowcount, NULL, NULL) ;
          
          COMMIT ;
          ----------------------------------------------------------------------------

          v_cnt := v_cnt+sql%rowcount;

          EXCEPTION
          WHEN OTHERS THEN
               begin
                     v_step_code    := '011' ;
                     v_step_desc    := v_wk_date || v_seq || ' : Deleting Data Error loop_bas_day with BAS_DAY = ' ;
                     v_time         := SYSDATE ;
                     P1_BSM_PROG_EXEC_LOG(v_program_id, v_program_type_name, v_step_code, v_step_desc||loop_bas_day.BAS_DAY, v_time, v_cnt, TO_CHAR(SQLCODE), SQLERRM) ;
                     CONTINUE;
               end;
    End;
    END LOOP;

    ----------------------------------------------------------------------------
    --  1.2 Inserting Data
    ----------------------------------------------------------------------------

    v_cnt := 0;

    FOR loop_bas_day in v_sbmt_day(v_wk_date,v_st_date_01,v_end_date_01)
    LOOP
    BEGIN

          INSERT INTO TM23_DDLY_BAL_SHET_A
          (   BAS_DAY
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
          SELECT BAS_YMD,
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
          FROM   TB02_G32_020_TTGS_01_A
          WHERE  BAS_YMD = loop_bas_day.BAS_DAY
          AND    PCF_ID IN (SELECT PCF_ID
                            FROM   TBSM_INPT_RPT_SUBMIT_L
                            WHERE  BTCH_BAS_DAY     = v_st_date_01
                            AND    INPT_RPT_ID      = 'G32_020_TTGS_01'
                            AND    DATA_BAS_DAY     = loop_bas_day.BAS_DAY
                            GROUP BY PCF_ID
                           )
          AND    (OPENING_DEBIT_BAL <> 0 OR OPENING_CREDIT_BAL <> 0 OR THIS_PERIOD_DEBIT_BAL <> 0 OR THIS_PERIOD_CREDIT_BAL <> 0 OR CLOSING_DEBIT_BAL <> 0 OR CLOSING_CREDIT_BAL <> 0);

          ----------------------------------------------------------------------------
          --  Transaction Log
          ----------------------------------------------------------------------------
          v_step_code    := '020' ;
          v_step_desc    := v_wk_date || v_seq || ' ' || loop_bas_day.BAS_DAY ||' : Inserting Data Result';
          v_time         := SYSDATE ;

          P1_BSM_PROG_EXEC_LOG(v_program_id, v_program_type_name, v_step_code, v_step_desc, v_time, sql%rowcount, NULL, NULL) ;

          COMMIT;

          ----------------------------------------------------------------------------

          v_cnt := v_cnt+sql%rowcount;

          EXCEPTION
          WHEN OTHERS THEN
               begin
                     v_step_code    := '021' ;
                     v_step_desc    := v_wk_date || v_seq || ' : Inserting Data Error loop_bas_day with BAS_DAY = ' ;
                     v_time         := SYSDATE ;
                     P1_BSM_PROG_EXEC_LOG(v_program_id, v_program_type_name, v_step_code, v_step_desc||loop_bas_day.BAS_DAY, v_time, v_cnt, TO_CHAR(SQLCODE), SQLERRM) ;
                     CONTINUE;
               end;
    End;
    END LOOP;

/*
    ----------------------------------------------------------------------------
    --  1.3 Delete Historical Data(Holiday_Resubmit Report)
    ----------------------------------------------------------------------------

    v_cnt := 0;

    FOR loop_delete_set in v_delete_set(v_wk_date,v_st_date_01,v_end_date_01)
    LOOP
    BEGIN

    v_step_code    := '030' ;
    v_step_desc    := v_wk_date || v_seq || ' : Delete Historical Data(Holiday_Resubmit Report)' ;

           DELETE
           FROM   TM23_DDLY_BAL_SHET_A W1
           WHERE  W1.BAS_DAY = loop_delete_set.BAS_DAY
           AND    W1.PCF_ID  = loop_delete_set.PCF_ID
           AND    W1.BAS_DAY||W1.PCF_ID NOT IN (SELECT T1.BAS_DAY||T1.PCF_ID
                                                FROM   TSPF_G32_020_TTGS_01_X T1 INNER JOIN (SELECT K1.PREV_WRK_DAY, K2.DATA_BAS_DAY, K1.BAS_DAY, K2.PCF_ID
                                                                                                        FROM   TM00_DDLY_CAL_D K1 INNER JOIN (SELECT DATA_BAS_DAY, PCF_ID
                                                                                                                                              FROM   TBSM_INPT_RPT_SUBMIT_L
                                                                                                                                              WHERE  BTCH_BAS_DAY = v_st_date_01
                                                                                                                                              AND    INPT_RPT_ID  = 'G32_020_TTGS_01'
                                                                                                                                              AND    RSUBMIT_YN = 'Y'
                                                                                                                                              GROUP BY DATA_BAS_DAY, PCF_ID
                                                                                                                                              ORDER BY 1,2
                                                                                                                                             )K2
                                                                                                                                          ON K2.DATA_BAS_DAY = K1.PREV_WRK_DAY
                                                                                                        WHERE  (K1.WEEKEND_YN = 'Y' OR K1.PUB_HLDAY_YN = 'Y')
                                                                                                        GROUP BY K1.PREV_WRK_DAY, K2.DATA_BAS_DAY,  K1.BAS_DAY, K2.PCF_ID, K2.SUBMIT_INST_CD
                                                                                                       ) T2
                                                                                                    ON T2.BAS_DAY = T1.BAS_DAY
                                                                                                   AND T2.PCF_ID  = T1.PCF_ID
                                                                                            INNER JOIN TM00_PCF_D T3
                                                                                                    ON T1.PCF_ID = T1.PCF_ID
                                                GROUP BY T1.BAS_DAY||T1.PCF_ID
                                               );

          ----------------------------------------------------------------------------
          --  Transaction Log
          ----------------------------------------------------------------------------
          v_time         := SYSDATE ;

          P1_BSM_PROG_EXEC_LOG(v_program_id, v_program_type_name, v_step_code, v_step_desc||loop_delete_set.BAS_DAY, v_time, sql%rowcount, NULL, NULL) ;
          COMMIT ;
          ----------------------------------------------------------------------------

          v_cnt := v_cnt+sql%rowcount;

          EXCEPTION
          WHEN OTHERS THEN
               begin
                     v_step_code    := '031' ;
                     v_step_desc    := v_wk_date || v_seq || ' : Deleting Data Error loop_delete_set with BAS_DAY = ' ;
                     v_time         := SYSDATE ;
                     P1_BSM_PROG_EXEC_LOG(v_program_id, v_program_type_name, v_step_code, v_step_desc||loop_delete_set.BAS_DAY, v_time, v_cnt, TO_CHAR(SQLCODE), SQLERRM) ;
                     CONTINUE;
               end;
    End;
    END LOOP;
    ----------------------------------------------------------------------------
    --  1.4 Copy data from previous work day to holiday
    ----------------------------------------------------------------------------

    v_cnt := 0;

    FOR loop_holiday in v_holiday(v_wk_date,v_st_date_01,v_end_date_01)
    LOOP
    BEGIN

          INSERT INTO TM23_DDLY_BAL_SHET_A
          SELECT *
          FROM
          (
          SELECT T2.BAS_DAY
                ,T1.PCF_ID
                ,T1.PCF_COA_ID
                ,T1.ON_OFF_TYP_CD
                ,T1.OPN_DR_BAL
                ,T1.OPN_CR_BAL
                ,T1.INCR_DR_BAL
                ,T1.INCR_CR_BAL
                ,T1.CLO_DR_BAL
                ,T1.CLO_CR_BAL
                ,T1.TRGT_DATA_LST_MOD_TM
          FROM   TM23_DDLY_BAL_SHET_A T1 INNER JOIN (SELECT BAS_DAY, TO_CHAR((TO_DATE(BAS_DAY, 'YYYYMMDD') -1), 'YYYYMMDD') AS PREV_WRK_DAY
                                                     FROM   TM00_DDLY_CAL_D
                                                     WHERE  BAS_DAY = loop_holiday.BAS_DAY
                                                     ) T2
                                                  ON T2.PREV_WRK_DAY = T1.BAS_DAY
          )
          WHERE  BAS_DAY||PCF_ID NOT IN (SELECT BAS_DAY||PCF_ID
                                         FROM   TM23_DDLY_BAL_SHET_A
                                         WHERE  BAS_DAY = loop_holiday.BAS_DAY
                                         GROUP BY BAS_DAY||PCF_ID);

          ----------------------------------------------------------------------------
          --  Transaction Log
          ----------------------------------------------------------------------------
          v_step_code    := '040' ;
          v_step_desc    := v_wk_date || v_seq || ' ' || loop_holiday.BAS_DAY ||' : Inserting Data Result';
          v_time         := SYSDATE ;

          P1_BSM_PROG_EXEC_LOG(v_program_id, v_program_type_name, v_step_code, v_step_desc, v_time, sql%rowcount, NULL, NULL) ;

          COMMIT;
          ----------------------------------------------------------------------------

          v_cnt := v_cnt+sql%rowcount;

          EXCEPTION
          WHEN OTHERS THEN
               begin
                     v_step_code    := '041' ;
                     v_step_desc    := v_wk_date || v_seq || ' : Inserting Data Error loop_holiday with BAS_DAY = ' ;
                     v_time         := SYSDATE ;
                     P1_BSM_PROG_EXEC_LOG(v_program_id, v_program_type_name, v_step_code, v_step_desc||loop_holiday.BAS_DAY, v_time, v_cnt, TO_CHAR(SQLCODE), SQLERRM) ;
                     CONTINUE;
               end;
    End;
    END LOOP;
*/
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

    v_time := SYSDATE ;

    P1_BSM_PROG_EXEC_LOG(v_program_id, v_program_type_name, v_step_code, v_step_desc, v_time, NULL, TO_CHAR(SQLCODE), SQLERRM) ;

END ;
/