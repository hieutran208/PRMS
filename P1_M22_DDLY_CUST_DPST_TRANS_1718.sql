create or replace PROCEDURE            "P1_M22_DDLY_CUST_DPST_TRANS_1718"
                 ( p_wkdate    IN  CHAR DEFAULT NULL
                 , p_stdate_01 IN  CHAR DEFAULT NULL
                 , p_eddate_01 IN  CHAR DEFAULT NULL
                 )
IS
    /*--------------------------------------------------------------------------
     * PROGRAM ID    : P1_M22_DDLY_CUST_DPST_TRANS_1718
     * PROGRAM NAME  : A program for insert data to TM22_DDLY_CUST_DPST_TRANS_A
     * SOURCE TABLE  : TB06_G32_003_TTGS_A
     * TARGET TABLE  : TM22_DDLY_CUST_DPST_TRANS_A
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

    v_program_id              VARCHAR2(100) DEFAULT 'P1_M22_DDLY_CUST_DPST_TRANS_1718' ;
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
    SELECT T1.BAS_DAY
    FROM   TM00_DDLY_CAL_D T1 INNER JOIN (SELECT NVL(MIN(DATA_BAS_DAY), TO_CHAR(TO_DATE(v_dt2, 'YYYYMMDD') -2, 'YYYYMMDD')) AS MIN_DAY, TO_CHAR(TO_DATE(v_dt2, 'YYYYMMDD') -2, 'YYYYMMDD') AS MAX_DAY
                                          FROM   TBSM_INPT_RPT_SUBMIT_L
                                          WHERE  BTCH_BAS_DAY = v_dt2
                                          AND    INPT_RPT_ID  = 'G32_003_TTGS'
                                         ) T2
                                      ON T1.BAS_DAY BETWEEN T2.MIN_DAY AND T2.MAX_DAY
    ORDER BY 1;

/*
    CURSOR v_sbmt_day(v_dt1 VARCHAR2, v_dt2 VARCHAR2, v_dt3 VARCHAR2) IS
    SELECT T1.BAS_DAY
    FROM   TM00_DDLY_CAL_D T1 INNER JOIN (SELECT '20200401' AS MIN_DAY, '20200521' AS MAX_DAY
                                          FROM   DUAL
                                         ) T2
                                      ON T1.BAS_DAY BETWEEN T2.MIN_DAY AND T2.MAX_DAY
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
    v_step_desc    := v_wk_date || v_seq || ' 00000000 : Start Procedure(' || TRIM(v_st_date_01) || ',' || TRIM(v_end_date_01) || ')';
    v_time         := SYSDATE ;

    P1_BSM_PROG_EXEC_LOG(v_program_id, v_program_type_name, v_step_code, v_step_desc, v_time, NULL, NULL, NULL) ;
    ----------------------------------------------------------------------------
    --  1.1 Delete data from two days ago on TM22_DDLY_CUST_DPST_TRANS_A
    ----------------------------------------------------------------------------

    DELETE
    FROM   TM22_DDLY_CUST_DPST_TRANS_A
    WHERE  BAS_DAY = TO_CHAR(TO_DATE(v_st_date_01, 'YYYYMMDD') -2, 'YYYYMMDD')
    ;

    ----------------------------------------------------------------------------
    --  Transaction Log
    ----------------------------------------------------------------------------
    v_step_code    := '010' ;
    v_step_desc    := v_wk_date || v_seq || ' : Delete data from two days ago on TM22_DDLY_CUST_DPST_TRANS_A ' ;
    v_time         := SYSDATE ;

    P1_BSM_PROG_EXEC_LOG(v_program_id, v_program_type_name, v_step_code, v_step_desc, v_time, sql%rowcount, NULL, NULL) ;

    COMMIT ;
    ----------------------------------------------------------------------------
    --  1.2 Replicate data from three days ago to two days ago
    ----------------------------------------------------------------------------

    INSERT /*+ APPEND PARALLEL(TM22_DDLY_CUST_DPST_TRANS_A, 4) */ INTO TM22_DDLY_CUST_DPST_TRANS_A
    SELECT TO_CHAR(TO_DATE(v_st_date_01, 'YYYYMMDD') -2, 'YYYYMMDD') AS BAS_DAY
          ,PCF_ID
          ,INPT_DATA_SEQ_NUM
          ,RPLC_DATA_BAS_DAY
          ,'Y' AS RPLC_DATA_YN
          ,CUST_ID
          ,CUST_TYP_CD
          ,INIT_DPST_TRM_CD
          ,RMTRT_DPST_TRM_CD
          ,DPST_BAL
          ,OPN_DAY
          ,MTRT_DAY
          ,ACTL_IR
          ,SYSTIMESTAMP
    FROM   TM22_DDLY_CUST_DPST_TRANS_A
    WHERE  BAS_DAY = TO_CHAR(TO_DATE(v_st_date_01, 'YYYYMMDD') -3, 'YYYYMMDD')
    AND    DPST_BAL > 0
    AND    (MTRT_DAY >= TO_CHAR(TO_DATE(BAS_DAY, 'YYYYMMDD') - 30, 'YYYYMMDD') OR INIT_DPST_TRM_CD = 'ZZZ')
    ;

    ----------------------------------------------------------------------------
    --  Transaction Log
    ----------------------------------------------------------------------------
    v_step_code    := '020' ;
    v_step_desc    := v_wk_date || v_seq || ' : Replicate data from three days ago to two days ago ' ;
    v_time         := SYSDATE ;

    P1_BSM_PROG_EXEC_LOG(v_program_id, v_program_type_name, v_step_code, v_step_desc, v_time, sql%rowcount, NULL, NULL) ;

    COMMIT ;
    ----------------------------------------------------------------------------
    --  1.3 Delete Historical Data
    ----------------------------------------------------------------------------

    FOR loop_bas_day in v_sbmt_day(v_wk_date,v_st_date_01,v_end_date_01)
    LOOP
    BEGIN


          DELETE
            FROM TM22_DDLY_CUST_DPST_TRANS_A T1
           WHERE T1.BAS_DAY = loop_bas_day.BAS_DAY
           AND   EXISTS (SELECT *
                         FROM   (SELECT PCF_ID
                                 FROM   TBSM_INPT_RPT_SUBMIT_L
                                 WHERE  BTCH_BAS_DAY     = v_st_date_01
                                 AND    INPT_RPT_ID      = 'G32_003_TTGS'
                                 AND    DATA_BAS_DAY    <= loop_bas_day.BAS_DAY
                                 GROUP BY PCF_ID
                                ) T2
                         WHERE  T2.PCF_ID     = T1.PCF_ID
                        );

          ----------------------------------------------------------------------------
          --  Transaction Log
          ----------------------------------------------------------------------------
          v_step_code    := '030' ;
          v_step_desc    := v_wk_date || v_seq || ' ' || loop_bas_day.BAS_DAY ||' : Deleting Data Result';
          v_time         := SYSDATE ;

          P1_BSM_PROG_EXEC_LOG(v_program_id, v_program_type_name, v_step_code, v_step_desc, v_time, sql%rowcount, NULL, NULL) ;

          COMMIT ;
          ----------------------------------------------------------------------------

          v_cnt := v_cnt+sql%rowcount;

          EXCEPTION
          WHEN OTHERS THEN
               begin
                     v_step_code    := '031' ;
                     v_step_desc    := v_wk_date || v_seq || ' : Deleting Data Error with BAS_DAY = ' ;
                     v_time         := SYSDATE ;
                     P1_BSM_PROG_EXEC_LOG(v_program_id, v_program_type_name, v_step_code, v_step_desc||loop_bas_day.BAS_DAY, v_time, v_cnt, TO_CHAR(SQLCODE), SQLERRM) ;
                     CONTINUE;
               end;
    End;
    END LOOP;

    ----------------------------------------------------------------------------
    --  1.4 Inserting Data Results
    ----------------------------------------------------------------------------

    FOR loop_bas_day in v_sbmt_day(v_wk_date,v_st_date_01,v_end_date_01)
    LOOP
    BEGIN

          INSERT /*+ APPEND PARALLEL(TM22_DDLY_CUST_DPST_TRANS_A, 4) */ INTO TM22_DDLY_CUST_DPST_TRANS_A
          (   BAS_DAY
             ,PCF_ID
             ,INPT_DATA_SEQ_NUM
             ,RPLC_DATA_BAS_DAY
             ,RPLC_DATA_YN
             ,CUST_ID
             ,CUST_TYP_CD
             ,INIT_DPST_TRM_CD
             ,RMTRT_DPST_TRM_CD
             ,DPST_BAL
             ,OPN_DAY
             ,MTRT_DAY
             ,ACTL_IR
             ,TRGT_DATA_LST_MOD_TM
          )
          WITH
          NEW_DATA AS
          (SELECT T1.BAS_YMD AS BAS_DAY,
                  T1.PCF_ID,
                  T1.DISPLAY_ORDER AS INPT_DATA_SEQ_NUM,
                  T1.BAS_YMD AS RPLC_DATA_BAS_DAY,
                  CASE WHEN EXISTS (SELECT 1
                                        FROM   TBSM_INPT_RPT_SUBMIT_L T2
                                        WHERE  T2.PCF_ID        = T1.PCF_ID
                                        AND    T2.INPT_RPT_ID   = 'G32_003_TTGS'
                                        AND    T2.BTCH_BAS_DAY  < v_st_date_01
                                        AND    T2.DATA_BAS_DAY  = T1.BAS_YMD
                                        ) 
                            THEN 'Y'
                            ELSE 'N' END AS RPLC_DATA_YN,
                  T1.TX_CD_ID AS CUST_ID,
                  CASE WHEN T1.GENDER_CODE IN ('M', 'F') THEN CASE WHEN T1.IS_MEMBER = '1' THEN '1'
                                                                   WHEN T1.IS_MEMBER = '2' THEN '3'
                                                                   ELSE '8' 
                                                                   END
                       WHEN T1.GENDER_CODE IN ('8') THEN CASE WHEN T1.IS_MEMBER = '1' THEN '2'
                                                              WHEN T1.IS_MEMBER = '2' THEN '4'
                                                              ELSE '8' 
                                                              END
                       ELSE '8'
                       END AS CUST_TYP_CD,
                  NVL(T1.TERM_CODE, 'ZZZ') AS INIT_DPST_TRM_CD,
                  CASE WHEN TO_NUMBER(TO_DATE(T1.DUE_DATE, 'YYYYMMDD') - TO_DATE(T1.BAS_YMD, 'YYYYMMDD')) > 29970
                            THEN 'XXX'
                       WHEN TO_NUMBER(TO_DATE(T1.DUE_DATE, 'YYYYMMDD') - TO_DATE(T1.BAS_YMD, 'YYYYMMDD')) > 0 AND TO_NUMBER(TO_DATE(T1.DUE_DATE, 'YYYYMMDD') - TO_DATE(T1.BAS_YMD, 'YYYYMMDD')) <= 29970
                            THEN  CASE WHEN MOD(TO_NUMBER(TO_DATE(T1.DUE_DATE, 'YYYYMMDD') - TO_DATE(T1.BAS_YMD, 'YYYYMMDD')) / 30, 1) = 0
                                            THEN LPAD(TRUNC(TO_NUMBER(TO_DATE(T1.DUE_DATE, 'YYYYMMDD') - TO_DATE(T1.BAS_YMD, 'YYYYMMDD')) / 30, 0), 3, '0')
                                       ELSE LPAD(TRUNC(TO_NUMBER(TO_DATE(T1.DUE_DATE, 'YYYYMMDD') - TO_DATE(T1.BAS_YMD, 'YYYYMMDD')) / 30, 0) + 1, 3, '0')
                                       END
                       ELSE 'ZZZ'
                       END AS RMTRT_DPST_TRM_CD,
                  T1.DPST_BAL,
                  T1.OPEN_DATE AS OPN_DAY,
                  T1.DUE_DATE AS MTRT_DAY,
                  T1.INT_RATE AS ACTL_IR
           FROM   (SELECT T1.BAS_YMD, T1.PCF_ID, NVL(T1.CUSTOMER_CODE, 'XXXX') AS TX_CD_ID, T1.GENDER_CODE, T1.TERM_CODE, T1.OPEN_DATE, T1.DUE_DATE, T1.INT_RATE, SUM(T1.DPST_CUR_BAL) AS DPST_BAL, MAX(T1.DISPLAY_ORDER) AS DISPLAY_ORDER, T1.IS_MEMBER
                   FROM   TB06_G32_003_TTGS_A T1 INNER JOIN (SELECT PCF_ID
                                                        FROM   TBSM_INPT_RPT_SUBMIT_L
                                                        WHERE  BTCH_BAS_DAY     = v_st_date_01
                                                        AND    INPT_RPT_ID      = 'G32_003_TTGS'
                                                        AND    DATA_BAS_DAY    <= loop_bas_day.BAS_DAY
                                                        GROUP BY PCF_ID
                                                       ) T2
                                                    ON T2.PCF_ID = T1.PCF_ID
                   WHERE  T1.BAS_YMD = loop_bas_day.BAS_DAY
                   GROUP BY T1.BAS_YMD, T1.PCF_ID, NVL(T1.CUSTOMER_CODE, 'XXXX'), T1.GENDER_CODE, T1.TERM_CODE, T1.OPEN_DATE, T1.DUE_DATE, T1.INT_RATE, T1.IS_MEMBER
                  ) T1
          ),
          DATA_SET AS
          (SELECT PCF_ID, CUST_ID, CUST_TYP_CD, INIT_DPST_TRM_CD, OPN_DAY, ACTL_IR
           FROM   (
                   SELECT PCF_ID, CUST_ID, CUST_TYP_CD, INIT_DPST_TRM_CD, OPN_DAY, ACTL_IR
                   FROM   NEW_DATA
                   GROUP BY PCF_ID, CUST_ID, CUST_TYP_CD, INIT_DPST_TRM_CD, OPN_DAY, ACTL_IR

                   UNION ALL

                   SELECT PCF_ID, CUST_ID, CUST_TYP_CD, INIT_DPST_TRM_CD, MTRT_DAY AS OPN_DAY, ACTL_IR
                   FROM   NEW_DATA
                   GROUP BY PCF_ID, CUST_ID, CUST_TYP_CD, INIT_DPST_TRM_CD, MTRT_DAY, ACTL_IR
                  )
           GROUP BY PCF_ID, CUST_ID, CUST_TYP_CD, INIT_DPST_TRM_CD, OPN_DAY, ACTL_IR
          ),
          COPY_DATA AS
          (SELECT TO_CHAR(TO_DATE(T1.BAS_DAY, 'YYYYMMDD') + 1, 'YYYYMMDD') AS NEW_BAS_DAY
                 ,T1.BAS_DAY
                 ,T1.PCF_ID
                 ,T1.INPT_DATA_SEQ_NUM
                 ,T1.RPLC_DATA_BAS_DAY
                 ,'Y' AS RPLC_DATA_YN
                 ,T1.CUST_ID
                 ,T1.CUST_TYP_CD
                 ,T1.INIT_DPST_TRM_CD
                 ,T1.RMTRT_DPST_TRM_CD
                 ,T1.DPST_BAL
                 ,T1.OPN_DAY
                 ,T1.MTRT_DAY
                 ,T1.ACTL_IR
           FROM   TM22_DDLY_CUST_DPST_TRANS_A T1 INNER JOIN (SELECT PCF_ID
                                                             FROM   TBSM_INPT_RPT_SUBMIT_L
                                                             WHERE  BTCH_BAS_DAY     = v_st_date_01
                                                             AND    INPT_RPT_ID      = 'G32_003_TTGS'
                                                             AND    DATA_BAS_DAY    <= loop_bas_day.BAS_DAY
                                                             GROUP BY PCF_ID
                                                            ) T3
                                                         ON T3.PCF_ID = T1.PCF_ID
           WHERE  NOT EXISTS (SELECT *
                              FROM   DATA_SET T2
                              WHERE  T2.PCF_ID           = T1.PCF_ID
                              AND    T2.CUST_ID          = T1.CUST_ID
                              AND    T2.CUST_TYP_CD      = T1.CUST_TYP_CD
                              AND    T2.INIT_DPST_TRM_CD = T1.INIT_DPST_TRM_CD
                              AND    T2.OPN_DAY          = T1.OPN_DAY
                              AND    T2.ACTL_IR          = T1.ACTL_IR
                             )
           AND    T1.BAS_DAY = TO_CHAR(TO_DATE(loop_bas_day.BAS_DAY, 'YYYYMMDD') -1, 'YYYYMMDD')
           AND    T1.DPST_BAL > 0
           AND    (T1.MTRT_DAY >= TO_CHAR(TO_DATE(T1.BAS_DAY, 'YYYYMMDD') - 30, 'YYYYMMDD') OR T1.INIT_DPST_TRM_CD = 'ZZZ')
          )
          SELECT BAS_DAY
                ,PCF_ID
                ,INPT_DATA_SEQ_NUM
                ,RPLC_DATA_BAS_DAY
                ,RPLC_DATA_YN
                ,CUST_ID
                ,CUST_TYP_CD
                ,INIT_DPST_TRM_CD
                ,RMTRT_DPST_TRM_CD
                ,DPST_BAL
                ,OPN_DAY
                ,MTRT_DAY
                ,ACTL_IR
                ,SYSTIMESTAMP
          FROM   NEW_DATA

          UNION ALL

          SELECT NEW_BAS_DAY
                ,PCF_ID
                ,INPT_DATA_SEQ_NUM
                ,RPLC_DATA_BAS_DAY
                ,RPLC_DATA_YN
                ,CUST_ID
                ,CUST_TYP_CD
                ,INIT_DPST_TRM_CD
                ,CASE WHEN TO_NUMBER(TO_DATE(MTRT_DAY, 'YYYYMMDD') - TO_DATE(NEW_BAS_DAY, 'YYYYMMDD')) > 29970
                           THEN 'XXX'
                      WHEN TO_NUMBER(TO_DATE(MTRT_DAY, 'YYYYMMDD') - TO_DATE(NEW_BAS_DAY, 'YYYYMMDD')) > 0 AND TO_NUMBER(TO_DATE(MTRT_DAY, 'YYYYMMDD') - TO_DATE(NEW_BAS_DAY, 'YYYYMMDD')) <= 29970
                           THEN  CASE WHEN MOD(TO_NUMBER(TO_DATE(MTRT_DAY, 'YYYYMMDD') - TO_DATE(NEW_BAS_DAY, 'YYYYMMDD')) / 30, 1) = 0
                                           THEN LPAD(TRUNC(TO_NUMBER(TO_DATE(MTRT_DAY, 'YYYYMMDD') - TO_DATE(NEW_BAS_DAY, 'YYYYMMDD')) / 30, 0), 3, '0')
                                      ELSE LPAD(TRUNC(TO_NUMBER(TO_DATE(MTRT_DAY, 'YYYYMMDD') - TO_DATE(NEW_BAS_DAY, 'YYYYMMDD')) / 30, 0) + 1, 3, '0')
                                      END
                      ELSE 'ZZZ'
                      END AS RMTRT_DPST_TRM_CD
                ,DPST_BAL
                ,OPN_DAY
                ,MTRT_DAY
                ,ACTL_IR
                ,SYSTIMESTAMP
          FROM   COPY_DATA;

          ----------------------------------------------------------------------------
          --  Transaction Log
          ----------------------------------------------------------------------------
          v_step_code    := '040' ;
          v_step_desc    := v_wk_date || v_seq || ' ' || loop_bas_day.BAS_DAY ||' : Inserting Data Result';
          v_time         := SYSDATE ;

          P1_BSM_PROG_EXEC_LOG(v_program_id, v_program_type_name, v_step_code, v_step_desc, v_time, sql%rowcount, NULL, NULL) ;

          COMMIT ;
          ----------------------------------------------------------------------------

          v_cnt := v_cnt+sql%rowcount;

          EXCEPTION
          WHEN OTHERS THEN
               begin
                     v_step_code    := '041' ;
                     v_step_desc    := v_wk_date || v_seq || ' : Inserting Data Error with BAS_DAY = ' ;
                     v_time         := SYSDATE ;
                     P1_BSM_PROG_EXEC_LOG(v_program_id, v_program_type_name, v_step_code, v_step_desc||loop_bas_day.BAS_DAY, v_time, v_cnt, TO_CHAR(SQLCODE), SQLERRM) ;
                     CONTINUE;
               end;
    End;
    END LOOP;

    ----------------------------------------------------------------------------
    --  Write Program End Log
    ----------------------------------------------------------------------------
    v_step_code    := '999' ;
    v_step_desc    := v_wk_date || v_seq || ' 99999999 : End Procedure' ;
    v_time         := SYSDATE ;

    P1_BSM_PROG_EXEC_LOG(v_program_id, v_program_type_name, v_step_code, v_step_desc, v_time, NULL, NULL, NULL) ;
    COMMIT;
    ----------------------------------------------------------------------------
    --  EXCEPTION
    ----------------------------------------------------------------------------
    EXCEPTION
    WHEN OTHERS THEN ROLLBACK ;

    P1_BSM_PROG_EXEC_LOG(v_program_id, v_program_type_name, v_step_code, v_step_desc, v_time, NULL, TO_CHAR(SQLCODE), SQLERRM) ;

END ;
/