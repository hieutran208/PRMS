create or replace PROCEDURE "P1_M21_DDLY_CUST_CR_TRANS_1718"
                 ( p_wkdate    IN  CHAR DEFAULT NULL
                 , p_stdate_01 IN  CHAR DEFAULT NULL
                 , p_eddate_01 IN  CHAR DEFAULT NULL
                 )
IS
    /*--------------------------------------------------------------------------
     * PROGRAM ID    : P1_M21_DDLY_CUST_CR_TRANS_1718
     * PROGRAM NAME  : A program for insert data to TM21_DDLY_CUST_CR_TRANS_A
     * SOURCE TABLE  : TB05_G32_012_TTGS_A
                       TBSM_INPT_RPT_SUBMIT_L
                       TM21_DDLY_CUST_CR_TRANS_A
                       TM00_DDLY_CAL_D
     * TARGET TABLE  : TM21_DDLY_CUST_CR_TRANS_A
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

    v_program_id              VARCHAR2(100) DEFAULT 'P1_M21_DDLY_CUST_CR_TRANS_1718' ;
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
                                          AND    INPT_RPT_ID  = 'G32_012_TTGS'
                                         ) T2
                                      ON T1.BAS_DAY BETWEEN T2.MIN_DAY AND T2.MAX_DAY
    ORDER BY 1;

   /* CURSOR v_sbmt_day(v_dt1 VARCHAR2, v_dt2 VARCHAR2, v_dt3 VARCHAR2) IS
    SELECT T1.BAS_DAY
    FROM   TM00_DDLY_CAL_D T1 INNER JOIN (SELECT '20240101' AS MIN_DAY, '20240930' AS MAX_DAY
                                          FROM   DUAL
                                         ) T2
                                      ON T1.BAS_DAY BETWEEN T2.MIN_DAY AND T2.MAX_DAY
    ORDER BY 1;*/

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
    --  1.1 Delete data from two days ago on TM21_DDLY_CUST_CR_TRANS_A
    ----------------------------------------------------------------------------

    DELETE
    FROM   TM21_DDLY_CUST_CR_TRANS_A
    WHERE  BAS_DAY = TO_CHAR(TO_DATE(v_st_date_01, 'YYYYMMDD') -2, 'YYYYMMDD')
    ;

    ----------------------------------------------------------------------------
    --  Transaction Log
    ----------------------------------------------------------------------------
    v_step_code    := '010' ;
    v_step_desc    := v_wk_date || v_seq || ' : Delete data from two days ago on TM21_DDLY_CUST_CR_TRANS_A ' ;
    v_time         := SYSDATE ;

    P1_BSM_PROG_EXEC_LOG(v_program_id, v_program_type_name, v_step_code, v_step_desc, v_time, sql%rowcount, NULL, NULL) ;

    COMMIT ;
    ----------------------------------------------------------------------------
    --  1.2 Replicate data from three days ago to two days ago
    ----------------------------------------------------------------------------

    INSERT INTO TM21_DDLY_CUST_CR_TRANS_A
    SELECT TO_CHAR(TO_DATE(v_st_date_01, 'YYYYMMDD') -2, 'YYYYMMDD') AS BAS_DAY
          ,PCF_ID
          ,INPT_DATA_SEQ_NUM
          ,RPLC_DATA_BAS_DAY
          ,'Y' AS RPLC_DATA_YN
          ,CUST_ID
          ,CUST_NM
          ,LN_CNTR_NUM_INFO
          ,CUST_TYP_CD
          ,INIT_LN_TRM_CD
          ,RMTRT_LN_TRM_CD
          ,ES_CD
          ,COLL_TYP_INFO
          ,COLL_TYP_CD
          ,LN_TRANS_TYP_CD
          ,OLD_NORML_DBT_GRP_CD
          ,OLD_OVD_DBT_GRP_CD
          ,NEW_NORML_DBT_GRP_CD
          ,NEW_OVD_DBT_GRP_CD
          ,LN_CNTR_AMT
          ,LN_BAL
          ,TRANS_AMT
          ,COLL_VAL_AMT
          ,SPEC_PRVS_AMT
          ,OPN_DAY
          ,MTRT_DAY
          ,ACTL_IR
          ,SYSTIMESTAMP
    FROM   TM21_DDLY_CUST_CR_TRANS_A
    WHERE  BAS_DAY = TO_CHAR(TO_DATE(v_st_date_01, 'YYYYMMDD') -3, 'YYYYMMDD')
    AND    LN_BAL > 0
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
            FROM TM21_DDLY_CUST_CR_TRANS_A T1
           WHERE T1.BAS_DAY = loop_bas_day.BAS_DAY
           AND   EXISTS (SELECT *
                         FROM   (SELECT PCF_ID
                                 FROM   TBSM_INPT_RPT_SUBMIT_L
                                 WHERE  BTCH_BAS_DAY     = v_st_date_01
                                 AND    INPT_RPT_ID      = 'G32_012_TTGS'
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

          INSERT INTO TM21_DDLY_CUST_CR_TRANS_A
          (   BAS_DAY
             ,PCF_ID
             ,INPT_DATA_SEQ_NUM
             ,RPLC_DATA_BAS_DAY
             ,RPLC_DATA_YN
             ,CUST_ID
             ,CUST_NM
             ,LN_CNTR_NUM_INFO
             ,CUST_TYP_CD
             ,INIT_LN_TRM_CD
             ,RMTRT_LN_TRM_CD
             ,ES_CD
             ,COLL_TYP_INFO
             ,COLL_TYP_CD
             ,LN_TRANS_TYP_CD
             ,OLD_NORML_DBT_GRP_CD
             ,OLD_OVD_DBT_GRP_CD
             ,NEW_NORML_DBT_GRP_CD
             ,NEW_OVD_DBT_GRP_CD
             ,LN_CNTR_AMT
             ,LN_BAL
             ,TRANS_AMT
             ,COLL_VAL_AMT
             ,SPEC_PRVS_AMT
             ,OPN_DAY
             ,MTRT_DAY
             ,ACTL_IR
             ,TRGT_DATA_LST_MOD_TM
          )
          WITH
          NEW_DATA AS
          (SELECT T1.BAS_YMD AS BAS_DAY,
                  T1.PCF_ID,
                  MAX(TO_NUMBER(T1.ORDINAL_NUMBER)) AS INPT_DATA_SEQ_NUM,
                  T1.BAS_YMD AS RPLC_DATA_BAS_DAY,
                  'N' AS RPLC_DATA_YN,
                  MAX(T1.CUSTOMER_CODE) AS CUST_ID,
                  MAX(T1.CUSTOMER_NAME) AS CUST_NM,
                  T1.CREDIT_CONTRACT_NO AS LN_CNTR_NUM_INFO,
                  MAX(T1.CUSTOMER_TYPE) AS CUST_TYP_CD,
                  NVL(MAX(T1.LOAN_TERM_CONTRACT), 'ZZZ') AS INIT_LN_TRM_CD,
                  MAX(CASE WHEN TO_NUMBER(TO_DATE(T1.EXTEND_MATURITY_DATE, 'YYYYMMDD') - TO_DATE(T1.BAS_YMD, 'YYYYMMDD')) > 29970
                                THEN 'XXX'
                           WHEN TO_NUMBER(TO_DATE(T1.EXTEND_MATURITY_DATE, 'YYYYMMDD') - TO_DATE(T1.BAS_YMD, 'YYYYMMDD')) > 0 AND TO_NUMBER(TO_DATE(T1.EXTEND_MATURITY_DATE, 'YYYYMMDD') - TO_DATE(T1.BAS_YMD, 'YYYYMMDD')) <= 29970
                                THEN  CASE WHEN MOD(TO_NUMBER(TO_DATE(T1.EXTEND_MATURITY_DATE, 'YYYYMMDD') - TO_DATE(T1.BAS_YMD, 'YYYYMMDD')) / 30, 1) = 0
                                                THEN LPAD(TRUNC(TO_NUMBER(TO_DATE(T1.EXTEND_MATURITY_DATE, 'YYYYMMDD') - TO_DATE(T1.BAS_YMD, 'YYYYMMDD')) / 30, 0), 3, '0')
                                           ELSE LPAD(TRUNC(TO_NUMBER(TO_DATE(T1.EXTEND_MATURITY_DATE, 'YYYYMMDD') - TO_DATE(T1.BAS_YMD, 'YYYYMMDD')) / 30, 0) + 1, 3, '0')
                                           END
                           ELSE 'ZZZ'
                           END) AS RMTRT_LN_TRM_CD,
                  MAX(T1.ECONOMIC_SECTOR_CODE) AS ES_CD,
                  MAX(CASE WHEN T1.COLL_TYPE_CODE = '201' THEN 'Sổ tiền gửi tại QTDND'
                           WHEN SUBSTR(T1.COLL_TYPE_CODE,1,1) = '4' THEN 'Bất Động Sản'
                           WHEN SUBSTR(T1.COLL_TYPE_CODE,1,1) IN ('8', '9') THEN 'Tín chấp, không có Tài sản đảm bảo'
                           ELSE 'Tài sản đảm bảo khác' END) AS COLL_TYP_INFO,
                  MAX(CASE WHEN T1.COLL_TYPE_CODE = '201' THEN  '02'
                           WHEN SUBSTR(T1.COLL_TYPE_CODE,1,1) = '4' THEN '01'
                           WHEN SUBSTR(T1.COLL_TYPE_CODE,1,1) IN ('8', '9') THEN '00'
                           ELSE '03' END) AS COLL_TYP_CD,
                  CASE WHEN SUM(T1.DISBURSE_AMT) > 0 THEN '1'
                           ELSE '5' END AS LN_TRANS_TYP_CD,
                  MAX(CASE WHEN T1.PREV_DEBT_GROUP IS NOT NULL THEN CASE WHEN T1.BAS_YMD <= T1.NEXT_PRINCIPAL_PAY_DATE
                                                                             AND T1.NEXT_PRINCIPAL_PAY_DATE <= T1.EXTEND_MATURITY_DATE
                                                                             AND T1.BAS_YMD <= T1.NEXT_INT_PAY_DATE 
                                                                             AND T1.NEXT_INT_PAY_DATE <= T1.EXTEND_MATURITY_DATE THEN T1.PREV_DEBT_GROUP
                                                                         ELSE NULL END
                           ELSE NULL END) AS OLD_NORML_DBT_GRP_CD,
                  MAX(CASE WHEN T1.PREV_DEBT_GROUP IS NOT NULL THEN CASE WHEN T1.BAS_YMD > T1.NEXT_PRINCIPAL_PAY_DATE
                                                                             OR T1.NEXT_PRINCIPAL_PAY_DATE > T1.EXTEND_MATURITY_DATE
                                                                             OR T1.BAS_YMD > T1.NEXT_INT_PAY_DATE 
                                                                             OR T1.NEXT_INT_PAY_DATE > T1.EXTEND_MATURITY_DATE THEN T1.PREV_DEBT_GROUP
                                                                         ELSE NULL END
                           ELSE NULL END) AS OLD_OVD_DBT_GRP_CD,
                  MAX(CASE WHEN T1.CUR_DEBT_GROUP IS NOT NULL THEN CASE WHEN T1.BAS_YMD <= T1.NEXT_PRINCIPAL_PAY_DATE
                                                                             AND T1.NEXT_PRINCIPAL_PAY_DATE <= T1.EXTEND_MATURITY_DATE
                                                                             AND T1.BAS_YMD <= T1.NEXT_INT_PAY_DATE 
                                                                             AND T1.NEXT_INT_PAY_DATE <= T1.EXTEND_MATURITY_DATE THEN T1.CUR_DEBT_GROUP
                                                                         ELSE NULL END
                           ELSE NULL END) AS NEW_NORML_DBT_GRP_CD,
                  MAX(CASE WHEN T1.CUR_DEBT_GROUP IS NOT NULL THEN CASE WHEN T1.BAS_YMD > T1.NEXT_PRINCIPAL_PAY_DATE
                                                                             OR T1.NEXT_PRINCIPAL_PAY_DATE > T1.EXTEND_MATURITY_DATE
                                                                             OR T1.BAS_YMD > T1.NEXT_INT_PAY_DATE 
                                                                             OR T1.NEXT_INT_PAY_DATE > T1.EXTEND_MATURITY_DATE THEN T1.CUR_DEBT_GROUP
                                                                         ELSE NULL END
                           ELSE NULL END) AS NEW_OVD_DBT_GRP_CD,
                  MAX(T1.TOTAL_LOAN_AMT) AS LN_CNTR_AMT,
                  MAX(T1.OUTSTND_BAL) AS LN_BAL,
                  CASE WHEN SUM(T1.DISBURSE_AMT) > 0 THEN SUM(T1.DISBURSE_AMT)
                        ELSE 0 END AS TRANS_AMT,
                  MAX(T1.COLL_VAL_VALUE) AS COLL_VAL_AMT,
                  MAX(T1.SPEC_PROVISION_TO_MAKE) AS SPEC_PRVS_AMT,
                  MAX(T1.LOAN_DATE) AS OPN_DAY,
                  MAX(T1.EXTEND_MATURITY_DATE) AS MTRT_DAY,
                  MAX(T1.CUR_INT_RATE) AS ACTL_IR
           FROM   TB05_G32_012_TTGS_A T1 INNER JOIN (SELECT PCF_ID
                                                FROM   TBSM_INPT_RPT_SUBMIT_L
                                                WHERE  BTCH_BAS_DAY     = v_st_date_01
                                                AND    INPT_RPT_ID      = 'G32_012_TTGS'
                                                AND    DATA_BAS_DAY    <= loop_bas_day.BAS_DAY
                                                GROUP BY PCF_ID
                                               ) T2
                                            ON T2.PCF_ID = T1.PCF_ID
           WHERE  T1.BAS_YMD = loop_bas_day.BAS_DAY
           AND    T1.CREDIT_CONTRACT_NO IS NOT NULL
           GROUP BY T1.BAS_YMD, T1.PCF_ID, T1.CREDIT_CONTRACT_NO
          ),
          COPY_DATA AS
          (SELECT TO_CHAR(TO_DATE(T1.BAS_DAY, 'YYYYMMDD') + 1, 'YYYYMMDD') AS NEW_BAS_DAY
                 ,T1.BAS_DAY
                 ,T1.PCF_ID
                 ,T1.INPT_DATA_SEQ_NUM
                 ,T1.RPLC_DATA_BAS_DAY
                 ,'Y' AS RPLC_DATA_YN
                 ,T1.CUST_ID
                 ,T1.CUST_NM
                 ,T1.LN_CNTR_NUM_INFO
                 ,T1.CUST_TYP_CD
                 ,T1.INIT_LN_TRM_CD
                 ,T1.RMTRT_LN_TRM_CD
                 ,T1.ES_CD
                 ,T1.COLL_TYP_INFO
                 ,T1.COLL_TYP_CD
                 ,T1.LN_TRANS_TYP_CD
                 ,T1.OLD_NORML_DBT_GRP_CD
                 ,T1.OLD_OVD_DBT_GRP_CD
                 ,T1.NEW_NORML_DBT_GRP_CD
                 ,T1.NEW_OVD_DBT_GRP_CD
                 ,T1.LN_CNTR_AMT
                 ,T1.LN_BAL
                 ,T1.TRANS_AMT
                 ,T1.COLL_VAL_AMT
                 ,T1.SPEC_PRVS_AMT
                 ,T1.OPN_DAY
                 ,T1.MTRT_DAY
                 ,T1.ACTL_IR
           FROM   TM21_DDLY_CUST_CR_TRANS_A T1 INNER JOIN (SELECT PCF_ID
                                                           FROM   TBSM_INPT_RPT_SUBMIT_L
                                                           WHERE  BTCH_BAS_DAY     = v_st_date_01
                                                           AND    INPT_RPT_ID      = 'G32_012_TTGS'
                                                           AND    DATA_BAS_DAY    <= loop_bas_day.BAS_DAY
                                                           GROUP BY PCF_ID
                                                          ) T3
                                                       ON T3.PCF_ID = T1.PCF_ID
           WHERE  NOT EXISTS (SELECT *
                              FROM   (SELECT BAS_DAY, PCF_ID, LN_CNTR_NUM_INFO
                                      FROM   NEW_DATA
                                      GROUP BY BAS_DAY, PCF_ID, LN_CNTR_NUM_INFO
                                     ) T2
                              WHERE  T2.BAS_DAY          = TO_CHAR(TO_DATE(T1.BAS_DAY, 'YYYYMMDD') + 1, 'YYYYMMDD')
                              AND    T2.PCF_ID           = T1.PCF_ID
                              AND    T2.LN_CNTR_NUM_INFO = T1.LN_CNTR_NUM_INFO
                             )
           AND    T1.BAS_DAY = TO_CHAR(TO_DATE(loop_bas_day.BAS_DAY, 'YYYYMMDD') -1, 'YYYYMMDD')
           AND    T1.LN_BAL > 0
          )
          SELECT BAS_DAY
                ,PCF_ID
                ,INPT_DATA_SEQ_NUM
                ,RPLC_DATA_BAS_DAY
                ,RPLC_DATA_YN
                ,CUST_ID
                ,CUST_NM
                ,LN_CNTR_NUM_INFO
                ,CUST_TYP_CD
                ,INIT_LN_TRM_CD
                ,RMTRT_LN_TRM_CD
                ,NVL(ES_CD, '9998')
                ,COLL_TYP_INFO
                ,COLL_TYP_CD
                ,LN_TRANS_TYP_CD
                ,OLD_NORML_DBT_GRP_CD
                ,OLD_OVD_DBT_GRP_CD
                ,NEW_NORML_DBT_GRP_CD
                ,NEW_OVD_DBT_GRP_CD
                ,LN_CNTR_AMT
                ,LN_BAL
                ,TRANS_AMT
                ,COLL_VAL_AMT
                ,SPEC_PRVS_AMT
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
                ,CUST_NM
                ,LN_CNTR_NUM_INFO
                ,CUST_TYP_CD
                ,INIT_LN_TRM_CD
                ,CASE WHEN TO_NUMBER(TO_DATE(MTRT_DAY, 'YYYYMMDD') - TO_DATE(NEW_BAS_DAY, 'YYYYMMDD')) > 29970
                           THEN 'XXX'
                      WHEN TO_NUMBER(TO_DATE(MTRT_DAY, 'YYYYMMDD') - TO_DATE(NEW_BAS_DAY, 'YYYYMMDD')) > 0 AND TO_NUMBER(TO_DATE(MTRT_DAY, 'YYYYMMDD') - TO_DATE(NEW_BAS_DAY, 'YYYYMMDD')) <= 29970
                           THEN  CASE WHEN MOD(TO_NUMBER(TO_DATE(MTRT_DAY, 'YYYYMMDD') - TO_DATE(NEW_BAS_DAY, 'YYYYMMDD')) / 30, 1) = 0
                                           THEN LPAD(TRUNC(TO_NUMBER(TO_DATE(MTRT_DAY, 'YYYYMMDD') - TO_DATE(NEW_BAS_DAY, 'YYYYMMDD')) / 30, 0), 3, '0')
                                      ELSE LPAD(TRUNC(TO_NUMBER(TO_DATE(MTRT_DAY, 'YYYYMMDD') - TO_DATE(NEW_BAS_DAY, 'YYYYMMDD')) / 30, 0) + 1, 3, '0')
                                      END
                           ELSE 'ZZZ'
                           END AS RMTRT_LN_TRM_CD
                ,ES_CD
                ,COLL_TYP_INFO
                ,COLL_TYP_CD
                ,LN_TRANS_TYP_CD
                ,OLD_NORML_DBT_GRP_CD
                ,OLD_OVD_DBT_GRP_CD
                ,NEW_NORML_DBT_GRP_CD
                ,NEW_OVD_DBT_GRP_CD
                ,LN_CNTR_AMT
                ,LN_BAL
                ,TRANS_AMT
                ,COLL_VAL_AMT
                ,SPEC_PRVS_AMT
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
    ----------------------------------------------------------------------------
    --  EXCEPTION
    ----------------------------------------------------------------------------
    EXCEPTION
    WHEN OTHERS THEN ROLLBACK ;

    P1_BSM_PROG_EXEC_LOG(v_program_id, v_program_type_name, v_step_code, v_step_desc, v_time, NULL, TO_CHAR(SQLCODE), SQLERRM) ;

END ;
/