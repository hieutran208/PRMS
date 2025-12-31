create or replace PROCEDURE            "P4_M24_MMLY_OTHR_IDC_1718"
                 ( p_wkdate    IN  CHAR DEFAULT NULL
                 , p_stdate_01 IN  CHAR DEFAULT NULL
                 , p_eddate_01 IN  CHAR DEFAULT NULL
                 )
IS
    /*--------------------------------------------------------------------------
     * PROGRAM ID    : P4_M24_MMLY_OTHR_IDC_1718
     * PROGRAM NAME  : A program for insert data to TM24_MMLY_OTHR_IDC_A
     * SOURCE TABLE  : TM21_DDLY_CUST_CR_TRANS_A
                       TM24_MMLY_IDC_CALC_CAR_A
                       TB05_G32_007_TTGS_A
     * TARGET TABLE  : TM24_MMLY_OTHR_IDC_A
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

    v_program_id              VARCHAR2(100) DEFAULT 'P4_M24_MMLY_OTHR_IDC_1718' ;
    v_program_type_name       VARCHAR2(100) DEFAULT 'Stored Procedure' ;
    v_step_code               VARCHAR2(3) ;
    v_step_desc               VARCHAR2(100) ;
    v_time                    DATE ;
    --v_cnt                     NUMBER DEFAULT 0 ;
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
            WHERE  BTCH_BAS_DAY >= v_st_date_01
            AND    INPT_RPT_ID IN ('G035841','G031341','G32_007_TTGS')

            UNION

            SELECT T1.BAS_YM
            FROM   TM00_MMLY_CAL_D T1 INNER JOIN (SELECT SUBSTR(MIN(DATA_BAS_DAY), 1, 6) AS MIN_YM, SUBSTR(MAX(DATA_BAS_DAY), 1, 6) AS MAX_YM
                                                  FROM   TBSM_INPT_RPT_SUBMIT_L
                                                  WHERE  BTCH_BAS_DAY >= v_st_date_01
                                                  AND    INPT_RPT_ID  = 'G32_012_TTGS'
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
     DBMS_OUTPUT.put_line(v_st_date_01);
    P1_BSM_PROG_EXEC_LOG(v_program_id, v_program_type_name, v_step_code, v_step_desc, v_time, NULL, NULL, NULL) ;
    ----------------------------------------------------------------------------
    --  1.1 Delete & Insert
    ----------------------------------------------------------------------------
    FOR loop_bas_day in v_sbmt_day(v_wk_date,v_st_date_01,v_end_date_01)
    LOOP
    BEGIN

          DELETE
            FROM TM24_MMLY_OTHR_IDC_A T1
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
          DBMS_OUTPUT.put_line(1);
          INSERT INTO TM24_MMLY_OTHR_IDC_A
          (   BAS_YM
             ,PCF_ID
             ,EQT_AMT
             ,LRGST_LN_BRWR_BAL
             ,LRGST_LN_BRWR_RLT_GRP_BAL
             ,LRGST_LN_BRWR_CR_CNTR_INFO
             ,LRGST_LN_BRWR_BAD_DBT_YN
             ,LRGST_LN_BRWR_BAD_DBT_GRP_CD
             ,VIO_RT_CUST_NUM_CNT
             ,SNGL_CUST_BIG_LN_VS_EQT_RTO
             ,RLT_GRP_BIG_LN_VS_EQT_RTO
             ,REGLT_RPT_SUBMIT_STS_INFO
             ,LST_QQ_INTR_CR_RT_RSLT_INFO
             ,TRGT_DATA_LST_MOD_TM
          )
          WITH
          FULL_SET AS
          (SELECT  SUBSTR(T1.BAS_DAY, 1, 6) AS BAS_YM, T1.PCF_ID
           FROM   (SELECT BAS_DAY, PCF_ID
                   FROM   (SELECT SUBSTR(BAS_DAY,1,6) AS BAS_YM, PCF_ID, MAX(BAS_DAY) AS BAS_DAY
                           FROM   TM21_DDLY_CUST_CR_TRANS_A
                           WHERE  BAS_DAY = (SELECT THIS_MM_LST_DAY AS BAS_DAY
                                             FROM   TM00_DDLY_CAL_D
                                             WHERE  BAS_YM = loop_bas_day.BAS_YM
                                             GROUP BY THIS_MM_LST_DAY)
                           GROUP BY SUBSTR(BAS_DAY,1,6), PCF_ID
                          )
                  ) T1
          ),
          FROM_DDLY_CUST_CR_TRANS_A AS
          (SELECT SUBSTR(BAS_DAY, 1, 6) AS BAS_YM,
                  PCF_ID,
                  CUST_ID,
                  CUST_LN_BAL AS LN_BAL,
                  CUST_DBT_GRP AS FINAL_DBT_GRP,
                  LN_CNTR_NUM_INFO AS LRGST_LN_BRWR_CR_CNTR_INFO
           FROM (SELECT BAS_DAY,
                        PCF_ID,
                        CUST_ID,
                        CUST_LN_BAL,
                        CUST_DBT_GRP,
                        LN_CNTR_NUM_INFO,
                        RANK() OVER(PARTITION BY BAS_DAY, PCF_ID ORDER BY CUST_LN_BAL DESC NULLS LAST, CUST_ID DESC) AS RNK_NUM
                 FROM   (SELECT BAS_DAY,
                                PCF_ID,
                                CUST_ID,
                                MAX(LN_CNTR_NUM_INFO) AS LN_CNTR_NUM_INFO,
                                SUM(CNTR_LN_BAL) AS CUST_LN_BAL,
                                MAX(CNTR_DBT_GRP) AS CUST_DBT_GRP
                         FROM   (SELECT A.BAS_DAY,
                                        A.PCF_ID,
                                        A.CUST_ID,
                                        A.LN_CNTR_NUM_INFO,
                                        MIN(CASE WHEN A.COLL_TYP_CD = '02' THEN CASE WHEN A.LN_BAL - NVL(A.COLL_VAL_AMT, 0) < 0 THEN 0
                                                                                     ELSE A.LN_BAL - NVL(A.COLL_VAL_AMT, 0)
                                                                                     END
                                                      ELSE A.LN_BAL
                                                      END) AS CNTR_LN_BAL,
                                        MAX(CASE WHEN TO_NUMBER(NVL(NEW_NORML_DBT_GRP_CD,0)) + TO_NUMBER(NVL(NEW_OVD_DBT_GRP_CD,0)) > 0
                                                      THEN TO_NUMBER(NVL(NEW_NORML_DBT_GRP_CD,0)) + TO_NUMBER(NVL(NEW_OVD_DBT_GRP_CD,0))
                                                 ELSE TO_NUMBER(NVL(OLD_NORML_DBT_GRP_CD,0)) + TO_NUMBER(NVL(OLD_OVD_DBT_GRP_CD,0))
                                                 END) CNTR_DBT_GRP
                                 FROM   TM21_DDLY_CUST_CR_TRANS_A A
                                 WHERE  A.BAS_DAY = (SELECT THIS_MM_LST_DAY AS BAS_DAY
                                                     FROM   TM00_DDLY_CAL_D
                                                     WHERE  BAS_YM = loop_bas_day.BAS_YM
                                                     GROUP BY THIS_MM_LST_DAY)
                                 AND    A.CUST_ID IS NOT NULL
                                 GROUP BY A.BAS_DAY, A.PCF_ID, A.CUST_ID, A.LN_CNTR_NUM_INFO
                                )
                         GROUP BY BAS_DAY, PCF_ID, CUST_ID
                        )
                )
           WHERE RNK_NUM = 1
          ),
          FROM_TB05_G32_007_TTGS_A_1 AS
          (SELECT BAS_YM,
                  PCF_ID,
                  GROUP_ID,
                  CUSTOMER_CODE_MAIN,
                  TOTAL_GROUP_BALANCE,
                  MAX(TOTAL_GROUP_BALANCE) AS LN_BAL
           FROM (SELECT SUBSTR(BAS_YMD, 1, 6) AS BAS_YM,
                        PCF_ID,
                        TRUNC(TO_NUMBER(ORDINAL_NUMBER)) AS GROUP_ID, 
                        SUM(OUTSTND_BAL) AS TOTAL_GROUP_BALANCE,
                        MAX(CASE WHEN ORDINAL_NUMBER = TRUNC(TO_NUMBER(ORDINAL_NUMBER)) 
                                   THEN CUSTOMER_CODE 
                                 ELSE NULL 
                                 END) KEEP (DENSE_RANK LAST ORDER BY ORDINAL_NUMBER) AS CUSTOMER_CODE_MAIN
                 FROM TB05_G32_007_TTGS_A
                 GROUP BY SUBSTR(BAS_YMD, 1, 6), PCF_ID, TRUNC(TO_NUMBER(ORDINAL_NUMBER))
                 )
          ),
          FROM_MMLY_IDC_CALC_CAR AS
          (SELECT BAS_YM,
                  PCF_ID,
                  EQT_AMT
           FROM   TM24_MMLY_IDC_CALC_CAR_A
           WHERE  BAS_YM = loop_bas_day.BAS_YM
          ),
          FROM_TB05_G32_007_TTGS_A_2 AS
          (SELECT A.BAS_YM,
                  A.PCF_ID,
                  SUM(CASE WHEN B.EQT_AMT IS NULL
                               THEN 0
                          ELSE CASE WHEN A.TOTAL_GROUP_BALANCE / B.EQT_AMT >= 0.15
                                         THEN 1
                                    ELSE 0
                                    END
                          END) AS VIO_RT_CUST_NUM_CNT
           FROM (SELECT SUBSTR(BAS_YMD, 1, 6) AS BAS_YM,
                        PCF_ID,
                        TRUNC(TO_NUMBER(ORDINAL_NUMBER)) AS GROUP_ID, 
                        SUM(OUTSTND_BAL) AS TOTAL_GROUP_BALANCE
                 FROM TB05_G32_007_TTGS_A
                 GROUP BY SUBSTR(BAS_YMD, 1, 6), PCF_ID, TRUNC(TO_NUMBER(ORDINAL_NUMBER))
                 ) A LEFT OUTER JOIN FROM_MMLY_IDC_CALC_CAR B
                                   ON B.BAS_YM = A.BAS_YM
                                  AND B.PCF_ID = A.PCF_ID
           GROUP BY A.BAS_YM, A.PCF_ID 
          )
          SELECT A.BAS_YM
                ,A.PCF_ID
                ,D.EQT_AMT
                ,B.LN_BAL AS LRGST_LN_BRWR_BAL
                ,C.LN_BAL AS LRGST_LN_BRWR_RLT_GRP_BAL
                ,B.LRGST_LN_BRWR_CR_CNTR_INFO AS LRGST_LN_BRWR_CR_CNTR_INFO
                ,CASE WHEN B.FINAL_DBT_GRP >= 3
                           THEN 'Y'
                      ELSE 'N'
                      END AS LRGST_LN_BRWR_BAD_DBT_YN
                ,TO_CHAR(B.FINAL_DBT_GRP) AS LRGST_LN_BRWR_BAD_DBT_GRP_CD
                ,E.VIO_RT_CUST_NUM_CNT
                ,CASE WHEN D.EQT_AMT IS NULL
                           THEN NULL
                      ELSE ROUND(B.LN_BAL / D.EQT_AMT * 100, 2)
                      END AS SNGL_CUST_BIG_LN_VS_EQT_RTO
                ,CASE WHEN D.EQT_AMT IS NULL
                           THEN NULL
                      ELSE ROUND(C.LN_BAL / D.EQT_AMT * 100, 2)
                      END AS RLT_GRP_BIG_LN_VS_EQT_RTO
                ,NULL AS REGLT_RPT_SUBMIT_STS_INFO
                ,NULL AS LST_QQ_INTR_CR_RT_RSLT_INFO
                ,SYSTIMESTAMP
          FROM   FULL_SET A LEFT OUTER JOIN FROM_DDLY_CUST_CR_TRANS_A B
                                         ON A.BAS_YM = B.BAS_YM
                                        AND A.PCF_ID = B.PCF_ID
                            LEFT OUTER JOIN FROM_TB05_G32_007_TTGS_A_1 C
                                         ON A.BAS_YM  = C.BAS_YM
                                        AND A.PCF_ID  = C.PCF_ID
                            LEFT OUTER JOIN FROM_MMLY_IDC_CALC_CAR D
                                         ON A.BAS_YM = D.BAS_YM
                                        AND A.PCF_ID = D.PCF_ID
                            LEFT OUTER JOIN FROM_TB05_G32_007_TTGS_A_2 E
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

          EXCEPTION
          WHEN OTHERS THEN
               begin
                     v_step_code    := '098' ;
                     v_step_desc    := v_wk_date || v_seq || ' : Errors occurred when data was deleted or inserted on BAS_YM = ' ;
                     v_time         := SYSDATE ;
                     P1_BSM_PROG_EXEC_LOG(v_program_id, v_program_type_name, v_step_code, v_step_desc||loop_bas_day.BAS_YM, v_time, NULL, TO_CHAR(SQLCODE), SQLERRM) ;
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