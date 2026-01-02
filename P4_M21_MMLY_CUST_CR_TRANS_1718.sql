create or replace PROCEDURE            "P4_M21_MMLY_CUST_CR_TRANS_1718"
                 ( p_wkdate    IN  CHAR DEFAULT NULL
                 , p_stdate_01 IN  CHAR DEFAULT NULL
                 , p_eddate_01 IN  CHAR DEFAULT NULL
                 )
IS
    /*--------------------------------------------------------------------------
     * PROGRAM ID    : P4_M21_MMLY_CUST_CR_TRANS_1718
     * PROGRAM NAME  : A program for insert data to TM21_MMLY_CUST_CR_TRANS_A
     * SOURCE TABLE  : TM21_DDLY_CUST_CR_TRANS_A
     * TARGET TABLE  : TM21_MMLY_CUST_CR_TRANS_A
     * PROGRAMER     : HIEU
     * LAST MODIFICATION DATE : 2026-01-02
     * UNIQUENESS    : N/A
     * COMMENTS      : N/A
    ----------------------------------------------------------------------------
     * Revision History : 2026-01-02 : Create
     * Revision History :
    --------------------------------------------------------------------------*/
    ----------------------------------------------------------------------------
    --  Declare Local Variables
    ----------------------------------------------------------------------------
    v_wk_date                 CHAR(8) DEFAULT NULL ;
    v_st_date_01              CHAR(8) DEFAULT NULL ;
    v_end_date_01             CHAR(8) DEFAULT NULL ;

    v_program_id              VARCHAR2(100) DEFAULT 'P4_M21_MMLY_CUST_CR_TRANS_1718' ;
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
                                          WHERE  BTCH_BAS_DAY = v_dt2
                                          AND    INPT_RPT_ID  = 'G32_012_TTGS'
                                         ) T2
                                      ON T1.BAS_YM BETWEEN T2.MIN_DAY AND T2.MAX_DAY
    ORDER BY 1
    ;


    /*CURSOR v_sbmt_day(v_dt1 VARCHAR2, v_dt2 VARCHAR2, v_dt3 VARCHAR2) IS
    SELECT T1.BAS_YM
    FROM   TM00_MMLY_CAL_D T1
    WHERE  T1.BAS_YM BETWEEN '202401' AND '202410'
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
            FROM TM21_MMLY_CUST_CR_TRANS_A T1
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

          INSERT /*+ APPEND PARALLEL(TM21_MMLY_CUST_CR_TRANS_A, 4) */ INTO TM21_MMLY_CUST_CR_TRANS_A
          (   BAS_YM
             ,PCF_ID
             ,INIT_LN_TRM_CD
             ,RMTRT_LN_TRM_CD
             ,ES_CD
             ,CUST_TYP_CD
             ,COLL_TYP_CD
             ,NORML_DBT_GRP_CD
             ,OVD_DBT_GRP_CD
             ,TOT_LN_BAL
             ,TOT_LN_CNTR_NUM_CNT
             ,TOT_BRWR_NUM_CNT
             ,NEW_ARISN_LN_AMT
             ,NEW_ARISN_LN_CNTR_NUM_CNT
             ,NEW_BRWR_NUM_CNT
             ,COLL_VAL_AMT
             ,SPEC_PRVS_AMT
             ,AVG_ACTL_IR
             ,INT_MULTY_TOT_LN_BAL
             ,TRGT_DATA_LST_MOD_TM
          )
          WITH
          TM21_DDLY_CUST_CR_TRANS_V AS
          (SELECT  BAS_DAY
                  ,PCF_ID
                  ,RPLC_DATA_YN
                  ,LN_CNTR_NUM_INFO
                  ,MAX(CUST_ID             ) AS CUST_ID
                  ,MAX(CUST_NM             ) AS CUST_NM
                  ,MAX(CUST_TYP_CD         ) AS CUST_TYP_CD
                  ,MAX(INIT_LN_TRM_CD      ) AS INIT_LN_TRM_CD
                  ,MAX(RMTRT_LN_TRM_CD     ) AS RMTRT_LN_TRM_CD
                  ,MAX(ES_CD               ) AS ES_CD
                  ,MAX(COLL_TYP_INFO       ) AS COLL_TYP_INFO
                  ,MAX(COLL_TYP_CD         ) AS COLL_TYP_CD
                  ,MAX(LN_TRANS_TYP_CD     ) AS LN_TRANS_TYP_CD
                  ,MAX(OLD_NORML_DBT_GRP_CD) AS OLD_NORML_DBT_GRP_CD
                  ,MAX(OLD_OVD_DBT_GRP_CD  ) AS OLD_OVD_DBT_GRP_CD
                  ,MAX(NEW_NORML_DBT_GRP_CD) AS NEW_NORML_DBT_GRP_CD
                  ,MAX(NEW_OVD_DBT_GRP_CD  ) AS NEW_OVD_DBT_GRP_CD
                  ,MAX(LN_CNTR_AMT         ) AS LN_CNTR_AMT
                  ,MAX(LN_BAL              ) AS LN_BAL
                  ,SUM(TRANS_AMT           ) AS TRANS_AMT
                  ,MAX(COLL_VAL_AMT        ) AS COLL_VAL_AMT
                  ,MAX(SPEC_PRVS_AMT       ) AS SPEC_PRVS_AMT
                  ,MAX(OPN_DAY             ) AS OPN_DAY
                  ,MAX(MTRT_DAY            ) AS MTRT_DAY
                  ,MAX(ACTL_IR             ) AS ACTL_IR
           FROM   TM21_DDLY_CUST_CR_TRANS_A
           WHERE  BAS_DAY BETWEEN loop_bas_day.BAS_YM||'01' AND loop_bas_day.BAS_YM||'31'
           GROUP BY BAS_DAY
                   ,PCF_ID
                   ,RPLC_DATA_YN
                   ,LN_CNTR_NUM_INFO
          ),
          TM21_DDLY_CUST_CR_TRANS_W AS
          (SELECT SUBSTR(BAS_DAY, 1, 6) AS BAS_YM,
                  PCF_ID,
                  INIT_LN_TRM_CD,
                  RMTRT_LN_TRM_CD,
                  NVL(ES_CD, '9998') AS ES_CD,
                  CUST_TYP_CD,
                  COLL_TYP_CD,
                  NVL(CASE WHEN NEW_NORML_DBT_GRP_CD IS NULL
                                THEN OLD_NORML_DBT_GRP_CD
                           ELSE NEW_NORML_DBT_GRP_CD
                           END, 8) AS NORML_DBT_GRP_CD,
                  NVL(CASE WHEN NEW_OVD_DBT_GRP_CD IS NULL
                                THEN OLD_OVD_DBT_GRP_CD
                           ELSE NEW_OVD_DBT_GRP_CD
                           END, 8) AS OVD_DBT_GRP_CD,
                  SUM(CASE WHEN LN_TRANS_TYP_CD = '1'
                                THEN TRANS_AMT
                           ELSE 0
                           END
                     ) AS NEW_ARISN_LN_AMT,
                  COUNT(DISTINCT CASE WHEN LN_TRANS_TYP_CD = '1' AND TRANS_AMT > 0
                                           THEN LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END
                       ) AS NEW_ARISN_LN_CNTR_NUM_CNT,
                  COUNT(DISTINCT CASE WHEN LN_TRANS_TYP_CD = '1' AND TRANS_AMT > 0
                                           THEN CUST_ID
                                      ELSE NULL
                                      END
                       ) AS NEW_BRWR_NUM_CNT
           FROM   TM21_DDLY_CUST_CR_TRANS_A
           WHERE  BAS_DAY BETWEEN loop_bas_day.BAS_YM||'01' AND loop_bas_day.BAS_YM||'31'
           AND    RPLC_DATA_YN = 'N'
           GROUP BY SUBSTR(BAS_DAY, 1, 6),
                    PCF_ID,
                    INIT_LN_TRM_CD,
                    RMTRT_LN_TRM_CD,
                    NVL(ES_CD, '9998'),
                    CUST_TYP_CD,
                    COLL_TYP_CD,
                    NVL(CASE WHEN NEW_NORML_DBT_GRP_CD IS NULL
                                  THEN OLD_NORML_DBT_GRP_CD
                             ELSE NEW_NORML_DBT_GRP_CD
                             END, 8),
                    NVL(CASE WHEN NEW_OVD_DBT_GRP_CD IS NULL
                                  THEN OLD_OVD_DBT_GRP_CD
                             ELSE NEW_OVD_DBT_GRP_CD
                             END, 8)
          ),
          TM21_DDLY_CUST_CR_TRANS_X AS
          (SELECT SUBSTR(A.BAS_DAY, 1, 6) AS BAS_YM,
                  A.PCF_ID,
                  A.INIT_LN_TRM_CD,
                  A.RMTRT_LN_TRM_CD,
                  NVL(A.ES_CD, '9998') AS ES_CD,
                  A.CUST_TYP_CD,
                  A.COLL_TYP_CD,
                  NVL(CASE WHEN A.NEW_NORML_DBT_GRP_CD IS NULL
                                THEN A.OLD_NORML_DBT_GRP_CD
                           ELSE A.NEW_NORML_DBT_GRP_CD
                           END, 8) AS NORML_DBT_GRP_CD,
                  NVL(CASE WHEN A.NEW_OVD_DBT_GRP_CD IS NULL
                                THEN A.OLD_OVD_DBT_GRP_CD
                           ELSE A.NEW_OVD_DBT_GRP_CD
                           END, 8) AS OVD_DBT_GRP_CD,
                  SUM(A.LN_BAL) AS TOT_LN_BAL,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS TOT_LN_CNTR_NUM_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 THEN
                                                             CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO
                                                                  ELSE CUST_ID
                                                                  END
                                      ELSE NULL
                                      END) AS TOT_BRWR_NUM_CNT,
                  SUM(A.COLL_VAL_AMT) AS COLL_VAL_AMT,
                  SUM(A.SPEC_PRVS_AMT) AS SPEC_PRVS_AMT,
                  ROUND(AVG(A.ACTL_IR), 2) AS AVG_ACTL_IR,
                  SUM(A.LN_BAL * A.ACTL_IR) AS INT_MULTY_TOT_LN_BAL
           FROM   TM21_DDLY_CUST_CR_TRANS_V A INNER JOIN (SELECT SUBSTR(BAS_DAY, 1, 6) AS BAS_YM, PCF_ID, MAX(BAS_DAY) AS BAS_DAY
                                                          FROM   TM21_DDLY_CUST_CR_TRANS_V
                                                          GROUP BY SUBSTR(BAS_DAY, 1, 6), PCF_ID
                                                         ) B
                                                      ON B.BAS_DAY = A.BAS_DAY
                                                     AND B.PCF_ID  = A.PCF_ID
           GROUP BY SUBSTR(A.BAS_DAY, 1, 6),
                    A.PCF_ID,
                    A.INIT_LN_TRM_CD,
                    A.RMTRT_LN_TRM_CD,
                    NVL(A.ES_CD, '9998'),
                    A.CUST_TYP_CD,
                    A.COLL_TYP_CD,
                    NVL(CASE WHEN A.NEW_NORML_DBT_GRP_CD IS NULL
                                  THEN A.OLD_NORML_DBT_GRP_CD
                             ELSE A.NEW_NORML_DBT_GRP_CD
                             END, 8),
                    NVL(CASE WHEN A.NEW_OVD_DBT_GRP_CD IS NULL
                                  THEN A.OLD_OVD_DBT_GRP_CD
                             ELSE A.NEW_OVD_DBT_GRP_CD
                             END, 8)
          )
          SELECT T1.BAS_YM,
                 T1.PCF_ID,
                 T1.INIT_LN_TRM_CD,
                 T1.RMTRT_LN_TRM_CD,
                 T1.ES_CD,
                 T1.CUST_TYP_CD,
                 T1.COLL_TYP_CD,
                 T1.NORML_DBT_GRP_CD,
                 T1.OVD_DBT_GRP_CD,
                 T1.TOT_LN_BAL,
                 T1.TOT_LN_CNTR_NUM_CNT,
                 T1.TOT_BRWR_NUM_CNT,
                 T2.NEW_ARISN_LN_AMT,
                 T2.NEW_ARISN_LN_CNTR_NUM_CNT,
                 T2.NEW_BRWR_NUM_CNT,
                 T1.COLL_VAL_AMT,
                 T1.SPEC_PRVS_AMT,
                 T1.AVG_ACTL_IR,
                 T1.INT_MULTY_TOT_LN_BAL,
                 SYSTIMESTAMP
          FROM   TM21_DDLY_CUST_CR_TRANS_X T1 LEFT OUTER JOIN TM21_DDLY_CUST_CR_TRANS_W T2
                                                           ON T2.BAS_YM           = T1.BAS_YM
                                                          AND T2.PCF_ID           = T1.PCF_ID
                                                          AND T2.INIT_LN_TRM_CD   = T1.INIT_LN_TRM_CD
                                                          AND T2.RMTRT_LN_TRM_CD  = T1.RMTRT_LN_TRM_CD
                                                          AND T2.ES_CD            = T1.ES_CD
                                                          AND T2.CUST_TYP_CD      = T1.CUST_TYP_CD
                                                          AND T2.COLL_TYP_CD      = T1.COLL_TYP_CD
                                                          AND T2.NORML_DBT_GRP_CD = T1.NORML_DBT_GRP_CD
                                                          AND T2.OVD_DBT_GRP_CD   = T1.OVD_DBT_GRP_CD;

          ----------------------------------------------------------------------------
          --  Transaction Log
          ----------------------------------------------------------------------------
          v_step_code    := '020' ;
          v_step_desc    := v_wk_date || v_seq || ' ' || loop_bas_day.BAS_YM ||' : Inserting Data Result';
          v_time         := SYSDATE ;

          P1_BSM_PROG_EXEC_LOG(v_program_id, v_program_type_name, v_step_code, v_step_desc, v_time, sql%rowcount, NULL, NULL) ;

          COMMIT ;
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