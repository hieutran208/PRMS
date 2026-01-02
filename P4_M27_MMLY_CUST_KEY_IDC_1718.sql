create or replace PROCEDURE            "P4_M27_MMLY_CUST_KEY_IDC_1718"
                 ( p_wkdate    IN  CHAR DEFAULT NULL
                 , p_stdate_01 IN  CHAR DEFAULT NULL
                 , p_eddate_01 IN  CHAR DEFAULT NULL
                 )
IS
    /*--------------------------------------------------------------------------
     * PROGRAM ID    : P4_M27_MMLY_CUST_KEY_IDC_1718
     * PROGRAM NAME  : A program for insert data to TM27_MMLY_CUST_KEY_IDC_A
     * SOURCE TABLE  : TM00_NEW_CUST_D
                       TM21_DDLY_CUST_CR_TRANS_A
                       TM22_DDLY_CUST_DPST_TRANS_A
     * TARGET TABLE  : TM27_MMLY_CUST_KEY_IDC_A
     * PROGRAMER     : HIEU
     * LAST MODIFICATION DATE : 2025-12-24
     * UNIQUENESS    : N/A
     * COMMENTS      : N/A
    ----------------------------------------------------------------------------
     * Revision History : 2025-12-24 : Create
     * Revision History :
    --------------------------------------------------------------------------*/
    ----------------------------------------------------------------------------
    --  Declare Local Variables
    ----------------------------------------------------------------------------
    v_wk_date                 CHAR(8) DEFAULT NULL ;
    v_st_date_01              CHAR(8) DEFAULT NULL ;
    v_end_date_01             CHAR(8) DEFAULT NULL ;

    v_program_id              VARCHAR2(100) DEFAULT 'P4_M27_MMLY_CUST_KEY_IDC_1718' ;
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
    FROM   TM00_MMLY_CAL_D T1 INNER JOIN (SELECT MIN(MIN_DAY) AS MIN_DAY, MAX(MAX_DAY) AS MAX_DAY
                                          FROM   (SELECT DISTINCT SUBSTR(DATA_BAS_DAY,1,4)||'01' AS MIN_DAY, TO_CHAR(SYSDATE - 35, 'YYYYMM') AS MAX_DAY
                                                  FROM   TBSM_INPT_RPT_SUBMIT_L
                                                  WHERE  BTCH_BAS_DAY >= v_dt2
                                                  AND    INPT_RPT_ID IN ('G32_019_TTGS_01', 'G32_019_TTGS_02', 'G32_019_TTGS_03')

                                                  UNION ALL

                                                  SELECT DISTINCT MIN(CASE WHEN SUBSTR(DATA_BAS_DAY,1,6) >= TO_CHAR(SYSDATE - 35, 'YYYYMM') THEN TO_CHAR(SYSDATE - 35, 'YYYYMM')
                                                                           ELSE SUBSTR(DATA_BAS_DAY,1,6)
                                                                           END) AS MIN_DAY,
                                                         TO_CHAR(SYSDATE - 35, 'YYYYMM') AS MAX_DAY
                                                  FROM   TBSM_INPT_RPT_SUBMIT_L
                                                  WHERE  BTCH_BAS_DAY >= v_dt2
                                                  AND    INPT_RPT_ID IN ('G32_003_TTGS','G32_012_TTGS')
                                                 )
                                         ) T2
                                      ON T1.BAS_YM BETWEEN T2.MIN_DAY AND T2.MAX_DAY
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
    --  1.1 Inserting Data
    ----------------------------------------------------------------------------

    FOR loop_bas_day in v_sbmt_day(v_wk_date,v_st_date_01,v_end_date_01)
    LOOP
    BEGIN

          DELETE
          FROM   TM27_MMLY_CUST_KEY_IDC_A
          WHERE  BAS_YM = loop_bas_day.BAS_YM;

          ----------------------------------------------------------------------------
          --  Transaction Log
          ----------------------------------------------------------------------------
          v_step_code    := '010' ;
          v_step_desc    := v_wk_date || v_seq || ' ' || loop_bas_day.BAS_YM||' : Deleting Data Result';
          v_time         := SYSDATE ;

          P1_BSM_PROG_EXEC_LOG(v_program_id, v_program_type_name, v_step_code, v_step_desc, v_time, sql%rowcount, NULL, NULL) ;
          
          COMMIT ;
          ----------------------------------------------------------------------------
          INSERT INTO TM27_MMLY_CUST_KEY_IDC_A
          ( BAS_YM
           ,PCF_ID
           ,TOT_CUST_NUM_CNT
           ,FEMALE_CUST_NUM_CNT
           ,MALE_CUST_NUM_CNT
           ,ETC_CUST_NUM_CNT
           ,TOT_ACT_CUST_NUM_CNT
           ,FEMALE_ACT_CUST_NUM_CNT
           ,MALE_ACT_CUST_NUM_CNT
           ,ETC_ACT_CUST_NUM_CNT
           ,FEMALE_DPST_ACT_CUST_NUM_CNT
           ,MALE_DPST_ACT_CUST_NUM_CNT
           ,ETC_DPST_ACT_CUST_NUM_CNT
           ,FEMALE_LN_ACT_CUST_NUM_CNT
           ,MALE_LN_ACT_CUST_NUM_CNT
           ,ETC_LN_ACT_CUST_NUM_CNT
           ,TRGT_DATA_LST_MOD_TM
          )
          WITH
          ACT_CUST AS
          (
           SELECT PCF_ID, '1' AS CUST_TYP_CD, CUST_ID
           FROM   TM22_DDLY_CUST_DPST_TRANS_A
           WHERE  BAS_DAY BETWEEN TO_CHAR(ADD_MONTHS(TO_DATE(loop_bas_day.BAS_YM,'YYYYMM'), -11), 'YYYYMMDD')
                                  AND
                                  TO_CHAR(LAST_DAY(TO_DATE(loop_bas_day.BAS_YM,'YYYYMM')), 'YYYYMMDD')
           AND    CUST_ID IS NOT NULL
           AND    DPST_BAL > 0
           GROUP BY PCF_ID, CUST_ID

           UNION ALL

           SELECT PCF_ID, '2' AS CUST_TYP_CD, CUST_ID
           FROM   TM21_DDLY_CUST_CR_TRANS_A
           WHERE  BAS_DAY BETWEEN TO_CHAR(ADD_MONTHS(TO_DATE(loop_bas_day.BAS_YM,'YYYYMM'), -11), 'YYYYMMDD')
                                  AND
                                  TO_CHAR(LAST_DAY(TO_DATE(loop_bas_day.BAS_YM,'YYYYMM')), 'YYYYMMDD')
           AND    CUST_ID IS NOT NULL
           AND    LN_BAL > 0
           GROUP BY PCF_ID, CUST_ID

           UNION ALL

           SELECT PCF_ID, '3' AS CUST_TYP_CD, TX_CD_ID AS CUST_ID
           FROM   TM00_MBR_D
           WHERE  BAS_YM = loop_bas_day.BAS_YM
           AND    CNTRBT_MSHP_CAP_AMT > 0
           GROUP BY PCF_ID, TX_CD_ID
          ),
          TOT_CNT AS
          (SELECT loop_bas_day.BAS_YM AS BAS_YM,
                  PCF_ID,
                  COUNT(TX_CD_ID) AS TOT_CUST_NUM_CNT,
                  SUM(CASE WHEN GEN_TYP_CD = 'F' THEN 1 ELSE 0 END) AS FEMALE_CUST_NUM_CNT,
                  SUM(CASE WHEN GEN_TYP_CD = 'M' THEN 1 ELSE 0 END) AS MALE_CUST_NUM_CNT,
                  SUM(CASE WHEN GEN_TYP_CD = '8' THEN 1 ELSE 0 END) AS ETC_CUST_NUM_CNT
           FROM   TM00_NEW_CUST_D
           WHERE  (MBR_FST_REG_DAY       <= TO_CHAR(LAST_DAY(TO_DATE(loop_bas_day.BAS_YM,'YYYYMM')), 'YYYYMMDD')
                   OR
                   LN_CUST_FST_REG_DAY   <= TO_CHAR(LAST_DAY(TO_DATE(loop_bas_day.BAS_YM,'YYYYMM')), 'YYYYMMDD')
                   OR
                   DPST_CUST_FST_REG_DAY <= TO_CHAR(LAST_DAY(TO_DATE(loop_bas_day.BAS_YM,'YYYYMM')), 'YYYYMMDD')
                  )
           GROUP BY PCF_ID
          ),
          ACT_CNT AS
          (SELECT loop_bas_day.BAS_YM AS BAS_YM,
                  T1.PCF_ID,
                  COUNT(DISTINCT T1.CUST_ID) AS TOT_ACT_CUST_NUM_CNT,

                  COUNT(DISTINCT CASE WHEN T2.GEN_TYP_CD = 'F' THEN T1.CUST_ID ELSE NULL END) AS FEMALE_ACT_CUST_NUM_CNT,
                  COUNT(DISTINCT CASE WHEN T2.GEN_TYP_CD = 'M' THEN T1.CUST_ID ELSE NULL END) AS MALE_ACT_CUST_NUM_CNT,
                  COUNT(DISTINCT CASE WHEN T2.GEN_TYP_CD = '8' THEN T1.CUST_ID ELSE NULL END) AS ETC_ACT_CUST_NUM_CNT,

                  COUNT(DISTINCT CASE WHEN T1.CUST_TYP_CD = '1' AND T2.GEN_TYP_CD = 'F' THEN T1.CUST_ID ELSE NULL END) AS FEMALE_DPST_ACT_CUST_NUM_CNT,
                  COUNT(DISTINCT CASE WHEN T1.CUST_TYP_CD = '1' AND T2.GEN_TYP_CD = 'M' THEN T1.CUST_ID ELSE NULL END) AS MALE_DPST_ACT_CUST_NUM_CNT,
                  COUNT(DISTINCT CASE WHEN T1.CUST_TYP_CD = '1' AND T2.GEN_TYP_CD = '8' THEN T1.CUST_ID ELSE NULL END) AS ETC_DPST_ACT_CUST_NUM_CNT,

                  COUNT(DISTINCT CASE WHEN T1.CUST_TYP_CD = '2' AND T2.GEN_TYP_CD = 'F' THEN T1.CUST_ID ELSE NULL END) AS FEMALE_LN_ACT_CUST_NUM_CNT,
                  COUNT(DISTINCT CASE WHEN T1.CUST_TYP_CD = '2' AND T2.GEN_TYP_CD = 'M' THEN T1.CUST_ID ELSE NULL END) AS MALE_LN_ACT_CUST_NUM_CNT,
                  COUNT(DISTINCT CASE WHEN T1.CUST_TYP_CD = '2' AND T2.GEN_TYP_CD = '8' THEN T1.CUST_ID ELSE NULL END) AS ETC_LN_ACT_CUST_NUM_CNT

           FROM   (SELECT PCF_ID, CUST_TYP_CD, CUST_ID
                   FROM   ACT_CUST
                   GROUP BY PCF_ID, CUST_TYP_CD, CUST_ID
                  ) T1 INNER JOIN TM00_NEW_CUST_D T2
                               ON T2.PCF_ID = T1.PCF_ID
                              AND T2.TX_CD_ID = T1.CUST_ID
           GROUP BY T1.PCF_ID
          )
          SELECT T1.BAS_YM,
                 T1.PCF_ID,
                 T1.TOT_CUST_NUM_CNT,
                 T1.FEMALE_CUST_NUM_CNT,
                 T1.MALE_CUST_NUM_CNT,
                 T1.ETC_CUST_NUM_CNT,
                 NVL(T2.TOT_ACT_CUST_NUM_CNT, 0),
                 NVL(T2.FEMALE_ACT_CUST_NUM_CNT, 0),
                 NVL(T2.MALE_ACT_CUST_NUM_CNT, 0),
                 NVL(T2.ETC_ACT_CUST_NUM_CNT, 0),
                 NVL(T2.FEMALE_DPST_ACT_CUST_NUM_CNT, 0),
                 NVL(T2.MALE_DPST_ACT_CUST_NUM_CNT, 0),
                 NVL(T2.ETC_DPST_ACT_CUST_NUM_CNT, 0),
                 NVL(T2.FEMALE_LN_ACT_CUST_NUM_CNT, 0),
                 NVL(T2.MALE_LN_ACT_CUST_NUM_CNT, 0),
                 NVL(T2.ETC_LN_ACT_CUST_NUM_CNT, 0),
                 SYSTIMESTAMP
          FROM   TOT_CNT T1 LEFT OUTER JOIN ACT_CNT T2
                                         ON T2.BAS_YM  = T1.BAS_YM
                                        AND T2.PCF_ID  = T1.PCF_ID
          ;
          ----------------------------------------------------------------------------
          --  Transaction Log
          ----------------------------------------------------------------------------
          v_step_code    := '020' ;
          v_step_desc    := v_wk_date || v_seq || ' ' || loop_bas_day.BAS_YM||' : Inserting Data Result';
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