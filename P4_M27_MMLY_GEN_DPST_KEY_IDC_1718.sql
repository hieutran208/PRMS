create or replace PROCEDURE            "P4_M27_MMLY_GEN_DPST_KEY_IDC_1718"
                 ( p_wkdate    IN  CHAR DEFAULT NULL
                 , p_stdate_01 IN  CHAR DEFAULT NULL
                 , p_eddate_01 IN  CHAR DEFAULT NULL
                 )
IS
    /*--------------------------------------------------------------------------
     * PROGRAM ID    : P4_M27_MMLY_GEN_DPST_KEY_IDC_1718
     * PROGRAM NAME  : A program for insert data to TM27_MMLY_GEN_DPST_KEY_IDC_A
     * SOURCE TABLE  : TM00_NEW_CUST_D
                       TM22_DDLY_CUST_DPST_TRANS_A
     * TARGET TABLE  : TM27_MMLY_GEN_DPST_KEY_IDC_A
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

    v_program_id              VARCHAR2(100) DEFAULT 'P4_M27_MMLY_GEN_DPST_KEY_IDC_1718' ;
    v_program_type_name       VARCHAR2(100) DEFAULT 'Stored Procedure' ;
    v_step_code               VARCHAR2(3) ;
    v_step_desc               VARCHAR2(100) ;
    v_time                    DATE ;
    v_cnt                     NUMBER DEFAULT 0 ;
    v_seq                     CHAR(14) DEFAULT ' '||to_char(SEQ_UID.NEXTVAL,'000000000000');
    ----------------------------------------------------------------------------
    --  Declare Cursor
    ----------------------------------------------------------------------------
    CURSOR v_sbmt_day_1(v_dt1 VARCHAR2, v_dt2 VARCHAR2, v_dt3 VARCHAR2) IS
    SELECT T1.BAS_YM
    FROM   TM00_MMLY_CAL_D T1 INNER JOIN (SELECT MIN(MIN_DAY) AS MIN_DAY, MAX(MAX_DAY) AS MAX_DAY
                                          FROM   (SELECT DISTINCT SUBSTR(DATA_BAS_DAY,1,4)||'01' AS MIN_DAY, TO_CHAR(SYSDATE - 35, 'YYYYMM') AS MAX_DAY
                                                  FROM   TBSM_INPT_RPT_SUBMIT_L
                                                  WHERE  BTCH_BAS_DAY >= v_dt2
                                                  AND    INPT_RPT_ID IN ('G32_019_TTGS_01','G32_019_TTGS_02','G32_019_TTGS_03')
                                                 )
                                         ) T2
                                      ON T1.BAS_YM BETWEEN T2.MIN_DAY AND T2.MAX_DAY
    WHERE  T1.BAS_YM NOT IN (SELECT T1.BAS_YM
                             FROM   TM00_MMLY_CAL_D T1 INNER JOIN (SELECT MIN(MIN_DAY) AS MIN_DAY, MAX(MAX_DAY) AS MAX_DAY
                                                                   FROM   (SELECT DISTINCT MIN(CASE WHEN SUBSTR(DATA_BAS_DAY,1,6) >= TO_CHAR(SYSDATE - 35, 'YYYYMM') THEN TO_CHAR(SYSDATE - 35, 'YYYYMM')
                                                                                                    ELSE SUBSTR(DATA_BAS_DAY,1,6)
                                                                                                    END) AS MIN_DAY,
                                                                                  TO_CHAR(SYSDATE - 35, 'YYYYMM') AS MAX_DAY
                                                                           FROM   TBSM_INPT_RPT_SUBMIT_L
                                                                           WHERE  BTCH_BAS_DAY >= v_dt2
                                                                           AND    INPT_RPT_ID = 'G32_003_TTGS'
                                                                          )
                                                                  ) T2
                                                               ON T1.BAS_YM BETWEEN T2.MIN_DAY AND T2.MAX_DAY
                            )
    ORDER BY 1;

    CURSOR v_sbmt_day_2(v_dt1 VARCHAR2, v_dt2 VARCHAR2, v_dt3 VARCHAR2) IS
    SELECT T1.BAS_YM
    FROM   TM00_MMLY_CAL_D T1 INNER JOIN (SELECT MIN(MIN_DAY) AS MIN_DAY, MAX(MAX_DAY) AS MAX_DAY
                                          FROM   (SELECT DISTINCT MIN(CASE WHEN SUBSTR(DATA_BAS_DAY,1,6) >= TO_CHAR(SYSDATE - 35, 'YYYYMM') THEN TO_CHAR(SYSDATE - 35, 'YYYYMM')
                                                                           ELSE SUBSTR(DATA_BAS_DAY,1,6)
                                                                           END) AS MIN_DAY,
                                                         TO_CHAR(SYSDATE - 35, 'YYYYMM') AS MAX_DAY
                                                  FROM   TBSM_INPT_RPT_SUBMIT_L
                                                  WHERE  BTCH_BAS_DAY > v_dt2
                                                  AND    INPT_RPT_ID = 'G32_003_TTGS'
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
    --  1.0 Deleting & Inserting Data
    ----------------------------------------------------------------------------
    FOR loop_bas_day in v_sbmt_day_1(v_wk_date,v_st_date_01,v_end_date_01)
    LOOP
    BEGIN
          ----------------------------------------------------------------------------
          --  1.1 Delete Historical Data
          ----------------------------------------------------------------------------
          DELETE
          FROM   TM27_MMLY_GEN_DPST_KEY_IDC_A
          WHERE  BAS_YM = loop_bas_day.BAS_YM
          AND    PCF_ID IN (SELECT DISTINCT PCF_ID
                            FROM   TBSM_INPT_RPT_SUBMIT_L
                            WHERE  BTCH_BAS_DAY = v_st_date_01
                            AND    INPT_RPT_ID IN ('G32_019_TTGS_01','G32_019_TTGS_02','G32_019_TTGS_03')
                           );
          ----------------------------------------------------------------------------
          --  Transaction Log
          ----------------------------------------------------------------------------
          v_step_code    := '010' ;
          v_step_desc    := v_wk_date || v_seq || ' ' || loop_bas_day.BAS_YM ||' : Deleting Data Result';
          v_time         := SYSDATE ;

          P1_BSM_PROG_EXEC_LOG(v_program_id, v_program_type_name, v_step_code, v_step_desc, v_time, sql%rowcount, NULL, NULL) ;
          
          COMMIT ;
          ----------------------------------------------------------------------------
          --  1.2 Inserting Data by TM27_MMLY_GEN_DPST_KEY_IDC_A
          ----------------------------------------------------------------------------
          INSERT INTO TM27_MMLY_GEN_DPST_KEY_IDC_A
          (   BAS_YM
             ,PCF_ID
             ,TOT_DPST_BAL
             ,F_DPST_BAL
             ,M_DPST_BAL
             ,E_DPST_BAL
             ,TOT_DPST_CUST_CNT
             ,F_DPST_CUST_CNT
             ,M_DPST_CUST_CNT
             ,E_DPST_CUST_CNT
             ,TOT_NEW_ISS_TRM_DPST_CNT
             ,F_NEW_ISS_TRM_DPST_CNT
             ,M_NEW_ISS_TRM_DPST_CNT
             ,E_NEW_ISS_TRM_DPST_CNT
             ,TOT_NEW_ISS_TRM_DPST_AMT
             ,F_NEW_ISS_TRM_DPST_AMT
             ,M_NEW_ISS_TRM_DPST_AMT
             ,E_NEW_ISS_TRM_DPST_AMT
             ,TOT_WDR_TRM_DPST_CNT
             ,F_WDR_TRM_DPST_CNT
             ,M_WDR_TRM_DPST_CNT
             ,E_WDR_TRM_DPST_CNT
             ,TOT_WDR_TRM_DPST_AMT
             ,F_WDR_TRM_DPST_AMT
             ,M_WDR_TRM_DPST_AMT
             ,E_WDR_TRM_DPST_AMT
             ,F_INIT_TRM_LSTHN_6MM_DPST_BAL
             ,M_INIT_TRM_LSTHN_6MM_DPST_BAL
             ,E_INIT_TRM_LSTHN_6MM_DPST_BAL
             ,F_INIT_TRM_LSTHN_12MM_DPST_BAL
             ,M_INIT_TRM_LSTHN_12MM_DPST_BAL
             ,E_INIT_TRM_LSTHN_12MM_DPST_BAL
             ,F_INIT_TRM_OVER_12MM_DPST_BAL
             ,M_INIT_TRM_OVER_12MM_DPST_BAL
             ,E_INIT_TRM_OVER_12MM_DPST_BAL
             ,TRGT_DATA_LST_MOD_TM
          )
          WITH
          TOT_DPST AS
          (SELECT loop_bas_day.BAS_YM AS BAS_YM
                 ,T1.PCF_ID
                 ,SUM(T1.DPST_BAL) AS TOT_DPST_BAL
                 ,SUM(CASE WHEN T2.GEN_TYP_CD = 'F' THEN T1.DPST_BAL ELSE 0 END) AS F_DPST_BAL
                 ,SUM(CASE WHEN T2.GEN_TYP_CD = 'M' THEN T1.DPST_BAL ELSE 0 END) AS M_DPST_BAL
                 ,SUM(CASE WHEN T2.GEN_TYP_CD = '8' THEN T1.DPST_BAL ELSE 0 END) AS E_DPST_BAL
                 ,COUNT(DISTINCT T1.CUST_ID) AS TOT_DPST_CUST_CNT
                 ,COUNT(DISTINCT CASE WHEN T2.GEN_TYP_CD = 'F' THEN T1.CUST_ID ELSE NULL END) AS F_DPST_CUST_CNT
                 ,COUNT(DISTINCT CASE WHEN T2.GEN_TYP_CD = 'M' THEN T1.CUST_ID ELSE NULL END) AS M_DPST_CUST_CNT
                 ,COUNT(DISTINCT CASE WHEN T2.GEN_TYP_CD = '8' THEN T1.CUST_ID ELSE NULL END) AS E_DPST_CUST_CNT
                 ,SUM(CASE WHEN T1.INIT_DPST_TRM_CD <= '006' AND T2.GEN_TYP_CD = 'F' THEN T1.DPST_BAL ELSE 0 END) AS F_INIT_TRM_LSTHN_6MM_DPST_BAL
                 ,SUM(CASE WHEN T1.INIT_DPST_TRM_CD <= '006' AND T2.GEN_TYP_CD = 'M' THEN T1.DPST_BAL ELSE 0 END) AS M_INIT_TRM_LSTHN_6MM_DPST_BAL
                 ,SUM(CASE WHEN T1.INIT_DPST_TRM_CD <= '006' AND T2.GEN_TYP_CD = '8' THEN T1.DPST_BAL ELSE 0 END) AS E_INIT_TRM_LSTHN_6MM_DPST_BAL
                 ,SUM(CASE WHEN T1.INIT_DPST_TRM_CD BETWEEN '007' AND '012' AND T2.GEN_TYP_CD = 'F' THEN T1.DPST_BAL ELSE 0 END) AS F_INIT_TRM_LSTHN_12MM_DPST_BAL
                 ,SUM(CASE WHEN T1.INIT_DPST_TRM_CD BETWEEN '007' AND '012' AND T2.GEN_TYP_CD = 'M' THEN T1.DPST_BAL ELSE 0 END) AS M_INIT_TRM_LSTHN_12MM_DPST_BAL
                 ,SUM(CASE WHEN T1.INIT_DPST_TRM_CD BETWEEN '007' AND '012' AND T2.GEN_TYP_CD = '8' THEN T1.DPST_BAL ELSE 0 END) AS E_INIT_TRM_LSTHN_12MM_DPST_BAL
                 ,SUM(CASE WHEN T1.INIT_DPST_TRM_CD > '012' AND T2.GEN_TYP_CD = 'F' THEN T1.DPST_BAL ELSE 0 END) AS F_INIT_TRM_OVER_12MM_DPST_BAL
                 ,SUM(CASE WHEN T1.INIT_DPST_TRM_CD > '012' AND T2.GEN_TYP_CD = 'M' THEN T1.DPST_BAL ELSE 0 END) AS M_INIT_TRM_OVER_12MM_DPST_BAL
                 ,SUM(CASE WHEN T1.INIT_DPST_TRM_CD > '012' AND T2.GEN_TYP_CD = '8' THEN T1.DPST_BAL ELSE 0 END) AS E_INIT_TRM_OVER_12MM_DPST_BAL
           FROM   TM22_DDLY_CUST_DPST_TRANS_A T1 INNER JOIN (SELECT SUBSTR(BAS_DAY, 1, 6) AS BAS_YM, PCF_ID, MAX(BAS_DAY) AS BAS_DAY
                                                             FROM   TM22_DDLY_CUST_DPST_TRANS_A
                                                             WHERE  BAS_DAY BETWEEN loop_bas_day.BAS_YM||'01' AND loop_bas_day.BAS_YM||'31'
                                                             AND    PCF_ID IN (SELECT DISTINCT PCF_ID
                                                                               FROM   TBSM_INPT_RPT_SUBMIT_L
                                                                               WHERE  BTCH_BAS_DAY = v_st_date_01
                                                                               AND    INPT_RPT_ID IN ('G32_019_TTGS_01','G32_019_TTGS_02','G32_019_TTGS_03')
                                                                              )
                                                             GROUP BY SUBSTR(BAS_DAY, 1, 6), PCF_ID
                                                            ) T3
                                                         ON T3.BAS_DAY = T1.BAS_DAY
                                                        AND T3.PCF_ID  = T1.PCF_ID
                                                 INNER JOIN TM00_NEW_CUST_D T2
                                                         ON T2.PCF_ID   = T1.PCF_ID
                                                        AND T2.TX_CD_ID = T1.CUST_ID
           WHERE  T1.BAS_DAY BETWEEN loop_bas_day.BAS_YM||'01' AND loop_bas_day.BAS_YM||'31'
           AND    T1.PCF_ID IN (SELECT DISTINCT PCF_ID
                                FROM   TBSM_INPT_RPT_SUBMIT_L
                                WHERE  BTCH_BAS_DAY = v_st_date_01
                                AND    INPT_RPT_ID IN ('G32_019_TTGS_01','G32_019_TTGS_02','G32_019_TTGS_03')
                               )
           AND    T1.CUST_ID IS NOT NULL
           AND    T1.CUST_ID <> 'XXXX'
           AND    T1.DPST_BAL > 0
           GROUP BY T1.PCF_ID
          ),
          NEW_DPST AS
          (SELECT loop_bas_day.BAS_YM AS BAS_YM
                 ,PCF_ID
                 ,COUNT(*) AS TOT_NEW_ISS_TRM_DPST_CNT
                 ,SUM(CASE WHEN GEN_TYP_CD = 'F' THEN 1 ELSE 0 END) AS F_NEW_ISS_TRM_DPST_CNT
                 ,SUM(CASE WHEN GEN_TYP_CD = 'M' THEN 1 ELSE 0 END) AS M_NEW_ISS_TRM_DPST_CNT
                 ,SUM(CASE WHEN GEN_TYP_CD = '8' THEN 1 ELSE 0 END) AS E_NEW_ISS_TRM_DPST_CNT
                 ,SUM(DPST_BAL) AS TOT_NEW_ISS_TRM_DPST_AMT
                 ,SUM(CASE WHEN GEN_TYP_CD = 'F' THEN DPST_BAL ELSE 0 END) AS F_NEW_ISS_TRM_DPST_AMT
                 ,SUM(CASE WHEN GEN_TYP_CD = 'M' THEN DPST_BAL ELSE 0 END) AS M_NEW_ISS_TRM_DPST_AMT
                 ,SUM(CASE WHEN GEN_TYP_CD = '8' THEN DPST_BAL ELSE 0 END) AS E_NEW_ISS_TRM_DPST_AMT
           FROM   (SELECT T1.PCF_ID
                         ,T1.CUST_ID
                         ,T2.GEN_TYP_CD
                         ,T1.CUST_TYP_CD
                         ,T1.INIT_DPST_TRM_CD
                         ,T1.OPN_DAY
                         ,T1.MTRT_DAY
                         ,T1.ACTL_IR
                         ,SUM(T1.DPST_BAL) AS DPST_BAL
                   FROM   TM22_DDLY_CUST_DPST_TRANS_A T1 INNER JOIN TM00_NEW_CUST_D T2
                                                                 ON T2.PCF_ID   = T1.PCF_ID
                                                                AND T2.TX_CD_ID = T1.CUST_ID
                   WHERE  T1.BAS_DAY BETWEEN loop_bas_day.BAS_YM||'01' AND loop_bas_day.BAS_YM||'31'
                   AND    T1.PCF_ID IN (SELECT DISTINCT PCF_ID
                                        FROM   TBSM_INPT_RPT_SUBMIT_L
                                        WHERE  BTCH_BAS_DAY = v_st_date_01
                                        AND    INPT_RPT_ID IN ('G32_019_TTGS_01','G32_019_TTGS_02','G32_019_TTGS_03')
                                       )
                   AND    T1.BAS_DAY = T1.OPN_DAY
                   AND    T1.CUST_ID IS NOT NULL
                   AND    T1.CUST_ID <> 'XXXX'
                   AND    T1.DPST_BAL > 0
                   GROUP BY T1.PCF_ID, T1.CUST_ID, T2.GEN_TYP_CD,
                            T1.CUST_TYP_CD, T1.INIT_DPST_TRM_CD,
                            T1.OPN_DAY, T1.MTRT_DAY, T1.ACTL_IR
                  )
            GROUP BY PCF_ID
          ),
          OUT_DPST AS
          (SELECT BAS_YM
                 ,PCF_ID
                 ,COUNT(*) AS TOT_WDR_TRM_DPST_CNT
                 ,SUM(CASE WHEN GEN_TYP_CD = 'F' THEN 1 ELSE 0 END) AS F_WDR_TRM_DPST_CNT
                 ,SUM(CASE WHEN GEN_TYP_CD = 'M' THEN 1 ELSE 0 END) AS M_WDR_TRM_DPST_CNT
                 ,SUM(CASE WHEN GEN_TYP_CD = '8' THEN 1 ELSE 0 END) AS E_WDR_TRM_DPST_CNT
                 ,SUM(WDR_AMT * -1) AS TOT_WDR_TRM_DPST_AMT
                 ,SUM(CASE WHEN GEN_TYP_CD = 'F' THEN WDR_AMT * -1 ELSE 0 END) AS F_WDR_TRM_DPST_AMT
                 ,SUM(CASE WHEN GEN_TYP_CD = 'M' THEN WDR_AMT * -1 ELSE 0 END) AS M_WDR_TRM_DPST_AMT
                 ,SUM(CASE WHEN GEN_TYP_CD = '8' THEN WDR_AMT * -1 ELSE 0 END) AS E_WDR_TRM_DPST_AMT
           FROM   (SELECT T1.BAS_YM
                         ,T1.PCF_ID
                         ,T1.CUST_ID
                         ,T1.GEN_TYP_CD
                         ,T1.CUST_TYP_CD
                         ,T1.INIT_DPST_TRM_CD
                         ,T1.OPN_DAY
                         ,T1.MTRT_DAY
                         ,T1.ACTL_IR
                         ,T1.DPST_BAL - NVL(T2.DPST_BAL,0) AS WDR_AMT
                   FROM   (SELECT loop_bas_day.BAS_YM AS BAS_YM
                                 ,W1.PCF_ID
                                 ,W1.CUST_ID
                                 ,W2.GEN_TYP_CD
                                 ,W1.CUST_TYP_CD
                                 ,W1.INIT_DPST_TRM_CD
                                 ,W1.OPN_DAY
                                 ,W1.MTRT_DAY
                                 ,W1.ACTL_IR
                                 ,SUM(W1.DPST_BAL) AS DPST_BAL
                           FROM   TM22_DDLY_CUST_DPST_TRANS_A W1 INNER JOIN (SELECT SUBSTR(BAS_DAY, 1, 6) AS BAS_YM, PCF_ID, MAX(BAS_DAY) AS BAS_DAY
                                                                             FROM   TM22_DDLY_CUST_DPST_TRANS_A
                                                                             WHERE  BAS_DAY BETWEEN loop_bas_day.BAS_YM||'01' AND loop_bas_day.BAS_YM||'31'
                                                                             AND    PCF_ID IN (SELECT DISTINCT PCF_ID
                                                                                               FROM   TBSM_INPT_RPT_SUBMIT_L
                                                                                               WHERE  BTCH_BAS_DAY = v_st_date_01
                                                                                               AND    INPT_RPT_ID IN ('G32_019_TTGS_01','G32_019_TTGS_02','G32_019_TTGS_03')
                                                                                              )
                                                                             GROUP BY SUBSTR(BAS_DAY, 1, 6), PCF_ID
                                                                            ) W3
                                                                         ON W3.BAS_DAY = W1.BAS_DAY
                                                                        AND W3.PCF_ID  = W1.PCF_ID
                                                                 INNER JOIN TM00_NEW_CUST_D W2
                                                                         ON W2.PCF_ID   = W1.PCF_ID
                                                                        AND W2.TX_CD_ID = W1.CUST_ID
                           WHERE  W1.BAS_DAY BETWEEN loop_bas_day.BAS_YM||'01' AND loop_bas_day.BAS_YM||'31'
                           AND    W1.PCF_ID IN (SELECT DISTINCT PCF_ID
                                                FROM   TBSM_INPT_RPT_SUBMIT_L
                                                WHERE  BTCH_BAS_DAY = v_st_date_01
                                                AND    INPT_RPT_ID IN ('G32_019_TTGS_01','G32_019_TTGS_02','G32_019_TTGS_03')
                                               )
                           AND    W1.CUST_ID IS NOT NULL
                           AND    W1.CUST_ID <> 'XXXX'
                           AND    W1.DPST_BAL > 0
                           GROUP BY W1.PCF_ID, W1.CUST_ID, W2.GEN_TYP_CD,
                                    W1.CUST_TYP_CD, W1.INIT_DPST_TRM_CD,
                                    W1.OPN_DAY, W1.MTRT_DAY, W1.ACTL_IR) T1 INNER JOIN (SELECT loop_bas_day.BAS_YM AS BAS_YM
                                                                                              ,X1.PCF_ID
                                                                                              ,X1.CUST_ID
                                                                                              ,X2.GEN_TYP_CD
                                                                                              ,X1.CUST_TYP_CD
                                                                                              ,X1.INIT_DPST_TRM_CD
                                                                                              ,X1.OPN_DAY
                                                                                              ,X1.MTRT_DAY
                                                                                              ,X1.ACTL_IR
                                                                                              ,SUM(X1.DPST_BAL) AS DPST_BAL
                                                                                        FROM   TM22_DDLY_CUST_DPST_TRANS_A X1 INNER JOIN (SELECT SUBSTR(BAS_DAY, 1, 6) AS BAS_YM, PCF_ID, MAX(BAS_DAY) AS BAS_DAY
                                                                                                                                          FROM   TM22_DDLY_CUST_DPST_TRANS_A
                                                                                                                                          WHERE  BAS_DAY BETWEEN TO_CHAR(ADD_MONTHS(TO_DATE(loop_bas_day.BAS_YM, 'YYYYMM'), -1), 'YYYYMM')||'01'
                                                                                                                                                             AND TO_CHAR(ADD_MONTHS(TO_DATE(loop_bas_day.BAS_YM, 'YYYYMM'), -1), 'YYYYMM')||'31'
                                                                                                                                          AND    PCF_ID IN (SELECT DISTINCT PCF_ID
                                                                                                                                                            FROM   TBSM_INPT_RPT_SUBMIT_L
                                                                                                                                                            WHERE  BTCH_BAS_DAY = v_st_date_01
                                                                                                                                                            AND    INPT_RPT_ID IN ('G32_019_TTGS_01','G32_019_TTGS_02','G32_019_TTGS_03')
                                                                                                                                                           )
                                                                                                                                          GROUP BY SUBSTR(BAS_DAY, 1, 6), PCF_ID
                                                                                                                                         ) X3
                                                                                                                                      ON X3.BAS_DAY = X1.BAS_DAY
                                                                                                                                     AND X3.PCF_ID  = X1.PCF_ID
                                                                                                                              INNER JOIN TM00_NEW_CUST_D X2
                                                                                                                                      ON X2.PCF_ID   = X1.PCF_ID
                                                                                                                                     AND X2.TX_CD_ID = X1.CUST_ID
                                                                                        WHERE  X1.BAS_DAY BETWEEN TO_CHAR(ADD_MONTHS(TO_DATE(loop_bas_day.BAS_YM, 'YYYYMM'), -1), 'YYYYMM')||'01'
                                                                                                              AND TO_CHAR(ADD_MONTHS(TO_DATE(loop_bas_day.BAS_YM, 'YYYYMM'), -1), 'YYYYMM')||'31'
                                                                                        AND    X1.PCF_ID IN (SELECT DISTINCT PCF_ID
                                                                                                             FROM   TBSM_INPT_RPT_SUBMIT_L
                                                                                                             WHERE  BTCH_BAS_DAY = v_st_date_01
                                                                                                             AND    INPT_RPT_ID IN ('G32_019_TTGS_01','G32_019_TTGS_02','G32_019_TTGS_03')
                                                                                                            )
                                                                                        AND    X1.CUST_ID IS NOT NULL
                                                                                        AND    X1.CUST_ID <> 'XXXX'
                                                                                        AND    X1.DPST_BAL > 0
                                                                                        GROUP BY X1.PCF_ID, X1.CUST_ID, X2.GEN_TYP_CD,
                                                                                                 X1.CUST_TYP_CD, X1.INIT_DPST_TRM_CD,
                                                                                                 X1.OPN_DAY, X1.MTRT_DAY, X1.ACTL_IR
                                                                                       ) T2
                                                                                    ON T2.BAS_YM = T1.BAS_YM
                                                                                   AND T2.PCF_ID = T1.PCF_ID
                                                                                   AND T2.CUST_ID = T1.CUST_ID
                                                                                   AND T2.GEN_TYP_CD = T1.GEN_TYP_CD
                                                                                   AND T2.CUST_TYP_CD = T1.CUST_TYP_CD
                                                                                   AND T2.INIT_DPST_TRM_CD = T1.INIT_DPST_TRM_CD
                                                                                   AND T2.OPN_DAY = T1.OPN_DAY
                                                                                   AND T2.MTRT_DAY = T1.MTRT_DAY
                                                                                   AND T2.ACTL_IR = T1.ACTL_IR
                   WHERE T1.DPST_BAL - NVL(T2.DPST_BAL, 0) < 0
                  )
            GROUP BY BAS_YM, PCF_ID
          )
          SELECT T1.BAS_YM
                ,T1.PCF_ID
                ,T1.TOT_DPST_BAL
                ,T1.F_DPST_BAL
                ,T1.M_DPST_BAL
                ,T1.E_DPST_BAL
                ,T1.TOT_DPST_CUST_CNT
                ,T1.F_DPST_CUST_CNT
                ,T1.M_DPST_CUST_CNT
                ,T1.E_DPST_CUST_CNT
                ,T2.TOT_NEW_ISS_TRM_DPST_CNT
                ,T2.F_NEW_ISS_TRM_DPST_CNT
                ,T2.M_NEW_ISS_TRM_DPST_CNT
                ,T2.E_NEW_ISS_TRM_DPST_CNT
                ,T2.TOT_NEW_ISS_TRM_DPST_AMT
                ,T2.F_NEW_ISS_TRM_DPST_AMT
                ,T2.M_NEW_ISS_TRM_DPST_AMT
                ,T2.E_NEW_ISS_TRM_DPST_AMT
                ,T3.TOT_WDR_TRM_DPST_CNT
                ,T3.F_WDR_TRM_DPST_CNT
                ,T3.M_WDR_TRM_DPST_CNT
                ,T3.E_WDR_TRM_DPST_CNT
                ,T3.TOT_WDR_TRM_DPST_AMT
                ,T3.F_WDR_TRM_DPST_AMT
                ,T3.M_WDR_TRM_DPST_AMT
                ,T3.E_WDR_TRM_DPST_AMT
                ,T1.F_INIT_TRM_LSTHN_6MM_DPST_BAL
                ,T1.M_INIT_TRM_LSTHN_6MM_DPST_BAL
                ,T1.E_INIT_TRM_LSTHN_6MM_DPST_BAL
                ,T1.F_INIT_TRM_LSTHN_12MM_DPST_BAL
                ,T1.M_INIT_TRM_LSTHN_12MM_DPST_BAL
                ,T1.E_INIT_TRM_LSTHN_12MM_DPST_BAL
                ,T1.F_INIT_TRM_OVER_12MM_DPST_BAL
                ,T1.M_INIT_TRM_OVER_12MM_DPST_BAL
                ,T1.E_INIT_TRM_OVER_12MM_DPST_BAL
                ,SYSTIMESTAMP
          FROM   TOT_DPST T1 LEFT OUTER JOIN NEW_DPST T2
                                          ON T2.BAS_YM = T1.BAS_YM
                                         AND T2.PCF_ID = T1.PCF_ID
                             LEFT OUTER JOIN OUT_DPST T3
                                          ON T3.BAS_YM = T1.BAS_YM
                                         AND T3.PCF_ID = T1.PCF_ID

          ;
          ----------------------------------------------------------------------------
          --  Transaction Log
          ----------------------------------------------------------------------------
          v_step_code    := '020' ;
          v_step_desc    := v_wk_date || v_seq || ' ' || loop_bas_day.BAS_YM ||' : Inserting Data Result';
          v_time         := SYSDATE ;

          P1_BSM_PROG_EXEC_LOG(v_program_id, v_program_type_name, v_step_code, v_step_desc, v_time, sql%rowcount, NULL, NULL) ;

          COMMIT;

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

    FOR loop_bas_day in v_sbmt_day_2(v_wk_date,v_st_date_01,v_end_date_01)
    LOOP
    BEGIN
          ----------------------------------------------------------------------------
          --  1.1 Delete Historical Data
          ----------------------------------------------------------------------------
          DELETE
          FROM   TM27_MMLY_GEN_DPST_KEY_IDC_A
          WHERE  BAS_YM = loop_bas_day.BAS_YM;
          ----------------------------------------------------------------------------
          --  Transaction Log
          ----------------------------------------------------------------------------
          v_step_code    := '030' ;
          v_step_desc    := v_wk_date || v_seq || ' ' || loop_bas_day.BAS_YM ||' : Deleting Data Result';
          v_time         := SYSDATE ;

          P1_BSM_PROG_EXEC_LOG(v_program_id, v_program_type_name, v_step_code, v_step_desc, v_time, sql%rowcount, NULL, NULL) ;

          ----------------------------------------------------------------------------
          --  1.2 Inserting Data by TM27_MMLY_GEN_DPST_KEY_IDC_A
          ----------------------------------------------------------------------------
          INSERT INTO TM27_MMLY_GEN_DPST_KEY_IDC_A
          (   BAS_YM
             ,PCF_ID
             ,TOT_DPST_BAL
             ,F_DPST_BAL
             ,M_DPST_BAL
             ,E_DPST_BAL
             ,TOT_DPST_CUST_CNT
             ,F_DPST_CUST_CNT
             ,M_DPST_CUST_CNT
             ,E_DPST_CUST_CNT
             ,TOT_NEW_ISS_TRM_DPST_CNT
             ,F_NEW_ISS_TRM_DPST_CNT
             ,M_NEW_ISS_TRM_DPST_CNT
             ,E_NEW_ISS_TRM_DPST_CNT
             ,TOT_NEW_ISS_TRM_DPST_AMT
             ,F_NEW_ISS_TRM_DPST_AMT
             ,M_NEW_ISS_TRM_DPST_AMT
             ,E_NEW_ISS_TRM_DPST_AMT
             ,TOT_WDR_TRM_DPST_CNT
             ,F_WDR_TRM_DPST_CNT
             ,M_WDR_TRM_DPST_CNT
             ,E_WDR_TRM_DPST_CNT
             ,TOT_WDR_TRM_DPST_AMT
             ,F_WDR_TRM_DPST_AMT
             ,M_WDR_TRM_DPST_AMT
             ,E_WDR_TRM_DPST_AMT
             ,F_INIT_TRM_LSTHN_6MM_DPST_BAL
             ,M_INIT_TRM_LSTHN_6MM_DPST_BAL
             ,E_INIT_TRM_LSTHN_6MM_DPST_BAL
             ,F_INIT_TRM_LSTHN_12MM_DPST_BAL
             ,M_INIT_TRM_LSTHN_12MM_DPST_BAL
             ,E_INIT_TRM_LSTHN_12MM_DPST_BAL
             ,F_INIT_TRM_OVER_12MM_DPST_BAL
             ,M_INIT_TRM_OVER_12MM_DPST_BAL
             ,E_INIT_TRM_OVER_12MM_DPST_BAL
             ,TRGT_DATA_LST_MOD_TM
          )
          WITH
          TOT_DPST AS
          (SELECT loop_bas_day.BAS_YM AS BAS_YM
                 ,T1.PCF_ID
                 ,SUM(T1.DPST_BAL) AS TOT_DPST_BAL
                 ,SUM(CASE WHEN T2.GEN_TYP_CD = 'F' THEN T1.DPST_BAL ELSE 0 END) AS F_DPST_BAL
                 ,SUM(CASE WHEN T2.GEN_TYP_CD = 'M' THEN T1.DPST_BAL ELSE 0 END) AS M_DPST_BAL
                 ,SUM(CASE WHEN T2.GEN_TYP_CD = '8' THEN T1.DPST_BAL ELSE 0 END) AS E_DPST_BAL
                 ,COUNT(DISTINCT T1.CUST_ID) AS TOT_DPST_CUST_CNT
                 ,COUNT(DISTINCT CASE WHEN T2.GEN_TYP_CD = 'F' THEN T1.CUST_ID ELSE NULL END) AS F_DPST_CUST_CNT
                 ,COUNT(DISTINCT CASE WHEN T2.GEN_TYP_CD = 'M' THEN T1.CUST_ID ELSE NULL END) AS M_DPST_CUST_CNT
                 ,COUNT(DISTINCT CASE WHEN T2.GEN_TYP_CD = '8' THEN T1.CUST_ID ELSE NULL END) AS E_DPST_CUST_CNT
                 ,SUM(CASE WHEN T1.INIT_DPST_TRM_CD <= '006' AND T2.GEN_TYP_CD = 'F' THEN T1.DPST_BAL ELSE 0 END) AS F_INIT_TRM_LSTHN_6MM_DPST_BAL
                 ,SUM(CASE WHEN T1.INIT_DPST_TRM_CD <= '006' AND T2.GEN_TYP_CD = 'M' THEN T1.DPST_BAL ELSE 0 END) AS M_INIT_TRM_LSTHN_6MM_DPST_BAL
                 ,SUM(CASE WHEN T1.INIT_DPST_TRM_CD <= '006' AND T2.GEN_TYP_CD = '8' THEN T1.DPST_BAL ELSE 0 END) AS E_INIT_TRM_LSTHN_6MM_DPST_BAL
                 ,SUM(CASE WHEN T1.INIT_DPST_TRM_CD BETWEEN '007' AND '012' AND T2.GEN_TYP_CD = 'F' THEN T1.DPST_BAL ELSE 0 END) AS F_INIT_TRM_LSTHN_12MM_DPST_BAL
                 ,SUM(CASE WHEN T1.INIT_DPST_TRM_CD BETWEEN '007' AND '012' AND T2.GEN_TYP_CD = 'M' THEN T1.DPST_BAL ELSE 0 END) AS M_INIT_TRM_LSTHN_12MM_DPST_BAL
                 ,SUM(CASE WHEN T1.INIT_DPST_TRM_CD BETWEEN '007' AND '012' AND T2.GEN_TYP_CD = '8' THEN T1.DPST_BAL ELSE 0 END) AS E_INIT_TRM_LSTHN_12MM_DPST_BAL
                 ,SUM(CASE WHEN T1.INIT_DPST_TRM_CD > '012' AND T2.GEN_TYP_CD = 'F' THEN T1.DPST_BAL ELSE 0 END) AS F_INIT_TRM_OVER_12MM_DPST_BAL
                 ,SUM(CASE WHEN T1.INIT_DPST_TRM_CD > '012' AND T2.GEN_TYP_CD = 'M' THEN T1.DPST_BAL ELSE 0 END) AS M_INIT_TRM_OVER_12MM_DPST_BAL
                 ,SUM(CASE WHEN T1.INIT_DPST_TRM_CD > '012' AND T2.GEN_TYP_CD = '8' THEN T1.DPST_BAL ELSE 0 END) AS E_INIT_TRM_OVER_12MM_DPST_BAL
           FROM   TM22_DDLY_CUST_DPST_TRANS_A T1 INNER JOIN (SELECT SUBSTR(BAS_DAY, 1, 6) AS BAS_YM, PCF_ID, MAX(BAS_DAY) AS BAS_DAY
                                                             FROM   TM22_DDLY_CUST_DPST_TRANS_A
                                                             WHERE  BAS_DAY BETWEEN loop_bas_day.BAS_YM||'01' AND loop_bas_day.BAS_YM||'31'
                                                             GROUP BY SUBSTR(BAS_DAY, 1, 6), PCF_ID
                                                            ) T3
                                                         ON T3.BAS_DAY = T1.BAS_DAY
                                                        AND T3.PCF_ID  = T1.PCF_ID
                                                 INNER JOIN TM00_NEW_CUST_D T2
                                                         ON T2.PCF_ID   = T1.PCF_ID
                                                        AND T2.TX_CD_ID = T1.CUST_ID
           WHERE  T1.BAS_DAY BETWEEN loop_bas_day.BAS_YM||'01' AND loop_bas_day.BAS_YM||'31'
           AND    T1.CUST_ID IS NOT NULL
           AND    T1.CUST_ID <> 'XXXX'
           AND    T1.DPST_BAL > 0
           GROUP BY T1.PCF_ID
          ),
          NEW_DPST AS
          (SELECT loop_bas_day.BAS_YM AS BAS_YM
                 ,PCF_ID
                 ,COUNT(*) AS TOT_NEW_ISS_TRM_DPST_CNT
                 ,SUM(CASE WHEN GEN_TYP_CD = 'F' THEN 1 ELSE 0 END) AS F_NEW_ISS_TRM_DPST_CNT
                 ,SUM(CASE WHEN GEN_TYP_CD = 'M' THEN 1 ELSE 0 END) AS M_NEW_ISS_TRM_DPST_CNT
                 ,SUM(CASE WHEN GEN_TYP_CD = '8' THEN 1 ELSE 0 END) AS E_NEW_ISS_TRM_DPST_CNT
                 ,SUM(DPST_BAL) AS TOT_NEW_ISS_TRM_DPST_AMT
                 ,SUM(CASE WHEN GEN_TYP_CD = 'F' THEN DPST_BAL ELSE 0 END) AS F_NEW_ISS_TRM_DPST_AMT
                 ,SUM(CASE WHEN GEN_TYP_CD = 'M' THEN DPST_BAL ELSE 0 END) AS M_NEW_ISS_TRM_DPST_AMT
                 ,SUM(CASE WHEN GEN_TYP_CD = '8' THEN DPST_BAL ELSE 0 END) AS E_NEW_ISS_TRM_DPST_AMT
           FROM   (SELECT T1.PCF_ID
                         ,T1.CUST_ID
                         ,T2.GEN_TYP_CD
                         ,T1.CUST_TYP_CD
                         ,T1.INIT_DPST_TRM_CD
                         ,T1.OPN_DAY
                         ,T1.MTRT_DAY
                         ,T1.ACTL_IR
                         ,SUM(T1.DPST_BAL) AS DPST_BAL
                   FROM   TM22_DDLY_CUST_DPST_TRANS_A T1 INNER JOIN TM00_NEW_CUST_D T2
                                                                 ON T2.PCF_ID   = T1.PCF_ID
                                                                AND T2.TX_CD_ID = T1.CUST_ID
                   WHERE  T1.BAS_DAY BETWEEN loop_bas_day.BAS_YM||'01' AND loop_bas_day.BAS_YM||'31'
                   AND    T1.BAS_DAY = T1.OPN_DAY
                   AND    T1.CUST_ID IS NOT NULL
                   AND    T1.CUST_ID <> 'XXXX'
                   AND    T1.DPST_BAL > 0
                   GROUP BY T1.PCF_ID, T1.CUST_ID, T2.GEN_TYP_CD,
                            T1.CUST_TYP_CD, T1.INIT_DPST_TRM_CD,
                            T1.OPN_DAY, T1.MTRT_DAY, T1.ACTL_IR
                  )
            GROUP BY PCF_ID
          ),
          OUT_DPST AS
          (SELECT BAS_YM
                 ,PCF_ID
                 ,COUNT(*) AS TOT_WDR_TRM_DPST_CNT
                 ,SUM(CASE WHEN GEN_TYP_CD = 'F' THEN 1 ELSE 0 END) AS F_WDR_TRM_DPST_CNT
                 ,SUM(CASE WHEN GEN_TYP_CD = 'M' THEN 1 ELSE 0 END) AS M_WDR_TRM_DPST_CNT
                 ,SUM(CASE WHEN GEN_TYP_CD = '8' THEN 1 ELSE 0 END) AS E_WDR_TRM_DPST_CNT
                 ,SUM(WDR_AMT * -1) AS TOT_WDR_TRM_DPST_AMT
                 ,SUM(CASE WHEN GEN_TYP_CD = 'F' THEN WDR_AMT * -1 ELSE 0 END) AS F_WDR_TRM_DPST_AMT
                 ,SUM(CASE WHEN GEN_TYP_CD = 'M' THEN WDR_AMT * -1 ELSE 0 END) AS M_WDR_TRM_DPST_AMT
                 ,SUM(CASE WHEN GEN_TYP_CD = '8' THEN WDR_AMT * -1 ELSE 0 END) AS E_WDR_TRM_DPST_AMT
           FROM   (SELECT T1.BAS_YM
                         ,T1.PCF_ID
                         ,T1.CUST_ID
                         ,T1.GEN_TYP_CD
                         ,T1.CUST_TYP_CD
                         ,T1.INIT_DPST_TRM_CD
                         ,T1.OPN_DAY
                         ,T1.MTRT_DAY
                         ,T1.ACTL_IR
                         ,T1.DPST_BAL - NVL(T2.DPST_BAL,0) AS WDR_AMT
                   FROM   (SELECT loop_bas_day.BAS_YM AS BAS_YM
                                 ,W1.PCF_ID
                                 ,W1.CUST_ID
                                 ,W2.GEN_TYP_CD
                                 ,W1.CUST_TYP_CD
                                 ,W1.INIT_DPST_TRM_CD
                                 ,W1.OPN_DAY
                                 ,W1.MTRT_DAY
                                 ,W1.ACTL_IR
                                 ,SUM(W1.DPST_BAL) AS DPST_BAL
                           FROM   TM22_DDLY_CUST_DPST_TRANS_A W1 INNER JOIN (SELECT SUBSTR(BAS_DAY, 1, 6) AS BAS_YM, PCF_ID, MAX(BAS_DAY) AS BAS_DAY
                                                                             FROM   TM22_DDLY_CUST_DPST_TRANS_A
                                                                             WHERE  BAS_DAY BETWEEN loop_bas_day.BAS_YM||'01' AND loop_bas_day.BAS_YM||'31'
                                                                             GROUP BY SUBSTR(BAS_DAY, 1, 6), PCF_ID
                                                                            ) W3
                                                                         ON W3.BAS_DAY = W1.BAS_DAY
                                                                        AND W3.PCF_ID  = W1.PCF_ID
                                                                 INNER JOIN TM00_NEW_CUST_D W2
                                                                         ON W2.PCF_ID   = W1.PCF_ID
                                                                        AND W2.TX_CD_ID = W1.CUST_ID
                           WHERE  W1.BAS_DAY BETWEEN loop_bas_day.BAS_YM||'01' AND loop_bas_day.BAS_YM||'31'
                           AND    W1.CUST_ID IS NOT NULL
                           AND    W1.CUST_ID <> 'XXXX'
                           AND    W1.DPST_BAL > 0
                           GROUP BY W1.PCF_ID, W1.CUST_ID, W2.GEN_TYP_CD,
                                    W1.CUST_TYP_CD, W1.INIT_DPST_TRM_CD,
                                    W1.OPN_DAY, W1.MTRT_DAY, W1.ACTL_IR) T1 INNER JOIN (SELECT loop_bas_day.BAS_YM AS BAS_YM
                                                                                              ,X1.PCF_ID
                                                                                              ,X1.CUST_ID
                                                                                              ,X2.GEN_TYP_CD
                                                                                              ,X1.CUST_TYP_CD
                                                                                              ,X1.INIT_DPST_TRM_CD
                                                                                              ,X1.OPN_DAY
                                                                                              ,X1.MTRT_DAY
                                                                                              ,X1.ACTL_IR
                                                                                              ,SUM(X1.DPST_BAL) AS DPST_BAL
                                                                                        FROM   TM22_DDLY_CUST_DPST_TRANS_A X1 INNER JOIN (SELECT SUBSTR(BAS_DAY, 1, 6) AS BAS_YM, PCF_ID, MAX(BAS_DAY) AS BAS_DAY
                                                                                                                                          FROM   TM22_DDLY_CUST_DPST_TRANS_A
                                                                                                                                          WHERE  BAS_DAY BETWEEN TO_CHAR(ADD_MONTHS(TO_DATE(loop_bas_day.BAS_YM, 'YYYYMM'), -1), 'YYYYMM')||'01'
                                                                                                                                                             AND TO_CHAR(ADD_MONTHS(TO_DATE(loop_bas_day.BAS_YM, 'YYYYMM'), -1), 'YYYYMM')||'31'
                                                                                                                                          GROUP BY SUBSTR(BAS_DAY, 1, 6), PCF_ID
                                                                                                                                         ) X3
                                                                                                                                      ON X3.BAS_DAY = X1.BAS_DAY
                                                                                                                                     AND X3.PCF_ID  = X1.PCF_ID
                                                                                                                              INNER JOIN TM00_NEW_CUST_D X2
                                                                                                                                      ON X2.PCF_ID   = X1.PCF_ID
                                                                                                                                     AND X2.TX_CD_ID = X1.CUST_ID
                                                                                        WHERE  X1.BAS_DAY BETWEEN TO_CHAR(ADD_MONTHS(TO_DATE(loop_bas_day.BAS_YM, 'YYYYMM'), -1), 'YYYYMM')||'01'
                                                                                                              AND TO_CHAR(ADD_MONTHS(TO_DATE(loop_bas_day.BAS_YM, 'YYYYMM'), -1), 'YYYYMM')||'31'
                                                                                        AND    X1.CUST_ID IS NOT NULL
                                                                                        AND    X1.CUST_ID <> 'XXXX'
                                                                                        AND    X1.DPST_BAL > 0
                                                                                        GROUP BY X1.PCF_ID, X1.CUST_ID, X2.GEN_TYP_CD,
                                                                                                 X1.CUST_TYP_CD, X1.INIT_DPST_TRM_CD,
                                                                                                 X1.OPN_DAY, X1.MTRT_DAY, X1.ACTL_IR
                                                                                       ) T2
                                                                                    ON T2.BAS_YM = T1.BAS_YM
                                                                                   AND T2.PCF_ID = T1.PCF_ID
                                                                                   AND T2.CUST_ID = T1.CUST_ID
                                                                                   AND T2.GEN_TYP_CD = T1.GEN_TYP_CD
                                                                                   AND T2.CUST_TYP_CD = T1.CUST_TYP_CD
                                                                                   AND T2.INIT_DPST_TRM_CD = T1.INIT_DPST_TRM_CD
                                                                                   AND T2.OPN_DAY = T1.OPN_DAY
                                                                                   AND T2.MTRT_DAY = T1.MTRT_DAY
                                                                                   AND T2.ACTL_IR = T1.ACTL_IR
                   WHERE T1.DPST_BAL - NVL(T2.DPST_BAL, 0) < 0
                  )
            GROUP BY BAS_YM, PCF_ID
          )
          SELECT T1.BAS_YM
                ,T1.PCF_ID
                ,T1.TOT_DPST_BAL
                ,T1.F_DPST_BAL
                ,T1.M_DPST_BAL
                ,T1.E_DPST_BAL
                ,T1.TOT_DPST_CUST_CNT
                ,T1.F_DPST_CUST_CNT
                ,T1.M_DPST_CUST_CNT
                ,T1.E_DPST_CUST_CNT
                ,T2.TOT_NEW_ISS_TRM_DPST_CNT
                ,T2.F_NEW_ISS_TRM_DPST_CNT
                ,T2.M_NEW_ISS_TRM_DPST_CNT
                ,T2.E_NEW_ISS_TRM_DPST_CNT
                ,T2.TOT_NEW_ISS_TRM_DPST_AMT
                ,T2.F_NEW_ISS_TRM_DPST_AMT
                ,T2.M_NEW_ISS_TRM_DPST_AMT
                ,T2.E_NEW_ISS_TRM_DPST_AMT
                ,T3.TOT_WDR_TRM_DPST_CNT
                ,T3.F_WDR_TRM_DPST_CNT
                ,T3.M_WDR_TRM_DPST_CNT
                ,T3.E_WDR_TRM_DPST_CNT
                ,T3.TOT_WDR_TRM_DPST_AMT
                ,T3.F_WDR_TRM_DPST_AMT
                ,T3.M_WDR_TRM_DPST_AMT
                ,T3.E_WDR_TRM_DPST_AMT
                ,T1.F_INIT_TRM_LSTHN_6MM_DPST_BAL
                ,T1.M_INIT_TRM_LSTHN_6MM_DPST_BAL
                ,T1.E_INIT_TRM_LSTHN_6MM_DPST_BAL
                ,T1.F_INIT_TRM_LSTHN_12MM_DPST_BAL
                ,T1.M_INIT_TRM_LSTHN_12MM_DPST_BAL
                ,T1.E_INIT_TRM_LSTHN_12MM_DPST_BAL
                ,T1.F_INIT_TRM_OVER_12MM_DPST_BAL
                ,T1.M_INIT_TRM_OVER_12MM_DPST_BAL
                ,T1.E_INIT_TRM_OVER_12MM_DPST_BAL
                ,SYSTIMESTAMP
          FROM   TOT_DPST T1 LEFT OUTER JOIN NEW_DPST T2
                                          ON T2.BAS_YM = T1.BAS_YM
                                         AND T2.PCF_ID = T1.PCF_ID
                             LEFT OUTER JOIN OUT_DPST T3
                                          ON T3.BAS_YM = T1.BAS_YM
                                         AND T3.PCF_ID = T1.PCF_ID

          ;
          ----------------------------------------------------------------------------
          --  Transaction Log
          ----------------------------------------------------------------------------
          v_step_code    := '040' ;
          v_step_desc    := v_wk_date || v_seq || ' ' || loop_bas_day.BAS_YM ||' : Inserting Data Result';
          v_time         := SYSDATE ;

          P1_BSM_PROG_EXEC_LOG(v_program_id, v_program_type_name, v_step_code, v_step_desc, v_time, sql%rowcount, NULL, NULL) ;

          COMMIT;

          EXCEPTION
          WHEN OTHERS THEN
               begin
                     v_step_code    := '041' ;
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