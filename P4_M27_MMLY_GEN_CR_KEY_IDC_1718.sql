create or replace PROCEDURE            "P4_M27_MMLY_GEN_CR_KEY_IDC_1718"
                 ( p_wkdate    IN  CHAR DEFAULT NULL
                 , p_stdate_01 IN  CHAR DEFAULT NULL
                 , p_eddate_01 IN  CHAR DEFAULT NULL
                 )
IS
    /*--------------------------------------------------------------------------
     * PROGRAM ID    : P4_M27_MMLY_GEN_CR_KEY_IDC_1718
     * PROGRAM NAME  : A program for insert data to TM27_MMLY_GEN_CR_KEY_IDC_A
     * SOURCE TABLE  : TM21_DDLY_CUST_CR_TRANS_A
     * TARGET TABLE  : TM27_MMLY_GEN_CR_KEY_IDC_A
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

    v_program_id              VARCHAR2(100) DEFAULT 'P4_M27_MMLY_GEN_CR_KEY_IDC_1718' ;
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
                                                                           AND    INPT_RPT_ID = 'G32_012_TTGS'
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
                                                  WHERE  BTCH_BAS_DAY >= v_dt2
                                                  AND    INPT_RPT_ID = 'G32_012_TTGS'
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
    --  1.0 Deleting and Inserting Data
    ----------------------------------------------------------------------------
    FOR loop_bas_day in v_sbmt_day_1(v_wk_date,v_st_date_01,v_end_date_01)
    LOOP
    BEGIN
          ----------------------------------------------------------------------------
          --  1.1 Deleting Data by TM27_MMLY_GEN_CR_KEY_IDC_A
          ----------------------------------------------------------------------------
          DELETE
          FROM   TM27_MMLY_GEN_CR_KEY_IDC_A
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
          --  1.1 Inserting Data by TM27_MMLY_GEN_CR_KEY_IDC_A
          ----------------------------------------------------------------------------
          INSERT INTO TM27_MMLY_GEN_CR_KEY_IDC_A
          (   BAS_YM
             ,PCF_ID
             ,TOT_LN_CUST_CNT
             ,F_LN_CUST_CNT
             ,M_LN_CUST_CNT
             ,TOT_ACT_LN_CUST_CNT
             ,F_ACT_LN_CUST_CNT
             ,M_ACT_LN_CUST_CNT
             ,TOT_LN_CNTR_CNT
             ,F_LN_CNTR_CNT
             ,M_LN_CNTR_CNT
             ,TOT_LN_BAL
             ,F_LN_BAL
             ,M_LN_BAL
             ,F_INIT_TRM_LSTHN_6MM_LN_BAL
             ,M_INIT_TRM_LSTHN_6MM_LN_BAL
             ,F_INIT_TRM_LSTHN_12MM_LN_BAL
             ,M_INIT_TRM_LSTHN_12MM_LN_BAL
             ,F_INIT_TRM_OVER_12MM_LN_BAL
             ,M_INIT_TRM_OVER_12MM_LN_BAL
             ,TOT_AGRCTR_LN_CNTR_CNT
             ,F_AGRCTR_LN_CNTR_CNT
             ,M_AGRCTR_LN_CNTR_CNT
             ,TOT_NON_AGRCTR_LN_CNTR_CNT
             ,F_NON_AGRCTR_LN_CNTR_CNT
             ,M_NON_AGRCTR_LN_CNTR_CNT
             ,TOT_NEW_ARISN_LN_CNTR_CNT
             ,F_NEW_ARISN_LN_CNTR_CNT
             ,M_NEW_ARISN_LN_CNTR_CNT
             ,TOT_NEW_ARISN_LN_CNTR_AMT
             ,F_NEW_ARISN_LN_CNTR_AMT
             ,M_NEW_ARISN_LN_CNTR_AMT
             ,TOT_L3M_NEW_ARISN_LN_CNTR_AMT
             ,F_L3M_NEW_ARISN_LN_CNTR_AMT
             ,M_L3M_NEW_ARISN_LN_CNTR_AMT
             ,TOT_OVD_LN_CNTR_CNT
             ,F_OVD_LN_CNTR_CNT
             ,M_OVD_LN_CNTR_CNT
             ,TOT_OVD_LN_BAL
             ,F_OVD_LN_BAL
             ,M_OVD_LN_BAL
             ,TOT_NORML_LN_CNTR_CNT
             ,F_NORML_LN_CNTR_CNT
             ,M_NORML_LN_CNTR_CNT
             ,TOT_NORML_LN_BAL
             ,F_NORML_LN_BAL
             ,M_NORML_LN_BAL
             ,TOT_WIT_COLL_LN_CNTR_CNT
             ,F_WIT_COLL_LN_CNTR_CNT
             ,M_WIT_COLL_LN_CNTR_CNT
             ,TOT_WO_COLL_LN_CNTR_CNT
             ,F_WO_COLL_LN_CNTR_CNT
             ,M_WO_COLL_LN_CNTR_CNT
             ,TOT_WIT_COLL_OVD_LN_CNTR_CNT
             ,F_WIT_COLL_OVD_LN_CNTR_CNT
             ,M_WIT_COLL_OVD_LN_CNTR_CNT
             ,TOT_WIT_COLL_OVD_LN_BAL
             ,F_WIT_COLL_OVD_LN_BAL
             ,M_WIT_COLL_OVD_LN_BAL
             ,TOT_WIT_COLL_OVD_LN_CNTR_AMT
             ,F_WIT_COLL_OVD_LN_CNTR_AMT
             ,M_WIT_COLL_OVD_LN_CNTR_AMT
             ,TOT_WO_COLL_OVD_LN_CNT
             ,F_WO_COLL_OVD_LN_CNT
             ,M_WO_COLL_OVD_LN_CNT
             ,TOT_WO_COLL_OVD_LN_BAL
             ,F_WO_COLL_OVD_LN_BAL
             ,M_WO_COLL_OVD_LN_BAL
             ,TOT_WO_COLL_OVD_LN_CNTR_AMT
             ,F_WO_COLL_OVD_LN_CNTR_AMT
             ,M_WO_COLL_OVD_LN_CNTR_AMT
             ,TOT_WIT_COLL_NORML_LN_CNTR_CNT
             ,F_WIT_COLL_NORML_LN_CNTR_CNT
             ,M_WIT_COLL_NORML_LN_CNTR_CNT
             ,TOT_WIT_COLL_NORML_LN_BAL
             ,F_WIT_COLL_NORML_LN_BAL
             ,M_WIT_COLL_NORML_LN_BAL
             ,TOT_WIT_COLL_NORML_LN_CNTR_AMT
             ,F_WIT_COLL_NORML_LN_CNTR_AMT
             ,M_WIT_COLL_NORML_LN_CNTR_AMT
             ,TOT_WO_COLL_NORML_LN_CNTR_CNT
             ,F_WO_COLL_NORML_LN_CNTR_CNT
             ,M_WO_COLL_NORML_LN_CNTR_CNT
             ,TOT_WO_COLL_NORML_LN_BAL
             ,F_WO_COLL_NORML_LN_BAL
             ,M_WO_COLL_NORML_LN_BAL
             ,TOT_WO_COLL_NORML_LN_CNTR_AMT
             ,F_WO_COLL_NORML_LN_CNTR_AMT
             ,M_WO_COLL_NORML_LN_CNTR_AMT
             ,TOT_OVD_LN_CUST_CNT
             ,F_OVD_LN_CUST_CNT
             ,M_OVD_LN_CUST_CNT
             ,TOT_NORML_LN_CUST_CNT
             ,F_NORML_LN_CUST_CNT
             ,M_NORML_LN_CUST_CNT
             ,TOT_WO_COLL_OVD_LN_CUST_CNT
             ,F_WO_COLL_OVD_LN_CUST_CNT
             ,M_WO_COLL_OVD_LN_CUST_CNT
             ,TOT_WO_COLL_NORML_LN_CUST_CNT
             ,F_WO_COLL_NORML_LN_CUST_CNT
             ,M_WO_COLL_NORML_LN_CUST_CNT
             ,TOT_WIT_COLL_OVD_LN_CUST_CNT
             ,F_WIT_COLL_OVD_LN_CUST_CNT
             ,M_WIT_COLL_OVD_LN_CUST_CNT
             ,TOT_WIT_COLL_NORML_LN_CUST_CNT
             ,F_WIT_COLL_NORML_LN_CUST_CNT
             ,M_WIT_COLL_NORML_LN_CUST_CNT
             ,TRGT_DATA_LST_MOD_TM
          )
          WITH
          TM21_DDLY_CUST_CR_TRANS_V AS
          (SELECT  BAS_DAY
                  ,PCF_ID
                  ,RPLC_DATA_YN
                  ,LN_CNTR_NUM_INFO
                  ,MAX(CUST_ID             ) AS CUST_ID
                  ,MAX(INIT_LN_TRM_CD      ) AS INIT_LN_TRM_CD
                  ,MAX(ES_CD               ) AS ES_CD
                  ,MAX(COLL_TYP_CD         ) AS COLL_TYP_CD
                  ,MAX(NEW_OVD_DBT_GRP_CD  ) AS NEW_OVD_DBT_GRP_CD
                  ,MAX(LN_CNTR_AMT         ) AS LN_CNTR_AMT
                  ,MAX(LN_BAL              ) AS LN_BAL
                  ,SUM(TRANS_AMT           ) AS TRANS_AMT
           FROM   TM21_DDLY_CUST_CR_TRANS_A
           WHERE  BAS_DAY BETWEEN loop_bas_day.BAS_YM||'01' AND loop_bas_day.BAS_YM||'31'
           AND    PCF_ID IN (SELECT DISTINCT PCF_ID
                             FROM   TBSM_INPT_RPT_SUBMIT_L
                             WHERE  BTCH_BAS_DAY = v_st_date_01
                             AND    INPT_RPT_ID IN ('G32_019_TTGS_01','G32_019_TTGS_02','G32_019_TTGS_03')
                            )
           AND    CUST_ID IS NOT NULL
           GROUP BY BAS_DAY
                   ,PCF_ID
                   ,RPLC_DATA_YN
                   ,LN_CNTR_NUM_INFO
          ),
          THIS_MM_NEW_LOAN AS
          (SELECT loop_bas_day.BAS_YM AS BAS_YM,
                  T1.PCF_ID,
                  COUNT(DISTINCT CASE WHEN T1.LN_TRANS_TYP_CD = '1' /* AND T1.TRANS_AMT > 0 */
                                           THEN T1.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END
                       ) AS TOT_NEW_ARISN_LN_CNTR_CNT,
                  COUNT(DISTINCT CASE WHEN T1.LN_TRANS_TYP_CD = '1' /* AND T1.TRANS_AMT > 0 */ AND T2.GEN_TYP_CD = 'F'
                                           THEN T1.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END
                       ) AS F_NEW_ARISN_LN_CNTR_CNT,
                  COUNT(DISTINCT CASE WHEN T1.LN_TRANS_TYP_CD = '1' /* AND T1.TRANS_AMT > 0 */ AND T2.GEN_TYP_CD = 'M'
                                           THEN T1.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END
                       ) AS M_NEW_ARISN_LN_CNTR_CNT,
                  SUM(CASE WHEN T1.LN_TRANS_TYP_CD = '1'
                                THEN T1.TRANS_AMT
                           ELSE 0
                           END
                     ) AS TOT_NEW_ARISN_LN_CNTR_AMT,
                  SUM(CASE WHEN T1.LN_TRANS_TYP_CD = '1' AND T2.GEN_TYP_CD = 'F'
                                THEN T1.TRANS_AMT
                           ELSE 0
                           END
                     ) AS F_NEW_ARISN_LN_CNTR_AMT,
                  SUM(CASE WHEN T1.LN_TRANS_TYP_CD = '1' AND T2.GEN_TYP_CD = 'M'
                                THEN T1.TRANS_AMT
                           ELSE 0
                           END
                     ) AS M_NEW_ARISN_LN_CNTR_AMT
           FROM   TM21_DDLY_CUST_CR_TRANS_A T1 INNER JOIN TM00_NEW_CUST_D T2
                                                       ON T2.PCF_ID   = T1.PCF_ID
                                                      AND T2.TX_CD_ID = T1.CUST_ID
           WHERE  T1.BAS_DAY BETWEEN loop_bas_day.BAS_YM||'01' AND loop_bas_day.BAS_YM||'31'
           AND    T1.PCF_ID IN (SELECT DISTINCT PCF_ID
                                FROM   TBSM_INPT_RPT_SUBMIT_L
                                WHERE  BTCH_BAS_DAY = v_st_date_01
                                AND    INPT_RPT_ID IN ('G32_019_TTGS_01','G32_019_TTGS_02','G32_019_TTGS_03')
                               )
           AND    T1.RPLC_DATA_YN = 'N'
           GROUP BY T1.PCF_ID
          ),
          LAST_3MM_NEW_LOAN AS
          (SELECT loop_bas_day.BAS_YM AS BAS_YM,
                  T1.PCF_ID,
                  SUM(CASE WHEN T1.LN_TRANS_TYP_CD = '1'
                                THEN T1.TRANS_AMT
                           ELSE 0
                           END
                     ) AS TOT_L3M_NEW_ARISN_LN_CNTR_AMT,
                  SUM(CASE WHEN T1.LN_TRANS_TYP_CD = '1' AND T2.GEN_TYP_CD = 'F'
                                THEN T1.TRANS_AMT
                           ELSE 0
                           END
                     ) AS F_L3M_NEW_ARISN_LN_CNTR_AMT,
                  SUM(CASE WHEN T1.LN_TRANS_TYP_CD = '1' AND T2.GEN_TYP_CD = 'M'
                                THEN T1.TRANS_AMT
                           ELSE 0
                           END
                     ) AS M_L3M_NEW_ARISN_LN_CNTR_AMT
           FROM   TM21_DDLY_CUST_CR_TRANS_A T1 INNER JOIN TM00_NEW_CUST_D T2
                                                       ON T2.PCF_ID   = T1.PCF_ID
                                                      AND T2.TX_CD_ID = T1.CUST_ID
           WHERE  T1.BAS_DAY BETWEEN TO_CHAR(ADD_MONTHS(TO_DATE(loop_bas_day.BAS_YM, 'YYYYMM'), -3), 'YYYYMM')||'01'
                                 AND loop_bas_day.BAS_YM||'31'
           AND    T1.PCF_ID IN (SELECT DISTINCT PCF_ID
                                FROM   TBSM_INPT_RPT_SUBMIT_L
                                WHERE  BTCH_BAS_DAY = v_st_date_01
                                AND    INPT_RPT_ID IN ('G32_019_TTGS_01','G32_019_TTGS_02','G32_019_TTGS_03')
                               )
           AND    T1.RPLC_DATA_YN = 'N'
           GROUP BY T1.PCF_ID
          ),
          TOT_LOAN AS
          (SELECT loop_bas_day.BAS_YM AS BAS_YM,
                  A.PCF_ID,

                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 THEN
                                                             CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO
                                                                  ELSE A.CUST_ID
                                                                  END
                                      ELSE NULL
                                      END) AS TOT_LN_CUST_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND C.GEN_TYP_CD = 'F' THEN
                                                                                    CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO
                                                                                         ELSE A.CUST_ID
                                                                                         END
                                      ELSE NULL
                                      END) AS F_LN_CUST_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND C.GEN_TYP_CD = 'M' THEN
                                                                                    CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO
                                                                                         ELSE A.CUST_ID
                                                                                         END
                                      ELSE NULL
                                      END) AS M_LN_CUST_CNT,

                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND
                                           A.NEW_OVD_DBT_GRP_CD >= '2' THEN
                                                                            CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO
                                                                                 ELSE A.CUST_ID
                                                                                 END
                                      ELSE NULL
                                      END) AS TOT_OVD_LN_CUST_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND
                                           A.NEW_OVD_DBT_GRP_CD >= '2' AND
                                           C.GEN_TYP_CD = 'F' THEN
                                                                   CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO
                                                                        ELSE A.CUST_ID
                                                                        END
                                      ELSE NULL
                                      END) AS F_OVD_LN_CUST_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND
                                           A.NEW_OVD_DBT_GRP_CD >= '2' AND
                                           C.GEN_TYP_CD = 'M' THEN
                                                                   CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO
                                                                        ELSE A.CUST_ID
                                                                        END
                                      ELSE NULL
                                      END) AS M_OVD_LN_CUST_CNT,

                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.COLL_TYP_CD = '00' AND
                                           A.NEW_OVD_DBT_GRP_CD >= '2' THEN
                                                                            CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO
                                                                                 ELSE A.CUST_ID
                                                                                 END
                                      ELSE NULL
                                      END) AS TOT_WO_COLL_OVD_LN_CUST_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.COLL_TYP_CD = '00' AND
                                           A.NEW_OVD_DBT_GRP_CD >= '2' AND
                                           C.GEN_TYP_CD = 'F' THEN
                                                                   CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO
                                                                        ELSE A.CUST_ID
                                                                        END
                                      ELSE NULL
                                      END) AS F_WO_COLL_OVD_LN_CUST_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.COLL_TYP_CD = '00' AND
                                           A.NEW_OVD_DBT_GRP_CD >= '2' AND
                                           C.GEN_TYP_CD = 'M' THEN
                                                                   CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO
                                                                        ELSE A.CUST_ID
                                                                        END
                                      ELSE NULL
                                      END) AS M_WO_COLL_OVD_LN_CUST_CNT,

                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.COLL_TYP_CD <> '00' AND
                                           A.NEW_OVD_DBT_GRP_CD >= '2' THEN
                                                                            CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO
                                                                                 ELSE A.CUST_ID
                                                                                 END
                                      ELSE NULL
                                      END) AS TOT_WIT_COLL_OVD_LN_CUST_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.COLL_TYP_CD <> '00' AND
                                           A.NEW_OVD_DBT_GRP_CD >= '2' AND
                                           C.GEN_TYP_CD = 'F' THEN
                                                                   CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO
                                                                        ELSE A.CUST_ID
                                                                        END
                                      ELSE NULL
                                      END) AS F_WIT_COLL_OVD_LN_CUST_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.COLL_TYP_CD <> '00' AND
                                           A.NEW_OVD_DBT_GRP_CD >= '2' AND
                                           C.GEN_TYP_CD = 'M' THEN
                                                                   CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO
                                                                        ELSE A.CUST_ID
                                                                        END
                                      ELSE NULL
                                      END) AS M_WIT_COLL_OVD_LN_CUST_CNT,

                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND
                                           (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR
                                            A.NEW_OVD_DBT_GRP_CD IS NULL) THEN
                                                                               CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO
                                                                                    ELSE A.CUST_ID
                                                                                    END
                                      ELSE NULL
                                      END) AS TOT_NORML_LN_CUST_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND
                                           (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR
                                            A.NEW_OVD_DBT_GRP_CD IS NULL) AND
                                           C.GEN_TYP_CD = 'F' THEN
                                                                   CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO
                                                                        ELSE A.CUST_ID
                                                                        END
                                      ELSE NULL
                                      END) AS F_NORML_LN_CUST_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND
                                           (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR
                                            A.NEW_OVD_DBT_GRP_CD IS NULL) AND
                                           C.GEN_TYP_CD = 'M' THEN
                                                                   CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO
                                                                        ELSE A.CUST_ID
                                                                        END
                                      ELSE NULL
                                      END) AS M_NORML_LN_CUST_CNT,

                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.COLL_TYP_CD = '00' AND
                                           (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR
                                            A.NEW_OVD_DBT_GRP_CD IS NULL) THEN
                                                                               CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO
                                                                                    ELSE A.CUST_ID
                                                                                    END
                                      ELSE NULL
                                      END) AS TOT_WO_COLL_NORML_LN_CUST_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.COLL_TYP_CD = '00' AND
                                           (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR
                                            A.NEW_OVD_DBT_GRP_CD IS NULL) AND
                                           C.GEN_TYP_CD = 'F' THEN
                                                                   CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO
                                                                        ELSE A.CUST_ID
                                                                        END
                                      ELSE NULL
                                      END) AS F_WO_COLL_NORML_LN_CUST_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.COLL_TYP_CD = '00' AND
                                           (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR
                                            A.NEW_OVD_DBT_GRP_CD IS NULL) AND
                                           C.GEN_TYP_CD = 'M' THEN
                                                                   CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO
                                                                        ELSE A.CUST_ID
                                                                        END
                                      ELSE NULL
                                      END) AS M_WO_COLL_NORML_LN_CUST_CNT,

                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.COLL_TYP_CD <> '00' AND
                                           (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR
                                            A.NEW_OVD_DBT_GRP_CD IS NULL) THEN
                                                                               CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO
                                                                                    ELSE A.CUST_ID
                                                                                    END
                                      ELSE NULL
                                      END) AS TOT_WIT_COLL_NORML_LN_CUST_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.COLL_TYP_CD <> '00' AND
                                           (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR
                                            A.NEW_OVD_DBT_GRP_CD IS NULL) AND
                                           C.GEN_TYP_CD = 'F' THEN
                                                                   CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO
                                                                        ELSE A.CUST_ID
                                                                        END
                                      ELSE NULL
                                      END) AS F_WIT_COLL_NORML_LN_CUST_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.COLL_TYP_CD <> '00' AND
                                           (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR
                                            A.NEW_OVD_DBT_GRP_CD IS NULL) AND
                                           C.GEN_TYP_CD = 'M' THEN
                                                                   CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO
                                                                        ELSE A.CUST_ID
                                                                        END
                                      ELSE NULL
                                      END) AS M_WIT_COLL_NORML_LN_CUST_CNT,

                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS TOT_LN_CNTR_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND C.GEN_TYP_CD = 'F' THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS F_LN_CNTR_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND C.GEN_TYP_CD = 'M' THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS M_LN_CNTR_CNT,

                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.NEW_OVD_DBT_GRP_CD >= '2' THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS TOT_OVD_LN_CNTR_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.NEW_OVD_DBT_GRP_CD >= '2' AND C.GEN_TYP_CD = 'F' THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS F_OVD_LN_CNTR_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.NEW_OVD_DBT_GRP_CD >= '2' AND C.GEN_TYP_CD = 'M' THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS M_OVD_LN_CNTR_CNT,

                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR A.NEW_OVD_DBT_GRP_CD IS NULL) THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS TOT_NORML_LN_CNTR_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR A.NEW_OVD_DBT_GRP_CD IS NULL) AND C.GEN_TYP_CD = 'F' THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS F_NORML_LN_CNTR_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR A.NEW_OVD_DBT_GRP_CD IS NULL) AND C.GEN_TYP_CD = 'M' THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS M_NORML_LN_CNTR_CNT,

                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.COLL_TYP_CD <> '00' THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS TOT_WIT_COLL_LN_CNTR_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.COLL_TYP_CD <> '00' AND C.GEN_TYP_CD = 'F' THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS F_WIT_COLL_LN_CNTR_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.COLL_TYP_CD <> '00' AND C.GEN_TYP_CD = 'M' THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS M_WIT_COLL_LN_CNTR_CNT,

                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.COLL_TYP_CD = '00' THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS TOT_WO_COLL_LN_CNTR_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.COLL_TYP_CD = '00' AND C.GEN_TYP_CD = 'F' THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS F_WO_COLL_LN_CNTR_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.COLL_TYP_CD = '00' AND C.GEN_TYP_CD = 'M' THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS M_WO_COLL_LN_CNTR_CNT,

                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.COLL_TYP_CD <> '00' AND A.NEW_OVD_DBT_GRP_CD >= '2' THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS TOT_WIT_COLL_OVD_LN_CNTR_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.COLL_TYP_CD <> '00' AND A.NEW_OVD_DBT_GRP_CD >= '2' AND C.GEN_TYP_CD = 'F' THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS F_WIT_COLL_OVD_LN_CNTR_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.COLL_TYP_CD <> '00' AND A.NEW_OVD_DBT_GRP_CD >= '2' AND C.GEN_TYP_CD = 'M' THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS M_WIT_COLL_OVD_LN_CNTR_CNT,

                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.COLL_TYP_CD = '00' AND A.NEW_OVD_DBT_GRP_CD >= '2' THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS TOT_WO_COLL_OVD_LN_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.COLL_TYP_CD = '00' AND A.NEW_OVD_DBT_GRP_CD >= '2' AND C.GEN_TYP_CD = 'F' THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS F_WO_COLL_OVD_LN_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.COLL_TYP_CD = '00' AND A.NEW_OVD_DBT_GRP_CD >= '2' AND C.GEN_TYP_CD = 'M' THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS M_WO_COLL_OVD_LN_CNT,

                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.COLL_TYP_CD <> '00' AND (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR A.NEW_OVD_DBT_GRP_CD IS NULL) THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS TOT_WIT_COLL_NORML_LN_CNTR_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.COLL_TYP_CD <> '00' AND (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR A.NEW_OVD_DBT_GRP_CD IS NULL) AND C.GEN_TYP_CD = 'F' THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS F_WIT_COLL_NORML_LN_CNTR_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.COLL_TYP_CD <> '00' AND (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR A.NEW_OVD_DBT_GRP_CD IS NULL) AND C.GEN_TYP_CD = 'M' THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS M_WIT_COLL_NORML_LN_CNTR_CNT,

                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.COLL_TYP_CD = '00' AND (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR A.NEW_OVD_DBT_GRP_CD IS NULL) THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS TOT_WO_COLL_NORML_LN_CNTR_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.COLL_TYP_CD = '00' AND (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR A.NEW_OVD_DBT_GRP_CD IS NULL) AND C.GEN_TYP_CD = 'F' THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS F_WO_COLL_NORML_LN_CNTR_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.COLL_TYP_CD = '00' AND (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR A.NEW_OVD_DBT_GRP_CD IS NULL) AND C.GEN_TYP_CD = 'M' THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS M_WO_COLL_NORML_LN_CNTR_CNT,

                  SUM(A.LN_BAL) AS TOT_LN_BAL,
                  SUM(CASE WHEN C.GEN_TYP_CD = 'F' THEN A.LN_BAL ELSE 0 END) AS F_LN_BAL,
                  SUM(CASE WHEN C.GEN_TYP_CD = 'M' THEN A.LN_BAL ELSE 0 END) AS M_LN_BAL,

                  SUM(CASE WHEN A.NEW_OVD_DBT_GRP_CD >= '2' THEN A.LN_BAL ELSE 0 END) AS TOT_OVD_LN_BAL,
                  SUM(CASE WHEN A.NEW_OVD_DBT_GRP_CD >= '2' AND C.GEN_TYP_CD = 'F' THEN A.LN_BAL ELSE 0 END) AS F_OVD_LN_BAL,
                  SUM(CASE WHEN A.NEW_OVD_DBT_GRP_CD >= '2' AND C.GEN_TYP_CD = 'M' THEN A.LN_BAL ELSE 0 END) AS M_OVD_LN_BAL,

                  SUM(CASE WHEN (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR A.NEW_OVD_DBT_GRP_CD IS NULL) THEN A.LN_BAL ELSE 0 END) AS TOT_NORML_LN_BAL,
                  SUM(CASE WHEN (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR A.NEW_OVD_DBT_GRP_CD IS NULL) AND C.GEN_TYP_CD = 'F' THEN A.LN_BAL ELSE 0 END) AS F_NORML_LN_BAL,
                  SUM(CASE WHEN (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR A.NEW_OVD_DBT_GRP_CD IS NULL) AND C.GEN_TYP_CD = 'M' THEN A.LN_BAL ELSE 0 END) AS M_NORML_LN_BAL,

                  SUM(CASE WHEN A.NEW_OVD_DBT_GRP_CD >= '2' AND A.COLL_TYP_CD <> '00' THEN A.LN_BAL ELSE 0 END) AS TOT_WIT_COLL_OVD_LN_BAL,
                  SUM(CASE WHEN A.NEW_OVD_DBT_GRP_CD >= '2' AND A.COLL_TYP_CD <> '00' AND C.GEN_TYP_CD = 'F' THEN A.LN_BAL ELSE 0 END) AS F_WIT_COLL_OVD_LN_BAL,
                  SUM(CASE WHEN A.NEW_OVD_DBT_GRP_CD >= '2' AND A.COLL_TYP_CD <> '00' AND C.GEN_TYP_CD = 'M' THEN A.LN_BAL ELSE 0 END) AS M_WIT_COLL_OVD_LN_BAL,

                  SUM(CASE WHEN A.NEW_OVD_DBT_GRP_CD >= '2' AND A.COLL_TYP_CD <> '00' THEN A.LN_CNTR_AMT ELSE 0 END) AS TOT_WIT_COLL_OVD_LN_CNTR_AMT,
                  SUM(CASE WHEN A.NEW_OVD_DBT_GRP_CD >= '2' AND A.COLL_TYP_CD <> '00' AND C.GEN_TYP_CD = 'F' THEN A.LN_CNTR_AMT ELSE 0 END) AS F_WIT_COLL_OVD_LN_CNTR_AMT,
                  SUM(CASE WHEN A.NEW_OVD_DBT_GRP_CD >= '2' AND A.COLL_TYP_CD <> '00' AND C.GEN_TYP_CD = 'M' THEN A.LN_CNTR_AMT ELSE 0 END) AS M_WIT_COLL_OVD_LN_CNTR_AMT,

                  SUM(CASE WHEN A.NEW_OVD_DBT_GRP_CD >= '2' AND A.COLL_TYP_CD = '00' THEN A.LN_BAL ELSE 0 END) AS TOT_WO_COLL_OVD_LN_BAL,
                  SUM(CASE WHEN A.NEW_OVD_DBT_GRP_CD >= '2' AND A.COLL_TYP_CD = '00' AND C.GEN_TYP_CD = 'F' THEN A.LN_BAL ELSE 0 END) AS F_WO_COLL_OVD_LN_BAL,
                  SUM(CASE WHEN A.NEW_OVD_DBT_GRP_CD >= '2' AND A.COLL_TYP_CD = '00' AND C.GEN_TYP_CD = 'M' THEN A.LN_BAL ELSE 0 END) AS M_WO_COLL_OVD_LN_BAL,

                  SUM(CASE WHEN A.NEW_OVD_DBT_GRP_CD >= '2' AND A.COLL_TYP_CD = '00' THEN A.LN_CNTR_AMT ELSE 0 END) AS TOT_WO_COLL_OVD_LN_CNTR_AMT,
                  SUM(CASE WHEN A.NEW_OVD_DBT_GRP_CD >= '2' AND A.COLL_TYP_CD = '00' AND C.GEN_TYP_CD = 'F' THEN A.LN_CNTR_AMT ELSE 0 END) AS F_WO_COLL_OVD_LN_CNTR_AMT,
                  SUM(CASE WHEN A.NEW_OVD_DBT_GRP_CD >= '2' AND A.COLL_TYP_CD = '00' AND C.GEN_TYP_CD = 'M' THEN A.LN_CNTR_AMT ELSE 0 END) AS M_WO_COLL_OVD_LN_CNTR_AMT,

                  SUM(CASE WHEN (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR A.NEW_OVD_DBT_GRP_CD IS NULL) AND A.COLL_TYP_CD <> '00' THEN A.LN_BAL ELSE 0 END) AS TOT_WIT_COLL_NORML_LN_BAL,
                  SUM(CASE WHEN (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR A.NEW_OVD_DBT_GRP_CD IS NULL) AND A.COLL_TYP_CD <> '00' AND C.GEN_TYP_CD = 'F' THEN A.LN_BAL ELSE 0 END) AS F_WIT_COLL_NORML_LN_BAL,
                  SUM(CASE WHEN (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR A.NEW_OVD_DBT_GRP_CD IS NULL) AND A.COLL_TYP_CD <> '00' AND C.GEN_TYP_CD = 'M' THEN A.LN_BAL ELSE 0 END) AS M_WIT_COLL_NORML_LN_BAL,

                  SUM(CASE WHEN (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR A.NEW_OVD_DBT_GRP_CD IS NULL) AND A.COLL_TYP_CD <> '00' THEN A.LN_CNTR_AMT ELSE 0 END) AS TOT_WIT_COLL_NORML_LN_CNTR_AMT,
                  SUM(CASE WHEN (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR A.NEW_OVD_DBT_GRP_CD IS NULL) AND A.COLL_TYP_CD <> '00' AND C.GEN_TYP_CD = 'F' THEN A.LN_CNTR_AMT ELSE 0 END) AS F_WIT_COLL_NORML_LN_CNTR_AMT,
                  SUM(CASE WHEN (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR A.NEW_OVD_DBT_GRP_CD IS NULL) AND A.COLL_TYP_CD <> '00' AND C.GEN_TYP_CD = 'M' THEN A.LN_CNTR_AMT ELSE 0 END) AS M_WIT_COLL_NORML_LN_CNTR_AMT,

                  SUM(CASE WHEN (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR A.NEW_OVD_DBT_GRP_CD IS NULL) AND A.COLL_TYP_CD = '00' THEN A.LN_BAL ELSE 0 END) AS TOT_WO_COLL_NORML_LN_BAL,
                  SUM(CASE WHEN (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR A.NEW_OVD_DBT_GRP_CD IS NULL) AND A.COLL_TYP_CD = '00' AND C.GEN_TYP_CD = 'F' THEN A.LN_BAL ELSE 0 END) AS F_WO_COLL_NORML_LN_BAL,
                  SUM(CASE WHEN (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR A.NEW_OVD_DBT_GRP_CD IS NULL) AND A.COLL_TYP_CD = '00' AND C.GEN_TYP_CD = 'M' THEN A.LN_BAL ELSE 0 END) AS M_WO_COLL_NORML_LN_BAL,

                  SUM(CASE WHEN (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR A.NEW_OVD_DBT_GRP_CD IS NULL) AND A.COLL_TYP_CD = '00' THEN A.LN_CNTR_AMT ELSE 0 END) AS TOT_WO_COLL_NORML_LN_CNTR_AMT,
                  SUM(CASE WHEN (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR A.NEW_OVD_DBT_GRP_CD IS NULL) AND A.COLL_TYP_CD = '00' AND C.GEN_TYP_CD = 'F' THEN A.LN_CNTR_AMT ELSE 0 END) AS F_WO_COLL_NORML_LN_CNTR_AMT,
                  SUM(CASE WHEN (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR A.NEW_OVD_DBT_GRP_CD IS NULL) AND A.COLL_TYP_CD = '00' AND C.GEN_TYP_CD = 'M' THEN A.LN_CNTR_AMT ELSE 0 END) AS M_WO_COLL_NORML_LN_CNTR_AMT,

                  SUM(CASE WHEN A.INIT_LN_TRM_CD IN ('001', '002', '003', '004', '005', '006') AND C.GEN_TYP_CD = 'F'
                                THEN A.LN_BAL
                           ELSE 0
                           END) AS F_INIT_TRM_LSTHN_6MM_LN_BAL,
                  SUM(CASE WHEN A.INIT_LN_TRM_CD IN ('001', '002', '003', '004', '005', '006') AND C.GEN_TYP_CD = 'M'
                                THEN A.LN_BAL
                           ELSE 0
                           END) AS M_INIT_TRM_LSTHN_6MM_LN_BAL,
                  SUM(CASE WHEN A.INIT_LN_TRM_CD IN ('007', '008', '009', '010', '011', '012') AND C.GEN_TYP_CD = 'F'
                                THEN A.LN_BAL
                           ELSE 0
                           END) AS F_INIT_TRM_LSTHN_12MM_LN_BAL,
                  SUM(CASE WHEN A.INIT_LN_TRM_CD IN ('007', '008', '009', '010', '011', '012') AND C.GEN_TYP_CD = 'M'
                                THEN A.LN_BAL
                           ELSE 0
                           END) AS M_INIT_TRM_LSTHN_12MM_LN_BAL,
                  SUM(CASE WHEN C.GEN_TYP_CD = 'F' AND (A.INIT_LN_TRM_CD = 'XXX' OR (A.INIT_LN_TRM_CD >= '013' AND A.INIT_LN_TRM_CD <> 'ZZZ'))
                                THEN A.LN_BAL
                           ELSE 0
                           END) AS F_INIT_TRM_OVER_12MM_LN_BAL,
                  SUM(CASE WHEN C.GEN_TYP_CD = 'M' AND (A.INIT_LN_TRM_CD = 'XXX' OR (A.INIT_LN_TRM_CD >= '013' AND A.INIT_LN_TRM_CD <> 'ZZZ'))
                                THEN A.LN_BAL
                           ELSE 0
                           END) AS M_INIT_TRM_OVER_12MM_LN_BAL,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.ES_CD = '0101' THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS TOT_AGRCTR_LN_CNTR_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.ES_CD = '0101' AND C.GEN_TYP_CD = 'F' THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS F_AGRCTR_LN_CNTR_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.ES_CD = '0101' AND C.GEN_TYP_CD = 'M' THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS M_AGRCTR_LN_CNTR_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.ES_CD <> '0101' THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS TOT_NON_AGRCTR_LN_CNTR_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.ES_CD <> '0101' AND C.GEN_TYP_CD = 'F' THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS F_NON_AGRCTR_LN_CNTR_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.ES_CD <> '0101' AND C.GEN_TYP_CD = 'M' THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS M_NON_AGRCTR_LN_CNTR_CNT
           FROM   TM21_DDLY_CUST_CR_TRANS_V A INNER JOIN (SELECT SUBSTR(BAS_DAY, 1, 6) AS BAS_YM, PCF_ID, MAX(BAS_DAY) AS BAS_DAY
                                                          FROM   TM21_DDLY_CUST_CR_TRANS_V
                                                          GROUP BY SUBSTR(BAS_DAY, 1, 6), PCF_ID
                                                         ) B
                                                      ON B.BAS_DAY = A.BAS_DAY
                                                     AND B.PCF_ID  = A.PCF_ID
                                              INNER JOIN TM00_NEW_CUST_D C
                                                      ON C.PCF_ID   = A.PCF_ID
                                                     AND C.TX_CD_ID = A.CUST_ID
           GROUP BY A.PCF_ID
          )
          SELECT T1.BAS_YM
                ,T1.PCF_ID
                ,T1.TOT_LN_CUST_CNT
                ,T1.F_LN_CUST_CNT
                ,T1.M_LN_CUST_CNT
                ,NULL AS TOT_ACT_LN_CUST_CNT
                ,NULL AS F_ACT_LN_CUST_CNT
                ,NULL AS M_ACT_LN_CUST_CNT
                ,T1.TOT_LN_CNTR_CNT
                ,T1.F_LN_CNTR_CNT
                ,T1.M_LN_CNTR_CNT
                ,T1.TOT_LN_BAL
                ,T1.F_LN_BAL
                ,T1.M_LN_BAL
                ,T1.F_INIT_TRM_LSTHN_6MM_LN_BAL
                ,T1.M_INIT_TRM_LSTHN_6MM_LN_BAL
                ,T1.F_INIT_TRM_LSTHN_12MM_LN_BAL
                ,T1.M_INIT_TRM_LSTHN_12MM_LN_BAL
                ,T1.F_INIT_TRM_OVER_12MM_LN_BAL
                ,T1.M_INIT_TRM_OVER_12MM_LN_BAL
                ,T1.TOT_AGRCTR_LN_CNTR_CNT
                ,T1.F_AGRCTR_LN_CNTR_CNT
                ,T1.M_AGRCTR_LN_CNTR_CNT
                ,T1.TOT_NON_AGRCTR_LN_CNTR_CNT
                ,T1.F_NON_AGRCTR_LN_CNTR_CNT
                ,T1.M_NON_AGRCTR_LN_CNTR_CNT
                ,T2.TOT_NEW_ARISN_LN_CNTR_CNT
                ,T2.F_NEW_ARISN_LN_CNTR_CNT
                ,T2.M_NEW_ARISN_LN_CNTR_CNT
                ,T2.TOT_NEW_ARISN_LN_CNTR_AMT
                ,T2.F_NEW_ARISN_LN_CNTR_AMT
                ,T2.M_NEW_ARISN_LN_CNTR_AMT
                ,T3.TOT_L3M_NEW_ARISN_LN_CNTR_AMT
                ,T3.F_L3M_NEW_ARISN_LN_CNTR_AMT
                ,T3.M_L3M_NEW_ARISN_LN_CNTR_AMT
                ,T1.TOT_OVD_LN_CNTR_CNT
                ,T1.F_OVD_LN_CNTR_CNT
                ,T1.M_OVD_LN_CNTR_CNT
                ,T1.TOT_OVD_LN_BAL
                ,T1.F_OVD_LN_BAL
                ,T1.M_OVD_LN_BAL
                ,T1.TOT_NORML_LN_CNTR_CNT
                ,T1.F_NORML_LN_CNTR_CNT
                ,T1.M_NORML_LN_CNTR_CNT
                ,T1.TOT_NORML_LN_BAL
                ,T1.F_NORML_LN_BAL
                ,T1.M_NORML_LN_BAL
                ,T1.TOT_WIT_COLL_LN_CNTR_CNT
                ,T1.F_WIT_COLL_LN_CNTR_CNT
                ,T1.M_WIT_COLL_LN_CNTR_CNT
                ,T1.TOT_WO_COLL_LN_CNTR_CNT
                ,T1.F_WO_COLL_LN_CNTR_CNT
                ,T1.M_WO_COLL_LN_CNTR_CNT
                ,T1.TOT_WIT_COLL_OVD_LN_CNTR_CNT
                ,T1.F_WIT_COLL_OVD_LN_CNTR_CNT
                ,T1.M_WIT_COLL_OVD_LN_CNTR_CNT
                ,T1.TOT_WIT_COLL_OVD_LN_BAL
                ,T1.F_WIT_COLL_OVD_LN_BAL
                ,T1.M_WIT_COLL_OVD_LN_BAL
                ,T1.TOT_WIT_COLL_OVD_LN_CNTR_AMT
                ,T1.F_WIT_COLL_OVD_LN_CNTR_AMT
                ,T1.M_WIT_COLL_OVD_LN_CNTR_AMT
                ,T1.TOT_WO_COLL_OVD_LN_CNT
                ,T1.F_WO_COLL_OVD_LN_CNT
                ,T1.M_WO_COLL_OVD_LN_CNT
                ,T1.TOT_WO_COLL_OVD_LN_BAL
                ,T1.F_WO_COLL_OVD_LN_BAL
                ,T1.M_WO_COLL_OVD_LN_BAL
                ,T1.TOT_WO_COLL_OVD_LN_CNTR_AMT
                ,T1.F_WO_COLL_OVD_LN_CNTR_AMT
                ,T1.M_WO_COLL_OVD_LN_CNTR_AMT
                ,T1.TOT_WIT_COLL_NORML_LN_CNTR_CNT
                ,T1.F_WIT_COLL_NORML_LN_CNTR_CNT
                ,T1.M_WIT_COLL_NORML_LN_CNTR_CNT
                ,T1.TOT_WIT_COLL_NORML_LN_BAL
                ,T1.F_WIT_COLL_NORML_LN_BAL
                ,T1.M_WIT_COLL_NORML_LN_BAL
                ,T1.TOT_WIT_COLL_NORML_LN_CNTR_AMT
                ,T1.F_WIT_COLL_NORML_LN_CNTR_AMT
                ,T1.M_WIT_COLL_NORML_LN_CNTR_AMT
                ,T1.TOT_WO_COLL_NORML_LN_CNTR_CNT
                ,T1.F_WO_COLL_NORML_LN_CNTR_CNT
                ,T1.M_WO_COLL_NORML_LN_CNTR_CNT
                ,T1.TOT_WO_COLL_NORML_LN_BAL
                ,T1.F_WO_COLL_NORML_LN_BAL
                ,T1.M_WO_COLL_NORML_LN_BAL
                ,T1.TOT_WO_COLL_NORML_LN_CNTR_AMT
                ,T1.F_WO_COLL_NORML_LN_CNTR_AMT
                ,T1.M_WO_COLL_NORML_LN_CNTR_AMT
                ,T1.TOT_OVD_LN_CUST_CNT
                ,T1.F_OVD_LN_CUST_CNT
                ,T1.M_OVD_LN_CUST_CNT
                ,T1.TOT_NORML_LN_CUST_CNT
                ,T1.F_NORML_LN_CUST_CNT
                ,T1.M_NORML_LN_CUST_CNT
                ,T1.TOT_WO_COLL_OVD_LN_CUST_CNT
                ,T1.F_WO_COLL_OVD_LN_CUST_CNT
                ,T1.M_WO_COLL_OVD_LN_CUST_CNT
                ,T1.TOT_WO_COLL_NORML_LN_CUST_CNT
                ,T1.F_WO_COLL_NORML_LN_CUST_CNT
                ,T1.M_WO_COLL_NORML_LN_CUST_CNT
                ,T1.TOT_WIT_COLL_OVD_LN_CUST_CNT
                ,T1.F_WIT_COLL_OVD_LN_CUST_CNT
                ,T1.M_WIT_COLL_OVD_LN_CUST_CNT
                ,T1.TOT_WIT_COLL_NORML_LN_CUST_CNT
                ,T1.F_WIT_COLL_NORML_LN_CUST_CNT
                ,T1.M_WIT_COLL_NORML_LN_CUST_CNT
                ,SYSTIMESTAMP
          FROM   TOT_LOAN T1 LEFT OUTER JOIN THIS_MM_NEW_LOAN T2
                                          ON T2.BAS_YM = T1.BAS_YM
                                         AND T2.PCF_ID = T1.PCF_ID
                             LEFT OUTER JOIN LAST_3MM_NEW_LOAN T3
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

          COMMIT ;

          EXCEPTION
          WHEN OTHERS THEN
               begin
                     v_step_code    := '021' ;
                     v_step_desc    := v_wk_date || v_seq || ' : Deleting or Inserting Data Error with BAS_YM = ' ;
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
          --  1.1 Deleting Data by TM27_MMLY_GEN_CR_KEY_IDC_A
          ----------------------------------------------------------------------------
          DELETE
          FROM   TM27_MMLY_GEN_CR_KEY_IDC_A
          WHERE  BAS_YM = loop_bas_day.BAS_YM;
          ----------------------------------------------------------------------------
          --  Transaction Log
          ----------------------------------------------------------------------------
          v_step_code    := '030' ;
          v_step_desc    := v_wk_date || v_seq || ' ' || loop_bas_day.BAS_YM ||' : Deleting Data Result';
          v_time         := SYSDATE ;

          P1_BSM_PROG_EXEC_LOG(v_program_id, v_program_type_name, v_step_code, v_step_desc, v_time, sql%rowcount, NULL, NULL) ;

          COMMIT ;
          ----------------------------------------------------------------------------
          --  1.1 Inserting Data by TM27_MMLY_GEN_CR_KEY_IDC_A
          ----------------------------------------------------------------------------
          INSERT INTO TM27_MMLY_GEN_CR_KEY_IDC_A
          (   BAS_YM
             ,PCF_ID
             ,TOT_LN_CUST_CNT
             ,F_LN_CUST_CNT
             ,M_LN_CUST_CNT
             ,TOT_ACT_LN_CUST_CNT
             ,F_ACT_LN_CUST_CNT
             ,M_ACT_LN_CUST_CNT
             ,TOT_LN_CNTR_CNT
             ,F_LN_CNTR_CNT
             ,M_LN_CNTR_CNT
             ,TOT_LN_BAL
             ,F_LN_BAL
             ,M_LN_BAL
             ,F_INIT_TRM_LSTHN_6MM_LN_BAL
             ,M_INIT_TRM_LSTHN_6MM_LN_BAL
             ,F_INIT_TRM_LSTHN_12MM_LN_BAL
             ,M_INIT_TRM_LSTHN_12MM_LN_BAL
             ,F_INIT_TRM_OVER_12MM_LN_BAL
             ,M_INIT_TRM_OVER_12MM_LN_BAL
             ,TOT_AGRCTR_LN_CNTR_CNT
             ,F_AGRCTR_LN_CNTR_CNT
             ,M_AGRCTR_LN_CNTR_CNT
             ,TOT_NON_AGRCTR_LN_CNTR_CNT
             ,F_NON_AGRCTR_LN_CNTR_CNT
             ,M_NON_AGRCTR_LN_CNTR_CNT
             ,TOT_NEW_ARISN_LN_CNTR_CNT
             ,F_NEW_ARISN_LN_CNTR_CNT
             ,M_NEW_ARISN_LN_CNTR_CNT
             ,TOT_NEW_ARISN_LN_CNTR_AMT
             ,F_NEW_ARISN_LN_CNTR_AMT
             ,M_NEW_ARISN_LN_CNTR_AMT
             ,TOT_L3M_NEW_ARISN_LN_CNTR_AMT
             ,F_L3M_NEW_ARISN_LN_CNTR_AMT
             ,M_L3M_NEW_ARISN_LN_CNTR_AMT
             ,TOT_OVD_LN_CNTR_CNT
             ,F_OVD_LN_CNTR_CNT
             ,M_OVD_LN_CNTR_CNT
             ,TOT_OVD_LN_BAL
             ,F_OVD_LN_BAL
             ,M_OVD_LN_BAL
             ,TOT_NORML_LN_CNTR_CNT
             ,F_NORML_LN_CNTR_CNT
             ,M_NORML_LN_CNTR_CNT
             ,TOT_NORML_LN_BAL
             ,F_NORML_LN_BAL
             ,M_NORML_LN_BAL
             ,TOT_WIT_COLL_LN_CNTR_CNT
             ,F_WIT_COLL_LN_CNTR_CNT
             ,M_WIT_COLL_LN_CNTR_CNT
             ,TOT_WO_COLL_LN_CNTR_CNT
             ,F_WO_COLL_LN_CNTR_CNT
             ,M_WO_COLL_LN_CNTR_CNT
             ,TOT_WIT_COLL_OVD_LN_CNTR_CNT
             ,F_WIT_COLL_OVD_LN_CNTR_CNT
             ,M_WIT_COLL_OVD_LN_CNTR_CNT
             ,TOT_WIT_COLL_OVD_LN_BAL
             ,F_WIT_COLL_OVD_LN_BAL
             ,M_WIT_COLL_OVD_LN_BAL
             ,TOT_WIT_COLL_OVD_LN_CNTR_AMT
             ,F_WIT_COLL_OVD_LN_CNTR_AMT
             ,M_WIT_COLL_OVD_LN_CNTR_AMT
             ,TOT_WO_COLL_OVD_LN_CNT
             ,F_WO_COLL_OVD_LN_CNT
             ,M_WO_COLL_OVD_LN_CNT
             ,TOT_WO_COLL_OVD_LN_BAL
             ,F_WO_COLL_OVD_LN_BAL
             ,M_WO_COLL_OVD_LN_BAL
             ,TOT_WO_COLL_OVD_LN_CNTR_AMT
             ,F_WO_COLL_OVD_LN_CNTR_AMT
             ,M_WO_COLL_OVD_LN_CNTR_AMT
             ,TOT_WIT_COLL_NORML_LN_CNTR_CNT
             ,F_WIT_COLL_NORML_LN_CNTR_CNT
             ,M_WIT_COLL_NORML_LN_CNTR_CNT
             ,TOT_WIT_COLL_NORML_LN_BAL
             ,F_WIT_COLL_NORML_LN_BAL
             ,M_WIT_COLL_NORML_LN_BAL
             ,TOT_WIT_COLL_NORML_LN_CNTR_AMT
             ,F_WIT_COLL_NORML_LN_CNTR_AMT
             ,M_WIT_COLL_NORML_LN_CNTR_AMT
             ,TOT_WO_COLL_NORML_LN_CNTR_CNT
             ,F_WO_COLL_NORML_LN_CNTR_CNT
             ,M_WO_COLL_NORML_LN_CNTR_CNT
             ,TOT_WO_COLL_NORML_LN_BAL
             ,F_WO_COLL_NORML_LN_BAL
             ,M_WO_COLL_NORML_LN_BAL
             ,TOT_WO_COLL_NORML_LN_CNTR_AMT
             ,F_WO_COLL_NORML_LN_CNTR_AMT
             ,M_WO_COLL_NORML_LN_CNTR_AMT
             ,TOT_OVD_LN_CUST_CNT
             ,F_OVD_LN_CUST_CNT
             ,M_OVD_LN_CUST_CNT
             ,TOT_NORML_LN_CUST_CNT
             ,F_NORML_LN_CUST_CNT
             ,M_NORML_LN_CUST_CNT
             ,TOT_WO_COLL_OVD_LN_CUST_CNT
             ,F_WO_COLL_OVD_LN_CUST_CNT
             ,M_WO_COLL_OVD_LN_CUST_CNT
             ,TOT_WO_COLL_NORML_LN_CUST_CNT
             ,F_WO_COLL_NORML_LN_CUST_CNT
             ,M_WO_COLL_NORML_LN_CUST_CNT
             ,TOT_WIT_COLL_OVD_LN_CUST_CNT
             ,F_WIT_COLL_OVD_LN_CUST_CNT
             ,M_WIT_COLL_OVD_LN_CUST_CNT
             ,TOT_WIT_COLL_NORML_LN_CUST_CNT
             ,F_WIT_COLL_NORML_LN_CUST_CNT
             ,M_WIT_COLL_NORML_LN_CUST_CNT
             ,TRGT_DATA_LST_MOD_TM
          )
          WITH
          TM21_DDLY_CUST_CR_TRANS_V AS
          (SELECT  BAS_DAY
                  ,PCF_ID
                  ,RPLC_DATA_YN
                  ,LN_CNTR_NUM_INFO
                  ,MAX(CUST_ID             ) AS CUST_ID
                  ,MAX(INIT_LN_TRM_CD      ) AS INIT_LN_TRM_CD
                  ,MAX(ES_CD               ) AS ES_CD
                  ,MAX(COLL_TYP_CD         ) AS COLL_TYP_CD
                  ,MAX(NEW_OVD_DBT_GRP_CD  ) AS NEW_OVD_DBT_GRP_CD
                  ,MAX(LN_CNTR_AMT         ) AS LN_CNTR_AMT
                  ,MAX(LN_BAL              ) AS LN_BAL
                  ,SUM(TRANS_AMT           ) AS TRANS_AMT
           FROM   TM21_DDLY_CUST_CR_TRANS_A
           WHERE  BAS_DAY BETWEEN loop_bas_day.BAS_YM||'01' AND loop_bas_day.BAS_YM||'31'
           AND    CUST_ID IS NOT NULL
           GROUP BY BAS_DAY
                   ,PCF_ID
                   ,RPLC_DATA_YN
                   ,LN_CNTR_NUM_INFO
          ),
          THIS_MM_NEW_LOAN AS
          (SELECT loop_bas_day.BAS_YM AS BAS_YM,
                  T1.PCF_ID,
                  COUNT(DISTINCT CASE WHEN T1.LN_TRANS_TYP_CD = '1' /* AND T1.TRANS_AMT > 0 */
                                           THEN T1.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END
                       ) AS TOT_NEW_ARISN_LN_CNTR_CNT,
                  COUNT(DISTINCT CASE WHEN T1.LN_TRANS_TYP_CD = '1' /* AND T1.TRANS_AMT > 0 */ AND T2.GEN_TYP_CD = 'F'
                                           THEN T1.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END
                       ) AS F_NEW_ARISN_LN_CNTR_CNT,
                  COUNT(DISTINCT CASE WHEN T1.LN_TRANS_TYP_CD = '1' /* AND T1.TRANS_AMT > 0 */ AND T2.GEN_TYP_CD = 'M'
                                           THEN T1.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END
                       ) AS M_NEW_ARISN_LN_CNTR_CNT,
                  SUM(CASE WHEN T1.LN_TRANS_TYP_CD = '1'
                                THEN T1.TRANS_AMT
                           ELSE 0
                           END
                     ) AS TOT_NEW_ARISN_LN_CNTR_AMT,
                  SUM(CASE WHEN T1.LN_TRANS_TYP_CD = '1' AND T2.GEN_TYP_CD = 'F'
                                THEN T1.TRANS_AMT
                           ELSE 0
                           END
                     ) AS F_NEW_ARISN_LN_CNTR_AMT,
                  SUM(CASE WHEN T1.LN_TRANS_TYP_CD = '1' AND T2.GEN_TYP_CD = 'M'
                                THEN T1.TRANS_AMT
                           ELSE 0
                           END
                     ) AS M_NEW_ARISN_LN_CNTR_AMT
           FROM   TM21_DDLY_CUST_CR_TRANS_A T1 INNER JOIN TM00_NEW_CUST_D T2
                                                       ON T2.PCF_ID   = T1.PCF_ID
                                                      AND T2.TX_CD_ID = T1.CUST_ID
           WHERE  T1.BAS_DAY BETWEEN loop_bas_day.BAS_YM||'01' AND loop_bas_day.BAS_YM||'31'
           AND    T1.RPLC_DATA_YN = 'N'
           GROUP BY T1.PCF_ID
          ),
          LAST_3MM_NEW_LOAN AS
          (SELECT loop_bas_day.BAS_YM AS BAS_YM,
                  T1.PCF_ID,
                  SUM(CASE WHEN T1.LN_TRANS_TYP_CD = '1'
                                THEN T1.TRANS_AMT
                           ELSE 0
                           END
                     ) AS TOT_L3M_NEW_ARISN_LN_CNTR_AMT,
                  SUM(CASE WHEN T1.LN_TRANS_TYP_CD = '1' AND T2.GEN_TYP_CD = 'F'
                                THEN T1.TRANS_AMT
                           ELSE 0
                           END
                     ) AS F_L3M_NEW_ARISN_LN_CNTR_AMT,
                  SUM(CASE WHEN T1.LN_TRANS_TYP_CD = '1' AND T2.GEN_TYP_CD = 'M'
                                THEN T1.TRANS_AMT
                           ELSE 0
                           END
                     ) AS M_L3M_NEW_ARISN_LN_CNTR_AMT
           FROM   TM21_DDLY_CUST_CR_TRANS_A T1 INNER JOIN TM00_NEW_CUST_D T2
                                                       ON T2.PCF_ID   = T1.PCF_ID
                                                      AND T2.TX_CD_ID = T1.CUST_ID
           WHERE  T1.BAS_DAY BETWEEN TO_CHAR(ADD_MONTHS(TO_DATE(loop_bas_day.BAS_YM, 'YYYYMM'), -3), 'YYYYMM')||'01'
                                 AND loop_bas_day.BAS_YM||'31'
           AND    T1.RPLC_DATA_YN = 'N'
           GROUP BY T1.PCF_ID
          ),
          TOT_LOAN AS
          (SELECT loop_bas_day.BAS_YM AS BAS_YM,
                  A.PCF_ID,

                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 THEN
                                                             CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO
                                                                  ELSE A.CUST_ID
                                                                  END
                                      ELSE NULL
                                      END) AS TOT_LN_CUST_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND C.GEN_TYP_CD = 'F' THEN
                                                                                    CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO
                                                                                         ELSE A.CUST_ID
                                                                                         END
                                      ELSE NULL
                                      END) AS F_LN_CUST_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND C.GEN_TYP_CD = 'M' THEN
                                                                                    CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO
                                                                                         ELSE A.CUST_ID
                                                                                         END
                                      ELSE NULL
                                      END) AS M_LN_CUST_CNT,

                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND
                                           A.NEW_OVD_DBT_GRP_CD >= '2' THEN
                                                                            CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO
                                                                                 ELSE A.CUST_ID
                                                                                 END
                                      ELSE NULL
                                      END) AS TOT_OVD_LN_CUST_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND
                                           A.NEW_OVD_DBT_GRP_CD >= '2' AND
                                           C.GEN_TYP_CD = 'F' THEN
                                                                   CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO
                                                                        ELSE A.CUST_ID
                                                                        END
                                      ELSE NULL
                                      END) AS F_OVD_LN_CUST_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND
                                           A.NEW_OVD_DBT_GRP_CD >= '2' AND
                                           C.GEN_TYP_CD = 'M' THEN
                                                                   CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO
                                                                        ELSE A.CUST_ID
                                                                        END
                                      ELSE NULL
                                      END) AS M_OVD_LN_CUST_CNT,

                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.COLL_TYP_CD = '00' AND
                                           A.NEW_OVD_DBT_GRP_CD >= '2' THEN
                                                                            CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO
                                                                                 ELSE A.CUST_ID
                                                                                 END
                                      ELSE NULL
                                      END) AS TOT_WO_COLL_OVD_LN_CUST_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.COLL_TYP_CD = '00' AND
                                           A.NEW_OVD_DBT_GRP_CD >= '2' AND
                                           C.GEN_TYP_CD = 'F' THEN
                                                                   CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO
                                                                        ELSE A.CUST_ID
                                                                        END
                                      ELSE NULL
                                      END) AS F_WO_COLL_OVD_LN_CUST_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.COLL_TYP_CD = '00' AND
                                           A.NEW_OVD_DBT_GRP_CD >= '2' AND
                                           C.GEN_TYP_CD = 'M' THEN
                                                                   CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO
                                                                        ELSE A.CUST_ID
                                                                        END
                                      ELSE NULL
                                      END) AS M_WO_COLL_OVD_LN_CUST_CNT,

                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.COLL_TYP_CD <> '00' AND
                                           A.NEW_OVD_DBT_GRP_CD >= '2' THEN
                                                                            CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO
                                                                                 ELSE A.CUST_ID
                                                                                 END
                                      ELSE NULL
                                      END) AS TOT_WIT_COLL_OVD_LN_CUST_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.COLL_TYP_CD <> '00' AND
                                           A.NEW_OVD_DBT_GRP_CD >= '2' AND
                                           C.GEN_TYP_CD = 'F' THEN
                                                                   CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO
                                                                        ELSE A.CUST_ID
                                                                        END
                                      ELSE NULL
                                      END) AS F_WIT_COLL_OVD_LN_CUST_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.COLL_TYP_CD <> '00' AND
                                           A.NEW_OVD_DBT_GRP_CD >= '2' AND
                                           C.GEN_TYP_CD = 'M' THEN
                                                                   CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO
                                                                        ELSE A.CUST_ID
                                                                        END
                                      ELSE NULL
                                      END) AS M_WIT_COLL_OVD_LN_CUST_CNT,

                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND
                                           (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR
                                            A.NEW_OVD_DBT_GRP_CD IS NULL) THEN
                                                                               CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO
                                                                                    ELSE A.CUST_ID
                                                                                    END
                                      ELSE NULL
                                      END) AS TOT_NORML_LN_CUST_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND
                                           (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR
                                            A.NEW_OVD_DBT_GRP_CD IS NULL) AND
                                           C.GEN_TYP_CD = 'F' THEN
                                                                   CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO
                                                                        ELSE A.CUST_ID
                                                                        END
                                      ELSE NULL
                                      END) AS F_NORML_LN_CUST_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND
                                           (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR
                                            A.NEW_OVD_DBT_GRP_CD IS NULL) AND
                                           C.GEN_TYP_CD = 'M' THEN
                                                                   CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO
                                                                        ELSE A.CUST_ID
                                                                        END
                                      ELSE NULL
                                      END) AS M_NORML_LN_CUST_CNT,

                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.COLL_TYP_CD = '00' AND
                                           (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR
                                            A.NEW_OVD_DBT_GRP_CD IS NULL) THEN
                                                                               CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO
                                                                                    ELSE A.CUST_ID
                                                                                    END
                                      ELSE NULL
                                      END) AS TOT_WO_COLL_NORML_LN_CUST_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.COLL_TYP_CD = '00' AND
                                           (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR
                                            A.NEW_OVD_DBT_GRP_CD IS NULL) AND
                                           C.GEN_TYP_CD = 'F' THEN
                                                                   CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO
                                                                        ELSE A.CUST_ID
                                                                        END
                                      ELSE NULL
                                      END) AS F_WO_COLL_NORML_LN_CUST_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.COLL_TYP_CD = '00' AND
                                           (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR
                                            A.NEW_OVD_DBT_GRP_CD IS NULL) AND
                                           C.GEN_TYP_CD = 'M' THEN
                                                                   CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO
                                                                        ELSE A.CUST_ID
                                                                        END
                                      ELSE NULL
                                      END) AS M_WO_COLL_NORML_LN_CUST_CNT,

                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.COLL_TYP_CD <> '00' AND
                                           (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR
                                            A.NEW_OVD_DBT_GRP_CD IS NULL) THEN
                                                                               CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO
                                                                                    ELSE A.CUST_ID
                                                                                    END
                                      ELSE NULL
                                      END) AS TOT_WIT_COLL_NORML_LN_CUST_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.COLL_TYP_CD <> '00' AND
                                           (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR
                                            A.NEW_OVD_DBT_GRP_CD IS NULL) AND
                                           C.GEN_TYP_CD = 'F' THEN
                                                                   CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO
                                                                        ELSE A.CUST_ID
                                                                        END
                                      ELSE NULL
                                      END) AS F_WIT_COLL_NORML_LN_CUST_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.COLL_TYP_CD <> '00' AND
                                           (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR
                                            A.NEW_OVD_DBT_GRP_CD IS NULL) AND
                                           C.GEN_TYP_CD = 'M' THEN
                                                                   CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO
                                                                        ELSE A.CUST_ID
                                                                        END
                                      ELSE NULL
                                      END) AS M_WIT_COLL_NORML_LN_CUST_CNT,

                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS TOT_LN_CNTR_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND C.GEN_TYP_CD = 'F' THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS F_LN_CNTR_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND C.GEN_TYP_CD = 'M' THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS M_LN_CNTR_CNT,

                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.NEW_OVD_DBT_GRP_CD >= '2' THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS TOT_OVD_LN_CNTR_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.NEW_OVD_DBT_GRP_CD >= '2' AND C.GEN_TYP_CD = 'F' THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS F_OVD_LN_CNTR_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.NEW_OVD_DBT_GRP_CD >= '2' AND C.GEN_TYP_CD = 'M' THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS M_OVD_LN_CNTR_CNT,

                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR A.NEW_OVD_DBT_GRP_CD IS NULL) THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS TOT_NORML_LN_CNTR_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR A.NEW_OVD_DBT_GRP_CD IS NULL) AND C.GEN_TYP_CD = 'F' THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS F_NORML_LN_CNTR_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR A.NEW_OVD_DBT_GRP_CD IS NULL) AND C.GEN_TYP_CD = 'M' THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS M_NORML_LN_CNTR_CNT,

                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.COLL_TYP_CD <> '00' THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS TOT_WIT_COLL_LN_CNTR_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.COLL_TYP_CD <> '00' AND C.GEN_TYP_CD = 'F' THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS F_WIT_COLL_LN_CNTR_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.COLL_TYP_CD <> '00' AND C.GEN_TYP_CD = 'M' THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS M_WIT_COLL_LN_CNTR_CNT,

                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.COLL_TYP_CD = '00' THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS TOT_WO_COLL_LN_CNTR_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.COLL_TYP_CD = '00' AND C.GEN_TYP_CD = 'F' THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS F_WO_COLL_LN_CNTR_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.COLL_TYP_CD = '00' AND C.GEN_TYP_CD = 'M' THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS M_WO_COLL_LN_CNTR_CNT,

                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.COLL_TYP_CD <> '00' AND A.NEW_OVD_DBT_GRP_CD >= '2' THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS TOT_WIT_COLL_OVD_LN_CNTR_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.COLL_TYP_CD <> '00' AND A.NEW_OVD_DBT_GRP_CD >= '2' AND C.GEN_TYP_CD = 'F' THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS F_WIT_COLL_OVD_LN_CNTR_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.COLL_TYP_CD <> '00' AND A.NEW_OVD_DBT_GRP_CD >= '2' AND C.GEN_TYP_CD = 'M' THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS M_WIT_COLL_OVD_LN_CNTR_CNT,

                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.COLL_TYP_CD = '00' AND A.NEW_OVD_DBT_GRP_CD >= '2' THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS TOT_WO_COLL_OVD_LN_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.COLL_TYP_CD = '00' AND A.NEW_OVD_DBT_GRP_CD >= '2' AND C.GEN_TYP_CD = 'F' THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS F_WO_COLL_OVD_LN_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.COLL_TYP_CD = '00' AND A.NEW_OVD_DBT_GRP_CD >= '2' AND C.GEN_TYP_CD = 'M' THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS M_WO_COLL_OVD_LN_CNT,

                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.COLL_TYP_CD <> '00' AND (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR A.NEW_OVD_DBT_GRP_CD IS NULL) THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS TOT_WIT_COLL_NORML_LN_CNTR_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.COLL_TYP_CD <> '00' AND (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR A.NEW_OVD_DBT_GRP_CD IS NULL) AND C.GEN_TYP_CD = 'F' THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS F_WIT_COLL_NORML_LN_CNTR_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.COLL_TYP_CD <> '00' AND (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR A.NEW_OVD_DBT_GRP_CD IS NULL) AND C.GEN_TYP_CD = 'M' THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS M_WIT_COLL_NORML_LN_CNTR_CNT,

                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.COLL_TYP_CD = '00' AND (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR A.NEW_OVD_DBT_GRP_CD IS NULL) THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS TOT_WO_COLL_NORML_LN_CNTR_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.COLL_TYP_CD = '00' AND (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR A.NEW_OVD_DBT_GRP_CD IS NULL) AND C.GEN_TYP_CD = 'F' THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS F_WO_COLL_NORML_LN_CNTR_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.COLL_TYP_CD = '00' AND (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR A.NEW_OVD_DBT_GRP_CD IS NULL) AND C.GEN_TYP_CD = 'M' THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS M_WO_COLL_NORML_LN_CNTR_CNT,

                  SUM(A.LN_BAL) AS TOT_LN_BAL,
                  SUM(CASE WHEN C.GEN_TYP_CD = 'F' THEN A.LN_BAL ELSE 0 END) AS F_LN_BAL,
                  SUM(CASE WHEN C.GEN_TYP_CD = 'M' THEN A.LN_BAL ELSE 0 END) AS M_LN_BAL,

                  SUM(CASE WHEN A.NEW_OVD_DBT_GRP_CD >= '2' THEN A.LN_BAL ELSE 0 END) AS TOT_OVD_LN_BAL,
                  SUM(CASE WHEN A.NEW_OVD_DBT_GRP_CD >= '2' AND C.GEN_TYP_CD = 'F' THEN A.LN_BAL ELSE 0 END) AS F_OVD_LN_BAL,
                  SUM(CASE WHEN A.NEW_OVD_DBT_GRP_CD >= '2' AND C.GEN_TYP_CD = 'M' THEN A.LN_BAL ELSE 0 END) AS M_OVD_LN_BAL,

                  SUM(CASE WHEN (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR A.NEW_OVD_DBT_GRP_CD IS NULL) THEN A.LN_BAL ELSE 0 END) AS TOT_NORML_LN_BAL,
                  SUM(CASE WHEN (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR A.NEW_OVD_DBT_GRP_CD IS NULL) AND C.GEN_TYP_CD = 'F' THEN A.LN_BAL ELSE 0 END) AS F_NORML_LN_BAL,
                  SUM(CASE WHEN (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR A.NEW_OVD_DBT_GRP_CD IS NULL) AND C.GEN_TYP_CD = 'M' THEN A.LN_BAL ELSE 0 END) AS M_NORML_LN_BAL,

                  SUM(CASE WHEN A.NEW_OVD_DBT_GRP_CD >= '2' AND A.COLL_TYP_CD <> '00' THEN A.LN_BAL ELSE 0 END) AS TOT_WIT_COLL_OVD_LN_BAL,
                  SUM(CASE WHEN A.NEW_OVD_DBT_GRP_CD >= '2' AND A.COLL_TYP_CD <> '00' AND C.GEN_TYP_CD = 'F' THEN A.LN_BAL ELSE 0 END) AS F_WIT_COLL_OVD_LN_BAL,
                  SUM(CASE WHEN A.NEW_OVD_DBT_GRP_CD >= '2' AND A.COLL_TYP_CD <> '00' AND C.GEN_TYP_CD = 'M' THEN A.LN_BAL ELSE 0 END) AS M_WIT_COLL_OVD_LN_BAL,

                  SUM(CASE WHEN A.NEW_OVD_DBT_GRP_CD >= '2' AND A.COLL_TYP_CD <> '00' THEN A.LN_CNTR_AMT ELSE 0 END) AS TOT_WIT_COLL_OVD_LN_CNTR_AMT,
                  SUM(CASE WHEN A.NEW_OVD_DBT_GRP_CD >= '2' AND A.COLL_TYP_CD <> '00' AND C.GEN_TYP_CD = 'F' THEN A.LN_CNTR_AMT ELSE 0 END) AS F_WIT_COLL_OVD_LN_CNTR_AMT,
                  SUM(CASE WHEN A.NEW_OVD_DBT_GRP_CD >= '2' AND A.COLL_TYP_CD <> '00' AND C.GEN_TYP_CD = 'M' THEN A.LN_CNTR_AMT ELSE 0 END) AS M_WIT_COLL_OVD_LN_CNTR_AMT,

                  SUM(CASE WHEN A.NEW_OVD_DBT_GRP_CD >= '2' AND A.COLL_TYP_CD = '00' THEN A.LN_BAL ELSE 0 END) AS TOT_WO_COLL_OVD_LN_BAL,
                  SUM(CASE WHEN A.NEW_OVD_DBT_GRP_CD >= '2' AND A.COLL_TYP_CD = '00' AND C.GEN_TYP_CD = 'F' THEN A.LN_BAL ELSE 0 END) AS F_WO_COLL_OVD_LN_BAL,
                  SUM(CASE WHEN A.NEW_OVD_DBT_GRP_CD >= '2' AND A.COLL_TYP_CD = '00' AND C.GEN_TYP_CD = 'M' THEN A.LN_BAL ELSE 0 END) AS M_WO_COLL_OVD_LN_BAL,

                  SUM(CASE WHEN A.NEW_OVD_DBT_GRP_CD >= '2' AND A.COLL_TYP_CD = '00' THEN A.LN_CNTR_AMT ELSE 0 END) AS TOT_WO_COLL_OVD_LN_CNTR_AMT,
                  SUM(CASE WHEN A.NEW_OVD_DBT_GRP_CD >= '2' AND A.COLL_TYP_CD = '00' AND C.GEN_TYP_CD = 'F' THEN A.LN_CNTR_AMT ELSE 0 END) AS F_WO_COLL_OVD_LN_CNTR_AMT,
                  SUM(CASE WHEN A.NEW_OVD_DBT_GRP_CD >= '2' AND A.COLL_TYP_CD = '00' AND C.GEN_TYP_CD = 'M' THEN A.LN_CNTR_AMT ELSE 0 END) AS M_WO_COLL_OVD_LN_CNTR_AMT,

                  SUM(CASE WHEN (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR A.NEW_OVD_DBT_GRP_CD IS NULL) AND A.COLL_TYP_CD <> '00' THEN A.LN_BAL ELSE 0 END) AS TOT_WIT_COLL_NORML_LN_BAL,
                  SUM(CASE WHEN (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR A.NEW_OVD_DBT_GRP_CD IS NULL) AND A.COLL_TYP_CD <> '00' AND C.GEN_TYP_CD = 'F' THEN A.LN_BAL ELSE 0 END) AS F_WIT_COLL_NORML_LN_BAL,
                  SUM(CASE WHEN (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR A.NEW_OVD_DBT_GRP_CD IS NULL) AND A.COLL_TYP_CD <> '00' AND C.GEN_TYP_CD = 'M' THEN A.LN_BAL ELSE 0 END) AS M_WIT_COLL_NORML_LN_BAL,

                  SUM(CASE WHEN (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR A.NEW_OVD_DBT_GRP_CD IS NULL) AND A.COLL_TYP_CD <> '00' THEN A.LN_CNTR_AMT ELSE 0 END) AS TOT_WIT_COLL_NORML_LN_CNTR_AMT,
                  SUM(CASE WHEN (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR A.NEW_OVD_DBT_GRP_CD IS NULL) AND A.COLL_TYP_CD <> '00' AND C.GEN_TYP_CD = 'F' THEN A.LN_CNTR_AMT ELSE 0 END) AS F_WIT_COLL_NORML_LN_CNTR_AMT,
                  SUM(CASE WHEN (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR A.NEW_OVD_DBT_GRP_CD IS NULL) AND A.COLL_TYP_CD <> '00' AND C.GEN_TYP_CD = 'M' THEN A.LN_CNTR_AMT ELSE 0 END) AS M_WIT_COLL_NORML_LN_CNTR_AMT,

                  SUM(CASE WHEN (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR A.NEW_OVD_DBT_GRP_CD IS NULL) AND A.COLL_TYP_CD = '00' THEN A.LN_BAL ELSE 0 END) AS TOT_WO_COLL_NORML_LN_BAL,
                  SUM(CASE WHEN (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR A.NEW_OVD_DBT_GRP_CD IS NULL) AND A.COLL_TYP_CD = '00' AND C.GEN_TYP_CD = 'F' THEN A.LN_BAL ELSE 0 END) AS F_WO_COLL_NORML_LN_BAL,
                  SUM(CASE WHEN (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR A.NEW_OVD_DBT_GRP_CD IS NULL) AND A.COLL_TYP_CD = '00' AND C.GEN_TYP_CD = 'M' THEN A.LN_BAL ELSE 0 END) AS M_WO_COLL_NORML_LN_BAL,

                  SUM(CASE WHEN (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR A.NEW_OVD_DBT_GRP_CD IS NULL) AND A.COLL_TYP_CD = '00' THEN A.LN_CNTR_AMT ELSE 0 END) AS TOT_WO_COLL_NORML_LN_CNTR_AMT,
                  SUM(CASE WHEN (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR A.NEW_OVD_DBT_GRP_CD IS NULL) AND A.COLL_TYP_CD = '00' AND C.GEN_TYP_CD = 'F' THEN A.LN_CNTR_AMT ELSE 0 END) AS F_WO_COLL_NORML_LN_CNTR_AMT,
                  SUM(CASE WHEN (A.NEW_OVD_DBT_GRP_CD IN ('0','1') OR A.NEW_OVD_DBT_GRP_CD IS NULL) AND A.COLL_TYP_CD = '00' AND C.GEN_TYP_CD = 'M' THEN A.LN_CNTR_AMT ELSE 0 END) AS M_WO_COLL_NORML_LN_CNTR_AMT,

                  SUM(CASE WHEN A.INIT_LN_TRM_CD IN ('001', '002', '003', '004', '005', '006') AND C.GEN_TYP_CD = 'F'
                                THEN A.LN_BAL
                           ELSE 0
                           END) AS F_INIT_TRM_LSTHN_6MM_LN_BAL,
                  SUM(CASE WHEN A.INIT_LN_TRM_CD IN ('001', '002', '003', '004', '005', '006') AND C.GEN_TYP_CD = 'M'
                                THEN A.LN_BAL
                           ELSE 0
                           END) AS M_INIT_TRM_LSTHN_6MM_LN_BAL,
                  SUM(CASE WHEN A.INIT_LN_TRM_CD IN ('007', '008', '009', '010', '011', '012') AND C.GEN_TYP_CD = 'F'
                                THEN A.LN_BAL
                           ELSE 0
                           END) AS F_INIT_TRM_LSTHN_12MM_LN_BAL,
                  SUM(CASE WHEN A.INIT_LN_TRM_CD IN ('007', '008', '009', '010', '011', '012') AND C.GEN_TYP_CD = 'M'
                                THEN A.LN_BAL
                           ELSE 0
                           END) AS M_INIT_TRM_LSTHN_12MM_LN_BAL,
                  SUM(CASE WHEN C.GEN_TYP_CD = 'F' AND (A.INIT_LN_TRM_CD = 'XXX' OR (A.INIT_LN_TRM_CD >= '013' AND A.INIT_LN_TRM_CD <> 'ZZZ'))
                                THEN A.LN_BAL
                           ELSE 0
                           END) AS F_INIT_TRM_OVER_12MM_LN_BAL,
                  SUM(CASE WHEN C.GEN_TYP_CD = 'M' AND (A.INIT_LN_TRM_CD = 'XXX' OR (A.INIT_LN_TRM_CD >= '013' AND A.INIT_LN_TRM_CD <> 'ZZZ'))
                                THEN A.LN_BAL
                           ELSE 0
                           END) AS M_INIT_TRM_OVER_12MM_LN_BAL,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.ES_CD = '0101' THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS TOT_AGRCTR_LN_CNTR_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.ES_CD = '0101' AND C.GEN_TYP_CD = 'F' THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS F_AGRCTR_LN_CNTR_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.ES_CD = '0101' AND C.GEN_TYP_CD = 'M' THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS M_AGRCTR_LN_CNTR_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.ES_CD <> '0101' THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS TOT_NON_AGRCTR_LN_CNTR_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.ES_CD <> '0101' AND C.GEN_TYP_CD = 'F' THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS F_NON_AGRCTR_LN_CNTR_CNT,
                  COUNT(DISTINCT CASE WHEN A.LN_BAL > 0 AND A.ES_CD <> '0101' AND C.GEN_TYP_CD = 'M' THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS M_NON_AGRCTR_LN_CNTR_CNT
           FROM   TM21_DDLY_CUST_CR_TRANS_V A INNER JOIN (SELECT SUBSTR(BAS_DAY, 1, 6) AS BAS_YM, PCF_ID, MAX(BAS_DAY) AS BAS_DAY
                                                          FROM   TM21_DDLY_CUST_CR_TRANS_V
                                                          GROUP BY SUBSTR(BAS_DAY, 1, 6), PCF_ID
                                                         ) B
                                                      ON B.BAS_DAY = A.BAS_DAY
                                                     AND B.PCF_ID  = A.PCF_ID
                                              INNER JOIN TM00_NEW_CUST_D C
                                                      ON C.PCF_ID   = A.PCF_ID
                                                     AND C.TX_CD_ID = A.CUST_ID
           GROUP BY A.PCF_ID
          )
          SELECT T1.BAS_YM
                ,T1.PCF_ID
                ,T1.TOT_LN_CUST_CNT
                ,T1.F_LN_CUST_CNT
                ,T1.M_LN_CUST_CNT
                ,NULL AS TOT_ACT_LN_CUST_CNT
                ,NULL AS F_ACT_LN_CUST_CNT
                ,NULL AS M_ACT_LN_CUST_CNT
                ,T1.TOT_LN_CNTR_CNT
                ,T1.F_LN_CNTR_CNT
                ,T1.M_LN_CNTR_CNT
                ,T1.TOT_LN_BAL
                ,T1.F_LN_BAL
                ,T1.M_LN_BAL
                ,T1.F_INIT_TRM_LSTHN_6MM_LN_BAL
                ,T1.M_INIT_TRM_LSTHN_6MM_LN_BAL
                ,T1.F_INIT_TRM_LSTHN_12MM_LN_BAL
                ,T1.M_INIT_TRM_LSTHN_12MM_LN_BAL
                ,T1.F_INIT_TRM_OVER_12MM_LN_BAL
                ,T1.M_INIT_TRM_OVER_12MM_LN_BAL
                ,T1.TOT_AGRCTR_LN_CNTR_CNT
                ,T1.F_AGRCTR_LN_CNTR_CNT
                ,T1.M_AGRCTR_LN_CNTR_CNT
                ,T1.TOT_NON_AGRCTR_LN_CNTR_CNT
                ,T1.F_NON_AGRCTR_LN_CNTR_CNT
                ,T1.M_NON_AGRCTR_LN_CNTR_CNT
                ,T2.TOT_NEW_ARISN_LN_CNTR_CNT
                ,T2.F_NEW_ARISN_LN_CNTR_CNT
                ,T2.M_NEW_ARISN_LN_CNTR_CNT
                ,T2.TOT_NEW_ARISN_LN_CNTR_AMT
                ,T2.F_NEW_ARISN_LN_CNTR_AMT
                ,T2.M_NEW_ARISN_LN_CNTR_AMT
                ,T3.TOT_L3M_NEW_ARISN_LN_CNTR_AMT
                ,T3.F_L3M_NEW_ARISN_LN_CNTR_AMT
                ,T3.M_L3M_NEW_ARISN_LN_CNTR_AMT
                ,T1.TOT_OVD_LN_CNTR_CNT
                ,T1.F_OVD_LN_CNTR_CNT
                ,T1.M_OVD_LN_CNTR_CNT
                ,T1.TOT_OVD_LN_BAL
                ,T1.F_OVD_LN_BAL
                ,T1.M_OVD_LN_BAL
                ,T1.TOT_NORML_LN_CNTR_CNT
                ,T1.F_NORML_LN_CNTR_CNT
                ,T1.M_NORML_LN_CNTR_CNT
                ,T1.TOT_NORML_LN_BAL
                ,T1.F_NORML_LN_BAL
                ,T1.M_NORML_LN_BAL
                ,T1.TOT_WIT_COLL_LN_CNTR_CNT
                ,T1.F_WIT_COLL_LN_CNTR_CNT
                ,T1.M_WIT_COLL_LN_CNTR_CNT
                ,T1.TOT_WO_COLL_LN_CNTR_CNT
                ,T1.F_WO_COLL_LN_CNTR_CNT
                ,T1.M_WO_COLL_LN_CNTR_CNT
                ,T1.TOT_WIT_COLL_OVD_LN_CNTR_CNT
                ,T1.F_WIT_COLL_OVD_LN_CNTR_CNT
                ,T1.M_WIT_COLL_OVD_LN_CNTR_CNT
                ,T1.TOT_WIT_COLL_OVD_LN_BAL
                ,T1.F_WIT_COLL_OVD_LN_BAL
                ,T1.M_WIT_COLL_OVD_LN_BAL
                ,T1.TOT_WIT_COLL_OVD_LN_CNTR_AMT
                ,T1.F_WIT_COLL_OVD_LN_CNTR_AMT
                ,T1.M_WIT_COLL_OVD_LN_CNTR_AMT
                ,T1.TOT_WO_COLL_OVD_LN_CNT
                ,T1.F_WO_COLL_OVD_LN_CNT
                ,T1.M_WO_COLL_OVD_LN_CNT
                ,T1.TOT_WO_COLL_OVD_LN_BAL
                ,T1.F_WO_COLL_OVD_LN_BAL
                ,T1.M_WO_COLL_OVD_LN_BAL
                ,T1.TOT_WO_COLL_OVD_LN_CNTR_AMT
                ,T1.F_WO_COLL_OVD_LN_CNTR_AMT
                ,T1.M_WO_COLL_OVD_LN_CNTR_AMT
                ,T1.TOT_WIT_COLL_NORML_LN_CNTR_CNT
                ,T1.F_WIT_COLL_NORML_LN_CNTR_CNT
                ,T1.M_WIT_COLL_NORML_LN_CNTR_CNT
                ,T1.TOT_WIT_COLL_NORML_LN_BAL
                ,T1.F_WIT_COLL_NORML_LN_BAL
                ,T1.M_WIT_COLL_NORML_LN_BAL
                ,T1.TOT_WIT_COLL_NORML_LN_CNTR_AMT
                ,T1.F_WIT_COLL_NORML_LN_CNTR_AMT
                ,T1.M_WIT_COLL_NORML_LN_CNTR_AMT
                ,T1.TOT_WO_COLL_NORML_LN_CNTR_CNT
                ,T1.F_WO_COLL_NORML_LN_CNTR_CNT
                ,T1.M_WO_COLL_NORML_LN_CNTR_CNT
                ,T1.TOT_WO_COLL_NORML_LN_BAL
                ,T1.F_WO_COLL_NORML_LN_BAL
                ,T1.M_WO_COLL_NORML_LN_BAL
                ,T1.TOT_WO_COLL_NORML_LN_CNTR_AMT
                ,T1.F_WO_COLL_NORML_LN_CNTR_AMT
                ,T1.M_WO_COLL_NORML_LN_CNTR_AMT
                ,T1.TOT_OVD_LN_CUST_CNT
                ,T1.F_OVD_LN_CUST_CNT
                ,T1.M_OVD_LN_CUST_CNT
                ,T1.TOT_NORML_LN_CUST_CNT
                ,T1.F_NORML_LN_CUST_CNT
                ,T1.M_NORML_LN_CUST_CNT
                ,T1.TOT_WO_COLL_OVD_LN_CUST_CNT
                ,T1.F_WO_COLL_OVD_LN_CUST_CNT
                ,T1.M_WO_COLL_OVD_LN_CUST_CNT
                ,T1.TOT_WO_COLL_NORML_LN_CUST_CNT
                ,T1.F_WO_COLL_NORML_LN_CUST_CNT
                ,T1.M_WO_COLL_NORML_LN_CUST_CNT
                ,T1.TOT_WIT_COLL_OVD_LN_CUST_CNT
                ,T1.F_WIT_COLL_OVD_LN_CUST_CNT
                ,T1.M_WIT_COLL_OVD_LN_CUST_CNT
                ,T1.TOT_WIT_COLL_NORML_LN_CUST_CNT
                ,T1.F_WIT_COLL_NORML_LN_CUST_CNT
                ,T1.M_WIT_COLL_NORML_LN_CUST_CNT
                ,SYSTIMESTAMP
          FROM   TOT_LOAN T1 LEFT OUTER JOIN THIS_MM_NEW_LOAN T2
                                          ON T2.BAS_YM = T1.BAS_YM
                                         AND T2.PCF_ID = T1.PCF_ID
                             LEFT OUTER JOIN LAST_3MM_NEW_LOAN T3
                                          ON T3.BAS_YM = T1.BAS_YM
                                         AND T3.PCF_ID = T1.PCF_ID;

          ----------------------------------------------------------------------------
          --  Transaction Log
          ----------------------------------------------------------------------------
          v_step_code    := '040' ;
          v_step_desc    := v_wk_date || v_seq || ' ' || loop_bas_day.BAS_YM ||' : Inserting Data Result';
          v_time         := SYSDATE ;

          P1_BSM_PROG_EXEC_LOG(v_program_id, v_program_type_name, v_step_code, v_step_desc, v_time, sql%rowcount, NULL, NULL) ;

          COMMIT ;

          EXCEPTION
          WHEN OTHERS THEN
               begin
                     v_step_code    := '041' ;
                     v_step_desc    := v_wk_date || v_seq || ' : Deleting or Inserting Data Error with BAS_YM = ' ;
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