create or replace PROCEDURE            "P1_M27_DDLY_PRVN_KEY_IDC_1718"
                 ( p_wkdate    IN  CHAR DEFAULT NULL
                 , p_stdate_01 IN  CHAR DEFAULT NULL
                 , p_eddate_01 IN  CHAR DEFAULT NULL
                 )
IS
    /*--------------------------------------------------------------------------
     * PROGRAM ID    : P1_M27_DDLY_PRVN_KEY_IDC_1718
     * PROGRAM NAME  : A program for insert data to TM27_DDLY_PRVN_KEY_IDC_A
     * SOURCE TABLE  : TB01_G32_016_TTGS_A
                       TM27_DDLY_PCF_KEY_IDC_A
     * TARGET TABLE  : TM27_DDLY_PRVN_KEY_IDC_A
     * PROGRAMER     : HIEU
     * LAST MODIFICATION DATE : 2025-12-29
     * UNIQUENESS    : N/A
     * COMMENTS      : N/A
    ----------------------------------------------------------------------------
     * Revision History : 2025-12-29 : Create
     * Revision History :
    --------------------------------------------------------------------------*/
    ----------------------------------------------------------------------------
    --  Declare Local Variables
    ----------------------------------------------------------------------------
    v_wk_date                 CHAR(8) DEFAULT NULL ;
    v_st_date_01              CHAR(8) DEFAULT NULL ;
    v_end_date_01             CHAR(8) DEFAULT NULL ;

    v_program_id              VARCHAR2(100) DEFAULT 'P1_M27_DDLY_PRVN_KEY_IDC_1718' ;
    v_program_type_name       VARCHAR2(100) DEFAULT 'Stored Procedure' ;
    v_step_code               VARCHAR2(3) ;
    v_step_desc               VARCHAR2(100) ;
    v_time                    DATE ;
    v_cnt                     NUMBER DEFAULT 0 ;
    v_seq                     CHAR(14) DEFAULT ' '||to_char(SEQ_UID.NEXTVAL,'000000000000');
    ----------------------------------------------------------------------------
    --  Declare Cursor
    ----------------------------------------------------------------------------
/*
    CURSOR v_sbmt_day(v_dt1 VARCHAR2, v_dt2 VARCHAR2, v_dt3 VARCHAR2) IS
    SELECT BAS_DAY
    FROM   TM00_DDLY_CAL_D
    WHERE  BAS_DAY BETWEEN '20210403' AND '20220914'
    ORDER BY 1;
*/
    CURSOR v_sbmt_day(v_dt1 VARCHAR2, v_dt2 VARCHAR2, v_dt3 VARCHAR2) IS
    SELECT W2.BAS_DAY
    FROM   (SELECT NVL(MIN(BAS_DAY), TO_CHAR(TO_DATE(v_dt2, 'YYYYMMDD') -2, 'YYYYMMDD')) AS MIN_DAY, TO_CHAR(TO_DATE(v_dt2, 'YYYYMMDD') -2, 'YYYYMMDD') AS MAX_DAY
            FROM   (

--                  SELECT T1.BAS_DAY
--                  FROM   TM00_DDLY_CAL_D T1
--                  WHERE  T1.BAS_DAY BETWEEN '20200522' AND '20201024'

                    SELECT MAX(BAS_DAY) AS BAS_DAY
                    FROM   TM23_DDLY_BAL_SHET_A

                    UNION ALL

                    SELECT T1.BAS_DAY
                    FROM   TM00_DDLY_CAL_D T1 INNER JOIN (SELECT MIN(DATA_BAS_DAY) AS MIN_DAY, TO_CHAR(TO_DATE(v_dt2, 'YYYYMMDD') -2, 'YYYYMMDD') AS MAX_DAY
                                                          FROM   TBSM_INPT_RPT_SUBMIT_L
                                                          WHERE  BTCH_BAS_DAY = v_dt2
                                                          AND    INPT_RPT_ID  = 'G32_012_TTGS'
                                                         ) T2
                                                      ON T1.BAS_DAY BETWEEN T2.MIN_DAY AND T2.MAX_DAY
                    UNION ALL

                    SELECT T1.BAS_DAY
                    FROM   TM00_DDLY_CAL_D T1 INNER JOIN (SELECT MIN(DATA_BAS_DAY) AS MIN_DAY, TO_CHAR(TO_DATE(v_dt2, 'YYYYMMDD') -2, 'YYYYMMDD') AS MAX_DAY
                                                          FROM   TBSM_INPT_RPT_SUBMIT_L
                                                          WHERE  BTCH_BAS_DAY = v_dt2
                                                          AND    INPT_RPT_ID  = 'G32_003_TTGS'
                                                         ) T2
                                                      ON T1.BAS_DAY BETWEEN T2.MIN_DAY AND T2.MAX_DAY

                    UNION ALL

                    SELECT T1.BAS_DAY
                    FROM   TM00_DDLY_CAL_D T1 INNER JOIN (SELECT MIN(DATA_BAS_DAY) AS MIN_DAY, MAX(DATA_BAS_DAY) AS MAX_DAY
                                                          FROM   TBSM_INPT_RPT_SUBMIT_L
                                                          WHERE  BTCH_BAS_DAY = v_dt2
                                                          AND    INPT_RPT_ID  = 'G32_020_TTGS_01'
                                                         ) T2
                                                      ON T1.BAS_DAY BETWEEN T2.MIN_DAY AND T2.MAX_DAY

                    UNION ALL

                    SELECT T1.BAS_DAY
                    FROM   TM00_DDLY_CAL_D T1 INNER JOIN (SELECT SUBSTR(NVL(MIN(DATA_BAS_DAY), '999912'), 1, 6)||'01' AS MIN_DAY, TO_CHAR(SYSDATE - 10, 'YYYYMMDD') AS MAX_DAY
                                                          FROM   TBSM_INPT_RPT_SUBMIT_L
                                                          WHERE  BTCH_BAS_DAY = v_dt2
                                                          AND    INPT_RPT_ID  = 'G32_016_TTGS'
                                                         ) T2
                                                      ON T1.BAS_DAY BETWEEN T2.MIN_DAY AND T2.MAX_DAY

                    UNION ALL

                    SELECT T1.BAS_DAY
                    FROM   TM00_DDLY_CAL_D T1 INNER JOIN (SELECT MIN(DATA_BAS_DAY) AS MIN_DAY, MAX(DATA_BAS_DAY) AS MAX_DAY
                                                          FROM   TBSM_INPT_RPT_SUBMIT_L
                                                          WHERE  BTCH_BAS_DAY = v_dt2
                                                          AND    INPT_RPT_ID  = 'G32_005_TTGS'
                                                         ) T2
                                                      ON T1.BAS_DAY BETWEEN T2.MIN_DAY AND T2.MAX_DAY

                   )
           ) W1 INNER JOIN TM00_DDLY_CAL_D W2
                        ON W2.BAS_DAY BETWEEN W1.MIN_DAY AND W1.MAX_DAY
    WHERE W2.BAS_DAY >= '20210102'
    GROUP BY W2.BAS_DAY
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
            FROM TM27_DDLY_PRVN_KEY_IDC_A T1
           WHERE T1.BAS_DAY = loop_bas_day.BAS_DAY;

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
                     v_step_desc    := v_wk_date || v_seq || ' : Deleting Data Error with BAS_DAY = ' ;
                     v_time         := SYSDATE ;
                     P1_BSM_PROG_EXEC_LOG(v_program_id, v_program_type_name, v_step_code, v_step_desc||loop_bas_day.BAS_DAY, v_time, v_cnt, TO_CHAR(SQLCODE), SQLERRM) ;
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

          INSERT INTO TM27_DDLY_PRVN_KEY_IDC_A
          (   BAS_DAY
             ,PRVN_CD
             ,KPI_TYP_CD
             ,AGT_METH_CD
             ,TOP_BTOM_TYP_CD
             ,RNK_NUM
             ,PCF_ID
             ,AGT_VAL
             ,TRGT_DATA_LST_MOD_TM
          )
          WITH
          FULL_SET AS
          (SELECT  T1.BAS_DAY, T1.PCF_ID
           FROM   (SELECT BAS_DAY, PCF_ID FROM TM27_DDLY_PCF_KEY_IDC_A WHERE BAS_DAY = loop_bas_day.BAS_DAY GROUP BY BAS_DAY, PCF_ID
                  ) T1
          ),
          FROM_G32_016_TTGS AS
          (SELECT B.BAS_YM,
                  A.PCF_ID,
                  A.TOTAL_CURRENT_MEMBERS AS TOT_PCF_MBR_NUM_CNT
           FROM   TB01_G32_016_TTGS_A A INNER JOIN TM00_MMLY_CAL_D B
                                           ON B.BAS_YM = A.BAS_YM

           UNION ALL

           SELECT TO_CHAR(ADD_MONTHS(TO_DATE(B.BAS_YM, 'YYYYMM'), 1), 'YYYYMM') AS BAS_YM,
                  A.PCF_ID,
                  A.TOTAL_CURRENT_MEMBERS AS TOT_PCF_MBR_NUM_CNT
           FROM   TB01_G32_016_TTGS_A A INNER JOIN TM00_MMLY_CAL_D B
                                           ON B.BAS_YM = A.BAS_YM

           UNION ALL

           SELECT TO_CHAR(ADD_MONTHS(TO_DATE(B.BAS_YM, 'YYYYMM'), 2), 'YYYYMM') AS BAS_YM,
                  A.PCF_ID,
                  A.TOTAL_CURRENT_MEMBERS AS TOT_PCF_MBR_NUM_CNT
           FROM   TB01_G32_016_TTGS_A A INNER JOIN TM00_MMLY_CAL_D B
                                           ON B.BAS_YM = A.BAS_YM
          ),
          FROM_TM27_DDLY_PCF_KEY_IDC_A AS
          (SELECT BAS_DAY
                 ,PCF_ID
                 ,EQT_AMT
                 ,CNSLDT_EQT_AMT
                 ,CCAP_AMT
                 ,CCAP_RSRV_FUND_SUPL_CCAP_AMT
                 ,TOT_AST_AMT
                 ,TOT_LN_BAL
                 ,SHRT_TRM_LN_BAL
                 ,MED_LT_LN_BAL
                 ,WO_COLL_LN_BAL
                 ,WIT_COLL_LN_BAL
                 ,TOT_LN_CNTR_NUM_CNT
                 ,TOT_BRWR_NUM_CNT
                 ,NEW_ARISN_LN_AMT
                 ,NEW_ARISN_LN_CNTR_NUM_CNT
                 ,NEW_BRWR_NUM_CNT
                 ,BAD_DBT_AMT
                 ,GENL_PRVS_AMT
                 ,SPEC_PRVS_AMT
                 ,TDP_BAL
                 ,MBR_DPST_BAL
                 ,NON_MBR_DPST_BAL
                 ,SHRT_TRM_DPST_BAL
                 ,MED_LT_DPST_BAL
                 ,TOT_DPSTR_NUM_CNT
                 ,MBR_DPSTR_NUM_CNT
                 ,NON_MBR_DPSTR_NUM_CNT
                 ,MBR_DPST_VS_TDP_RTO
                 ,CBV_BOR_EXCL_SFTY_FUND_AMT
                 ,CBV_OVD_BOR_AMT
                 ,SFTY_FUND_BOR_AMT
                 ,OTHR_INST_BOR_AMT
                 ,CBV_SVG_AMT
                 ,OTHR_INST_SVG_AMT
                 ,IN_HAND_CSH_AMT
                 ,CUR_TOT_INCM_AMT
                 ,CUR_TOT_EXP_AMT
                 ,CUR_NET_PNL_AMT
                 ,INCL_ALOSS_INCM_MNS_EXP_AMT
                 ,NXT_WRK_DAY_AST_AMT
                 ,NXT_WRK_DAY_PAY_LBLTY_AMT
                 ,NXT_WRK_DAY_LQDTY_RTO
                 ,NXT_7_WRK_DAY_AST_AMT
                 ,NXT_7_WRK_DAY_PAY_LBLTY_AMT
                 ,NXT_7_WRK_DAY_LQDTY_RTO
                 ,L3M_NXT_WRK_DAY_LQDTY_RTO
                 ,L3M_NXT_7_WRK_DAY_LQDTY_RTO
                 ,BAL_SHET_BAS_SHRT_TRM_LN_BAL
                 ,BAL_SHET_BAS_MED_LT_LN_BAL
                 ,RISK_PRVS_FUND_AMT
           FROM   TM27_DDLY_PCF_KEY_IDC_A
           WHERE  BAS_DAY = loop_bas_day.BAS_DAY
          ),
          FROM_KPI_TYP_001 AS
          (SELECT T2.BAS_DAY,
                  T2.PRVN_CD,
                  '001' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  T1.TOT_PCF_MBR_NUM_CNT AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   (SELECT A.BAS_YM,
                          C.PRVN_CD,
                          SUM(A.TOT_PCF_MBR_NUM_CNT) AS TOT_PCF_MBR_NUM_CNT
                   FROM   FROM_G32_016_TTGS A INNER JOIN TM00_PCF_D B
                                                 ON A.PCF_ID = B.PCF_ID
                                         INNER JOIN TM00_LOC_D C
                                                 ON B.LOC_ID = C.LOC_ID
                   GROUP BY A.BAS_YM,C.PRVN_CD
                  ) T1 INNER JOIN (SELECT A.BAS_DAY,
                                          C.PRVN_CD
                                   FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A A INNER JOIN TM00_PCF_D B
                                                                                 ON A.PCF_ID = B.PCF_ID
                                                                         INNER JOIN TM00_LOC_D C
                                                                                 ON B.LOC_ID = C.LOC_ID
                                   GROUP BY A.BAS_DAY, C.PRVN_CD) T2
                                ON SUBSTR(T2.BAS_DAY,1,6) = T1.BAS_YM
                               AND T2.PRVN_CD             = T1.PRVN_CD

           UNION ALL

           SELECT T2.BAS_DAY,
                  T2.PRVN_CD,
                  '001' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  T1.TOT_PCF_MBR_NUM_CNT AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   (SELECT A.BAS_YM,
                          C.PRVN_CD,
                          ROUND(AVG(A.TOT_PCF_MBR_NUM_CNT), 0) AS TOT_PCF_MBR_NUM_CNT
                   FROM   FROM_G32_016_TTGS A INNER JOIN TM00_PCF_D B
                                                 ON A.PCF_ID = B.PCF_ID
                                         INNER JOIN TM00_LOC_D C
                                                 ON B.LOC_ID = C.LOC_ID
                   GROUP BY A.BAS_YM,C.PRVN_CD
                  ) T1 INNER JOIN (SELECT A.BAS_DAY,
                                          C.PRVN_CD
                                   FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A A INNER JOIN TM00_PCF_D B
                                                                                 ON A.PCF_ID = B.PCF_ID
                                                                         INNER JOIN TM00_LOC_D C
                                                                                 ON B.LOC_ID = C.LOC_ID
                                   GROUP BY A.BAS_DAY, C.PRVN_CD) T2
                                ON SUBSTR(T2.BAS_DAY,1,6) = T1.BAS_YM
                               AND T2.PRVN_CD             = T1.PRVN_CD
          ),
          FROM_KPI_TYP_002 AS
          (SELECT T1.BAS_DAY,
                  T3.PRVN_CD,
                  '002' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.TDP_BAL) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
                                                  INNER JOIN TM00_LOC_D T3
                                                          ON T2.LOC_ID = T3.LOC_ID
           GROUP BY T1.BAS_DAY, T3.PRVN_CD

           UNION ALL

           SELECT T1.BAS_DAY,
                  T3.PRVN_CD,
                  '002' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.TDP_BAL),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
                                                  INNER JOIN TM00_LOC_D T3
                                                          ON T2.LOC_ID = T3.LOC_ID
           GROUP BY T1.BAS_DAY, T3.PRVN_CD
          ),
          FROM_KPI_TYP_003 AS
          (SELECT T1.BAS_DAY,
                  T3.PRVN_CD,
                  '003' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.TOT_LN_BAL) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
                                                  INNER JOIN TM00_LOC_D T3
                                                          ON T2.LOC_ID = T3.LOC_ID
           GROUP BY T1.BAS_DAY, T3.PRVN_CD

           UNION ALL

           SELECT T1.BAS_DAY,
                  T3.PRVN_CD,
                  '003' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.TOT_LN_BAL),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
                                                  INNER JOIN TM00_LOC_D T3
                                                          ON T2.LOC_ID = T3.LOC_ID
           GROUP BY T1.BAS_DAY, T3.PRVN_CD
          ),
          FROM_KPI_TYP_004 AS
          (SELECT T1.BAS_DAY,
                  T3.PRVN_CD,
                  '004' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.WO_COLL_LN_BAL),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
                                                  INNER JOIN TM00_LOC_D T3
                                                          ON T2.LOC_ID = T3.LOC_ID
           GROUP BY T1.BAS_DAY, T3.PRVN_CD
          ),
          FROM_KPI_TYP_005 AS
          (SELECT T1.BAS_DAY,
                  T3.PRVN_CD,
                  '005' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.BAD_DBT_AMT),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
                                                  INNER JOIN TM00_LOC_D T3
                                                          ON T2.LOC_ID = T3.LOC_ID
           GROUP BY T1.BAS_DAY, T3.PRVN_CD
          ),
          FROM_KPI_TYP_006 AS
          (SELECT T1.BAS_DAY,
                  T3.PRVN_CD,
                  '006' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.EQT_AMT) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
                                                  INNER JOIN TM00_LOC_D T3
                                                          ON T2.LOC_ID = T3.LOC_ID
           GROUP BY T1.BAS_DAY, T3.PRVN_CD

           UNION ALL

           SELECT T1.BAS_DAY,
                  T3.PRVN_CD,
                  '006' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.EQT_AMT),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
                                                  INNER JOIN TM00_LOC_D T3
                                                          ON T2.LOC_ID = T3.LOC_ID
           GROUP BY T1.BAS_DAY, T3.PRVN_CD
          ),
          FROM_KPI_TYP_007 AS
          (SELECT T1.BAS_DAY,
                  T3.PRVN_CD,
                  '007' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.CCAP_AMT) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
                                                  INNER JOIN TM00_LOC_D T3
                                                          ON T2.LOC_ID = T3.LOC_ID
           GROUP BY T1.BAS_DAY, T3.PRVN_CD

           UNION ALL

           SELECT T1.BAS_DAY,
                  T3.PRVN_CD,
                  '007' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.CCAP_AMT),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
                                                  INNER JOIN TM00_LOC_D T3
                                                          ON T2.LOC_ID = T3.LOC_ID
           GROUP BY T1.BAS_DAY, T3.PRVN_CD
          ),
          FROM_KPI_TYP_008 AS
          (SELECT T1.BAS_DAY,
                  T3.PRVN_CD,
                  '008' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.CBV_BOR_EXCL_SFTY_FUND_AMT) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
                                                  INNER JOIN TM00_LOC_D T3
                                                          ON T2.LOC_ID = T3.LOC_ID
           GROUP BY T1.BAS_DAY, T3.PRVN_CD

           UNION ALL

           SELECT T1.BAS_DAY,
                  T3.PRVN_CD,
                  '008' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.CBV_BOR_EXCL_SFTY_FUND_AMT),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
                                                  INNER JOIN TM00_LOC_D T3
                                                          ON T2.LOC_ID = T3.LOC_ID
           GROUP BY T1.BAS_DAY, T3.PRVN_CD
          ),
          FROM_KPI_TYP_009 AS
          (SELECT T1.BAS_DAY,
                  T3.PRVN_CD,
                  '009' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.CBV_SVG_AMT),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
                                                  INNER JOIN TM00_LOC_D T3
                                                          ON T2.LOC_ID = T3.LOC_ID
           GROUP BY T1.BAS_DAY, T3.PRVN_CD
          ),
          FROM_KPI_TYP_010 AS
          (SELECT T1.BAS_DAY,
                  T3.PRVN_CD,
                  '010' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.OTHR_INST_SVG_AMT) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
                                                  INNER JOIN TM00_LOC_D T3
                                                          ON T2.LOC_ID = T3.LOC_ID
           GROUP BY T1.BAS_DAY, T3.PRVN_CD

           UNION ALL

           SELECT T1.BAS_DAY,
                  T3.PRVN_CD,
                  '010' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.OTHR_INST_SVG_AMT),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
                                                  INNER JOIN TM00_LOC_D T3
                                                          ON T2.LOC_ID = T3.LOC_ID
           GROUP BY T1.BAS_DAY, T3.PRVN_CD
          ),
          FROM_KPI_TYP_067 AS
          (SELECT T1.BAS_DAY,
                  T3.PRVN_CD,
                  '067' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.NXT_WRK_DAY_LQDTY_RTO), 3) AS AGT_VAL
           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
                                                  INNER JOIN TM00_LOC_D T3
                                                          ON T2.LOC_ID = T3.LOC_ID
           GROUP BY T1.BAS_DAY, T3.PRVN_CD
          ),
          FROM_KPI_TYP_068 AS
          (SELECT T1.BAS_DAY,
                  T3.PRVN_CD,
                  '068' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.NXT_7_WRK_DAY_LQDTY_RTO), 3) AS AGT_VAL
           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
                                                  INNER JOIN TM00_LOC_D T3
                                                          ON T2.LOC_ID = T3.LOC_ID
           GROUP BY T1.BAS_DAY, T3.PRVN_CD
          ),
          FROM_KPI_TYP_143 AS
          (SELECT T1.BAS_DAY,
                  T3.PRVN_CD,
                  '143' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.CUR_TOT_INCM_AMT - T1.CUR_TOT_EXP_AMT) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
                                                  INNER JOIN TM00_LOC_D T3
                                                          ON T2.LOC_ID = T3.LOC_ID
           GROUP BY T1.BAS_DAY, T3.PRVN_CD

           UNION ALL

           SELECT T1.BAS_DAY,
                  T3.PRVN_CD,
                  '143' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.CUR_TOT_INCM_AMT - T1.CUR_TOT_EXP_AMT),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
                                                  INNER JOIN TM00_LOC_D T3
                                                          ON T2.LOC_ID = T3.LOC_ID
           GROUP BY T1.BAS_DAY, T3.PRVN_CD
          ),
          FROM_KPI_TYP_133 AS
          (SELECT T1.BAS_DAY,
                  T3.PRVN_CD,
                  '133' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.WIT_COLL_LN_BAL) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
                                                  INNER JOIN TM00_LOC_D T3
                                                          ON T2.LOC_ID = T3.LOC_ID
           GROUP BY T1.BAS_DAY, T3.PRVN_CD

           UNION ALL

           SELECT T1.BAS_DAY,
                  T3.PRVN_CD,
                  '133' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.WIT_COLL_LN_BAL),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
                                                  INNER JOIN TM00_LOC_D T3
                                                          ON T2.LOC_ID = T3.LOC_ID
           GROUP BY T1.BAS_DAY, T3.PRVN_CD
          ),
          FROM_KPI_TYP_139 AS
          (SELECT T1.BAS_DAY,
                  T3.PRVN_CD,
                  '139' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '5'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.L3M_NXT_WRK_DAY_LQDTY_RTO), 3) AS AGT_VAL
           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
                                                  INNER JOIN TM00_LOC_D T3
                                                          ON T2.LOC_ID = T3.LOC_ID
           GROUP BY T1.BAS_DAY, T3.PRVN_CD
          ),
          FROM_KPI_TYP_140 AS
          (SELECT T1.BAS_DAY,
                  T3.PRVN_CD,
                  '140' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '5'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.L3M_NXT_7_WRK_DAY_LQDTY_RTO), 3) AS AGT_VAL
           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
                                                  INNER JOIN TM00_LOC_D T3
                                                          ON T2.LOC_ID = T3.LOC_ID
           GROUP BY T1.BAS_DAY, T3.PRVN_CD
          ),
          FROM_KPI_TYP_144 AS
          (SELECT T1.BAS_DAY,
                  T3.PRVN_CD,
                  '144' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '1'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  SUM(T1.INCL_ALOSS_INCM_MNS_EXP_AMT) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
                                                  INNER JOIN TM00_LOC_D T3
                                                          ON T2.LOC_ID = T3.LOC_ID
           GROUP BY T1.BAS_DAY, T3.PRVN_CD

           UNION ALL

           SELECT T1.BAS_DAY,
                  T3.PRVN_CD,
                  '144' AS KPI_TYP_CD,   /* Code of variable corresponding to each KPI item */
                  '2'   AS AGT_METH_CD,  /* Code of variable corresponding to each aggregation method */
                  '0'   AS TOP_BTOM_TYP_CD,
                  0     AS RNK_NUM,
                  NULL  AS PCF_ID,
                  ROUND(AVG(T1.INCL_ALOSS_INCM_MNS_EXP_AMT),0) AS AGT_VAL /* In case average required : ROUND(AVG(T1.TOT_PCF_MBR_NUM_CNT)) AS AGT_VAL */
           FROM   FROM_TM27_DDLY_PCF_KEY_IDC_A T1 INNER JOIN TM00_PCF_D T2
                                                          ON T1.PCF_ID = T2.PCF_ID
                                                  INNER JOIN TM00_LOC_D T3
                                                          ON T2.LOC_ID = T3.LOC_ID
           GROUP BY T1.BAS_DAY, T3.PRVN_CD
          )
          SELECT BAS_DAY, PRVN_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_001
          UNION ALL
          SELECT BAS_DAY, PRVN_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_002
          UNION ALL
          SELECT BAS_DAY, PRVN_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_003
          UNION ALL
          SELECT BAS_DAY, PRVN_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_004
          UNION ALL
          SELECT BAS_DAY, PRVN_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_005
          UNION ALL
          SELECT BAS_DAY, PRVN_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_006
          UNION ALL
          SELECT BAS_DAY, PRVN_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_007
          UNION ALL
          SELECT BAS_DAY, PRVN_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_008
          UNION ALL
          SELECT BAS_DAY, PRVN_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_009
          UNION ALL
          SELECT BAS_DAY, PRVN_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_010
          UNION ALL
          SELECT BAS_DAY, PRVN_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_067
          UNION ALL
          SELECT BAS_DAY, PRVN_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_068
          UNION ALL
          SELECT BAS_DAY, PRVN_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_143
          UNION ALL
          SELECT BAS_DAY, PRVN_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_133
          UNION ALL
          SELECT BAS_DAY, PRVN_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_139
          UNION ALL
          SELECT BAS_DAY, PRVN_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_140
          UNION ALL
          SELECT BAS_DAY, PRVN_CD, KPI_TYP_CD, AGT_METH_CD, TOP_BTOM_TYP_CD, RNK_NUM, PCF_ID, AGT_VAL, SYSTIMESTAMP FROM FROM_KPI_TYP_144
          ;

          ----------------------------------------------------------------------------
          --  Transaction Log
          ----------------------------------------------------------------------------
          v_step_code    := '020' ;
          v_step_desc    := v_wk_date || v_seq || ' ' || loop_bas_day.BAS_DAY ||' : Inserting Data Result';
          v_time         := SYSDATE ;

          P1_BSM_PROG_EXEC_LOG(v_program_id, v_program_type_name, v_step_code, v_step_desc, v_time, sql%rowcount, NULL, NULL) ;

          COMMIT ;
          ----------------------------------------------------------------------------

          v_cnt := v_cnt+sql%rowcount;

          EXCEPTION
          WHEN OTHERS THEN
               begin
                     v_step_code    := '021' ;
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