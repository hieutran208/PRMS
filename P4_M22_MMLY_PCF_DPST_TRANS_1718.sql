create or replace PROCEDURE            "P1_M27_DDLY_PCF_LN_DPST_SMR_1718"
                 ( p_wkdate    IN  CHAR DEFAULT NULL
                 , p_stdate_01 IN  CHAR DEFAULT NULL
                 , p_eddate_01 IN  CHAR DEFAULT NULL
                 )
IS
    /*--------------------------------------------------------------------------
     * PROGRAM ID    : P1_M27_DDLY_PCF_LN_DPST_SMR_1718
     * PROGRAM NAME  : A program for insert data to TM27_DDLY_PCF_LN_DPST_SMR_A
     * SOURCE TABLE  : TM21_DDLY_CUST_CR_TRANS_A
                       TM22_DDLY_CUST_DPST_TRANS_A
     * TARGET TABLE  : TM27_DDLY_PCF_LN_DPST_SMR_A
     * PROGRAMER     : HIEU
     * LAST MODIFICATION DATE : 2025-12-16
     * UNIQUENESS    : N/A
     * COMMENTS      : N/A
    ----------------------------------------------------------------------------
     * Revision History : 2025-12-16 : Create
     * Revision History :
    --------------------------------------------------------------------------*/
    ----------------------------------------------------------------------------
    --  Declare Local Variables
    ----------------------------------------------------------------------------
    v_wk_date                 CHAR(8) DEFAULT NULL ;
    v_st_date_01              CHAR(8) DEFAULT NULL ;
    v_end_date_01             CHAR(8) DEFAULT NULL ;

    v_program_id              VARCHAR2(100) DEFAULT 'P1_M27_DDLY_PCF_LN_DPST_SMR_1718' ;
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
    SELECT BAS_DAY
    FROM   (
            SELECT T1.BAS_DAY
            FROM   TM00_DDLY_CAL_D T1 INNER JOIN (SELECT NVL(MIN(DATA_BAS_DAY), TO_CHAR(TO_DATE(v_dt2, 'YYYYMMDD') -2, 'YYYYMMDD')) AS MIN_DAY, TO_CHAR(TO_DATE(v_dt2, 'YYYYMMDD') -2, 'YYYYMMDD') AS MAX_DAY
                                                  FROM   TBSM_INPT_RPT_SUBMIT_L
                                                  WHERE  BTCH_BAS_DAY = v_dt2
                                                  AND    INPT_RPT_ID  = 'G32_012_TTGS'
                                                 ) T2
                                              ON T1.BAS_DAY BETWEEN T2.MIN_DAY AND T2.MAX_DAY
            UNION

            SELECT T1.BAS_DAY
            FROM   TM00_DDLY_CAL_D T1 INNER JOIN (SELECT NVL(MIN(DATA_BAS_DAY), TO_CHAR(TO_DATE(v_dt2, 'YYYYMMDD') -2, 'YYYYMMDD')) AS MIN_DAY, TO_CHAR(TO_DATE(v_dt2, 'YYYYMMDD') -2, 'YYYYMMDD') AS MAX_DAY
                                                  FROM   TBSM_INPT_RPT_SUBMIT_L
                                                  WHERE  BTCH_BAS_DAY = v_dt2
                                                  AND    INPT_RPT_ID  = 'G32_003_TTGS'
                                                 ) T2
                                              ON T1.BAS_DAY BETWEEN T2.MIN_DAY AND T2.MAX_DAY
           )
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
    --  1.1 Delete data from two days ago on TM27_DDLY_PCF_LN_DPST_SMR_A
    ----------------------------------------------------------------------------

    DELETE
    FROM   TM27_DDLY_PCF_LN_DPST_SMR_A
    WHERE  BAS_DAY = TO_CHAR(TO_DATE(v_st_date_01, 'YYYYMMDD') -2, 'YYYYMMDD')
    ;

    ----------------------------------------------------------------------------
    --  Transaction Log
    ----------------------------------------------------------------------------
    v_step_code    := '010' ;
    v_step_desc    := v_wk_date || v_seq || ' : Delete data from two days ago on TM27_DDLY_PCF_LN_DPST_SMR_A ' ;
    v_time         := SYSDATE ;

    P1_BSM_PROG_EXEC_LOG(v_program_id, v_program_type_name, v_step_code, v_step_desc, v_time, sql%rowcount, NULL, NULL) ;

    ----------------------------------------------------------------------------
    --  1.2 Replicate data from three days ago to two days ago
    ----------------------------------------------------------------------------

    INSERT /*+ APPEND PARALLEL(TM27_DDLY_PCF_LN_DPST_SMR_A, 4) */ INTO TM27_DDLY_PCF_LN_DPST_SMR_A
    (   BAS_DAY
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
    (SELECT  T1.BAS_DAY, T1.PCF_ID
     FROM   (SELECT A.BAS_DAY, A.PCF_ID
             FROM   TM22_DDLY_CUST_DPST_TRANS_A A
             WHERE  A.BAS_DAY = TO_CHAR(TO_DATE(v_st_date_01, 'YYYYMMDD') -2, 'YYYYMMDD')
             GROUP BY A.BAS_DAY, A.PCF_ID

             UNION

             SELECT A.BAS_DAY, A.PCF_ID
             FROM   TM21_DDLY_CUST_CR_TRANS_A A
             WHERE  A.BAS_DAY = TO_CHAR(TO_DATE(v_st_date_01, 'YYYYMMDD') -2, 'YYYYMMDD')
             GROUP BY A.BAS_DAY, A.PCF_ID
            ) T1
    GROUP BY T1.BAS_DAY, T1.PCF_ID
    ),
    TM21_DDLY_CUST_CR_TRANS_V AS
    (SELECT  A.BAS_DAY
            ,A.PCF_ID
            ,A.RPLC_DATA_YN
            ,A.LN_CNTR_NUM_INFO
            ,MAX(A.CUST_ID             ) AS CUST_ID
            ,MAX(A.CUST_NM             ) AS CUST_NM
            ,MAX(A.CUST_TYP_CD         ) AS CUST_TYP_CD
            ,MAX(A.INIT_LN_TRM_CD      ) AS INIT_LN_TRM_CD
            ,MAX(A.RMTRT_LN_TRM_CD     ) AS RMTRT_LN_TRM_CD
            ,MAX(A.ES_CD               ) AS ES_CD
            ,MAX(A.COLL_TYP_INFO       ) AS COLL_TYP_INFO
            ,MAX(A.COLL_TYP_CD         ) AS COLL_TYP_CD
            ,MAX(A.LN_TRANS_TYP_CD     ) AS LN_TRANS_TYP_CD
            ,MAX(A.OLD_NORML_DBT_GRP_CD) AS OLD_NORML_DBT_GRP_CD
            ,MAX(A.OLD_OVD_DBT_GRP_CD  ) AS OLD_OVD_DBT_GRP_CD
            ,MAX(A.NEW_NORML_DBT_GRP_CD) AS NEW_NORML_DBT_GRP_CD
            ,MAX(A.NEW_OVD_DBT_GRP_CD  ) AS NEW_OVD_DBT_GRP_CD
            ,MAX(A.LN_CNTR_AMT         ) AS LN_CNTR_AMT
            ,MAX(A.LN_BAL              ) AS LN_BAL
            ,SUM(A.TRANS_AMT           ) AS TRANS_AMT
            ,MAX(A.COLL_VAL_AMT        ) AS COLL_VAL_AMT
            ,MAX(A.SPEC_PRVS_AMT       ) AS SPEC_PRVS_AMT
            ,MAX(A.OPN_DAY             ) AS OPN_DAY
            ,MAX(A.MTRT_DAY            ) AS MTRT_DAY
            ,MAX(A.ACTL_IR             ) AS ACTL_IR
     FROM   TM21_DDLY_CUST_CR_TRANS_A A
     WHERE  A.BAS_DAY = TO_CHAR(TO_DATE(v_st_date_01, 'YYYYMMDD') -2, 'YYYYMMDD')
     GROUP BY A.BAS_DAY
             ,A.PCF_ID
             ,A.RPLC_DATA_YN
             ,A.LN_CNTR_NUM_INFO
    ),
    DEPOSIT AS
    (SELECT A.BAS_DAY,
            A.PCF_ID,
            SUM(CASE WHEN A.INIT_DPST_TRM_CD IN ('000', '001', 'ZZZ') THEN A.DPST_BAL ELSE 0 END) AS INIT_TRM_LSTHN_1_MM_DPST_BAL,
            SUM(CASE WHEN A.INIT_DPST_TRM_CD IN ('002', '003') THEN A.DPST_BAL ELSE 0 END) AS INIT_TRM_LSTHN_3_MM_DPST_BAL,
            SUM(CASE WHEN A.INIT_DPST_TRM_CD IN ('004', '005', '006') THEN A.DPST_BAL ELSE 0 END) AS INIT_TRM_LSTHN_6_MM_DPST_BAL,
            SUM(CASE WHEN A.INIT_DPST_TRM_CD IN ('007', '008', '009') THEN A.DPST_BAL ELSE 0 END) AS INIT_TRM_LSTHN_9_MM_DPST_BAL,
            SUM(CASE WHEN A.INIT_DPST_TRM_CD IN ('010', '011', '012') THEN A.DPST_BAL ELSE 0 END) AS INIT_TRM_LSTHN_12_MM_DPST_BAL,
            SUM(CASE WHEN A.INIT_DPST_TRM_CD = 'XXX' OR (A.INIT_DPST_TRM_CD >= '013' AND A.INIT_DPST_TRM_CD <> 'ZZZ') THEN DPST_BAL ELSE 0 END) AS INIT_TRM_OVR_12_MM_DPST_BAL,
            SUM(CASE WHEN A.RMTRT_DPST_TRM_CD IN ('000', '001', 'ZZZ') THEN A.DPST_BAL ELSE 0 END) AS RMTRT_LSTHN_1_MM_DPST_BAL,
            SUM(CASE WHEN A.RMTRT_DPST_TRM_CD IN ('002', '003') THEN A.DPST_BAL ELSE 0 END) AS RMTRT_LSTHN_3_MM_DPST_BAL,
            SUM(CASE WHEN A.RMTRT_DPST_TRM_CD IN ('004', '005', '006') THEN A.DPST_BAL ELSE 0 END) AS RMTRT_LSTHN_6_MM_DPST_BAL,
            SUM(CASE WHEN A.RMTRT_DPST_TRM_CD IN ('007', '008', '009') THEN A.DPST_BAL ELSE 0 END) AS RMTRT_LSTHN_9_MM_DPST_BAL,
            SUM(CASE WHEN A.RMTRT_DPST_TRM_CD IN ('010', '011', '012') THEN A.DPST_BAL ELSE 0 END) AS RMTRT_LSTHN_12_MM_DPST_BAL,
            SUM(CASE WHEN A.RMTRT_DPST_TRM_CD = 'XXX' OR (A.RMTRT_DPST_TRM_CD >= '013' AND A.RMTRT_DPST_TRM_CD <> 'ZZZ') THEN A.DPST_BAL ELSE 0 END) AS RMTRT_OVR_12_MM_DPST_BAL,
            COUNT(DISTINCT CASE WHEN A.INIT_DPST_TRM_CD IN ('000', '001', 'ZZZ') AND A.DPST_BAL > 0 THEN A.CUST_ID ELSE NULL END) AS INIT_TRM_LSTHN_1_MM_DPSTR_CNT,
            COUNT(DISTINCT CASE WHEN A.INIT_DPST_TRM_CD IN ('002', '003') AND A.DPST_BAL > 0 THEN A.CUST_ID ELSE NULL END) AS INIT_TRM_LSTHN_3_MM_DPSTR_CNT,
            COUNT(DISTINCT CASE WHEN A.INIT_DPST_TRM_CD IN ('004', '005', '006') AND A.DPST_BAL > 0 THEN A.CUST_ID ELSE NULL END) AS INIT_TRM_LSTHN_6_MM_DPSTR_CNT,
            COUNT(DISTINCT CASE WHEN A.INIT_DPST_TRM_CD IN ('007', '008', '009') AND A.DPST_BAL > 0 THEN A.CUST_ID ELSE NULL END) AS INIT_TRM_LSTHN_9_MM_DPSTR_CNT,
            COUNT(DISTINCT CASE WHEN A.INIT_DPST_TRM_CD IN ('010', '011', '012') AND A.DPST_BAL > 0 THEN A.CUST_ID ELSE NULL END) AS INIT_TRM_LSTHN_12_MM_DPSTR_CNT,
            COUNT(DISTINCT CASE WHEN (A.INIT_DPST_TRM_CD = 'XXX' OR (A.INIT_DPST_TRM_CD >= '013' AND INIT_DPST_TRM_CD <> 'ZZZ')) AND A.DPST_BAL > 0 THEN A.CUST_ID ELSE NULL END) AS INIT_TRM_OVR_12_MM_DPSTR_CNT,
            COUNT(DISTINCT CASE WHEN A.RMTRT_DPST_TRM_CD IN ('000', '001', 'ZZZ') AND A.DPST_BAL > 0 THEN A.CUST_ID ELSE NULL END) AS RMTRT_LSTHN_1_MM_DPSTR_CNT,
            COUNT(DISTINCT CASE WHEN A.RMTRT_DPST_TRM_CD IN ('002', '003') AND A.DPST_BAL > 0 THEN A.CUST_ID ELSE NULL END) AS RMTRT_LSTHN_3_MM_DPSTR_CNT,
            COUNT(DISTINCT CASE WHEN A.RMTRT_DPST_TRM_CD IN ('004', '005', '006') AND A.DPST_BAL > 0 THEN A.CUST_ID ELSE NULL END) AS RMTRT_LSTHN_6_MM_DPSTR_CNT,
            COUNT(DISTINCT CASE WHEN A.RMTRT_DPST_TRM_CD IN ('007', '008', '009') AND A.DPST_BAL > 0 THEN A.CUST_ID ELSE NULL END) AS RMTRT_LSTHN_9_MM_DPSTR_CNT,
            COUNT(DISTINCT CASE WHEN A.RMTRT_DPST_TRM_CD IN ('010', '011', '012') AND A.DPST_BAL > 0 THEN A.CUST_ID ELSE NULL END) AS RMTRT_LSTHN_12_MM_DPSTR_CNT,
            COUNT(DISTINCT CASE WHEN (A.RMTRT_DPST_TRM_CD = 'XXX' OR (RMTRT_DPST_TRM_CD >= '013' AND RMTRT_DPST_TRM_CD <> 'ZZZ')) AND A.DPST_BAL > 0 THEN A.CUST_ID ELSE NULL END) AS RMTRT_OVR_12_MM_DPSTR_CNT
     FROM   TM22_DDLY_CUST_DPST_TRANS_A A
     WHERE  A.BAS_DAY = TO_CHAR(TO_DATE(v_st_date_01, 'YYYYMMDD') -2, 'YYYYMMDD')
     GROUP BY A.BAS_DAY, A.PCF_ID
    ),
    LOAN_1 AS
    (SELECT A.BAS_DAY,
            A.PCF_ID,
            SUM(CASE WHEN A.ES_CD = '0101' AND A.INIT_LN_TRM_CD BETWEEN '001' AND '012'
                          THEN A.TRANS_AMT
                     ELSE 0
                     END) AS AGRCTR_NASTLN_AMT,
            SUM(CASE WHEN A.ES_CD = '0101' AND ((A.INIT_LN_TRM_CD BETWEEN '013' AND '999') OR (A.INIT_LN_TRM_CD = 'XXX'))
                          THEN A.TRANS_AMT
                     ELSE 0
                     END) AS AGRCTR_NAMLTLN_AMT,
            SUM(CASE WHEN A.ES_CD <> '0101' AND A.INIT_LN_TRM_CD BETWEEN '001' AND '012'
                          THEN A.TRANS_AMT
                     ELSE 0
                     END) AS NON_AGRCTR_NASTLN_AMT,
            SUM(CASE WHEN A.ES_CD <> '0101' AND ((A.INIT_LN_TRM_CD BETWEEN '013' AND '999') OR (A.INIT_LN_TRM_CD = 'XXX'))
                          THEN A.TRANS_AMT
                     ELSE 0
                     END) AS NON_AGRCTR_NAMLTLN_AMT,
            SUM(CASE WHEN A.INIT_LN_TRM_CD IN ('001', '002', '003')
                          THEN A.TRANS_AMT
                     ELSE 0
                     END) AS INIT_TRM_LSTHN_3_MM_NALN_AMT,
            SUM(CASE WHEN A.INIT_LN_TRM_CD IN ('004', '005', '006')
                          THEN A.TRANS_AMT
                     ELSE 0
                     END) AS INIT_TRM_LSTHN_6_MM_NALN_AMT,
            SUM(CASE WHEN A.INIT_LN_TRM_CD IN ('007', '008', '009')
                          THEN A.TRANS_AMT
                     ELSE 0
                     END) AS INIT_TRM_LSTHN_9_MM_NALN_AMT,
            SUM(CASE WHEN A.INIT_LN_TRM_CD IN ('010', '011', '012')
                          THEN A.TRANS_AMT
                     ELSE 0
                     END) AS INIT_TRM_LSTHN_12_MM_NALN_AMT,
            SUM(CASE WHEN A.INIT_LN_TRM_CD = 'XXX' OR (A.INIT_LN_TRM_CD >= '013' AND A.INIT_LN_TRM_CD <> 'ZZZ')
                          THEN A.TRANS_AMT
                     ELSE 0
                     END) AS INIT_TRM_OVR_12_MM_NALN_AMT,
            SUM(CASE WHEN A.RMTRT_LN_TRM_CD IN ('001', '002', '003', 'ZZZ')
                          THEN A.TRANS_AMT
                     ELSE 0
                     END) AS RMTRT_LSTHN_3_MM_NALN_AMT,
            SUM(CASE WHEN A.RMTRT_LN_TRM_CD IN ('004', '005', '006')
                          THEN A.TRANS_AMT
                     ELSE 0
                     END) AS RMTRT_LSTHN_6_MM_NALN_AMT,
            SUM(CASE WHEN A.RMTRT_LN_TRM_CD IN ('007', '008', '009')
                          THEN A.TRANS_AMT
                     ELSE 0
                     END) AS RMTRT_LSTHN_9_MM_NALN_AMT,
            SUM(CASE WHEN A.RMTRT_LN_TRM_CD IN ('010', '011', '012')
                          THEN A.TRANS_AMT
                     ELSE 0
                     END) AS RMTRT_LSTHN_12_MM_NALN_AMT,
            SUM(CASE WHEN A.RMTRT_LN_TRM_CD = 'XXX' OR (A.RMTRT_LN_TRM_CD >= '013' AND A.RMTRT_LN_TRM_CD <> 'ZZZ')
                          THEN A.TRANS_AMT
                     ELSE 0
                     END) AS RMTRT_OVR_12_MM_NALN_AMT,
            COUNT(DISTINCT CASE WHEN A.ES_CD = '0101' AND A.INIT_LN_TRM_CD BETWEEN '001' AND '012'
                                     THEN A.LN_CNTR_NUM_INFO
                                ELSE NULL
                                END) AS AGRCTR_NASTLN_CCNT,
            COUNT(DISTINCT CASE WHEN A.ES_CD = '0101' AND ((A.INIT_LN_TRM_CD BETWEEN '013' AND '999') OR (A.INIT_LN_TRM_CD = 'XXX'))
                                     THEN A.LN_CNTR_NUM_INFO
                                ELSE NULL
                                END) AS AGRCTR_NAMLTLN_CCNT,
            COUNT(DISTINCT CASE WHEN A.ES_CD <> '0101' AND A.INIT_LN_TRM_CD BETWEEN '001' AND '012'
                                     THEN A.LN_CNTR_NUM_INFO
                                ELSE NULL
                                END) AS NON_AGRCTR_NASTLN_CCNT,
            COUNT(DISTINCT CASE WHEN A.ES_CD <> '0101' AND ((A.INIT_LN_TRM_CD BETWEEN '013' AND '999') OR (A.INIT_LN_TRM_CD = 'XXX'))
                                     THEN A.LN_CNTR_NUM_INFO
                                ELSE NULL
                                END) AS NON_AGRCTR_NAMLTLN_CCNT,
            COUNT(DISTINCT CASE WHEN A.INIT_LN_TRM_CD IN ('001', '002', '003')
                                     THEN A.LN_CNTR_NUM_INFO
                                ELSE NULL
                                END) AS INIT_TRM_LSTHN_3_MM_NALN_CCNT,
            COUNT(DISTINCT CASE WHEN A.INIT_LN_TRM_CD IN ('004', '005', '006')
                                     THEN A.LN_CNTR_NUM_INFO
                                ELSE NULL
                                END) AS INIT_TRM_LSTHN_6_MM_NALN_CCNT,
            COUNT(DISTINCT CASE WHEN A.INIT_LN_TRM_CD IN ('007', '008', '009')
                                     THEN A.LN_CNTR_NUM_INFO
                                ELSE NULL
                                END) AS INIT_TRM_LSTHN_9_MM_NALN_CCNT,
            COUNT(DISTINCT CASE WHEN A.INIT_LN_TRM_CD IN ('010', '011', '012')
                                     THEN A.LN_CNTR_NUM_INFO
                                ELSE NULL
                                END) AS INIT_TRM_LSTHN_12_MM_NALN_CCNT,
            COUNT(DISTINCT CASE WHEN A.INIT_LN_TRM_CD = 'XXX' OR (A.INIT_LN_TRM_CD >= '013' AND A.INIT_LN_TRM_CD <> 'ZZZ')
                                     THEN A.LN_CNTR_NUM_INFO
                                ELSE NULL
                                END) AS INIT_TRM_OVR_12_MM_NALN_CCNT,
            COUNT(DISTINCT CASE WHEN A.RMTRT_LN_TRM_CD IN ('001', '002', '003', 'ZZZ')
                                     THEN A.LN_CNTR_NUM_INFO
                                ELSE NULL
                                END) AS RMTRT_LSTHN_3_MM_NALN_CCNT,
            COUNT(DISTINCT CASE WHEN A.RMTRT_LN_TRM_CD IN ('004', '005', '006')
                                     THEN A.LN_CNTR_NUM_INFO
                                ELSE NULL
                                END) AS RMTRT_LSTHN_6_MM_NALN_CCNT,
            COUNT(DISTINCT CASE WHEN A.RMTRT_LN_TRM_CD IN ('007', '008', '009')
                                     THEN A.LN_CNTR_NUM_INFO
                                ELSE NULL
                                END) AS RMTRT_LSTHN_9_MM_NALN_CCNT,
            COUNT(DISTINCT CASE WHEN A.RMTRT_LN_TRM_CD IN ('010', '011', '012')
                                     THEN A.LN_CNTR_NUM_INFO
                                ELSE NULL
                                END) AS RMTRT_LSTHN_12_MM_NALN_CCNT,
            COUNT(DISTINCT CASE WHEN A.RMTRT_LN_TRM_CD = 'XXX' OR (A.RMTRT_LN_TRM_CD >= '013' AND A.RMTRT_LN_TRM_CD <> 'ZZZ')
                                     THEN A.LN_CNTR_NUM_INFO
                                ELSE NULL
                                END) AS RMTRT_OVR_12_MM_NALN_CCNT
     FROM   TM21_DDLY_CUST_CR_TRANS_A A
     WHERE  A.BAS_DAY = TO_CHAR(TO_DATE(v_st_date_01, 'YYYYMMDD') -2, 'YYYYMMDD')
     AND    A.RPLC_DATA_YN = 'N'
     AND    A.LN_TRANS_TYP_CD = '1'
     AND    A.TRANS_AMT > 0
     GROUP BY A.BAS_DAY, A.PCF_ID
    ),
    LOAN_2 AS
    (SELECT A.BAS_DAY,
            A.PCF_ID,
            SUM(CASE WHEN A.ES_CD = '0101' AND ((A.RMTRT_LN_TRM_CD BETWEEN '001' AND '012') OR (A.RMTRT_LN_TRM_CD = 'ZZZ'))
                          THEN A.LN_BAL
                     ELSE 0
                     END) AS AGRCTR_SHRT_TRM_LN_BAL,
            SUM(CASE WHEN A.ES_CD = '0101' AND ((A.RMTRT_LN_TRM_CD BETWEEN '013' AND '999') OR (A.RMTRT_LN_TRM_CD = 'XXX'))
                          THEN A.LN_BAL
                     ELSE 0
                     END) AS AGRCTR_MED_LT_LN_BAL,
            SUM(CASE WHEN A.ES_CD <> '0101' AND ((A.RMTRT_LN_TRM_CD BETWEEN '001' AND '012') OR (A.RMTRT_LN_TRM_CD = 'ZZZ'))
                          THEN A.LN_BAL
                     ELSE 0
                     END) AS NON_AGRCTR_SHRT_TRM_LN_BAL,
            SUM(CASE WHEN A.ES_CD <> '0101' AND ((A.RMTRT_LN_TRM_CD BETWEEN '013' AND '999') OR (A.RMTRT_LN_TRM_CD = 'XXX'))
                          THEN A.LN_BAL
                     ELSE 0
                     END) AS NON_AGRCTR_MED_LT_LN_BAL,
            SUM(CASE WHEN A.INIT_LN_TRM_CD IN ('001', '002', '003')
                          THEN A.LN_BAL
                     ELSE 0
                     END) AS INIT_TRM_LSTHN_3_MM_LN_BAL,
            SUM(CASE WHEN A.INIT_LN_TRM_CD IN ('004', '005', '006')
                          THEN A.LN_BAL
                     ELSE 0
                     END) AS INIT_TRM_LSTHN_6_MM_LN_BAL,
            SUM(CASE WHEN A.INIT_LN_TRM_CD IN ('007', '008', '009')
                          THEN A.LN_BAL
                     ELSE 0
                     END) AS INIT_TRM_LSTHN_9_MM_LN_BAL,
            SUM(CASE WHEN A.INIT_LN_TRM_CD IN ('010', '011', '012')
                          THEN A.LN_BAL
                     ELSE 0
                     END) AS INIT_TRM_LSTHN_12_MM_LN_BAL,
            SUM(CASE WHEN A.INIT_LN_TRM_CD = 'XXX' OR (A.INIT_LN_TRM_CD >= '013' AND A.INIT_LN_TRM_CD <> 'ZZZ')
                          THEN A.LN_BAL
                     ELSE 0
                     END) AS INIT_TRM_OVR_12_MM_LN_BAL,
            SUM(CASE WHEN A.RMTRT_LN_TRM_CD IN ('001', '002', '003', 'ZZZ')
                          THEN A.LN_BAL
                     ELSE 0
                     END) AS RMTRT_LSTHN_3_MM_LN_BAL,
            SUM(CASE WHEN A.RMTRT_LN_TRM_CD IN ('004', '005', '006')
                          THEN A.LN_BAL
                     ELSE 0
                     END) AS RMTRT_LSTHN_6_MM_LN_BAL,
            SUM(CASE WHEN A.RMTRT_LN_TRM_CD IN ('007', '008', '009')
                          THEN A.LN_BAL
                     ELSE 0
                     END) AS RMTRT_LSTHN_9_MM_LN_BAL,
            SUM(CASE WHEN A.RMTRT_LN_TRM_CD IN ('010', '011', '012')
                          THEN A.LN_BAL
                     ELSE 0
                     END) AS RMTRT_LSTHN_12_MM_LN_BAL,
            SUM(CASE WHEN A.RMTRT_LN_TRM_CD = 'XXX' OR (A.RMTRT_LN_TRM_CD >= '013' AND A.RMTRT_LN_TRM_CD <> 'ZZZ')
                          THEN A.LN_BAL
                     ELSE 0
                     END) AS RMTRT_OVR_12_MM_LN_BAL,
            COUNT(DISTINCT CASE WHEN A.ES_CD = '0101' AND ((A.RMTRT_LN_TRM_CD BETWEEN '001' AND '012') OR A.RMTRT_LN_TRM_CD = 'ZZZ')
                                     THEN A.LN_CNTR_NUM_INFO
                                ELSE NULL
                                END) AS AGRCTR_SHRT_TRM_LN_CCNT,
            COUNT(DISTINCT CASE WHEN A.ES_CD = '0101' AND ((A.RMTRT_LN_TRM_CD BETWEEN '013' AND '999') OR (A.RMTRT_LN_TRM_CD = 'XXX'))
                                     THEN A.LN_CNTR_NUM_INFO
                                ELSE NULL
                                END) AS AGRCTR_MED_LT_LN_CCNT,
            COUNT(DISTINCT CASE WHEN A.ES_CD <> '0101' AND ((A.RMTRT_LN_TRM_CD BETWEEN '001' AND '012') OR A.RMTRT_LN_TRM_CD = 'ZZZ')
                                     THEN A.LN_CNTR_NUM_INFO
                                ELSE NULL
                                END) AS NON_AGRCTR_SHRT_TRM_LN_CCNT,
            COUNT(DISTINCT CASE WHEN A.ES_CD <> '0101' AND ((A.RMTRT_LN_TRM_CD BETWEEN '013' AND '999') OR (A.RMTRT_LN_TRM_CD = 'XXX'))
                                     THEN A.LN_CNTR_NUM_INFO
                                ELSE NULL
                                END) AS NON_AGRCTR_MED_LT_LN_CCNT,
            COUNT(DISTINCT CASE WHEN A.INIT_LN_TRM_CD IN ('001', '002', '003')
                                     THEN A.LN_CNTR_NUM_INFO
                                ELSE NULL
                                END) AS INIT_TRM_LSTHN_3_MM_LN_CCNT,
            COUNT(DISTINCT CASE WHEN A.INIT_LN_TRM_CD IN ('001', '002', '003')
                                     THEN CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO ELSE A.CUST_ID END
                                ELSE NULL
                                END) AS INIT_TRM_LSTHN_3_MM_BRWR_CNT,
            COUNT(DISTINCT CASE WHEN A.INIT_LN_TRM_CD IN ('004', '005', '006')
                                     THEN A.LN_CNTR_NUM_INFO
                                ELSE NULL
                                END) AS INIT_TRM_LSTHN_6_MM_LN_CCNT,
            COUNT(DISTINCT CASE WHEN A.INIT_LN_TRM_CD IN ('004', '005', '006')
                                     THEN CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO ELSE A.CUST_ID END
                                ELSE NULL
                                END) AS INIT_TRM_LSTHN_6_MM_BRWR_CNT,
            COUNT(DISTINCT CASE WHEN A.INIT_LN_TRM_CD IN ('007', '008', '009')
                                     THEN A.LN_CNTR_NUM_INFO
                                ELSE NULL
                                END) AS INIT_TRM_LSTHN_9_MM_LN_CCNT,
            COUNT(DISTINCT CASE WHEN A.INIT_LN_TRM_CD IN ('007', '008', '009')
                                     THEN CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO ELSE A.CUST_ID END
                                ELSE NULL
                                END) AS INIT_TRM_LSTHN_9_MM_BRWR_CNT,
            COUNT(DISTINCT CASE WHEN A.INIT_LN_TRM_CD IN ('010', '011', '012')
                                     THEN A.LN_CNTR_NUM_INFO
                                ELSE NULL
                                END) AS INIT_TRM_LSTHN_12_MM_LN_CCNT,
            COUNT(DISTINCT CASE WHEN A.INIT_LN_TRM_CD IN ('010', '011', '012')
                                     THEN CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO ELSE A.CUST_ID END
                                ELSE NULL
                                END) AS INIT_TRM_LSTHN_12_MM_BRWR_CNT,
            COUNT(DISTINCT CASE WHEN (A.INIT_LN_TRM_CD = 'XXX' OR (A.INIT_LN_TRM_CD >= '013' AND A.INIT_LN_TRM_CD <> 'ZZZ'))
                                     THEN A.LN_CNTR_NUM_INFO
                                ELSE NULL
                                END) AS INIT_TRM_OVR_12_MM_LN_CCNT,
            COUNT(DISTINCT CASE WHEN (A.INIT_LN_TRM_CD = 'XXX' OR (A.INIT_LN_TRM_CD >= '013' AND A.INIT_LN_TRM_CD <> 'ZZZ'))
                                     THEN CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO ELSE A.CUST_ID END
                                ELSE NULL
                                END) AS INIT_TRM_OVR_12_MM_BRWR_CNT,
            COUNT(DISTINCT CASE WHEN A.RMTRT_LN_TRM_CD IN ('001', '002', '003', 'ZZZ')
                                     THEN A.LN_CNTR_NUM_INFO
                                ELSE NULL
                                END) AS RMTRT_LSTHN_3_MM_LN_CCNT,
            COUNT(DISTINCT CASE WHEN A.RMTRT_LN_TRM_CD IN ('001', '002', '003', 'ZZZ')
                                     THEN CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO ELSE A.CUST_ID END
                                ELSE NULL
                                END) AS RMTRT_LSTHN_3_MM_BRWR_CNT,
            COUNT(DISTINCT CASE WHEN A.RMTRT_LN_TRM_CD IN ('004', '005', '006')
                                     THEN A.LN_CNTR_NUM_INFO
                                ELSE NULL
                                END) AS RMTRT_LSTHN_6_MM_LN_CCNT,
            COUNT(DISTINCT CASE WHEN A.RMTRT_LN_TRM_CD IN ('004', '005', '006')
                                     THEN CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO ELSE A.CUST_ID END
                                ELSE NULL
                                END) AS RMTRT_LSTHN_6_MM_BRWR_CNT,
            COUNT(DISTINCT CASE WHEN A.RMTRT_LN_TRM_CD IN ('007', '008', '009')
                                     THEN A.LN_CNTR_NUM_INFO
                                ELSE NULL
                                END) AS RMTRT_LSTHN_9_MM_LN_CCNT,
            COUNT(DISTINCT CASE WHEN A.RMTRT_LN_TRM_CD IN ('007', '008', '009')
                                     THEN CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO ELSE A.CUST_ID END
                                ELSE NULL
                                END) AS RMTRT_LSTHN_9_MM_BRWR_CNT,
            COUNT(DISTINCT CASE WHEN A.RMTRT_LN_TRM_CD IN ('010', '011', '012')
                                     THEN A.LN_CNTR_NUM_INFO
                                ELSE NULL
                                END) AS RMTRT_LSTHN_12_MM_LN_CCNT,
            COUNT(DISTINCT CASE WHEN A.RMTRT_LN_TRM_CD IN ('010', '011', '012')
                                     THEN CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO ELSE A.CUST_ID END
                                ELSE NULL
                                END) AS RMTRT_LSTHN_12_MM_BRWR_CNT,
            COUNT(DISTINCT CASE WHEN (A.RMTRT_LN_TRM_CD = 'XXX' OR (A.RMTRT_LN_TRM_CD >= '013' AND A.RMTRT_LN_TRM_CD <> 'ZZZ'))
                                     THEN A.LN_CNTR_NUM_INFO
                                ELSE NULL
                                END) AS RMTRT_OVR_12_MM_LN_CCNT,
            COUNT(DISTINCT CASE WHEN (A.RMTRT_LN_TRM_CD = 'XXX' OR (A.RMTRT_LN_TRM_CD >= '013' AND A.RMTRT_LN_TRM_CD <> 'ZZZ'))
                                     THEN CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO ELSE A.CUST_ID END
                                ELSE NULL
                                END) AS RMTRT_OVR_12_MM_BRWR_CNT
     FROM   TM21_DDLY_CUST_CR_TRANS_V A
     WHERE  A.BAS_DAY = TO_CHAR(TO_DATE(v_st_date_01, 'YYYYMMDD') -2, 'YYYYMMDD')
     AND    A.LN_BAL > 0
     GROUP BY A.BAS_DAY, A.PCF_ID
    )
    SELECT A.BAS_DAY
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
          ,D.AGRCTR_SHRT_TRM_LN_BAL
          ,D.AGRCTR_MED_LT_LN_BAL
          ,D.AGRCTR_SHRT_TRM_LN_CCNT
          ,D.AGRCTR_MED_LT_LN_CCNT
          ,D.NON_AGRCTR_SHRT_TRM_LN_BAL
          ,D.NON_AGRCTR_MED_LT_LN_BAL
          ,D.NON_AGRCTR_SHRT_TRM_LN_CCNT
          ,D.NON_AGRCTR_MED_LT_LN_CCNT
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
          ,D.INIT_TRM_LSTHN_3_MM_LN_BAL
          ,D.INIT_TRM_LSTHN_3_MM_LN_CCNT
          ,D.INIT_TRM_LSTHN_3_MM_BRWR_CNT
          ,D.INIT_TRM_LSTHN_6_MM_LN_BAL
          ,D.INIT_TRM_LSTHN_6_MM_LN_CCNT
          ,D.INIT_TRM_LSTHN_6_MM_BRWR_CNT
          ,D.INIT_TRM_LSTHN_9_MM_LN_BAL
          ,D.INIT_TRM_LSTHN_9_MM_LN_CCNT
          ,D.INIT_TRM_LSTHN_9_MM_BRWR_CNT
          ,D.INIT_TRM_LSTHN_12_MM_LN_BAL
          ,D.INIT_TRM_LSTHN_12_MM_LN_CCNT
          ,D.INIT_TRM_LSTHN_12_MM_BRWR_CNT
          ,D.INIT_TRM_OVR_12_MM_LN_BAL
          ,D.INIT_TRM_OVR_12_MM_LN_CCNT
          ,D.INIT_TRM_OVR_12_MM_BRWR_CNT
          ,D.RMTRT_LSTHN_3_MM_LN_BAL
          ,D.RMTRT_LSTHN_3_MM_LN_CCNT
          ,D.RMTRT_LSTHN_3_MM_BRWR_CNT
          ,D.RMTRT_LSTHN_6_MM_LN_BAL
          ,D.RMTRT_LSTHN_6_MM_LN_CCNT
          ,D.RMTRT_LSTHN_6_MM_BRWR_CNT
          ,D.RMTRT_LSTHN_9_MM_LN_BAL
          ,D.RMTRT_LSTHN_9_MM_LN_CCNT
          ,D.RMTRT_LSTHN_9_MM_BRWR_CNT
          ,D.RMTRT_LSTHN_12_MM_LN_BAL
          ,D.RMTRT_LSTHN_12_MM_LN_CCNT
          ,D.RMTRT_LSTHN_12_MM_BRWR_CNT
          ,D.RMTRT_OVR_12_MM_LN_BAL
          ,D.RMTRT_OVR_12_MM_LN_CCNT
          ,D.RMTRT_OVR_12_MM_BRWR_CNT
          ,SYSTIMESTAMP
    FROM   FULL_SET A LEFT OUTER JOIN DEPOSIT B
                                   ON A.BAS_DAY = B.BAS_DAY
                                  AND A.PCF_ID  = B.PCF_ID
                      LEFT OUTER JOIN LOAN_1 C
                                   ON A.BAS_DAY = C.BAS_DAY
                                  AND A.PCF_ID  = C.PCF_ID
                      LEFT OUTER JOIN LOAN_2 D
                                   ON A.BAS_DAY = D.BAS_DAY
                                  AND A.PCF_ID  = D.PCF_ID;

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
            FROM TM27_DDLY_PCF_LN_DPST_SMR_A T1
           WHERE T1.BAS_DAY = loop_bas_day.BAS_DAY
           AND   EXISTS (SELECT *
                         FROM   (SELECT PCF_ID
                                 FROM   TBSM_INPT_RPT_SUBMIT_L
                                 WHERE  BTCH_BAS_DAY     = v_st_date_01
                                 AND    INPT_RPT_ID      IN ('G32_012_TTGS', 'G32_003_TTGS')
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
    --  1.4 Inserting Data verification results
    ----------------------------------------------------------------------------
    FOR loop_bas_day in v_sbmt_day(v_wk_date,v_st_date_01,v_end_date_01)
    LOOP
    BEGIN

          INSERT /*+ APPEND PARALLEL(TM27_DDLY_PCF_LN_DPST_SMR_A, 4) */ INTO TM27_DDLY_PCF_LN_DPST_SMR_A
          (   BAS_DAY
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
          (SELECT  T1.BAS_DAY, T1.PCF_ID
           FROM   (SELECT T1.BAS_DAY, T1.PCF_ID
                   FROM   (SELECT BAS_DAY, PCF_ID
                           FROM   TM22_DDLY_CUST_DPST_TRANS_A
                           WHERE  BAS_DAY = loop_bas_day.BAS_DAY
                           GROUP BY BAS_DAY, PCF_ID
                          ) T1 INNER JOIN (SELECT PCF_ID
                                           FROM   TBSM_INPT_RPT_SUBMIT_L
                                           WHERE  BTCH_BAS_DAY     = v_st_date_01
                                           AND    INPT_RPT_ID      IN ('G32_012_TTGS', 'G32_003_TTGS')
                                           AND    DATA_BAS_DAY    <= loop_bas_day.BAS_DAY
                                           GROUP BY PCF_ID
                                          ) T2
                                       ON T2.PCF_ID = T1.PCF_ID

                   UNION

                   SELECT T1.BAS_DAY, T1.PCF_ID
                   FROM   (SELECT BAS_DAY, PCF_ID
                           FROM   TM21_DDLY_CUST_CR_TRANS_A
                           WHERE  BAS_DAY = loop_bas_day.BAS_DAY
                           GROUP BY BAS_DAY, PCF_ID
                          ) T1 INNER JOIN (SELECT PCF_ID
                                           FROM   TBSM_INPT_RPT_SUBMIT_L
                                           WHERE  BTCH_BAS_DAY     = v_st_date_01
                                           AND    INPT_RPT_ID      IN ('G32_012_TTGS', 'G32_003_TTGS')
                                           AND    DATA_BAS_DAY    <= loop_bas_day.BAS_DAY
                                           GROUP BY PCF_ID
                                          ) T2
                                       ON T2.PCF_ID = T1.PCF_ID
                  ) T1
          GROUP BY T1.BAS_DAY, T1.PCF_ID
          ),
          TM21_DDLY_CUST_CR_TRANS_V AS
          (SELECT  A.BAS_DAY
                  ,A.PCF_ID
                  ,A.RPLC_DATA_YN
                  ,A.LN_CNTR_NUM_INFO
                  ,MAX(A.CUST_ID             ) AS CUST_ID
                  ,MAX(A.CUST_NM             ) AS CUST_NM
                  ,MAX(A.CUST_TYP_CD         ) AS CUST_TYP_CD
                  ,MAX(A.INIT_LN_TRM_CD      ) AS INIT_LN_TRM_CD
                  ,MAX(A.RMTRT_LN_TRM_CD     ) AS RMTRT_LN_TRM_CD
                  ,MAX(A.ES_CD               ) AS ES_CD
                  ,MAX(A.COLL_TYP_INFO       ) AS COLL_TYP_INFO
                  ,MAX(A.COLL_TYP_CD         ) AS COLL_TYP_CD
                  ,MAX(A.LN_TRANS_TYP_CD     ) AS LN_TRANS_TYP_CD
                  ,MAX(A.OLD_NORML_DBT_GRP_CD) AS OLD_NORML_DBT_GRP_CD
                  ,MAX(A.OLD_OVD_DBT_GRP_CD  ) AS OLD_OVD_DBT_GRP_CD
                  ,MAX(A.NEW_NORML_DBT_GRP_CD) AS NEW_NORML_DBT_GRP_CD
                  ,MAX(A.NEW_OVD_DBT_GRP_CD  ) AS NEW_OVD_DBT_GRP_CD
                  ,MAX(A.LN_CNTR_AMT         ) AS LN_CNTR_AMT
                  ,MAX(A.LN_BAL              ) AS LN_BAL
                  ,SUM(A.TRANS_AMT           ) AS TRANS_AMT
                  ,MAX(A.COLL_VAL_AMT        ) AS COLL_VAL_AMT
                  ,MAX(A.SPEC_PRVS_AMT       ) AS SPEC_PRVS_AMT
                  ,MAX(A.OPN_DAY             ) AS OPN_DAY
                  ,MAX(A.MTRT_DAY            ) AS MTRT_DAY
                  ,MAX(A.ACTL_IR             ) AS ACTL_IR
           FROM   TM21_DDLY_CUST_CR_TRANS_A A INNER JOIN (SELECT PCF_ID
                                                          FROM   TBSM_INPT_RPT_SUBMIT_L
                                                          WHERE  BTCH_BAS_DAY     = v_st_date_01
                                                          AND    INPT_RPT_ID      IN ('G32_012_TTGS', 'G32_003_TTGS')
                                                          AND    DATA_BAS_DAY    <= loop_bas_day.BAS_DAY
                                                          GROUP BY PCF_ID
                                                         ) B
                                                      ON B.PCF_ID = A.PCF_ID
           WHERE  A.BAS_DAY = loop_bas_day.BAS_DAY
           GROUP BY A.BAS_DAY
                   ,A.PCF_ID
                   ,A.RPLC_DATA_YN
                   ,A.LN_CNTR_NUM_INFO
          ),
          DEPOSIT AS
          (SELECT T1.*
           FROM   (SELECT A.BAS_DAY,
                          A.PCF_ID,
                          SUM(CASE WHEN A.INIT_DPST_TRM_CD IN ('000', '001', 'ZZZ') THEN A.DPST_BAL ELSE 0 END) AS INIT_TRM_LSTHN_1_MM_DPST_BAL,
                          SUM(CASE WHEN A.INIT_DPST_TRM_CD IN ('002', '003') THEN A.DPST_BAL ELSE 0 END) AS INIT_TRM_LSTHN_3_MM_DPST_BAL,
                          SUM(CASE WHEN A.INIT_DPST_TRM_CD IN ('004', '005', '006') THEN A.DPST_BAL ELSE 0 END) AS INIT_TRM_LSTHN_6_MM_DPST_BAL,
                          SUM(CASE WHEN A.INIT_DPST_TRM_CD IN ('007', '008', '009') THEN A.DPST_BAL ELSE 0 END) AS INIT_TRM_LSTHN_9_MM_DPST_BAL,
                          SUM(CASE WHEN A.INIT_DPST_TRM_CD IN ('010', '011', '012') THEN A.DPST_BAL ELSE 0 END) AS INIT_TRM_LSTHN_12_MM_DPST_BAL,
                          SUM(CASE WHEN A.INIT_DPST_TRM_CD = 'XXX' OR (A.INIT_DPST_TRM_CD >= '013' AND A.INIT_DPST_TRM_CD <> 'ZZZ') THEN DPST_BAL ELSE 0 END) AS INIT_TRM_OVR_12_MM_DPST_BAL,
                          SUM(CASE WHEN A.RMTRT_DPST_TRM_CD IN ('000', '001', 'ZZZ') THEN A.DPST_BAL ELSE 0 END) AS RMTRT_LSTHN_1_MM_DPST_BAL,
                          SUM(CASE WHEN A.RMTRT_DPST_TRM_CD IN ('002', '003') THEN A.DPST_BAL ELSE 0 END) AS RMTRT_LSTHN_3_MM_DPST_BAL,
                          SUM(CASE WHEN A.RMTRT_DPST_TRM_CD IN ('004', '005', '006') THEN A.DPST_BAL ELSE 0 END) AS RMTRT_LSTHN_6_MM_DPST_BAL,
                          SUM(CASE WHEN A.RMTRT_DPST_TRM_CD IN ('007', '008', '009') THEN A.DPST_BAL ELSE 0 END) AS RMTRT_LSTHN_9_MM_DPST_BAL,
                          SUM(CASE WHEN A.RMTRT_DPST_TRM_CD IN ('010', '011', '012') THEN A.DPST_BAL ELSE 0 END) AS RMTRT_LSTHN_12_MM_DPST_BAL,
                          SUM(CASE WHEN A.RMTRT_DPST_TRM_CD = 'XXX' OR (A.RMTRT_DPST_TRM_CD >= '013' AND A.RMTRT_DPST_TRM_CD <> 'ZZZ') THEN A.DPST_BAL ELSE 0 END) AS RMTRT_OVR_12_MM_DPST_BAL,
                          COUNT(DISTINCT CASE WHEN A.INIT_DPST_TRM_CD IN ('000', '001', 'ZZZ') AND A.DPST_BAL > 0 THEN A.CUST_ID ELSE NULL END) AS INIT_TRM_LSTHN_1_MM_DPSTR_CNT,
                          COUNT(DISTINCT CASE WHEN A.INIT_DPST_TRM_CD IN ('002', '003') AND A.DPST_BAL > 0 THEN A.CUST_ID ELSE NULL END) AS INIT_TRM_LSTHN_3_MM_DPSTR_CNT,
                          COUNT(DISTINCT CASE WHEN A.INIT_DPST_TRM_CD IN ('004', '005', '006') AND A.DPST_BAL > 0 THEN A.CUST_ID ELSE NULL END) AS INIT_TRM_LSTHN_6_MM_DPSTR_CNT,
                          COUNT(DISTINCT CASE WHEN A.INIT_DPST_TRM_CD IN ('007', '008', '009') AND A.DPST_BAL > 0 THEN A.CUST_ID ELSE NULL END) AS INIT_TRM_LSTHN_9_MM_DPSTR_CNT,
                          COUNT(DISTINCT CASE WHEN A.INIT_DPST_TRM_CD IN ('010', '011', '012') AND A.DPST_BAL > 0 THEN A.CUST_ID ELSE NULL END) AS INIT_TRM_LSTHN_12_MM_DPSTR_CNT,
                          COUNT(DISTINCT CASE WHEN (A.INIT_DPST_TRM_CD = 'XXX' OR (A.INIT_DPST_TRM_CD >= '013' AND INIT_DPST_TRM_CD <> 'ZZZ')) AND A.DPST_BAL > 0 THEN A.CUST_ID ELSE NULL END) AS INIT_TRM_OVR_12_MM_DPSTR_CNT,
                          COUNT(DISTINCT CASE WHEN A.RMTRT_DPST_TRM_CD IN ('000', '001', 'ZZZ') AND A.DPST_BAL > 0 THEN A.CUST_ID ELSE NULL END) AS RMTRT_LSTHN_1_MM_DPSTR_CNT,
                          COUNT(DISTINCT CASE WHEN A.RMTRT_DPST_TRM_CD IN ('002', '003') AND A.DPST_BAL > 0 THEN A.CUST_ID ELSE NULL END) AS RMTRT_LSTHN_3_MM_DPSTR_CNT,
                          COUNT(DISTINCT CASE WHEN A.RMTRT_DPST_TRM_CD IN ('004', '005', '006') AND A.DPST_BAL > 0 THEN A.CUST_ID ELSE NULL END) AS RMTRT_LSTHN_6_MM_DPSTR_CNT,
                          COUNT(DISTINCT CASE WHEN A.RMTRT_DPST_TRM_CD IN ('007', '008', '009') AND A.DPST_BAL > 0 THEN A.CUST_ID ELSE NULL END) AS RMTRT_LSTHN_9_MM_DPSTR_CNT,
                          COUNT(DISTINCT CASE WHEN A.RMTRT_DPST_TRM_CD IN ('010', '011', '012') AND A.DPST_BAL > 0 THEN A.CUST_ID ELSE NULL END) AS RMTRT_LSTHN_12_MM_DPSTR_CNT,
                          COUNT(DISTINCT CASE WHEN (A.RMTRT_DPST_TRM_CD = 'XXX' OR (RMTRT_DPST_TRM_CD >= '013' AND RMTRT_DPST_TRM_CD <> 'ZZZ')) AND A.DPST_BAL > 0 THEN A.CUST_ID ELSE NULL END) AS RMTRT_OVR_12_MM_DPSTR_CNT
                   FROM   TM22_DDLY_CUST_DPST_TRANS_A A
                   WHERE  A.BAS_DAY = loop_bas_day.BAS_DAY
                   GROUP BY A.BAS_DAY, A.PCF_ID
                  ) T1 INNER JOIN (SELECT PCF_ID
                                   FROM   TBSM_INPT_RPT_SUBMIT_L
                                   WHERE  BTCH_BAS_DAY     = v_st_date_01
                                   AND    INPT_RPT_ID      IN ('G32_012_TTGS', 'G32_003_TTGS')
                                   AND    DATA_BAS_DAY    <= loop_bas_day.BAS_DAY
                                   GROUP BY PCF_ID
                                  ) T2
                               ON T2.PCF_ID = T1.PCF_ID

          ),
          LOAN_1 AS
          (SELECT A.BAS_DAY,
                  A.PCF_ID,
                  SUM(CASE WHEN A.ES_CD = '0101' AND A.INIT_LN_TRM_CD BETWEEN '001' AND '012'
                                THEN A.TRANS_AMT
                           ELSE 0
                           END) AS AGRCTR_NASTLN_AMT,
                  SUM(CASE WHEN A.ES_CD = '0101' AND ((A.INIT_LN_TRM_CD BETWEEN '013' AND '999') OR (A.INIT_LN_TRM_CD = 'XXX'))
                                THEN A.TRANS_AMT
                           ELSE 0
                           END) AS AGRCTR_NAMLTLN_AMT,
                  SUM(CASE WHEN A.ES_CD <> '0101' AND A.INIT_LN_TRM_CD BETWEEN '001' AND '012'
                                THEN A.TRANS_AMT
                           ELSE 0
                           END) AS NON_AGRCTR_NASTLN_AMT,
                  SUM(CASE WHEN A.ES_CD <> '0101' AND ((A.INIT_LN_TRM_CD BETWEEN '013' AND '999') OR (A.INIT_LN_TRM_CD = 'XXX'))
                                THEN A.TRANS_AMT
                           ELSE 0
                           END) AS NON_AGRCTR_NAMLTLN_AMT,
                  SUM(CASE WHEN A.INIT_LN_TRM_CD IN ('001', '002', '003')
                                THEN A.TRANS_AMT
                           ELSE 0
                           END) AS INIT_TRM_LSTHN_3_MM_NALN_AMT,
                  SUM(CASE WHEN A.INIT_LN_TRM_CD IN ('004', '005', '006')
                                THEN A.TRANS_AMT
                           ELSE 0
                           END) AS INIT_TRM_LSTHN_6_MM_NALN_AMT,
                  SUM(CASE WHEN A.INIT_LN_TRM_CD IN ('007', '008', '009')
                                THEN A.TRANS_AMT
                           ELSE 0
                           END) AS INIT_TRM_LSTHN_9_MM_NALN_AMT,
                  SUM(CASE WHEN A.INIT_LN_TRM_CD IN ('010', '011', '012')
                                THEN A.TRANS_AMT
                           ELSE 0
                           END) AS INIT_TRM_LSTHN_12_MM_NALN_AMT,
                  SUM(CASE WHEN A.INIT_LN_TRM_CD = 'XXX' OR (A.INIT_LN_TRM_CD >= '013' AND A.INIT_LN_TRM_CD <> 'ZZZ')
                                THEN A.TRANS_AMT
                           ELSE 0
                           END) AS INIT_TRM_OVR_12_MM_NALN_AMT,
                  SUM(CASE WHEN A.RMTRT_LN_TRM_CD IN ('001', '002', '003', 'ZZZ')
                                THEN A.TRANS_AMT
                           ELSE 0
                           END) AS RMTRT_LSTHN_3_MM_NALN_AMT,
                  SUM(CASE WHEN A.RMTRT_LN_TRM_CD IN ('004', '005', '006')
                                THEN A.TRANS_AMT
                           ELSE 0
                           END) AS RMTRT_LSTHN_6_MM_NALN_AMT,
                  SUM(CASE WHEN A.RMTRT_LN_TRM_CD IN ('007', '008', '009')
                                THEN A.TRANS_AMT
                           ELSE 0
                           END) AS RMTRT_LSTHN_9_MM_NALN_AMT,
                  SUM(CASE WHEN A.RMTRT_LN_TRM_CD IN ('010', '011', '012')
                                THEN A.TRANS_AMT
                           ELSE 0
                           END) AS RMTRT_LSTHN_12_MM_NALN_AMT,
                  SUM(CASE WHEN A.RMTRT_LN_TRM_CD = 'XXX' OR (A.RMTRT_LN_TRM_CD >= '013' AND A.RMTRT_LN_TRM_CD <> 'ZZZ')
                                THEN A.TRANS_AMT
                           ELSE 0
                           END) AS RMTRT_OVR_12_MM_NALN_AMT,
                  COUNT(DISTINCT CASE WHEN A.ES_CD = '0101' AND A.INIT_LN_TRM_CD BETWEEN '001' AND '012'
                                           THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS AGRCTR_NASTLN_CCNT,
                  COUNT(DISTINCT CASE WHEN A.ES_CD = '0101' AND ((A.INIT_LN_TRM_CD BETWEEN '013' AND '999') OR (A.INIT_LN_TRM_CD = 'XXX'))
                                           THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS AGRCTR_NAMLTLN_CCNT,
                  COUNT(DISTINCT CASE WHEN A.ES_CD <> '0101' AND A.INIT_LN_TRM_CD BETWEEN '001' AND '012'
                                           THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS NON_AGRCTR_NASTLN_CCNT,
                  COUNT(DISTINCT CASE WHEN A.ES_CD <> '0101' AND ((A.INIT_LN_TRM_CD BETWEEN '013' AND '999') OR (A.INIT_LN_TRM_CD = 'XXX'))
                                           THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS NON_AGRCTR_NAMLTLN_CCNT,
                  COUNT(DISTINCT CASE WHEN A.INIT_LN_TRM_CD IN ('001', '002', '003')
                                           THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS INIT_TRM_LSTHN_3_MM_NALN_CCNT,
                  COUNT(DISTINCT CASE WHEN A.INIT_LN_TRM_CD IN ('004', '005', '006')
                                           THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS INIT_TRM_LSTHN_6_MM_NALN_CCNT,
                  COUNT(DISTINCT CASE WHEN A.INIT_LN_TRM_CD IN ('007', '008', '009')
                                           THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS INIT_TRM_LSTHN_9_MM_NALN_CCNT,
                  COUNT(DISTINCT CASE WHEN A.INIT_LN_TRM_CD IN ('010', '011', '012')
                                           THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS INIT_TRM_LSTHN_12_MM_NALN_CCNT,
                  COUNT(DISTINCT CASE WHEN A.INIT_LN_TRM_CD = 'XXX' OR (A.INIT_LN_TRM_CD >= '013' AND A.INIT_LN_TRM_CD <> 'ZZZ')
                                           THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS INIT_TRM_OVR_12_MM_NALN_CCNT,
                  COUNT(DISTINCT CASE WHEN A.RMTRT_LN_TRM_CD IN ('001', '002', '003', 'ZZZ')
                                           THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS RMTRT_LSTHN_3_MM_NALN_CCNT,
                  COUNT(DISTINCT CASE WHEN A.RMTRT_LN_TRM_CD IN ('004', '005', '006')
                                           THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS RMTRT_LSTHN_6_MM_NALN_CCNT,
                  COUNT(DISTINCT CASE WHEN A.RMTRT_LN_TRM_CD IN ('007', '008', '009')
                                           THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS RMTRT_LSTHN_9_MM_NALN_CCNT,
                  COUNT(DISTINCT CASE WHEN A.RMTRT_LN_TRM_CD IN ('010', '011', '012')
                                           THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS RMTRT_LSTHN_12_MM_NALN_CCNT,
                  COUNT(DISTINCT CASE WHEN A.RMTRT_LN_TRM_CD = 'XXX' OR (A.RMTRT_LN_TRM_CD >= '013' AND A.RMTRT_LN_TRM_CD <> 'ZZZ')
                                           THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS RMTRT_OVR_12_MM_NALN_CCNT
           FROM   TM21_DDLY_CUST_CR_TRANS_A A INNER JOIN (SELECT PCF_ID
                                                          FROM   TBSM_INPT_RPT_SUBMIT_L
                                                          WHERE  BTCH_BAS_DAY     = v_st_date_01
                                                          AND    INPT_RPT_ID      IN ('G32_012_TTGS', 'G32_003_TTGS')
                                                          AND    DATA_BAS_DAY    <= loop_bas_day.BAS_DAY
                                                          GROUP BY PCF_ID
                                                         ) B
                                                      ON B.PCF_ID = A.PCF_ID
           WHERE  A.BAS_DAY = loop_bas_day.BAS_DAY
           AND    A.RPLC_DATA_YN = 'N'
           AND    A.LN_TRANS_TYP_CD = '1'
           AND    A.TRANS_AMT > 0
           GROUP BY A.BAS_DAY, A.PCF_ID
          ),
          LOAN_2 AS
          (SELECT A.BAS_DAY,
                  A.PCF_ID,
                  SUM(CASE WHEN A.ES_CD = '0101' AND ((A.RMTRT_LN_TRM_CD BETWEEN '001' AND '012') OR (A.RMTRT_LN_TRM_CD = 'ZZZ'))
                                THEN A.LN_BAL
                           ELSE 0
                           END) AS AGRCTR_SHRT_TRM_LN_BAL,
                  SUM(CASE WHEN A.ES_CD = '0101' AND ((A.RMTRT_LN_TRM_CD BETWEEN '013' AND '999') OR (A.RMTRT_LN_TRM_CD = 'XXX'))
                                THEN A.LN_BAL
                           ELSE 0
                           END) AS AGRCTR_MED_LT_LN_BAL,
                  SUM(CASE WHEN A.ES_CD <> '0101' AND ((A.RMTRT_LN_TRM_CD BETWEEN '001' AND '012') OR (A.RMTRT_LN_TRM_CD = 'ZZZ'))
                                THEN A.LN_BAL
                           ELSE 0
                           END) AS NON_AGRCTR_SHRT_TRM_LN_BAL,
                  SUM(CASE WHEN A.ES_CD <> '0101' AND ((A.RMTRT_LN_TRM_CD BETWEEN '013' AND '999') OR (A.RMTRT_LN_TRM_CD = 'XXX'))
                                THEN A.LN_BAL
                           ELSE 0
                           END) AS NON_AGRCTR_MED_LT_LN_BAL,
                  SUM(CASE WHEN A.INIT_LN_TRM_CD IN ('001', '002', '003')
                                THEN A.LN_BAL
                           ELSE 0
                           END) AS INIT_TRM_LSTHN_3_MM_LN_BAL,
                  SUM(CASE WHEN A.INIT_LN_TRM_CD IN ('004', '005', '006')
                                THEN A.LN_BAL
                           ELSE 0
                           END) AS INIT_TRM_LSTHN_6_MM_LN_BAL,
                  SUM(CASE WHEN A.INIT_LN_TRM_CD IN ('007', '008', '009')
                                THEN A.LN_BAL
                           ELSE 0
                           END) AS INIT_TRM_LSTHN_9_MM_LN_BAL,
                  SUM(CASE WHEN A.INIT_LN_TRM_CD IN ('010', '011', '012')
                                THEN A.LN_BAL
                           ELSE 0
                           END) AS INIT_TRM_LSTHN_12_MM_LN_BAL,
                  SUM(CASE WHEN A.INIT_LN_TRM_CD = 'XXX' OR (A.INIT_LN_TRM_CD >= '013' AND A.INIT_LN_TRM_CD <> 'ZZZ')
                                THEN A.LN_BAL
                           ELSE 0
                           END) AS INIT_TRM_OVR_12_MM_LN_BAL,
                  SUM(CASE WHEN A.RMTRT_LN_TRM_CD IN ('001', '002', '003', 'ZZZ')
                                THEN A.LN_BAL
                           ELSE 0
                           END) AS RMTRT_LSTHN_3_MM_LN_BAL,
                  SUM(CASE WHEN A.RMTRT_LN_TRM_CD IN ('004', '005', '006')
                                THEN A.LN_BAL
                           ELSE 0
                           END) AS RMTRT_LSTHN_6_MM_LN_BAL,
                  SUM(CASE WHEN A.RMTRT_LN_TRM_CD IN ('007', '008', '009')
                                THEN A.LN_BAL
                           ELSE 0
                           END) AS RMTRT_LSTHN_9_MM_LN_BAL,
                  SUM(CASE WHEN A.RMTRT_LN_TRM_CD IN ('010', '011', '012')
                                THEN A.LN_BAL
                           ELSE 0
                           END) AS RMTRT_LSTHN_12_MM_LN_BAL,
                  SUM(CASE WHEN A.RMTRT_LN_TRM_CD = 'XXX' OR (A.RMTRT_LN_TRM_CD >= '013' AND A.RMTRT_LN_TRM_CD <> 'ZZZ')
                                THEN A.LN_BAL
                           ELSE 0
                           END) AS RMTRT_OVR_12_MM_LN_BAL,
                  COUNT(DISTINCT CASE WHEN A.ES_CD = '0101' AND ((A.RMTRT_LN_TRM_CD BETWEEN '001' AND '012') OR A.RMTRT_LN_TRM_CD = 'ZZZ')
                                           THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS AGRCTR_SHRT_TRM_LN_CCNT,
                  COUNT(DISTINCT CASE WHEN A.ES_CD = '0101' AND ((A.RMTRT_LN_TRM_CD BETWEEN '013' AND '999') OR (A.RMTRT_LN_TRM_CD = 'XXX'))
                                           THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS AGRCTR_MED_LT_LN_CCNT,
                  COUNT(DISTINCT CASE WHEN A.ES_CD <> '0101' AND ((A.RMTRT_LN_TRM_CD BETWEEN '001' AND '012') OR A.RMTRT_LN_TRM_CD = 'ZZZ')
                                           THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS NON_AGRCTR_SHRT_TRM_LN_CCNT,
                  COUNT(DISTINCT CASE WHEN A.ES_CD <> '0101' AND ((A.RMTRT_LN_TRM_CD BETWEEN '013' AND '999') OR (A.RMTRT_LN_TRM_CD = 'XXX'))
                                           THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS NON_AGRCTR_MED_LT_LN_CCNT,
                  COUNT(DISTINCT CASE WHEN A.INIT_LN_TRM_CD IN ('001', '002', '003')
                                           THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS INIT_TRM_LSTHN_3_MM_LN_CCNT,
                  COUNT(DISTINCT CASE WHEN A.INIT_LN_TRM_CD IN ('001', '002', '003')
                                           THEN CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO ELSE A.CUST_ID END
                                      ELSE NULL
                                      END) AS INIT_TRM_LSTHN_3_MM_BRWR_CNT,
                  COUNT(DISTINCT CASE WHEN A.INIT_LN_TRM_CD IN ('004', '005', '006')
                                           THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS INIT_TRM_LSTHN_6_MM_LN_CCNT,
                  COUNT(DISTINCT CASE WHEN A.INIT_LN_TRM_CD IN ('004', '005', '006')
                                           THEN CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO ELSE A.CUST_ID END
                                      ELSE NULL
                                      END) AS INIT_TRM_LSTHN_6_MM_BRWR_CNT,
                  COUNT(DISTINCT CASE WHEN A.INIT_LN_TRM_CD IN ('007', '008', '009')
                                           THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS INIT_TRM_LSTHN_9_MM_LN_CCNT,
                  COUNT(DISTINCT CASE WHEN A.INIT_LN_TRM_CD IN ('007', '008', '009')
                                           THEN CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO ELSE A.CUST_ID END
                                      ELSE NULL
                                      END) AS INIT_TRM_LSTHN_9_MM_BRWR_CNT,
                  COUNT(DISTINCT CASE WHEN A.INIT_LN_TRM_CD IN ('010', '011', '012')
                                           THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS INIT_TRM_LSTHN_12_MM_LN_CCNT,
                  COUNT(DISTINCT CASE WHEN A.INIT_LN_TRM_CD IN ('010', '011', '012')
                                           THEN CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO ELSE A.CUST_ID END
                                      ELSE NULL
                                      END) AS INIT_TRM_LSTHN_12_MM_BRWR_CNT,
                  COUNT(DISTINCT CASE WHEN (A.INIT_LN_TRM_CD = 'XXX' OR (A.INIT_LN_TRM_CD >= '013' AND A.INIT_LN_TRM_CD <> 'ZZZ'))
                                           THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS INIT_TRM_OVR_12_MM_LN_CCNT,
                  COUNT(DISTINCT CASE WHEN (A.INIT_LN_TRM_CD = 'XXX' OR (A.INIT_LN_TRM_CD >= '013' AND A.INIT_LN_TRM_CD <> 'ZZZ'))
                                           THEN CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO ELSE A.CUST_ID END
                                      ELSE NULL
                                      END) AS INIT_TRM_OVR_12_MM_BRWR_CNT,
                  COUNT(DISTINCT CASE WHEN A.RMTRT_LN_TRM_CD IN ('001', '002', '003', 'ZZZ')
                                           THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS RMTRT_LSTHN_3_MM_LN_CCNT,
                  COUNT(DISTINCT CASE WHEN A.RMTRT_LN_TRM_CD IN ('001', '002', '003', 'ZZZ')
                                           THEN CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO ELSE A.CUST_ID END
                                      ELSE NULL
                                      END) AS RMTRT_LSTHN_3_MM_BRWR_CNT,
                  COUNT(DISTINCT CASE WHEN A.RMTRT_LN_TRM_CD IN ('004', '005', '006')
                                           THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS RMTRT_LSTHN_6_MM_LN_CCNT,
                  COUNT(DISTINCT CASE WHEN A.RMTRT_LN_TRM_CD IN ('004', '005', '006')
                                           THEN CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO ELSE A.CUST_ID END
                                      ELSE NULL
                                      END) AS RMTRT_LSTHN_6_MM_BRWR_CNT,
                  COUNT(DISTINCT CASE WHEN A.RMTRT_LN_TRM_CD IN ('007', '008', '009')
                                           THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS RMTRT_LSTHN_9_MM_LN_CCNT,
                  COUNT(DISTINCT CASE WHEN A.RMTRT_LN_TRM_CD IN ('007', '008', '009')
                                           THEN CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO ELSE A.CUST_ID END
                                      ELSE NULL
                                      END) AS RMTRT_LSTHN_9_MM_BRWR_CNT,
                  COUNT(DISTINCT CASE WHEN A.RMTRT_LN_TRM_CD IN ('010', '011', '012')
                                           THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS RMTRT_LSTHN_12_MM_LN_CCNT,
                  COUNT(DISTINCT CASE WHEN A.RMTRT_LN_TRM_CD IN ('010', '011', '012')
                                           THEN CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO ELSE A.CUST_ID END
                                      ELSE NULL
                                      END) AS RMTRT_LSTHN_12_MM_BRWR_CNT,
                  COUNT(DISTINCT CASE WHEN (A.RMTRT_LN_TRM_CD = 'XXX' OR (A.RMTRT_LN_TRM_CD >= '013' AND A.RMTRT_LN_TRM_CD <> 'ZZZ'))
                                           THEN A.LN_CNTR_NUM_INFO
                                      ELSE NULL
                                      END) AS RMTRT_OVR_12_MM_LN_CCNT,
                  COUNT(DISTINCT CASE WHEN (A.RMTRT_LN_TRM_CD = 'XXX' OR (A.RMTRT_LN_TRM_CD >= '013' AND A.RMTRT_LN_TRM_CD <> 'ZZZ'))
                                           THEN CASE WHEN A.CUST_ID IS NULL THEN A.LN_CNTR_NUM_INFO ELSE A.CUST_ID END
                                      ELSE NULL
                                      END) AS RMTRT_OVR_12_MM_BRWR_CNT
           FROM   TM21_DDLY_CUST_CR_TRANS_V A  
           WHERE  A.BAS_DAY = loop_bas_day.BAS_DAY
           AND    A.LN_BAL > 0
           GROUP BY A.BAS_DAY, A.PCF_ID
          )
          SELECT A.BAS_DAY
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
                ,D.AGRCTR_SHRT_TRM_LN_BAL
                ,D.AGRCTR_MED_LT_LN_BAL
                ,D.AGRCTR_SHRT_TRM_LN_CCNT
                ,D.AGRCTR_MED_LT_LN_CCNT
                ,D.NON_AGRCTR_SHRT_TRM_LN_BAL
                ,D.NON_AGRCTR_MED_LT_LN_BAL
                ,D.NON_AGRCTR_SHRT_TRM_LN_CCNT
                ,D.NON_AGRCTR_MED_LT_LN_CCNT
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
                ,D.INIT_TRM_LSTHN_3_MM_LN_BAL
                ,D.INIT_TRM_LSTHN_3_MM_LN_CCNT
                ,D.INIT_TRM_LSTHN_3_MM_BRWR_CNT
                ,D.INIT_TRM_LSTHN_6_MM_LN_BAL
                ,D.INIT_TRM_LSTHN_6_MM_LN_CCNT
                ,D.INIT_TRM_LSTHN_6_MM_BRWR_CNT
                ,D.INIT_TRM_LSTHN_9_MM_LN_BAL
                ,D.INIT_TRM_LSTHN_9_MM_LN_CCNT
                ,D.INIT_TRM_LSTHN_9_MM_BRWR_CNT
                ,D.INIT_TRM_LSTHN_12_MM_LN_BAL
                ,D.INIT_TRM_LSTHN_12_MM_LN_CCNT
                ,D.INIT_TRM_LSTHN_12_MM_BRWR_CNT
                ,D.INIT_TRM_OVR_12_MM_LN_BAL
                ,D.INIT_TRM_OVR_12_MM_LN_CCNT
                ,D.INIT_TRM_OVR_12_MM_BRWR_CNT
                ,D.RMTRT_LSTHN_3_MM_LN_BAL
                ,D.RMTRT_LSTHN_3_MM_LN_CCNT
                ,D.RMTRT_LSTHN_3_MM_BRWR_CNT
                ,D.RMTRT_LSTHN_6_MM_LN_BAL
                ,D.RMTRT_LSTHN_6_MM_LN_CCNT
                ,D.RMTRT_LSTHN_6_MM_BRWR_CNT
                ,D.RMTRT_LSTHN_9_MM_LN_BAL
                ,D.RMTRT_LSTHN_9_MM_LN_CCNT
                ,D.RMTRT_LSTHN_9_MM_BRWR_CNT
                ,D.RMTRT_LSTHN_12_MM_LN_BAL
                ,D.RMTRT_LSTHN_12_MM_LN_CCNT
                ,D.RMTRT_LSTHN_12_MM_BRWR_CNT
                ,D.RMTRT_OVR_12_MM_LN_BAL
                ,D.RMTRT_OVR_12_MM_LN_CCNT
                ,D.RMTRT_OVR_12_MM_BRWR_CNT
                ,SYSTIMESTAMP
          FROM   FULL_SET A LEFT OUTER JOIN DEPOSIT B
                                         ON A.BAS_DAY = B.BAS_DAY
                                        AND A.PCF_ID  = B.PCF_ID
                            LEFT OUTER JOIN LOAN_1 C
                                         ON A.BAS_DAY = C.BAS_DAY
                                        AND A.PCF_ID  = C.PCF_ID
                            LEFT OUTER JOIN LOAN_2 D
                                         ON A.BAS_DAY = D.BAS_DAY
                                        AND A.PCF_ID  = D.PCF_ID;

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
