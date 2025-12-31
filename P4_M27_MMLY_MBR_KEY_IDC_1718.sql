create or replace PROCEDURE            "P4_M27_MMLY_MBR_KEY_IDC_1718"
                 ( p_wkdate    IN  CHAR DEFAULT NULL
                 , p_stdate_01 IN  CHAR DEFAULT NULL
                 , p_eddate_01 IN  CHAR DEFAULT NULL
                 )
IS
    /*--------------------------------------------------------------------------
     * PROGRAM ID    : P4_M27_MMLY_MBR_KEY_IDC_1718
     * PROGRAM NAME  : A program for insert data to TM27_MMLY_MBR_KEY_IDC_A
     * SOURCE TABLE  : TM00_MBR_D
     * TARGET TABLE  : TM27_MMLY_MBR_KEY_IDC_A
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

    v_program_id              VARCHAR2(100) DEFAULT 'P4_M27_MMLY_MBR_KEY_IDC_1718' ;
    v_program_type_name       VARCHAR2(100) DEFAULT 'Stored Procedure' ;
    v_step_code               VARCHAR2(3) ;
    v_step_desc               VARCHAR2(100) ;
    v_time                    DATE ;
    v_cnt                     NUMBER DEFAULT 0 ;
    v_seq                     CHAR(14) DEFAULT ' '||to_char(SEQ_UID.NEXTVAL,'000000000000');
    ----------------------------------------------------------------------------
    --  Declare Cursor
    ----------------------------------------------------------------------------

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
    --  1.1 Table Initialize
    ----------------------------------------------------------------------------
    DELETE FROM TM27_MMLY_MBR_KEY_IDC_A;
    ----------------------------------------------------------------------------
    --  Transaction Log
    ----------------------------------------------------------------------------
    v_step_code    := '010' ;
    v_step_desc    := v_wk_date || v_seq || ' : Result of deleting data' ;
    v_time         := SYSDATE ;

    P1_BSM_PROG_EXEC_LOG(v_program_id, v_program_type_name, v_step_code, v_step_desc, v_time, sql%rowcount, NULL, NULL) ;

    COMMIT ;
    ----------------------------------------------------------------------------
    --  1.1 Initial Migration
    ----------------------------------------------------------------------------
    INSERT INTO TM27_MMLY_MBR_KEY_IDC_A
    (   BAS_YM
       ,PCF_ID
       ,TOT_MBR_NUM_CNT
       ,FEMALE_MBR_NUM_CNT
       ,MALE_MBR_NUM_CNT
       ,ETC_MBR_NUM_CNT
       ,FEMALE_BOD_NUM_CNT
       ,MALE_BOD_NUM_CNT
       ,ETC_BOD_NUM_CNT
       ,FEMALE_CEO_NUM_CNT
       ,MALE_CEO_NUM_CNT
       ,ETC_CEO_NUM_CNT
       ,FEMALE_MGR_NUM_CNT
       ,MALE_MGR_NUM_CNT
       ,ETC_MGR_NUM_CNT
       ,FEMALE_NO_MGR_NUM_CNT
       ,MALE_NO_MGR_NUM_CNT
       ,ETC_NO_MGR_NUM_CNT
       ,FEMALE_NO_POSN_INFO_NUM_CNT
       ,MALE_NO_POSN_INFO_NUM_CNT
       ,ETC_NO_POSN_INFO_NUM_CNT
       ,FEMALE_NO_POSN_NUM_CNT
       ,MALE_NO_POSN_NUM_CNT
       ,ETC_NO_POSN_NUM_CNT
       ,TRGT_DATA_LST_MOD_TM
    )
    WITH
    FROM_TM00_MBR_D AS
    (SELECT T1.BAS_YM,
            T1.PCF_ID,
            NVL(COUNT(T1.TX_CD_ID), 0) AS TOT_MBR_NUM_CNT,
            NVL(SUM(CASE WHEN T1.GEN_TYP_CD = 'F' THEN 1 ELSE 0 END), 0) AS FEMALE_MBR_NUM_CNT,
            NVL(SUM(CASE WHEN T1.GEN_TYP_CD = 'M' THEN 1 ELSE 0 END), 0) AS MALE_MBR_NUM_CNT,
            NVL(SUM(CASE WHEN T1.GEN_TYP_CD = '8' THEN 1 ELSE 0 END), 0) AS ETC_MBR_NUM_CNT,
            NVL(SUM(CASE WHEN T1.GEN_TYP_CD = 'F' AND T1.OUPT_RPT_MBR_POSN_INFO LIKE '%01%' THEN 1 ELSE 0 END), 0) AS FEMALE_BOD_NUM_CNT,
            NVL(SUM(CASE WHEN T1.GEN_TYP_CD = 'M' AND T1.OUPT_RPT_MBR_POSN_INFO LIKE '%01%' THEN 1 ELSE 0 END), 0) AS MALE_BOD_NUM_CNT,
            NVL(SUM(CASE WHEN T1.GEN_TYP_CD = '8' AND T1.OUPT_RPT_MBR_POSN_INFO LIKE '%01%' THEN 1 ELSE 0 END), 0) AS ETC_BOD_NUM_CNT,
            NVL(SUM(CASE WHEN T1.GEN_TYP_CD = 'F' AND T1.OUPT_RPT_MBR_POSN_INFO LIKE '%02%' THEN 1 ELSE 0 END), 0) AS FEMALE_CEO_NUM_CNT,
            NVL(SUM(CASE WHEN T1.GEN_TYP_CD = 'M' AND T1.OUPT_RPT_MBR_POSN_INFO LIKE '%02%' THEN 1 ELSE 0 END), 0) AS MALE_CEO_NUM_CNT,
            NVL(SUM(CASE WHEN T1.GEN_TYP_CD = '8' AND T1.OUPT_RPT_MBR_POSN_INFO LIKE '%02%' THEN 1 ELSE 0 END), 0) AS ETC_CEO_NUM_CNT,
            NVL(SUM(CASE WHEN T1.GEN_TYP_CD = 'F' AND T1.OUPT_RPT_MBR_POSN_INFO LIKE '%03%' THEN 1 ELSE 0 END), 0) AS FEMALE_MGR_NUM_CNT,
            NVL(SUM(CASE WHEN T1.GEN_TYP_CD = 'M' AND T1.OUPT_RPT_MBR_POSN_INFO LIKE '%03%' THEN 1 ELSE 0 END), 0) AS MALE_MGR_NUM_CNT,
            NVL(SUM(CASE WHEN T1.GEN_TYP_CD = '8' AND T1.OUPT_RPT_MBR_POSN_INFO LIKE '%03%' THEN 1 ELSE 0 END), 0) AS ETC_MGR_NUM_CNT,
            NVL(SUM(CASE WHEN T1.GEN_TYP_CD = 'F' AND T1.OUPT_RPT_MBR_POSN_INFO LIKE '%04%' THEN 1 ELSE 0 END), 0) AS FEMALE_NO_MGR_NUM_CNT,
            NVL(SUM(CASE WHEN T1.GEN_TYP_CD = 'M' AND T1.OUPT_RPT_MBR_POSN_INFO LIKE '%04%' THEN 1 ELSE 0 END), 0) AS MALE_NO_MGR_NUM_CNT,
            NVL(SUM(CASE WHEN T1.GEN_TYP_CD = '8' AND T1.OUPT_RPT_MBR_POSN_INFO LIKE '%04%' THEN 1 ELSE 0 END), 0) AS ETC_NO_MGR_NUM_CNT,
            NVL(SUM(CASE WHEN T1.GEN_TYP_CD = 'F' AND T1.OUPT_RPT_MBR_POSN_INFO LIKE '%05%' THEN 1 ELSE 0 END), 0) AS FEMALE_NO_POSN_INFO_NUM_CNT,
            NVL(SUM(CASE WHEN T1.GEN_TYP_CD = 'M' AND T1.OUPT_RPT_MBR_POSN_INFO LIKE '%05%' THEN 1 ELSE 0 END), 0) AS MALE_NO_POSN_INFO_NUM_CNT,
            NVL(SUM(CASE WHEN T1.GEN_TYP_CD = '8' AND T1.OUPT_RPT_MBR_POSN_INFO LIKE '%05%' THEN 1 ELSE 0 END), 0) AS ETC_NO_POSN_INFO_NUM_CNT,
            NVL(SUM(CASE WHEN T1.GEN_TYP_CD = 'F' AND T1.OUPT_RPT_MBR_POSN_INFO LIKE '%06%' THEN 1 ELSE 0 END), 0) AS FEMALE_NO_POSN_NUM_CNT,
            NVL(SUM(CASE WHEN T1.GEN_TYP_CD = 'M' AND T1.OUPT_RPT_MBR_POSN_INFO LIKE '%06%' THEN 1 ELSE 0 END), 0) AS MALE_NO_POSN_NUM_CNT,
            NVL(SUM(CASE WHEN T1.GEN_TYP_CD = '8' AND T1.OUPT_RPT_MBR_POSN_INFO LIKE '%06%' THEN 1 ELSE 0 END), 0) AS ETC_NO_POSN_NUM_CNT
     FROM   TM00_MBR_D T1
     GROUP BY T1.BAS_YM, T1.PCF_ID
    )
    SELECT T1.BAS_YM,
           T1.PCF_ID,
           T1.TOT_MBR_NUM_CNT,
           T1.FEMALE_MBR_NUM_CNT,
           T1.MALE_MBR_NUM_CNT,
           T1.ETC_MBR_NUM_CNT,
           T1.FEMALE_BOD_NUM_CNT,
           T1.MALE_BOD_NUM_CNT,
           T1.ETC_BOD_NUM_CNT,
           T1.FEMALE_CEO_NUM_CNT,
           T1.MALE_CEO_NUM_CNT,
           T1.ETC_CEO_NUM_CNT,
           T1.FEMALE_MGR_NUM_CNT,
           T1.MALE_MGR_NUM_CNT,
           T1.ETC_MGR_NUM_CNT,
           T1.FEMALE_NO_MGR_NUM_CNT,
           T1.MALE_NO_MGR_NUM_CNT,
           T1.ETC_NO_MGR_NUM_CNT,
           T1.FEMALE_NO_POSN_INFO_NUM_CNT,
           T1.MALE_NO_POSN_INFO_NUM_CNT,
           T1.ETC_NO_POSN_INFO_NUM_CNT,
           T1.FEMALE_NO_POSN_NUM_CNT,
           T1.MALE_NO_POSN_NUM_CNT,
           T1.ETC_NO_POSN_NUM_CNT,
           SYSTIMESTAMP
    FROM   FROM_TM00_MBR_D T1 ;
    ----------------------------------------------------------------------------
    --  Transaction Log
    ----------------------------------------------------------------------------
    v_step_code    := '020' ;
    v_step_desc    := v_wk_date || v_seq || ' : Result of inserting data(Initial Migration From 2018 to 2021)' ;
    v_time         := SYSDATE ;

    P1_BSM_PROG_EXEC_LOG(v_program_id, v_program_type_name, v_step_code, v_step_desc, v_time, sql%rowcount, NULL, NULL) ;

    COMMIT ;
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