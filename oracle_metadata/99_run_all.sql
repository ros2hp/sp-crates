-- ============================================================================
-- 99_run_all.sql
-- Master orchestration script for Oracle 7 metadata capture.
--
-- Usage:
--   SQL> @99_run_all.sql HR /tmp/metadata_out
--
-- Parameters:
--   &1 = Schema owner (e.g. HR, SCOTT, MYAPP)
--   &2 = OS directory path (must be listed in utl_file_dir in init.ora)
--
-- Prerequisites:
--   1. Run 01_create_tables.sql and 02_create_sequences.sql first
--   2. Run all extraction procedure scripts (10-19, 20, 30)
--   3. Ensure p_directory is listed in utl_file_dir init.ora parameter
-- ============================================================================

SET SERVEROUTPUT ON SIZE 1000000
SET VERIFY OFF
SET FEEDBACK OFF
SET ECHO OFF

DEFINE v_owner = &1
DEFINE v_dir   = &2

PROMPT
PROMPT ============================================================
PROMPT  Oracle Metadata Capture System
PROMPT  Owner:     &v_owner
PROMPT  Directory: &v_dir
PROMPT ============================================================
PROMPT

-- Create a run ID and log the run
VARIABLE v_run_id NUMBER
BEGIN
    SELECT seq_meta_run_id.NEXTVAL INTO :v_run_id FROM dual;
    INSERT INTO meta_run_log (run_id, p_owner, p_directory)
    VALUES (:v_run_id, '&v_owner', '&v_dir');
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('Run ID: ' || TO_CHAR(:v_run_id));
END;
/

-- ============================================================
-- Phase 1: PL/SQL Objects (VALID then INVALID)
-- ============================================================

PROMPT --- Extracting Packages (VALID) ---
BEGIN
    extract_packages('&v_owner', 'VALID', '&v_dir', :v_run_id);
    DBMS_OUTPUT.PUT_LINE('  Packages (VALID) complete.');
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('  ERROR: ' || SQLERRM);
END;
/

PROMPT --- Extracting Packages (INVALID) ---
BEGIN
    extract_packages('&v_owner', 'INVALID', '&v_dir', :v_run_id);
    DBMS_OUTPUT.PUT_LINE('  Packages (INVALID) complete.');
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('  ERROR: ' || SQLERRM);
END;
/

PROMPT --- Extracting Procedures (VALID) ---
BEGIN
    extract_procedures('&v_owner', 'VALID', '&v_dir', :v_run_id);
    DBMS_OUTPUT.PUT_LINE('  Procedures (VALID) complete.');
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('  ERROR: ' || SQLERRM);
END;
/

PROMPT --- Extracting Procedures (INVALID) ---
BEGIN
    extract_procedures('&v_owner', 'INVALID', '&v_dir', :v_run_id);
    DBMS_OUTPUT.PUT_LINE('  Procedures (INVALID) complete.');
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('  ERROR: ' || SQLERRM);
END;
/

PROMPT --- Extracting Functions (VALID) ---
BEGIN
    extract_functions('&v_owner', 'VALID', '&v_dir', :v_run_id);
    DBMS_OUTPUT.PUT_LINE('  Functions (VALID) complete.');
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('  ERROR: ' || SQLERRM);
END;
/

PROMPT --- Extracting Functions (INVALID) ---
BEGIN
    extract_functions('&v_owner', 'INVALID', '&v_dir', :v_run_id);
    DBMS_OUTPUT.PUT_LINE('  Functions (INVALID) complete.');
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('  ERROR: ' || SQLERRM);
END;
/

PROMPT --- Extracting Triggers (VALID) ---
BEGIN
    extract_triggers('&v_owner', 'VALID', '&v_dir', :v_run_id);
    DBMS_OUTPUT.PUT_LINE('  Triggers (VALID) complete.');
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('  ERROR: ' || SQLERRM);
END;
/

PROMPT --- Extracting Triggers (INVALID) ---
BEGIN
    extract_triggers('&v_owner', 'INVALID', '&v_dir', :v_run_id);
    DBMS_OUTPUT.PUT_LINE('  Triggers (INVALID) complete.');
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('  ERROR: ' || SQLERRM);
END;
/

PROMPT --- Extracting Views (VALID) ---
BEGIN
    extract_views('&v_owner', 'VALID', '&v_dir', :v_run_id);
    DBMS_OUTPUT.PUT_LINE('  Views (VALID) complete.');
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('  ERROR: ' || SQLERRM);
END;
/

PROMPT --- Extracting Views (INVALID) ---
BEGIN
    extract_views('&v_owner', 'INVALID', '&v_dir', :v_run_id);
    DBMS_OUTPUT.PUT_LINE('  Views (INVALID) complete.');
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('  ERROR: ' || SQLERRM);
END;
/

-- ============================================================
-- Phase 2: Non-status objects (always 'ALL')
-- ============================================================

PROMPT --- Extracting Tables ---
BEGIN
    extract_tables('&v_owner', 'ALL', '&v_dir', :v_run_id);
    DBMS_OUTPUT.PUT_LINE('  Tables complete.');
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('  ERROR: ' || SQLERRM);
END;
/

PROMPT --- Extracting Synonyms ---
BEGIN
    extract_synonyms('&v_owner', 'ALL', '&v_dir', :v_run_id);
    DBMS_OUTPUT.PUT_LINE('  Synonyms complete.');
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('  ERROR: ' || SQLERRM);
END;
/

PROMPT --- Extracting Sequences ---
BEGIN
    extract_sequences('&v_owner', 'ALL', '&v_dir', :v_run_id);
    DBMS_OUTPUT.PUT_LINE('  Sequences complete.');
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('  ERROR: ' || SQLERRM);
END;
/

PROMPT --- Extracting Indexes ---
BEGIN
    extract_indexes('&v_owner', 'ALL', '&v_dir', :v_run_id);
    DBMS_OUTPUT.PUT_LINE('  Indexes complete.');
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('  ERROR: ' || SQLERRM);
END;
/

PROMPT --- Extracting Constraints ---
BEGIN
    extract_constraints('&v_owner', 'ALL', '&v_dir', :v_run_id);
    DBMS_OUTPUT.PUT_LINE('  Constraints complete.');
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('  ERROR: ' || SQLERRM);
END;
/

-- ============================================================
-- Phase 3: Business Rules Identification and Export
-- ============================================================

PROMPT --- Identifying Business Rules ---
BEGIN
    identify_business_rules('&v_owner', '&v_dir', :v_run_id);
    DBMS_OUTPUT.PUT_LINE('  Business rules identification complete.');
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('  ERROR: ' || SQLERRM);
END;
/

PROMPT --- Exporting Business Rules Report ---
BEGIN
    export_business_rules('&v_owner', '&v_dir', :v_run_id);
    DBMS_OUTPUT.PUT_LINE('  Business rules export complete.');
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('  ERROR: ' || SQLERRM);
END;
/

-- ============================================================
-- Finalize: Update run status
-- ============================================================
BEGIN
    UPDATE meta_run_log
    SET    status = 'COMPLETED'
    WHERE  run_id = :v_run_id;
    COMMIT;
END;
/

-- Print summary
PROMPT
PROMPT ============================================================
PROMPT  Extraction Complete
PROMPT ============================================================
PRINT v_run_id
PROMPT

SET FEEDBACK ON

SELECT procedure_name,
       p_status,
       object_count,
       output_file,
       TO_CHAR(end_time - start_time, '99990.00') || 's' AS elapsed,
       NVL(error_message, 'OK') AS status
FROM   meta_extract_log
WHERE  run_id = :v_run_id
ORDER BY log_id;

PROMPT
PROMPT Check &v_dir for output files.
PROMPT ============================================================

SET VERIFY ON
SET FEEDBACK ON
SET ECHO ON
