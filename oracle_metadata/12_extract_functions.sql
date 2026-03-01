-- ============================================================================
-- 12_extract_functions.sql
-- Extracts standalone functions to OS file via UTL_FILE.
-- Oracle 7 compatible.
-- ============================================================================

CREATE OR REPLACE PROCEDURE extract_functions (
    p_owner     IN VARCHAR2,
    p_status    IN VARCHAR2,
    p_directory IN VARCHAR2,
    p_run_id    IN NUMBER
) IS
    v_file      UTL_FILE.FILE_TYPE;
    v_filename  VARCHAR2(200);
    v_count     NUMBER := 0;
    v_line      VARCHAR2(2000);
    v_log_id    NUMBER;
    v_start     DATE;

    CURSOR c_funcs IS
        SELECT object_name, status
        FROM   all_objects
        WHERE  owner       = p_owner
        AND    object_type = 'FUNCTION'
        AND    status      = p_status
        ORDER BY object_name;

    CURSOR c_source (cp_name VARCHAR2) IS
        SELECT line, text
        FROM   all_source
        WHERE  owner = p_owner
        AND    name  = cp_name
        AND    type  = 'FUNCTION'
        ORDER BY line;

BEGIN
    v_start := SYSDATE;
    SELECT seq_meta_log_id.NEXTVAL INTO v_log_id FROM dual;

    v_filename := p_owner || '_FUNCTIONS_' || p_status || '_' ||
                  TO_CHAR(SYSDATE, 'YYYYMMDD') || '.txt';

    v_file := UTL_FILE.FOPEN(p_directory, v_filename, 'W');

    UTL_FILE.PUT_LINE(v_file, RPAD('=', 80, '='));
    UTL_FILE.PUT_LINE(v_file, 'METADATA EXTRACT: FUNCTIONS (' || p_status || ')');
    UTL_FILE.PUT_LINE(v_file, 'OWNER: ' || p_owner);
    UTL_FILE.PUT_LINE(v_file, 'DATE: ' || TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS'));
    UTL_FILE.PUT_LINE(v_file, RPAD('=', 80, '='));
    UTL_FILE.NEW_LINE(v_file);

    FOR r_func IN c_funcs LOOP
        v_count := v_count + 1;

        UTL_FILE.PUT_LINE(v_file, '--- FUNCTION: ' || r_func.object_name ||
                          ' (' || r_func.status || ') ---');

        FOR r_src IN c_source(r_func.object_name) LOOP
            v_line := SUBSTR(LPAD(TO_CHAR(r_src.line), 5) || ': ' ||
                      RTRIM(r_src.text, CHR(10) || CHR(13)), 1, 1000);
            UTL_FILE.PUT_LINE(v_file, v_line);
        END LOOP;

        UTL_FILE.PUT_LINE(v_file, '--- END FUNCTION: ' || r_func.object_name || ' ---');
        UTL_FILE.NEW_LINE(v_file);
    END LOOP;

    UTL_FILE.NEW_LINE(v_file);
    UTL_FILE.PUT_LINE(v_file, 'TOTAL FUNCTIONS: ' || TO_CHAR(v_count));
    UTL_FILE.PUT_LINE(v_file, RPAD('=', 80, '='));

    UTL_FILE.FCLOSE(v_file);

    INSERT INTO meta_extract_log (log_id, run_id, procedure_name, p_owner,
        p_status, object_count, output_file, start_time, end_time)
    VALUES (v_log_id, p_run_id, 'EXTRACT_FUNCTIONS', p_owner,
        p_status, v_count, v_filename, v_start, SYSDATE);
    COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        IF UTL_FILE.IS_OPEN(v_file) THEN
            UTL_FILE.FCLOSE(v_file);
        END IF;
        INSERT INTO meta_extract_log (log_id, run_id, procedure_name, p_owner,
            p_status, object_count, output_file, start_time, end_time, error_message)
        VALUES (v_log_id, p_run_id, 'EXTRACT_FUNCTIONS', p_owner,
            p_status, v_count, v_filename, v_start, SYSDATE,
            SUBSTR(SQLERRM, 1, 2000));
        COMMIT;
        RAISE;
END extract_functions;
/

PROMPT Procedure EXTRACT_FUNCTIONS created.
