-- ============================================================================
-- 11_extract_procedures.sql
-- Extracts standalone procedures to OS file via UTL_FILE.
-- Oracle 7 compatible.
-- ============================================================================

CREATE OR REPLACE PROCEDURE extract_procedures (
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

    CURSOR c_procs IS
        SELECT object_name, status
        FROM   all_objects
        WHERE  owner       = p_owner
        AND    object_type = 'PROCEDURE'
        AND    status      = p_status
        ORDER BY object_name;

    CURSOR c_source (cp_name VARCHAR2) IS
        SELECT line, text
        FROM   all_source
        WHERE  owner = p_owner
        AND    name  = cp_name
        AND    type  = 'PROCEDURE'
        ORDER BY line;

BEGIN
    v_start := SYSDATE;
    SELECT seq_meta_log_id.NEXTVAL INTO v_log_id FROM dual;

    v_filename := p_owner || '_PROCEDURES_' || p_status || '_' ||
                  TO_CHAR(SYSDATE, 'YYYYMMDD') || '.txt';

    v_file := UTL_FILE.FOPEN(p_directory, v_filename, 'W');

    UTL_FILE.PUT_LINE(v_file, RPAD('=', 80, '='));
    UTL_FILE.PUT_LINE(v_file, 'METADATA EXTRACT: PROCEDURES (' || p_status || ')');
    UTL_FILE.PUT_LINE(v_file, 'OWNER: ' || p_owner);
    UTL_FILE.PUT_LINE(v_file, 'DATE: ' || TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS'));
    UTL_FILE.PUT_LINE(v_file, RPAD('=', 80, '='));
    UTL_FILE.NEW_LINE(v_file);

    FOR r_proc IN c_procs LOOP
        v_count := v_count + 1;

        UTL_FILE.PUT_LINE(v_file, '--- PROCEDURE: ' || r_proc.object_name ||
                          ' (' || r_proc.status || ') ---');

        FOR r_src IN c_source(r_proc.object_name) LOOP
            v_line := SUBSTR(LPAD(TO_CHAR(r_src.line), 5) || ': ' ||
                      RTRIM(r_src.text, CHR(10) || CHR(13)), 1, 1000);
            UTL_FILE.PUT_LINE(v_file, v_line);
        END LOOP;

        UTL_FILE.PUT_LINE(v_file, '--- END PROCEDURE: ' || r_proc.object_name || ' ---');
        UTL_FILE.NEW_LINE(v_file);
    END LOOP;

    UTL_FILE.NEW_LINE(v_file);
    UTL_FILE.PUT_LINE(v_file, 'TOTAL PROCEDURES: ' || TO_CHAR(v_count));
    UTL_FILE.PUT_LINE(v_file, RPAD('=', 80, '='));

    UTL_FILE.FCLOSE(v_file);

    INSERT INTO meta_extract_log (log_id, run_id, procedure_name, p_owner,
        p_status, object_count, output_file, start_time, end_time)
    VALUES (v_log_id, p_run_id, 'EXTRACT_PROCEDURES', p_owner,
        p_status, v_count, v_filename, v_start, SYSDATE);
    COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        IF UTL_FILE.IS_OPEN(v_file) THEN
            UTL_FILE.FCLOSE(v_file);
        END IF;
        INSERT INTO meta_extract_log (log_id, run_id, procedure_name, p_owner,
            p_status, object_count, output_file, start_time, end_time, error_message)
        VALUES (v_log_id, p_run_id, 'EXTRACT_PROCEDURES', p_owner,
            p_status, v_count, v_filename, v_start, SYSDATE,
            SUBSTR(SQLERRM, 1, 2000));
        COMMIT;
        RAISE;
END extract_procedures;
/

PROMPT Procedure EXTRACT_PROCEDURES created.
