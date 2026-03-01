-- ============================================================================
-- 15_extract_views.sql
-- Extracts view definitions to OS file via UTL_FILE.
-- Oracle 7 compatible. Handles LONG column (ALL_VIEWS.TEXT).
-- ============================================================================

CREATE OR REPLACE PROCEDURE extract_views (
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
    v_view_text VARCHAR2(2000);

    CURSOR c_views IS
        SELECT object_name, status
        FROM   all_objects
        WHERE  owner       = p_owner
        AND    object_type = 'VIEW'
        AND    status      = p_status
        ORDER BY object_name;

    CURSOR c_view_cols (cp_view VARCHAR2) IS
        SELECT column_name,
               data_type,
               data_length,
               nullable,
               column_id
        FROM   all_tab_columns
        WHERE  owner      = p_owner
        AND    table_name = cp_view
        ORDER BY column_id;

BEGIN
    v_start := SYSDATE;
    SELECT seq_meta_log_id.NEXTVAL INTO v_log_id FROM dual;

    v_filename := p_owner || '_VIEWS_' || p_status || '_' ||
                  TO_CHAR(SYSDATE, 'YYYYMMDD') || '.txt';

    v_file := UTL_FILE.FOPEN(p_directory, v_filename, 'W');

    UTL_FILE.PUT_LINE(v_file, RPAD('=', 80, '='));
    UTL_FILE.PUT_LINE(v_file, 'METADATA EXTRACT: VIEWS (' || p_status || ')');
    UTL_FILE.PUT_LINE(v_file, 'OWNER: ' || p_owner);
    UTL_FILE.PUT_LINE(v_file, 'DATE: ' || TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS'));
    UTL_FILE.PUT_LINE(v_file, RPAD('=', 80, '='));
    UTL_FILE.NEW_LINE(v_file);

    FOR r_view IN c_views LOOP
        v_count := v_count + 1;

        UTL_FILE.PUT_LINE(v_file, '--- VIEW: ' || r_view.object_name ||
                          ' (' || r_view.status || ') ---');

        -- View text (LONG column - handle truncation)
        UTL_FILE.PUT_LINE(v_file, '  -- DEFINITION --');
        BEGIN
            SELECT text INTO v_view_text
            FROM   all_views
            WHERE  owner     = p_owner
            AND    view_name = r_view.object_name;

            IF v_view_text IS NOT NULL THEN
                IF v_view_text.length <= 990 THEN
					UTL_FILE.PUT_LINE(v_file, '  ' || SUBSTR(v_view_text, 1, 990));
				ELSE
					UTL_FILE.PUT_LINE(v_file, '  ' || SUBSTR(v_view_text, 1, 990));
                    UTL_FILE.PUT_LINE(v_file, '  ' || SUBSTR(v_view_text, 991, 2000));
            END IF;
        EXCEPTION
            WHEN VALUE_ERROR THEN
                UTL_FILE.PUT_LINE(v_file,
                    '  (view text truncated - exceeds 2000 characters)');
            WHEN NO_DATA_FOUND THEN
                UTL_FILE.PUT_LINE(v_file, '  (no view text found)');
        END;

        -- View columns
        UTL_FILE.NEW_LINE(v_file);
        UTL_FILE.PUT_LINE(v_file, '  -- COLUMNS --');
        UTL_FILE.PUT_LINE(v_file, '  ' ||
            RPAD('COLUMN_NAME', 30) ||
            RPAD('DATA_TYPE', 20) ||
            RPAD('LENGTH', 8) ||
            'NULL');
        UTL_FILE.PUT_LINE(v_file, '  ' || RPAD('-', 65, '-'));

        FOR r_col IN c_view_cols(r_view.object_name) LOOP
            v_line := '  ' ||
                RPAD(r_col.column_name, 30) ||
                RPAD(r_col.data_type, 20) ||
                RPAD(NVL(TO_CHAR(r_col.data_length), ''), 8) ||
                r_col.nullable;
            UTL_FILE.PUT_LINE(v_file, v_line);
        END LOOP;

        UTL_FILE.PUT_LINE(v_file, '--- END VIEW: ' || r_view.object_name || ' ---');
        UTL_FILE.NEW_LINE(v_file);
    END LOOP;

    UTL_FILE.NEW_LINE(v_file);
    UTL_FILE.PUT_LINE(v_file, 'TOTAL VIEWS: ' || TO_CHAR(v_count));
    UTL_FILE.PUT_LINE(v_file, RPAD('=', 80, '='));

    UTL_FILE.FCLOSE(v_file);

    INSERT INTO meta_extract_log (log_id, run_id, procedure_name, p_owner,
        p_status, object_count, output_file, start_time, end_time)
    VALUES (v_log_id, p_run_id, 'EXTRACT_VIEWS', p_owner,
        p_status, v_count, v_filename, v_start, SYSDATE);
    COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        IF UTL_FILE.IS_OPEN(v_file) THEN
            UTL_FILE.FCLOSE(v_file);
        END IF;
        INSERT INTO meta_extract_log (log_id, run_id, procedure_name, p_owner,
            p_status, object_count, output_file, start_time, end_time, error_message)
        VALUES (v_log_id, p_run_id, 'EXTRACT_VIEWS', p_owner,
            p_status, v_count, v_filename, v_start, SYSDATE,
            SUBSTR(SQLERRM, 1, 2000));
        COMMIT;
        RAISE;
END extract_views;
/

PROMPT Procedure EXTRACT_VIEWS created.
