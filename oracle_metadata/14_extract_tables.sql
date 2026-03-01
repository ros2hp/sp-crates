-- ============================================================================
-- 14_extract_tables.sql
-- Extracts table structure, columns, and comments to OS file via UTL_FILE.
-- Oracle 7 compatible.
-- ============================================================================

CREATE OR REPLACE PROCEDURE extract_tables (
    p_owner     IN VARCHAR2,
    p_status    IN VARCHAR2,
    p_directory IN VARCHAR2,
    p_run_id    IN NUMBER
) IS
    v_file        UTL_FILE.FILE_TYPE;
    v_filename    VARCHAR2(200);
    v_count       NUMBER := 0;
    v_line        VARCHAR2(2000);
    v_log_id      NUMBER;
    v_start       DATE;
    v_tab_comment VARCHAR2(2000);
    v_col_comment VARCHAR2(2000);
    v_data_default VARCHAR2(2000);

    CURSOR c_tables IS
        SELECT object_name
        FROM   all_objects
        WHERE  owner       = p_owner
        AND    object_type = 'TABLE'
        ORDER BY object_name;

    CURSOR c_columns (cp_table VARCHAR2) IS
        SELECT column_name,
               data_type,
               data_length,
               data_precision,
               data_scale,
               nullable,
               column_id
        FROM   all_tab_columns
        WHERE  owner      = p_owner
        AND    table_name = cp_table
        ORDER BY column_id;

    CURSOR c_tab_comment (cp_table VARCHAR2) IS
        SELECT comments
        FROM   all_tab_comments
        WHERE  owner      = p_owner
        AND    table_name = cp_table;

    CURSOR c_col_comment (cp_table VARCHAR2, cp_column VARCHAR2) IS
        SELECT comments
        FROM   all_col_comments
        WHERE  owner       = p_owner
        AND    table_name  = cp_table
        AND    column_name = cp_column;

BEGIN
    v_start := SYSDATE;
    SELECT seq_meta_log_id.NEXTVAL INTO v_log_id FROM dual;

    v_filename := p_owner || '_TABLES_' || p_status || '_' ||
                  TO_CHAR(SYSDATE, 'YYYYMMDD') || '.txt';

    v_file := UTL_FILE.FOPEN(p_directory, v_filename, 'W');

    UTL_FILE.PUT_LINE(v_file, RPAD('=', 80, '='));
    UTL_FILE.PUT_LINE(v_file, 'METADATA EXTRACT: TABLES (' || p_status || ')');
    UTL_FILE.PUT_LINE(v_file, 'OWNER: ' || p_owner);
    UTL_FILE.PUT_LINE(v_file, 'DATE: ' || TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS'));
    UTL_FILE.PUT_LINE(v_file, RPAD('=', 80, '='));
    UTL_FILE.NEW_LINE(v_file);

    FOR r_tab IN c_tables LOOP
        v_count := v_count + 1;

        UTL_FILE.PUT_LINE(v_file, '--- TABLE: ' || r_tab.object_name || ' ---');

        -- Table comment
        v_tab_comment := NULL;
        OPEN c_tab_comment(r_tab.object_name);
        FETCH c_tab_comment INTO v_tab_comment;
        CLOSE c_tab_comment;

        IF v_tab_comment IS NOT NULL THEN
            UTL_FILE.PUT_LINE(v_file, '  Comment: ' || v_tab_comment);
        END IF;

        UTL_FILE.PUT_LINE(v_file, '  ' || RPAD('-', 78, '-'));
        UTL_FILE.PUT_LINE(v_file, '  ' ||
            RPAD('COLUMN_NAME', 30) ||
            RPAD('DATA_TYPE', 20) ||
            RPAD('LENGTH', 8) ||
            RPAD('PREC', 6) ||
            RPAD('SCALE', 6) ||
            'NULL');
        UTL_FILE.PUT_LINE(v_file, '  ' || RPAD('-', 78, '-'));

        FOR r_col IN c_columns(r_tab.object_name) LOOP
            v_line := '  ' ||
                RPAD(r_col.column_name, 30) ||
                RPAD(r_col.data_type, 20) ||
                RPAD(NVL(TO_CHAR(r_col.data_length), ''), 8) ||
                RPAD(NVL(TO_CHAR(r_col.data_precision), ''), 6) ||
                RPAD(NVL(TO_CHAR(r_col.data_scale), ''), 6) ||
                r_col.nullable;
            UTL_FILE.PUT_LINE(v_file, v_line);

            -- Column default (LONG column - handle with care)
            BEGIN
                SELECT data_default INTO v_data_default
                FROM   all_tab_columns
                WHERE  owner       = p_owner
                AND    table_name  = r_tab.object_name
                AND    column_name = r_col.column_name;

                IF v_data_default IS NOT NULL THEN
                    UTL_FILE.PUT_LINE(v_file, '    Default: ' ||
                        SUBSTR(v_data_default, 1, 900));
                END IF;
            EXCEPTION
                WHEN VALUE_ERROR THEN
                    UTL_FILE.PUT_LINE(v_file,
                        '    Default: (truncated - exceeds 2000 characters)');
                WHEN NO_DATA_FOUND THEN
                    NULL;
            END;

            -- Column comment
            v_col_comment := NULL;
            OPEN c_col_comment(r_tab.object_name, r_col.column_name);
            FETCH c_col_comment INTO v_col_comment;
            CLOSE c_col_comment;

            IF v_col_comment IS NOT NULL THEN
                UTL_FILE.PUT_LINE(v_file, '    Comment: ' || v_col_comment);
            END IF;
        END LOOP;

        UTL_FILE.PUT_LINE(v_file, '--- END TABLE: ' || r_tab.object_name || ' ---');
        UTL_FILE.NEW_LINE(v_file);
    END LOOP;

    UTL_FILE.NEW_LINE(v_file);
    UTL_FILE.PUT_LINE(v_file, 'TOTAL TABLES: ' || TO_CHAR(v_count));
    UTL_FILE.PUT_LINE(v_file, RPAD('=', 80, '='));

    UTL_FILE.FCLOSE(v_file);

    INSERT INTO meta_extract_log (log_id, run_id, procedure_name, p_owner,
        p_status, object_count, output_file, start_time, end_time)
    VALUES (v_log_id, p_run_id, 'EXTRACT_TABLES', p_owner,
        p_status, v_count, v_filename, v_start, SYSDATE);
    COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        IF UTL_FILE.IS_OPEN(v_file) THEN
            UTL_FILE.FCLOSE(v_file);
        END IF;
        INSERT INTO meta_extract_log (log_id, run_id, procedure_name, p_owner,
            p_status, object_count, output_file, start_time, end_time, error_message)
        VALUES (v_log_id, p_run_id, 'EXTRACT_TABLES', p_owner,
            p_status, v_count, v_filename, v_start, SYSDATE,
            SUBSTR(SQLERRM, 1, 2000));
        COMMIT;
        RAISE;
END extract_tables;
/

PROMPT Procedure EXTRACT_TABLES created.
