-- ============================================================================
-- 18_extract_indexes.sql
-- Extracts index definitions and columns to OS file via UTL_FILE.
-- Oracle 7 compatible.
-- ============================================================================

CREATE OR REPLACE PROCEDURE extract_indexes (
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

    CURSOR c_indexes IS
        SELECT index_name,
               table_owner,
               table_name,
               uniqueness
        FROM   all_indexes
        WHERE  owner = p_owner
        ORDER BY table_name, index_name;

    CURSOR c_ind_columns (cp_index VARCHAR2) IS
        SELECT column_name,
               column_position
        FROM   all_ind_columns
        WHERE  index_owner = p_owner
        AND    index_name  = cp_index
        ORDER BY column_position;

BEGIN
    v_start := SYSDATE;
    SELECT seq_meta_log_id.NEXTVAL INTO v_log_id FROM dual;

    v_filename := p_owner || '_INDEXES_' || p_status || '_' ||
                  TO_CHAR(SYSDATE, 'YYYYMMDD') || '.txt';

    v_file := UTL_FILE.FOPEN(p_directory, v_filename, 'W');

    UTL_FILE.PUT_LINE(v_file, RPAD('=', 80, '='));
    UTL_FILE.PUT_LINE(v_file, 'METADATA EXTRACT: INDEXES (' || p_status || ')');
    UTL_FILE.PUT_LINE(v_file, 'OWNER: ' || p_owner);
    UTL_FILE.PUT_LINE(v_file, 'DATE: ' || TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS'));
    UTL_FILE.PUT_LINE(v_file, RPAD('=', 80, '='));
    UTL_FILE.NEW_LINE(v_file);

    FOR r_idx IN c_indexes LOOP
        v_count := v_count + 1;

        UTL_FILE.PUT_LINE(v_file, '--- INDEX: ' || r_idx.index_name || ' ---');
        UTL_FILE.PUT_LINE(v_file, '  Table:      ' ||
            r_idx.table_owner || '.' || r_idx.table_name);
        UTL_FILE.PUT_LINE(v_file, '  Uniqueness: ' || r_idx.uniqueness);
        UTL_FILE.PUT_LINE(v_file, '  Columns:');

        FOR r_col IN c_ind_columns(r_idx.index_name) LOOP
            UTL_FILE.PUT_LINE(v_file, '    ' ||
                TO_CHAR(r_col.column_position) || '. ' || r_col.column_name);
        END LOOP;

        UTL_FILE.NEW_LINE(v_file);
    END LOOP;

    UTL_FILE.NEW_LINE(v_file);
    UTL_FILE.PUT_LINE(v_file, 'TOTAL INDEXES: ' || TO_CHAR(v_count));
    UTL_FILE.PUT_LINE(v_file, RPAD('=', 80, '='));

    UTL_FILE.FCLOSE(v_file);

    INSERT INTO meta_extract_log (log_id, run_id, procedure_name, p_owner,
        p_status, object_count, output_file, start_time, end_time)
    VALUES (v_log_id, p_run_id, 'EXTRACT_INDEXES', p_owner,
        p_status, v_count, v_filename, v_start, SYSDATE);
    COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        IF UTL_FILE.IS_OPEN(v_file) THEN
            UTL_FILE.FCLOSE(v_file);
        END IF;
        INSERT INTO meta_extract_log (log_id, run_id, procedure_name, p_owner,
            p_status, object_count, output_file, start_time, end_time, error_message)
        VALUES (v_log_id, p_run_id, 'EXTRACT_INDEXES', p_owner,
            p_status, v_count, v_filename, v_start, SYSDATE,
            SUBSTR(SQLERRM, 1, 2000));
        COMMIT;
        RAISE;
END extract_indexes;
/

PROMPT Procedure EXTRACT_INDEXES created.
