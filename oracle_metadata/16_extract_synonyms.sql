-- ============================================================================
-- 16_extract_synonyms.sql
-- Extracts synonym mappings to OS file via UTL_FILE.
-- Oracle 7 compatible.
-- ============================================================================

CREATE OR REPLACE PROCEDURE extract_synonyms (
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

    CURSOR c_synonyms IS
        SELECT synonym_name,
               table_owner,
               table_name,
               db_link
        FROM   all_synonyms
        WHERE  owner = p_owner
        ORDER BY synonym_name;

BEGIN
    v_start := SYSDATE;
    SELECT seq_meta_log_id.NEXTVAL INTO v_log_id FROM dual;

    v_filename := p_owner || '_SYNONYMS_' || p_status || '_' ||
                  TO_CHAR(SYSDATE, 'YYYYMMDD') || '.txt';

    v_file := UTL_FILE.FOPEN(p_directory, v_filename, 'W');

    UTL_FILE.PUT_LINE(v_file, RPAD('=', 80, '='));
    UTL_FILE.PUT_LINE(v_file, 'METADATA EXTRACT: SYNONYMS (' || p_status || ')');
    UTL_FILE.PUT_LINE(v_file, 'OWNER: ' || p_owner);
    UTL_FILE.PUT_LINE(v_file, 'DATE: ' || TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS'));
    UTL_FILE.PUT_LINE(v_file, RPAD('=', 80, '='));
    UTL_FILE.NEW_LINE(v_file);

    UTL_FILE.PUT_LINE(v_file,
        RPAD('SYNONYM_NAME', 32) ||
        RPAD('TABLE_OWNER', 32) ||
        RPAD('TABLE_NAME', 32) ||
        'DB_LINK');
    UTL_FILE.PUT_LINE(v_file, RPAD('-', 80, '-'));

    FOR r_syn IN c_synonyms LOOP
        v_count := v_count + 1;

        v_line := RPAD(r_syn.synonym_name, 32) ||
                  RPAD(NVL(r_syn.table_owner, ''), 32) ||
                  RPAD(NVL(r_syn.table_name, ''), 32) ||
                  NVL(r_syn.db_link, '');
        UTL_FILE.PUT_LINE(v_file, v_line);
    END LOOP;

    UTL_FILE.NEW_LINE(v_file);
    UTL_FILE.PUT_LINE(v_file, 'TOTAL SYNONYMS: ' || TO_CHAR(v_count));
    UTL_FILE.PUT_LINE(v_file, RPAD('=', 80, '='));

    UTL_FILE.FCLOSE(v_file);

    INSERT INTO meta_extract_log (log_id, run_id, procedure_name, p_owner,
        p_status, object_count, output_file, start_time, end_time)
    VALUES (v_log_id, p_run_id, 'EXTRACT_SYNONYMS', p_owner,
        p_status, v_count, v_filename, v_start, SYSDATE);
    COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        IF UTL_FILE.IS_OPEN(v_file) THEN
            UTL_FILE.FCLOSE(v_file);
        END IF;
        INSERT INTO meta_extract_log (log_id, run_id, procedure_name, p_owner,
            p_status, object_count, output_file, start_time, end_time, error_message)
        VALUES (v_log_id, p_run_id, 'EXTRACT_SYNONYMS', p_owner,
            p_status, v_count, v_filename, v_start, SYSDATE,
            SUBSTR(SQLERRM, 1, 2000));
        COMMIT;
        RAISE;
END extract_synonyms;
/

PROMPT Procedure EXTRACT_SYNONYMS created.
