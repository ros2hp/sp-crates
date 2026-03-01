-- ============================================================================
-- 17_extract_sequences.sql
-- Extracts sequence definitions to OS file via UTL_FILE.
-- Oracle 7 compatible.
-- ============================================================================

CREATE OR REPLACE PROCEDURE extract_sequences (
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

    CURSOR c_sequences IS
        SELECT sequence_name,
               min_value,
               max_value,
               increment_by,
               cycle_flag,
               order_flag,
               cache_size,
               last_number
        FROM   all_sequences
        WHERE  sequence_owner = p_owner
        ORDER BY sequence_name;

BEGIN
    v_start := SYSDATE;
    SELECT seq_meta_log_id.NEXTVAL INTO v_log_id FROM dual;

    v_filename := p_owner || '_SEQUENCES_' || p_status || '_' ||
                  TO_CHAR(SYSDATE, 'YYYYMMDD') || '.txt';

    v_file := UTL_FILE.FOPEN(p_directory, v_filename, 'W');

    UTL_FILE.PUT_LINE(v_file, RPAD('=', 80, '='));
    UTL_FILE.PUT_LINE(v_file, 'METADATA EXTRACT: SEQUENCES (' || p_status || ')');
    UTL_FILE.PUT_LINE(v_file, 'OWNER: ' || p_owner);
    UTL_FILE.PUT_LINE(v_file, 'DATE: ' || TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS'));
    UTL_FILE.PUT_LINE(v_file, RPAD('=', 80, '='));
    UTL_FILE.NEW_LINE(v_file);

    FOR r_seq IN c_sequences LOOP
        v_count := v_count + 1;

        UTL_FILE.PUT_LINE(v_file, '--- SEQUENCE: ' || r_seq.sequence_name || ' ---');
        UTL_FILE.PUT_LINE(v_file, '  Min Value:    ' ||
            NVL(TO_CHAR(r_seq.min_value), 'N/A'));
        UTL_FILE.PUT_LINE(v_file, '  Max Value:    ' ||
            NVL(TO_CHAR(r_seq.max_value), 'N/A'));
        UTL_FILE.PUT_LINE(v_file, '  Increment By: ' ||
            NVL(TO_CHAR(r_seq.increment_by), 'N/A'));
        UTL_FILE.PUT_LINE(v_file, '  Cycle:        ' ||
            NVL(r_seq.cycle_flag, 'N'));
        UTL_FILE.PUT_LINE(v_file, '  Order:        ' ||
            NVL(r_seq.order_flag, 'N'));
        UTL_FILE.PUT_LINE(v_file, '  Cache Size:   ' ||
            NVL(TO_CHAR(r_seq.cache_size), '0'));
        UTL_FILE.PUT_LINE(v_file, '  Last Number:  ' ||
            NVL(TO_CHAR(r_seq.last_number), 'N/A'));
        UTL_FILE.NEW_LINE(v_file);
    END LOOP;

    UTL_FILE.NEW_LINE(v_file);
    UTL_FILE.PUT_LINE(v_file, 'TOTAL SEQUENCES: ' || TO_CHAR(v_count));
    UTL_FILE.PUT_LINE(v_file, RPAD('=', 80, '='));

    UTL_FILE.FCLOSE(v_file);

    INSERT INTO meta_extract_log (log_id, run_id, procedure_name, p_owner,
        p_status, object_count, output_file, start_time, end_time)
    VALUES (v_log_id, p_run_id, 'EXTRACT_SEQUENCES', p_owner,
        p_status, v_count, v_filename, v_start, SYSDATE);
    COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        IF UTL_FILE.IS_OPEN(v_file) THEN
            UTL_FILE.FCLOSE(v_file);
        END IF;
        INSERT INTO meta_extract_log (log_id, run_id, procedure_name, p_owner,
            p_status, object_count, output_file, start_time, end_time, error_message)
        VALUES (v_log_id, p_run_id, 'EXTRACT_SEQUENCES', p_owner,
            p_status, v_count, v_filename, v_start, SYSDATE,
            SUBSTR(SQLERRM, 1, 2000));
        COMMIT;
        RAISE;
END extract_sequences;
/

PROMPT Procedure EXTRACT_SEQUENCES created.
