-- ============================================================================
-- 13_extract_triggers.sql
-- Extracts trigger metadata and source to OS file via UTL_FILE.
-- Oracle 7 compatible.
-- ============================================================================

CREATE OR REPLACE PROCEDURE extract_triggers (
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

    CURSOR c_triggers IS
        SELECT o.object_name,
               o.status        AS compile_status,
               t.trigger_type,
               t.triggering_event,
               t.table_owner,
               t.table_name,
               t.status        AS trigger_status
        FROM   all_objects  o,
               all_triggers t
        WHERE  o.owner       = p_owner
        AND    o.object_type = 'TRIGGER'
        AND    o.status      = p_status
        AND    t.owner       = o.owner
        AND    t.trigger_name = o.object_name
        ORDER BY o.object_name;

    CURSOR c_source (cp_name VARCHAR2) IS
        SELECT line, text
        FROM   all_source
        WHERE  owner = p_owner
        AND    name  = cp_name
        AND    type  = 'TRIGGER'
        ORDER BY line;

BEGIN
    v_start := SYSDATE;
    SELECT seq_meta_log_id.NEXTVAL INTO v_log_id FROM dual;

    v_filename := p_owner || '_TRIGGERS_' || p_status || '_' ||
                  TO_CHAR(SYSDATE, 'YYYYMMDD') || '.txt';

    v_file := UTL_FILE.FOPEN(p_directory, v_filename, 'W');

    UTL_FILE.PUT_LINE(v_file, RPAD('=', 80, '='));
    UTL_FILE.PUT_LINE(v_file, 'METADATA EXTRACT: TRIGGERS (' || p_status || ')');
    UTL_FILE.PUT_LINE(v_file, 'OWNER: ' || p_owner);
    UTL_FILE.PUT_LINE(v_file, 'DATE: ' || TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS'));
    UTL_FILE.PUT_LINE(v_file, RPAD('=', 80, '='));
    UTL_FILE.NEW_LINE(v_file);

    FOR r_trg IN c_triggers LOOP
        v_count := v_count + 1;

        UTL_FILE.PUT_LINE(v_file, '--- TRIGGER: ' || r_trg.object_name ||
                          ' (Compile: ' || r_trg.compile_status || ') ---');
        UTL_FILE.PUT_LINE(v_file, '  Type:      ' || r_trg.trigger_type);
        UTL_FILE.PUT_LINE(v_file, '  Event:     ' || r_trg.triggering_event);
        UTL_FILE.PUT_LINE(v_file, '  On Table:  ' || r_trg.table_owner || '.' ||
                          r_trg.table_name);
        UTL_FILE.PUT_LINE(v_file, '  Status:    ' || r_trg.trigger_status);
        UTL_FILE.PUT_LINE(v_file, '  -- SOURCE --');

        FOR r_src IN c_source(r_trg.object_name) LOOP
            v_line := SUBSTR(LPAD(TO_CHAR(r_src.line), 5) || ': ' ||
                      RTRIM(r_src.text, CHR(10) || CHR(13)), 1, 1000);
            UTL_FILE.PUT_LINE(v_file, v_line);
        END LOOP;

        UTL_FILE.PUT_LINE(v_file, '--- END TRIGGER: ' || r_trg.object_name || ' ---');
        UTL_FILE.NEW_LINE(v_file);
    END LOOP;

    UTL_FILE.NEW_LINE(v_file);
    UTL_FILE.PUT_LINE(v_file, 'TOTAL TRIGGERS: ' || TO_CHAR(v_count));
    UTL_FILE.PUT_LINE(v_file, RPAD('=', 80, '='));

    UTL_FILE.FCLOSE(v_file);

    INSERT INTO meta_extract_log (log_id, run_id, procedure_name, p_owner,
        p_status, object_count, output_file, start_time, end_time)
    VALUES (v_log_id, p_run_id, 'EXTRACT_TRIGGERS', p_owner,
        p_status, v_count, v_filename, v_start, SYSDATE);
    COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        IF UTL_FILE.IS_OPEN(v_file) THEN
            UTL_FILE.FCLOSE(v_file);
        END IF;
        INSERT INTO meta_extract_log (log_id, run_id, procedure_name, p_owner,
            p_status, object_count, output_file, start_time, end_time, error_message)
        VALUES (v_log_id, p_run_id, 'EXTRACT_TRIGGERS', p_owner,
            p_status, v_count, v_filename, v_start, SYSDATE,
            SUBSTR(SQLERRM, 1, 2000));
        COMMIT;
        RAISE;
END extract_triggers;
/

PROMPT Procedure EXTRACT_TRIGGERS created.
