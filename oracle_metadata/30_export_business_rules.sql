-- ============================================================================
-- 30_export_business_rules.sql
-- Exports META_BUSINESS_RULES to OS file via UTL_FILE, grouped by object
-- with summary statistics.
-- Oracle 7 compatible.
-- ============================================================================

CREATE OR REPLACE PROCEDURE export_business_rules (
    p_owner     IN VARCHAR2,
    p_directory IN VARCHAR2,
    p_run_id    IN NUMBER
) IS
    v_file         UTL_FILE.FILE_TYPE;
    v_filename     VARCHAR2(200);
    v_log_id       NUMBER;
    v_start        DATE;
    v_total        NUMBER := 0;
    v_line         VARCHAR2(2000);
    v_prev_object  VARCHAR2(30) := NULL;
    v_prev_type    VARCHAR2(30) := NULL;
    v_line_range   VARCHAR2(40);

    -- Summary counters
    v_cnt_if_then        NUMBER := 0;
    v_cnt_case_when      NUMBER := 0;
    v_cnt_raise          NUMBER := 0;
    v_cnt_loop_exit      NUMBER := 0;
    v_cnt_exception      NUMBER := 0;
    v_cnt_return         NUMBER := 0;
    v_cnt_trigger        NUMBER := 0;
    v_cnt_cursor         NUMBER := 0;
    v_cnt_pk             NUMBER := 0;
    v_cnt_uk             NUMBER := 0;
    v_cnt_fk             NUMBER := 0;
    v_cnt_check          NUMBER := 0;

    CURSOR c_rules IS
        SELECT rule_id,
               rule_description,
               source_object,
               source_type,
               source_status,
               line_number_from,
               line_number_to,
               pattern_type
        FROM   meta_business_rules
        WHERE  run_id       = p_run_id
        AND    source_owner = p_owner
        ORDER BY source_type, source_object, line_number_from, rule_id;

    CURSOR c_total IS
        SELECT COUNT(*)
        FROM   meta_business_rules
        WHERE  run_id       = p_run_id
        AND    source_owner = p_owner;

BEGIN
    v_start := SYSDATE;
    SELECT seq_meta_log_id.NEXTVAL INTO v_log_id FROM dual;

    v_filename := p_owner || '_BUSINESS_RULES_' ||
                  TO_CHAR(SYSDATE, 'YYYYMMDD') || '.txt';

    v_file := UTL_FILE.FOPEN(p_directory, v_filename, 'W');

    -- Get total count
    OPEN c_total;
    FETCH c_total INTO v_total;
    CLOSE c_total;

    -- Header
    UTL_FILE.PUT_LINE(v_file, RPAD('=', 80, '='));
    UTL_FILE.PUT_LINE(v_file, 'BUSINESS RULES REPORT');
    UTL_FILE.PUT_LINE(v_file, 'RUN ID: ' || TO_CHAR(p_run_id) ||
                      '    OWNER: ' || p_owner ||
                      '    DATE: ' || TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS'));
    UTL_FILE.PUT_LINE(v_file, 'TOTAL RULES IDENTIFIED: ' || TO_CHAR(v_total));
    UTL_FILE.PUT_LINE(v_file, RPAD('=', 80, '='));
    UTL_FILE.NEW_LINE(v_file);

    FOR r_rule IN c_rules LOOP

        -- Group header when object changes
        IF r_rule.source_object != NVL(v_prev_object, '~') OR
           r_rule.source_type   != NVL(v_prev_type, '~') THEN

            IF v_prev_object IS NOT NULL THEN
                UTL_FILE.NEW_LINE(v_file);
            END IF;

            UTL_FILE.PUT_LINE(v_file, '--- ' || r_rule.source_type || ': ' ||
                              r_rule.source_object ||
                              ' (' || NVL(r_rule.source_status, 'N/A') || ') ---');

            v_prev_object := r_rule.source_object;
            v_prev_type   := r_rule.source_type;
        END IF;

        -- Format line range
        IF r_rule.line_number_from IS NOT NULL THEN
            IF r_rule.line_number_to IS NOT NULL AND
               r_rule.line_number_to != r_rule.line_number_from THEN
                v_line_range := 'Lines ' || TO_CHAR(r_rule.line_number_from) ||
                                '-' || TO_CHAR(r_rule.line_number_to);
            ELSE
                v_line_range := 'Line ' || TO_CHAR(r_rule.line_number_from);
            END IF;
        ELSE
            v_line_range := 'N/A';
        END IF;

        -- Write rule
        v_line := '  [Rule ' || TO_CHAR(r_rule.rule_id) || '] ' ||
                  RPAD(r_rule.pattern_type, 22) ||
                  '(' || v_line_range || '): ' ||
                  SUBSTR(r_rule.rule_description, 1, 500);
        UTL_FILE.PUT_LINE(v_file, SUBSTR(v_line, 1, 1000));

        -- Update summary counters
        IF r_rule.pattern_type = 'IF_THEN' THEN
            v_cnt_if_then := v_cnt_if_then + 1;
        ELSIF r_rule.pattern_type = 'CASE_WHEN' THEN
            v_cnt_case_when := v_cnt_case_when + 1;
        ELSIF r_rule.pattern_type = 'RAISE_EXCEPTION' THEN
            v_cnt_raise := v_cnt_raise + 1;
        ELSIF r_rule.pattern_type = 'LOOP_EXIT' THEN
            v_cnt_loop_exit := v_cnt_loop_exit + 1;
        ELSIF r_rule.pattern_type = 'EXCEPTION_HANDLER' THEN
            v_cnt_exception := v_cnt_exception + 1;
        ELSIF r_rule.pattern_type = 'RETURN_VALUE' THEN
            v_cnt_return := v_cnt_return + 1;
        ELSIF r_rule.pattern_type = 'TRIGGER_RULE' THEN
            v_cnt_trigger := v_cnt_trigger + 1;
        ELSIF r_rule.pattern_type = 'CURSOR_FILTER' THEN
            v_cnt_cursor := v_cnt_cursor + 1;
        ELSIF r_rule.pattern_type = 'PK_CONSTRAINT' THEN
            v_cnt_pk := v_cnt_pk + 1;
        ELSIF r_rule.pattern_type = 'UNIQUE_CONSTRAINT' THEN
            v_cnt_uk := v_cnt_uk + 1;
        ELSIF r_rule.pattern_type = 'FK_CONSTRAINT' THEN
            v_cnt_fk := v_cnt_fk + 1;
        ELSIF r_rule.pattern_type = 'CHECK_CONSTRAINT' THEN
            v_cnt_check := v_cnt_check + 1;
        END IF;

    END LOOP;

    -- Summary footer
    UTL_FILE.NEW_LINE(v_file);
    UTL_FILE.PUT_LINE(v_file, RPAD('=', 80, '='));
    UTL_FILE.PUT_LINE(v_file, 'SUMMARY BY PATTERN TYPE');
    UTL_FILE.PUT_LINE(v_file, RPAD('-', 40, '-'));
    UTL_FILE.PUT_LINE(v_file, '  ' || RPAD('IF_THEN:', 24)         || TO_CHAR(v_cnt_if_then));
    UTL_FILE.PUT_LINE(v_file, '  ' || RPAD('CASE_WHEN:', 24)       || TO_CHAR(v_cnt_case_when));
    UTL_FILE.PUT_LINE(v_file, '  ' || RPAD('RAISE_EXCEPTION:', 24) || TO_CHAR(v_cnt_raise));
    UTL_FILE.PUT_LINE(v_file, '  ' || RPAD('LOOP_EXIT:', 24)       || TO_CHAR(v_cnt_loop_exit));
    UTL_FILE.PUT_LINE(v_file, '  ' || RPAD('EXCEPTION_HANDLER:', 24) || TO_CHAR(v_cnt_exception));
    UTL_FILE.PUT_LINE(v_file, '  ' || RPAD('RETURN_VALUE:', 24)    || TO_CHAR(v_cnt_return));
    UTL_FILE.PUT_LINE(v_file, '  ' || RPAD('TRIGGER_RULE:', 24)    || TO_CHAR(v_cnt_trigger));
    UTL_FILE.PUT_LINE(v_file, '  ' || RPAD('CURSOR_FILTER:', 24)   || TO_CHAR(v_cnt_cursor));
    UTL_FILE.PUT_LINE(v_file, '  ' || RPAD('PK_CONSTRAINT:', 24)   || TO_CHAR(v_cnt_pk));
    UTL_FILE.PUT_LINE(v_file, '  ' || RPAD('UNIQUE_CONSTRAINT:', 24) || TO_CHAR(v_cnt_uk));
    UTL_FILE.PUT_LINE(v_file, '  ' || RPAD('FK_CONSTRAINT:', 24)   || TO_CHAR(v_cnt_fk));
    UTL_FILE.PUT_LINE(v_file, '  ' || RPAD('CHECK_CONSTRAINT:', 24) || TO_CHAR(v_cnt_check));
    UTL_FILE.PUT_LINE(v_file, RPAD('-', 40, '-'));
    UTL_FILE.PUT_LINE(v_file, '  ' || RPAD('TOTAL:', 24) || TO_CHAR(v_total));
    UTL_FILE.PUT_LINE(v_file, RPAD('=', 80, '='));

    UTL_FILE.FCLOSE(v_file);

    INSERT INTO meta_extract_log (log_id, run_id, procedure_name, p_owner,
        p_status, object_count, output_file, start_time, end_time)
    VALUES (v_log_id, p_run_id, 'EXPORT_BUSINESS_RULES', p_owner,
        NULL, v_total, v_filename, v_start, SYSDATE);
    COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        IF UTL_FILE.IS_OPEN(v_file) THEN
            UTL_FILE.FCLOSE(v_file);
        END IF;
        INSERT INTO meta_extract_log (log_id, run_id, procedure_name, p_owner,
            p_status, object_count, output_file, start_time, end_time, error_message)
        VALUES (v_log_id, p_run_id, 'EXPORT_BUSINESS_RULES', p_owner,
            NULL, v_total, v_filename, v_start, SYSDATE,
            SUBSTR(SQLERRM, 1, 2000));
        COMMIT;
        RAISE;
END export_business_rules;
/

PROMPT Procedure EXPORT_BUSINESS_RULES created.
