-- ============================================================================
-- 20_identify_business_rules.sql
-- Scans PL/SQL source and constraints for business rule patterns.
-- Inserts identified rules into META_BUSINESS_RULES table.
-- Oracle 7 compatible.
-- ============================================================================

CREATE OR REPLACE PROCEDURE identify_business_rules (
    p_owner     IN VARCHAR2,
    p_directory IN VARCHAR2,
    p_run_id    IN NUMBER
) IS
    v_log_id         NUMBER;
    v_start          DATE;
    v_count          NUMBER := 0;
    v_commit_count   NUMBER := 0;
    v_description    VARCHAR2(2000);
    v_trimmed        VARCHAR2(2000);
    v_upper_text     VARCHAR2(2000);
    v_pattern        VARCHAR2(30);
    v_prev_object    VARCHAR2(30) := NULL;
    v_prev_type      VARCHAR2(30) := NULL;
    v_in_exception   VARCHAR2(1)  := 'N';
    v_if_pending     VARCHAR2(1)  := 'N';
    v_if_start_line  NUMBER       := 0;
    v_case_pending   VARCHAR2(1)  := 'N';
    v_case_start_line NUMBER      := 0;
    v_obj_status     VARCHAR2(7);
    v_search_cond    VARCHAR2(2000);
    v_col_list       VARCHAR2(2000);
    v_ref_table      VARCHAR2(30);
    v_ref_owner      VARCHAR2(30);
    v_ref_cols       VARCHAR2(2000);

    -- IF/CASE nesting stacks (comma-delimited line_number_from values)
    -- Used to match END IF / END CASE back to the opening IF / CASE
    v_if_stack       VARCHAR2(2000) := '';
    v_case_stack     VARCHAR2(2000) := '';
    v_pop_tmp        VARCHAR2(2000);
    v_pop_pos        NUMBER;
    v_popped_line    NUMBER;

    -- Trigger body analysis variables
    v_trig_body      VARCHAR2(2000);
    v_trig_truncated VARCHAR2(1);
    v_trig_line      VARCHAR2(2000);
    v_trig_upper     VARCHAR2(2000);
    v_trig_pos       NUMBER;
    v_trig_next      NUMBER;
    v_trig_len       NUMBER;
    v_trig_linenum   NUMBER;
    v_trig_is_simple VARCHAR2(1);
    v_sig_lines      NUMBER;
    v_trig_in_exc    VARCHAR2(1);
    v_trig_if_pend   VARCHAR2(1);
    v_trig_if_start  NUMBER;
    v_trig_case_pend VARCHAR2(1);
    v_trig_case_start NUMBER;
    v_trig_if_stack  VARCHAR2(2000);
    v_trig_case_stack VARCHAR2(2000);

    -- Cursor for PL/SQL objects owned by p_owner
    CURSOR c_source_object IS
        SELECT object_name, object_type, status
        FROM   all_objects
        WHERE  owner       = p_owner
        AND    object_type IN ('PACKAGE', 'PACKAGE BODY', 'PROCEDURE',
                               'FUNCTION')
        AND    status      = 'VALID'
        ORDER BY object_name, object_type;

    -- Cursor for source lines of a specific object
    CURSOR c_source (cp_object_name VARCHAR2, cp_object_type VARCHAR2) IS
        SELECT line, text
        FROM   all_source
        WHERE  owner = p_owner
        AND    name  = cp_object_name
        AND    type  = cp_object_type
        ORDER BY line;

    -- Cursor for constraints
    CURSOR c_constraints IS
        SELECT constraint_name,
               constraint_type,
               table_name,
               status,
               r_owner,
               r_constraint_name
        FROM   all_constraints
        WHERE  owner = p_owner
        ORDER BY table_name, constraint_type, constraint_name;

    -- Cursor for constraint columns
    CURSOR c_cons_cols (cp_constraint VARCHAR2) IS
        SELECT column_name, position
        FROM   all_cons_columns
        WHERE  owner           = p_owner
        AND    constraint_name = cp_constraint
        ORDER BY position;

    -- Cursor for referenced table
    CURSOR c_ref_tab (cp_owner VARCHAR2, cp_constraint VARCHAR2) IS
        SELECT table_name
        FROM   all_constraints
        WHERE  owner           = cp_owner
        AND    constraint_name = cp_constraint;

    -- Cursor for referenced columns
    CURSOR c_ref_cols (cp_owner VARCHAR2, cp_constraint VARCHAR2) IS
        SELECT column_name, position
        FROM   all_cons_columns
        WHERE  owner           = cp_owner
        AND    constraint_name = cp_constraint
        ORDER BY position;

    -- Cursor for trigger metadata
    CURSOR c_trigger_meta IS
        SELECT trigger_name,
               trigger_type,
               triggering_event,
               table_owner,
               table_name,
               status
        FROM   all_triggers
        WHERE  owner = p_owner
        ORDER BY trigger_name;

    -- Helper: insert a rule and manage commits
    PROCEDURE insert_rule (
        p_desc        VARCHAR2,
        p_object      VARCHAR2,
        p_type        VARCHAR2,
        p_obj_status  VARCHAR2,
        p_line_from   NUMBER,
        p_line_to     NUMBER,
        p_pattern     VARCHAR2
    ) IS
    BEGIN
        INSERT INTO meta_business_rules (
            rule_id, run_id, rule_description, source_owner,
            source_object, source_type, source_status,
            line_number_from, line_number_to, pattern_type
        ) VALUES (
            seq_meta_rule_id.NEXTVAL, p_run_id,
            SUBSTR(p_desc, 1, 2000),
            p_owner, p_object, p_type, p_obj_status,
            p_line_from, p_line_to, p_pattern
        );
        v_count := v_count + 1;
        v_commit_count := v_commit_count + 1;
        IF v_commit_count >= 500 THEN
            COMMIT;
            v_commit_count := 0;
        END IF;
    END insert_rule;

    -- Helper: update line_number_to for an existing rule when END IF
    -- or END CASE is found. Matches on object, type, pattern, and the
    -- original line_number_from that was pushed onto the stack.
    PROCEDURE update_rule (
        p_object    VARCHAR2,
        p_type      VARCHAR2,
        p_line_from NUMBER,
        p_end_line  NUMBER,
        p_pattern   VARCHAR2
    ) IS
    BEGIN
        UPDATE meta_business_rules
        SET    line_number_to = p_end_line
        WHERE  run_id           = p_run_id
        AND    source_owner     = p_owner
        AND    source_object    = p_object
        AND    source_type      = p_type
        AND    line_number_from = p_line_from
        AND    pattern_type     = p_pattern;
    END update_rule;

BEGIN
    v_start := SYSDATE;
    SELECT seq_meta_log_id.NEXTVAL INTO v_log_id FROM dual;

    -- ================================================================
    -- PART 1: Scan PL/SQL source for business rule patterns
    -- ================================================================
    FOR r_obj IN c_source_object LOOP

        -- Reset state for each new object
        v_prev_object   := r_obj.object_name;
        v_prev_type     := r_obj.object_type;
        v_obj_status    := r_obj.status;
        v_in_exception  := 'N';
        v_if_pending    := 'N';
        v_case_pending  := 'N';
        v_if_stack      := '';
        v_case_stack    := '';

        FOR r_src IN c_source(r_obj.object_name, r_obj.object_type) LOOP

        -- Prepare trimmed and uppercase versions
        v_trimmed := LTRIM(RTRIM(r_src.text, CHR(10) || CHR(13) || ' '));
        v_upper_text := UPPER(v_trimmed);

        -- Skip blank lines and comment-only lines
        IF v_trimmed IS NULL THEN
            GOTO next_line;
        END IF;
        IF SUBSTR(v_trimmed, 1, 2) = '--' THEN
            GOTO next_line;
        END IF;

        -- ---- Multi-line IF/THEN resolution ----
        IF v_if_pending = 'Y' THEN
            IF INSTR(v_upper_text, 'THEN') > 0 THEN
                insert_rule(
                    'Conditional logic at lines ' ||
                    TO_CHAR(v_if_start_line) || '-' || TO_CHAR(r_src.line),
                    r_obj.object_name, r_obj.object_type, r_obj.status,
                    v_if_start_line, r_src.line, 'IF_THEN');
                -- Push the IF start line onto stack for END IF matching
                v_if_stack := v_if_stack || TO_CHAR(v_if_start_line) || ',';
                v_if_pending := 'N';
                GOTO next_line;
            END IF;
        END IF;

        -- ---- Multi-line CASE/WHEN resolution ----
        IF v_case_pending = 'Y' THEN
            IF INSTR(v_upper_text, 'WHEN') > 0 THEN
                insert_rule(
                    'Case expression at lines ' ||
                    TO_CHAR(v_case_start_line) || '-' || TO_CHAR(r_src.line),
                    r_obj.object_name, r_obj.object_type, r_obj.status,
                    v_case_start_line, r_src.line, 'CASE_WHEN');
                -- Push the CASE start line onto stack for END CASE matching
                v_case_stack := v_case_stack || TO_CHAR(v_case_start_line) || ',';
                v_case_pending := 'N';
                GOTO next_line;
            END IF;
        END IF;

        -- ---- Pattern: IF ... THEN ----
        IF INSTR(v_upper_text, 'IF ') > 0 AND
           INSTR(v_upper_text, 'END IF') = 0 AND
           INSTR(v_upper_text, 'ELSIF') = 0 THEN

            IF INSTR(v_upper_text, ' THEN') > 0 OR
               INSTR(v_upper_text, ')THEN') > 0 THEN
                -- Single-line IF/THEN
                insert_rule(
                    'Conditional logic: ' || SUBSTR(v_trimmed, 1, 200),
                    r_obj.object_name, r_obj.object_type, r_obj.status,
                    r_src.line, r_src.line, 'IF_THEN');
                -- Push onto IF stack for END IF matching
                v_if_stack := v_if_stack || TO_CHAR(r_src.line) || ',';
            ELSE
                -- IF without THEN - multi-line
                v_if_pending := 'Y';
                v_if_start_line := r_src.line;
            END IF;
            GOTO next_line;
        END IF;

        -- ---- Pattern: ELSIF ----
        IF INSTR(v_upper_text, 'ELSIF') > 0 THEN
            insert_rule(
                'Conditional branch (ELSIF): ' || SUBSTR(v_trimmed, 1, 200),
                r_obj.object_name, r_obj.object_type, r_obj.status,
                r_src.line, r_src.line, 'IF_THEN');
            GOTO next_line;
        END IF;

        -- ---- Pattern: CASE ... WHEN ----
        IF INSTR(v_upper_text, 'CASE') > 0 AND
           INSTR(v_upper_text, 'END CASE') = 0 THEN

            IF INSTR(v_upper_text, 'WHEN') > 0 THEN
                -- Single-line CASE/WHEN
                insert_rule(
                    'Case expression: ' || SUBSTR(v_trimmed, 1, 200),
                    r_obj.object_name, r_obj.object_type, r_obj.status,
                    r_src.line, r_src.line, 'CASE_WHEN');
                -- Push onto CASE stack for END CASE matching
                v_case_stack := v_case_stack || TO_CHAR(r_src.line) || ',';
            ELSE
                -- CASE without WHEN - multi-line
                v_case_pending := 'Y';
                v_case_start_line := r_src.line;
            END IF;
            GOTO next_line;
        END IF;

        -- ---- Pattern: END IF - update line_number_to ----
        IF INSTR(v_upper_text, 'END IF') > 0 THEN
            -- Pop most recent IF line_from from stack
            IF v_if_stack IS NOT NULL AND LENGTH(v_if_stack) > 0 THEN
                v_pop_tmp := RTRIM(v_if_stack, ',');
                v_pop_pos := INSTR(v_pop_tmp, ',', -1);
                IF v_pop_pos = 0 THEN
                    v_popped_line := TO_NUMBER(v_pop_tmp);
                    v_if_stack := '';
                ELSE
                    v_popped_line := TO_NUMBER(SUBSTR(v_pop_tmp, v_pop_pos + 1));
                    v_if_stack := SUBSTR(v_pop_tmp, 1, v_pop_pos) || ',';
                END IF;
                update_rule(r_obj.object_name, r_obj.object_type,
                            v_popped_line, r_src.line, 'IF_THEN');
            END IF;
            GOTO next_line;
        END IF;

        -- ---- Pattern: END CASE - update line_number_to ----
        IF INSTR(v_upper_text, 'END CASE') > 0 THEN
            -- Pop most recent CASE line_from from stack
            IF v_case_stack IS NOT NULL AND LENGTH(v_case_stack) > 0 THEN
                v_pop_tmp := RTRIM(v_case_stack, ',');
                v_pop_pos := INSTR(v_pop_tmp, ',', -1);
                IF v_pop_pos = 0 THEN
                    v_popped_line := TO_NUMBER(v_pop_tmp);
                    v_case_stack := '';
                ELSE
                    v_popped_line := TO_NUMBER(SUBSTR(v_pop_tmp, v_pop_pos + 1));
                    v_case_stack := SUBSTR(v_pop_tmp, 1, v_pop_pos) || ',';
                END IF;
                update_rule(r_obj.object_name, r_obj.object_type,
                            v_popped_line, r_src.line, 'CASE_WHEN');
            END IF;
            GOTO next_line;
        END IF;

        -- ---- Pattern: RAISE_APPLICATION_ERROR ----
        IF INSTR(v_upper_text, 'RAISE_APPLICATION_ERROR') > 0 THEN
            insert_rule(
                'Application error raised: ' || SUBSTR(v_trimmed, 1, 200),
                r_obj.object_name, r_obj.object_type, r_obj.status,
                r_src.line, r_src.line, 'RAISE_EXCEPTION');
            GOTO next_line;
        END IF;

        -- ---- Pattern: RAISE (re-raise or named exception) ----
        IF INSTR(v_upper_text, 'RAISE') > 0 AND
           INSTR(v_upper_text, 'RAISE_APPLICATION_ERROR') = 0 THEN
            -- Distinguish RAISE; (re-raise) from RAISE named_exception
            IF v_upper_text = 'RAISE;' OR v_upper_text = 'RAISE ;' THEN
                insert_rule(
                    'Exception re-raised',
                    r_obj.object_name, r_obj.object_type, r_obj.status,
                    r_src.line, r_src.line, 'RAISE_EXCEPTION');
            ELSE
                insert_rule(
                    'Named exception raised: ' || SUBSTR(v_trimmed, 1, 200),
                    r_obj.object_name, r_obj.object_type, r_obj.status,
                    r_src.line, r_src.line, 'RAISE_EXCEPTION');
            END IF;
            GOTO next_line;
        END IF;

        -- ---- Pattern: EXIT WHEN ----
        IF INSTR(v_upper_text, 'EXIT WHEN') > 0 OR
           INSTR(v_upper_text, 'EXIT  WHEN') > 0 THEN
            insert_rule(
                'Loop exit condition: ' || SUBSTR(v_trimmed, 1, 200),
                r_obj.object_name, r_obj.object_type, r_obj.status,
                r_src.line, r_src.line, 'LOOP_EXIT');
            GOTO next_line;
        END IF;

        -- ---- Pattern: EXCEPTION block ----
        IF v_upper_text = 'EXCEPTION' OR
           SUBSTR(v_upper_text, 1, 10) = 'EXCEPTION ' THEN
            v_in_exception := 'Y';
            insert_rule(
                'Exception handler block begins',
                r_obj.object_name, r_obj.object_type, r_obj.status,
                r_src.line, r_src.line, 'EXCEPTION_HANDLER');
            GOTO next_line;
        END IF;

        -- ---- Pattern: WHEN in exception handler (specific handler) ----
        IF v_in_exception = 'Y' AND INSTR(v_upper_text, 'WHEN ') > 0 THEN
            insert_rule(
                'Exception handler: ' || SUBSTR(v_trimmed, 1, 200),
                r_obj.object_name, r_obj.object_type, r_obj.status,
                r_src.line, r_src.line, 'EXCEPTION_HANDLER');
            GOTO next_line;
        END IF;

        -- Reset exception flag on BEGIN/END
        IF v_upper_text = 'BEGIN' OR
           SUBSTR(v_upper_text, 1, 4) = 'END;' OR
           SUBSTR(v_upper_text, 1, 4) = 'END ' THEN
            v_in_exception := 'N';
        END IF;

        -- ---- Pattern: RETURN with value ----
        IF INSTR(v_upper_text, 'RETURN ') > 0 AND
           v_upper_text != 'RETURN;' AND
           INSTR(v_upper_text, 'RETURN;') = 0 THEN
            insert_rule(
                'Return value logic: ' || SUBSTR(v_trimmed, 1, 200),
                r_obj.object_name, r_obj.object_type, r_obj.status,
                r_src.line, r_src.line, 'RETURN_VALUE');
        END IF;

        <<next_line>>
        NULL;
        END LOOP; -- end c_source inner loop

        -- Handle any trailing multi-line patterns for this object
        IF v_if_pending = 'Y' THEN
            insert_rule(
                'Conditional logic (IF without matching THEN)',
                r_obj.object_name, r_obj.object_type, r_obj.status,
                v_if_start_line, v_if_start_line, 'IF_THEN');
        END IF;
        IF v_case_pending = 'Y' THEN
            insert_rule(
                'Case expression (CASE without matching WHEN)',
                r_obj.object_name, r_obj.object_type, r_obj.status,
                v_case_start_line, v_case_start_line, 'CASE_WHEN');
        END IF;

    END LOOP; -- end c_source_object outer loop

    COMMIT;

    -- ================================================================
    -- PART 2: Trigger analysis using trigger_body from ALL_TRIGGERS
    -- ================================================================
    FOR r_trg IN c_trigger_meta LOOP

        -- Always record the trigger metadata rule
        insert_rule(
            'Trigger ' || r_trg.trigger_name || ': ' ||
            r_trg.trigger_type || ' ' || r_trg.triggering_event ||
            ' ON ' || r_trg.table_owner || '.' || r_trg.table_name,
            r_trg.trigger_name, 'TRIGGER', r_trg.status,
            NULL, NULL, 'TRIGGER_RULE');

        -- Read trigger_body (LONG column) into VARCHAR2(2000)
        v_trig_body := NULL;
        v_trig_truncated := 'N';
        BEGIN
            SELECT trigger_body INTO v_trig_body
            FROM   all_triggers
            WHERE  owner        = p_owner
            AND    trigger_name = r_trg.trigger_name;
        EXCEPTION
            WHEN VALUE_ERROR THEN
                -- Body exceeds 2000 chars; read what we can, mark truncated
                v_trig_body := NULL;
                v_trig_truncated := 'Y';
                insert_rule(
                    'Trigger body exceeds 2000 chars - analysis based on partial body',
                    r_trg.trigger_name, 'TRIGGER', r_trg.status,
                    NULL, NULL, 'TRIGGER_RULE');
            WHEN NO_DATA_FOUND THEN
                v_trig_body := NULL;
        END;

        IF v_trig_body IS NULL AND v_trig_truncated = 'N' THEN
            GOTO next_trigger;
        END IF;

        -- --------------------------------------------------------
        -- Determine if trigger is a simple procedure call.
        -- A simple trigger body looks like:
        --   BEGIN proc_name(args); END;
        --   BEGIN schema.proc_name(args); END;
        -- We count "significant" lines (not BEGIN, END, blank, comment).
        -- If only 1 significant line with no control flow keywords,
        -- it is a simple procedure call.
        -- --------------------------------------------------------
        v_sig_lines := 0;
        v_trig_is_simple := 'Y';
        v_trig_len := NVL(LENGTH(v_trig_body), 0);
        v_trig_pos := 1;

        -- First pass: count significant lines and check for complexity
        WHILE v_trig_pos <= v_trig_len LOOP
            v_trig_next := INSTR(v_trig_body, CHR(10), v_trig_pos);
            IF v_trig_next = 0 THEN
                v_trig_next := v_trig_len + 1;
            END IF;

            v_trig_line := SUBSTR(v_trig_body, v_trig_pos,
                                  v_trig_next - v_trig_pos);
            v_trig_line := LTRIM(RTRIM(v_trig_line,
                                 CHR(10) || CHR(13) || ' '));
            v_trig_upper := UPPER(v_trig_line);

            v_trig_pos := v_trig_next + 1;

            -- Skip blanks and comments
            IF v_trig_line IS NULL THEN
                GOTO next_trig_scan;
            END IF;
            IF SUBSTR(v_trig_line, 1, 2) = '--' THEN
                GOTO next_trig_scan;
            END IF;

            -- Skip BEGIN, END, DECLARE keywords
            IF v_trig_upper = 'BEGIN' OR
               v_trig_upper = 'END;' OR
               SUBSTR(v_trig_upper, 1, 4) = 'END ' OR
               v_trig_upper = 'DECLARE' THEN
                GOTO next_trig_scan;
            END IF;

            -- This is a significant line
            v_sig_lines := v_sig_lines + 1;

            -- Check for control flow / complexity keywords
            IF INSTR(v_trig_upper, 'IF ') > 0 OR
               INSTR(v_trig_upper, 'ELSIF') > 0 OR
               INSTR(v_trig_upper, 'CASE') > 0 OR
               INSTR(v_trig_upper, 'LOOP') > 0 OR
               INSTR(v_trig_upper, 'FOR ') > 0 OR
               INSTR(v_trig_upper, 'WHILE ') > 0 OR
               INSTR(v_trig_upper, 'RAISE') > 0 OR
               INSTR(v_trig_upper, 'EXCEPTION') > 0 OR
               INSTR(v_trig_upper, 'CURSOR') > 0 OR
               INSTR(v_trig_upper, 'SELECT') > 0 OR
               INSTR(v_trig_upper, 'INSERT') > 0 OR
               INSTR(v_trig_upper, 'UPDATE') > 0 OR
               INSTR(v_trig_upper, 'DELETE') > 0 THEN
                v_trig_is_simple := 'N';
            END IF;

            <<next_trig_scan>>
            NULL;
        END LOOP;

        -- More than 1 significant line also means not simple
        IF v_sig_lines > 1 THEN
            v_trig_is_simple := 'N';
        END IF;

        -- If truncated, cannot be certain it is simple
        IF v_trig_truncated = 'Y' THEN
            v_trig_is_simple := 'N';
        END IF;

        IF v_trig_is_simple = 'Y' THEN
            -- Simple procedure call trigger - just record that fact
            insert_rule(
                'Trigger delegates to procedure call (simple wrapper)',
                r_trg.trigger_name, 'TRIGGER', r_trg.status,
                NULL, NULL, 'TRIGGER_RULE');
            GOTO next_trigger;
        END IF;

        -- --------------------------------------------------------
        -- Complex trigger: parse body line-by-line for patterns
        -- (same analysis as Part 1 for PL/SQL source)
        -- --------------------------------------------------------
        v_trig_pos       := 1;
        v_trig_linenum   := 0;
        v_trig_in_exc    := 'N';
        v_trig_if_pend   := 'N';
        v_trig_case_pend := 'N';
        v_trig_if_stack  := '';
        v_trig_case_stack := '';

        WHILE v_trig_pos <= v_trig_len LOOP
            v_trig_next := INSTR(v_trig_body, CHR(10), v_trig_pos);
            IF v_trig_next = 0 THEN
                v_trig_next := v_trig_len + 1;
            END IF;

            v_trig_line := SUBSTR(v_trig_body, v_trig_pos,
                                  v_trig_next - v_trig_pos);
            v_trig_line := LTRIM(RTRIM(v_trig_line,
                                 CHR(10) || CHR(13) || ' '));
            v_trig_upper := UPPER(v_trig_line);
            v_trig_linenum := v_trig_linenum + 1;

            v_trig_pos := v_trig_next + 1;

            -- Skip blanks and comments
            IF v_trig_line IS NULL THEN
                GOTO next_trig_line;
            END IF;
            IF SUBSTR(v_trig_line, 1, 2) = '--' THEN
                GOTO next_trig_line;
            END IF;

            -- ---- Multi-line IF/THEN resolution ----
            IF v_trig_if_pend = 'Y' THEN
                IF INSTR(v_trig_upper, 'THEN') > 0 THEN
                    insert_rule(
                        'Trigger conditional logic at lines ' ||
                        TO_CHAR(v_trig_if_start) || '-' ||
                        TO_CHAR(v_trig_linenum),
                        r_trg.trigger_name, 'TRIGGER', r_trg.status,
                        v_trig_if_start, v_trig_linenum, 'IF_THEN');
                    v_trig_if_stack := v_trig_if_stack ||
                        TO_CHAR(v_trig_if_start) || ',';
                    v_trig_if_pend := 'N';
                    GOTO next_trig_line;
                END IF;
            END IF;

            -- ---- Multi-line CASE/WHEN resolution ----
            IF v_trig_case_pend = 'Y' THEN
                IF INSTR(v_trig_upper, 'WHEN') > 0 THEN
                    insert_rule(
                        'Trigger case expression at lines ' ||
                        TO_CHAR(v_trig_case_start) || '-' ||
                        TO_CHAR(v_trig_linenum),
                        r_trg.trigger_name, 'TRIGGER', r_trg.status,
                        v_trig_case_start, v_trig_linenum, 'CASE_WHEN');
                    v_trig_case_stack := v_trig_case_stack ||
                        TO_CHAR(v_trig_case_start) || ',';
                    v_trig_case_pend := 'N';
                    GOTO next_trig_line;
                END IF;
            END IF;

            -- ---- Pattern: IF ... THEN ----
            IF INSTR(v_trig_upper, 'IF ') > 0 AND
               INSTR(v_trig_upper, 'END IF') = 0 AND
               INSTR(v_trig_upper, 'ELSIF') = 0 THEN

                IF INSTR(v_trig_upper, ' THEN') > 0 OR
                   INSTR(v_trig_upper, ')THEN') > 0 THEN
                    insert_rule(
                        'Trigger conditional: ' ||
                        SUBSTR(v_trig_line, 1, 200),
                        r_trg.trigger_name, 'TRIGGER', r_trg.status,
                        v_trig_linenum, v_trig_linenum, 'IF_THEN');
                    v_trig_if_stack := v_trig_if_stack ||
                        TO_CHAR(v_trig_linenum) || ',';
                ELSE
                    v_trig_if_pend := 'Y';
                    v_trig_if_start := v_trig_linenum;
                END IF;
                GOTO next_trig_line;
            END IF;

            -- ---- Pattern: ELSIF ----
            IF INSTR(v_trig_upper, 'ELSIF') > 0 THEN
                insert_rule(
                    'Trigger conditional branch (ELSIF): ' ||
                    SUBSTR(v_trig_line, 1, 200),
                    r_trg.trigger_name, 'TRIGGER', r_trg.status,
                    v_trig_linenum, v_trig_linenum, 'IF_THEN');
                GOTO next_trig_line;
            END IF;

            -- ---- Pattern: CASE ... WHEN ----
            IF INSTR(v_trig_upper, 'CASE') > 0 AND
               INSTR(v_trig_upper, 'END CASE') = 0 THEN

                IF INSTR(v_trig_upper, 'WHEN') > 0 THEN
                    insert_rule(
                        'Trigger case expression: ' ||
                        SUBSTR(v_trig_line, 1, 200),
                        r_trg.trigger_name, 'TRIGGER', r_trg.status,
                        v_trig_linenum, v_trig_linenum, 'CASE_WHEN');
                    v_trig_case_stack := v_trig_case_stack ||
                        TO_CHAR(v_trig_linenum) || ',';
                ELSE
                    v_trig_case_pend := 'Y';
                    v_trig_case_start := v_trig_linenum;
                END IF;
                GOTO next_trig_line;
            END IF;

            -- ---- Pattern: END IF - update line_number_to ----
            IF INSTR(v_trig_upper, 'END IF') > 0 THEN
                IF v_trig_if_stack IS NOT NULL AND
                   LENGTH(v_trig_if_stack) > 0 THEN
                    v_pop_tmp := RTRIM(v_trig_if_stack, ',');
                    v_pop_pos := INSTR(v_pop_tmp, ',', -1);
                    IF v_pop_pos = 0 THEN
                        v_popped_line := TO_NUMBER(v_pop_tmp);
                        v_trig_if_stack := '';
                    ELSE
                        v_popped_line := TO_NUMBER(
                            SUBSTR(v_pop_tmp, v_pop_pos + 1));
                        v_trig_if_stack := SUBSTR(v_pop_tmp, 1,
                            v_pop_pos) || ',';
                    END IF;
                    update_rule(r_trg.trigger_name, 'TRIGGER',
                                v_popped_line, v_trig_linenum, 'IF_THEN');
                END IF;
                GOTO next_trig_line;
            END IF;

            -- ---- Pattern: END CASE - update line_number_to ----
            IF INSTR(v_trig_upper, 'END CASE') > 0 THEN
                IF v_trig_case_stack IS NOT NULL AND
                   LENGTH(v_trig_case_stack) > 0 THEN
                    v_pop_tmp := RTRIM(v_trig_case_stack, ',');
                    v_pop_pos := INSTR(v_pop_tmp, ',', -1);
                    IF v_pop_pos = 0 THEN
                        v_popped_line := TO_NUMBER(v_pop_tmp);
                        v_trig_case_stack := '';
                    ELSE
                        v_popped_line := TO_NUMBER(
                            SUBSTR(v_pop_tmp, v_pop_pos + 1));
                        v_trig_case_stack := SUBSTR(v_pop_tmp, 1,
                            v_pop_pos) || ',';
                    END IF;
                    update_rule(r_trg.trigger_name, 'TRIGGER',
                                v_popped_line, v_trig_linenum, 'CASE_WHEN');
                END IF;
                GOTO next_trig_line;
            END IF;

            -- ---- Pattern: RAISE_APPLICATION_ERROR ----
            IF INSTR(v_trig_upper, 'RAISE_APPLICATION_ERROR') > 0 THEN
                insert_rule(
                    'Trigger error raised: ' ||
                    SUBSTR(v_trig_line, 1, 200),
                    r_trg.trigger_name, 'TRIGGER', r_trg.status,
                    v_trig_linenum, v_trig_linenum, 'RAISE_EXCEPTION');
                GOTO next_trig_line;
            END IF;

            -- ---- Pattern: RAISE ----
            IF INSTR(v_trig_upper, 'RAISE') > 0 AND
               INSTR(v_trig_upper, 'RAISE_APPLICATION_ERROR') = 0 THEN
                IF v_trig_upper = 'RAISE;' OR
                   v_trig_upper = 'RAISE ;' THEN
                    insert_rule(
                        'Trigger exception re-raised',
                        r_trg.trigger_name, 'TRIGGER', r_trg.status,
                        v_trig_linenum, v_trig_linenum, 'RAISE_EXCEPTION');
                ELSE
                    insert_rule(
                        'Trigger named exception raised: ' ||
                        SUBSTR(v_trig_line, 1, 200),
                        r_trg.trigger_name, 'TRIGGER', r_trg.status,
                        v_trig_linenum, v_trig_linenum, 'RAISE_EXCEPTION');
                END IF;
                GOTO next_trig_line;
            END IF;

            -- ---- Pattern: EXIT WHEN ----
            IF INSTR(v_trig_upper, 'EXIT WHEN') > 0 OR
               INSTR(v_trig_upper, 'EXIT  WHEN') > 0 THEN
                insert_rule(
                    'Trigger loop exit: ' ||
                    SUBSTR(v_trig_line, 1, 200),
                    r_trg.trigger_name, 'TRIGGER', r_trg.status,
                    v_trig_linenum, v_trig_linenum, 'LOOP_EXIT');
                GOTO next_trig_line;
            END IF;

            -- ---- Pattern: EXCEPTION block ----
            IF v_trig_upper = 'EXCEPTION' OR
               SUBSTR(v_trig_upper, 1, 10) = 'EXCEPTION ' THEN
                v_trig_in_exc := 'Y';
                insert_rule(
                    'Trigger exception handler block',
                    r_trg.trigger_name, 'TRIGGER', r_trg.status,
                    v_trig_linenum, v_trig_linenum, 'EXCEPTION_HANDLER');
                GOTO next_trig_line;
            END IF;

            -- ---- Pattern: WHEN in exception handler ----
            IF v_trig_in_exc = 'Y' AND
               INSTR(v_trig_upper, 'WHEN ') > 0 THEN
                insert_rule(
                    'Trigger exception handler: ' ||
                    SUBSTR(v_trig_line, 1, 200),
                    r_trg.trigger_name, 'TRIGGER', r_trg.status,
                    v_trig_linenum, v_trig_linenum, 'EXCEPTION_HANDLER');
                GOTO next_trig_line;
            END IF;

            -- Reset exception flag on BEGIN/END
            IF v_trig_upper = 'BEGIN' OR
               SUBSTR(v_trig_upper, 1, 4) = 'END;' OR
               SUBSTR(v_trig_upper, 1, 4) = 'END ' THEN
                v_trig_in_exc := 'N';
            END IF;

            -- ---- Pattern: DML in trigger (business logic) ----
            IF INSTR(v_trig_upper, 'INSERT ') > 0 OR
               INSTR(v_trig_upper, 'INSERT' || CHR(10)) > 0 THEN
                insert_rule(
                    'Trigger performs INSERT: ' ||
                    SUBSTR(v_trig_line, 1, 200),
                    r_trg.trigger_name, 'TRIGGER', r_trg.status,
                    v_trig_linenum, v_trig_linenum, 'TRIGGER_RULE');
                GOTO next_trig_line;
            END IF;

            IF INSTR(v_trig_upper, 'UPDATE ') > 0 THEN
                insert_rule(
                    'Trigger performs UPDATE: ' ||
                    SUBSTR(v_trig_line, 1, 200),
                    r_trg.trigger_name, 'TRIGGER', r_trg.status,
                    v_trig_linenum, v_trig_linenum, 'TRIGGER_RULE');
                GOTO next_trig_line;
            END IF;

            IF INSTR(v_trig_upper, 'DELETE ') > 0 THEN
                insert_rule(
                    'Trigger performs DELETE: ' ||
                    SUBSTR(v_trig_line, 1, 200),
                    r_trg.trigger_name, 'TRIGGER', r_trg.status,
                    v_trig_linenum, v_trig_linenum, 'TRIGGER_RULE');
                GOTO next_trig_line;
            END IF;

            -- ---- Pattern: SELECT INTO (assignment/validation) ----
            IF INSTR(v_trig_upper, 'SELECT') > 0 AND
               INSTR(v_trig_upper, 'INTO') > 0 THEN
                insert_rule(
                    'Trigger performs SELECT INTO: ' ||
                    SUBSTR(v_trig_line, 1, 200),
                    r_trg.trigger_name, 'TRIGGER', r_trg.status,
                    v_trig_linenum, v_trig_linenum, 'TRIGGER_RULE');
                GOTO next_trig_line;
            END IF;

            -- ---- Pattern: :NEW / :OLD references ----
            IF INSTR(v_trig_upper, ':NEW.') > 0 OR
               INSTR(v_trig_upper, ':OLD.') > 0 THEN
                -- Assignment to :NEW (data modification rule)
                IF INSTR(v_trig_line, ':=') > 0 THEN
                    insert_rule(
                        'Trigger column assignment: ' ||
                        SUBSTR(v_trig_line, 1, 200),
                        r_trg.trigger_name, 'TRIGGER', r_trg.status,
                        v_trig_linenum, v_trig_linenum, 'TRIGGER_RULE');
                END IF;
            END IF;

            <<next_trig_line>>
            NULL;
        END LOOP;

        -- Close any pending multi-line patterns for this trigger
        IF v_trig_if_pend = 'Y' THEN
            insert_rule(
                'Trigger conditional (IF without matching THEN)',
                r_trg.trigger_name, 'TRIGGER', r_trg.status,
                v_trig_if_start, v_trig_if_start, 'IF_THEN');
        END IF;
        IF v_trig_case_pend = 'Y' THEN
            insert_rule(
                'Trigger case (CASE without matching WHEN)',
                r_trg.trigger_name, 'TRIGGER', r_trg.status,
                v_trig_case_start, v_trig_case_start, 'CASE_WHEN');
        END IF;

        <<next_trigger>>
        NULL;
    END LOOP;

    COMMIT;

    -- ================================================================
    -- PART 3: Constraints as business rules
    -- ================================================================
    FOR r_con IN c_constraints LOOP

        -- Build column list for this constraint
        v_col_list := '';
        FOR r_col IN c_cons_cols(r_con.constraint_name) LOOP
            IF v_col_list IS NOT NULL AND LENGTH(v_col_list) > 0 THEN
                v_col_list := v_col_list || ', ';
            END IF;
            v_col_list := v_col_list || r_col.column_name;
            -- Safety: prevent exceeding VARCHAR2 limit
            IF LENGTH(v_col_list) > 500 THEN
                v_col_list := v_col_list || '...';
                EXIT;
            END IF;
        END LOOP;

        IF r_con.constraint_type = 'P' THEN
            insert_rule(
                'Primary key ' || r_con.constraint_name ||
                ' on ' || r_con.table_name || '(' || v_col_list || ')',
                r_con.constraint_name, 'TABLE', r_con.status,
                NULL, NULL, 'PK_CONSTRAINT');

        ELSIF r_con.constraint_type = 'U' THEN
            insert_rule(
                'Unique constraint ' || r_con.constraint_name ||
                ' on ' || r_con.table_name || '(' || v_col_list || ')',
                r_con.constraint_name, 'TABLE', r_con.status,
                NULL, NULL, 'UNIQUE_CONSTRAINT');

        ELSIF r_con.constraint_type = 'R' THEN
            -- Resolve referenced table and columns
            v_ref_owner := NVL(r_con.r_owner, p_owner);
            v_ref_table := NULL;
            OPEN c_ref_tab(v_ref_owner, r_con.r_constraint_name);
            FETCH c_ref_tab INTO v_ref_table;
            CLOSE c_ref_tab;

            v_ref_cols := '';
            FOR r_ref IN c_ref_cols(v_ref_owner, r_con.r_constraint_name) LOOP
                IF v_ref_cols IS NOT NULL AND LENGTH(v_ref_cols) > 0 THEN
                    v_ref_cols := v_ref_cols || ', ';
                END IF;
                v_ref_cols := v_ref_cols || r_ref.column_name;
                IF LENGTH(v_ref_cols) > 500 THEN
                    v_ref_cols := v_ref_cols || '...';
                    EXIT;
                END IF;
            END LOOP;

            insert_rule(
                'Foreign key ' || r_con.constraint_name ||
                ' on ' || r_con.table_name || '(' || v_col_list ||
                ') references ' || NVL(v_ref_table, '?') ||
                '(' || v_ref_cols || ')',
                r_con.constraint_name, 'TABLE', r_con.status,
                NULL, NULL, 'FK_CONSTRAINT');

        ELSIF r_con.constraint_type = 'C' THEN
            -- Check constraint: get search condition (LONG column)
            v_search_cond := NULL;
            BEGIN
                SELECT search_condition INTO v_search_cond
                FROM   all_constraints
                WHERE  owner           = p_owner
                AND    constraint_name = r_con.constraint_name;
            EXCEPTION
                WHEN VALUE_ERROR THEN
                    v_search_cond := '(condition exceeds 2000 characters)';
                WHEN NO_DATA_FOUND THEN
                    v_search_cond := '(not found)';
            END;

            insert_rule(
                'Check constraint ' || r_con.constraint_name ||
                ' on ' || r_con.table_name || ': ' ||
                NVL(SUBSTR(v_search_cond, 1, 500), '(null)'),
                r_con.constraint_name, 'TABLE', r_con.status,
                NULL, NULL, 'CHECK_CONSTRAINT');
        END IF;

    END LOOP;

    COMMIT;

    -- Log completion
    INSERT INTO meta_extract_log (log_id, run_id, procedure_name, p_owner,
        p_status, object_count, output_file, start_time, end_time)
    VALUES (v_log_id, p_run_id, 'IDENTIFY_BUSINESS_RULES', p_owner,
        NULL, v_count, NULL, v_start, SYSDATE);
    COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        COMMIT; -- Save any rules inserted so far
        INSERT INTO meta_extract_log (log_id, run_id, procedure_name, p_owner,
            p_status, object_count, output_file, start_time, end_time, error_message)
        VALUES (v_log_id, p_run_id, 'IDENTIFY_BUSINESS_RULES', p_owner,
            NULL, v_count, NULL, v_start, SYSDATE,
            SUBSTR(SQLERRM, 1, 2000));
        COMMIT;
        RAISE;
END identify_business_rules;
/

PROMPT Procedure IDENTIFY_BUSINESS_RULES created.
