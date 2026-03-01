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

    -- Cursor for all PL/SQL source, ordered to process object-by-object
    CURSOR c_source IS
        SELECT s.name,
               s.type,
               s.line,
               s.text,
               o.status
        FROM   all_source  s,
               all_objects o
        WHERE  s.owner = p_owner
        AND    s.type IN ('PACKAGE', 'PACKAGE BODY', 'PROCEDURE',
                          'FUNCTION', 'TRIGGER')
        AND    o.owner       = s.owner
        AND    o.object_name = s.name
        AND    o.object_type = s.type
        ORDER BY s.name, s.type, s.line;

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

BEGIN
    v_start := SYSDATE;
    SELECT seq_meta_log_id.NEXTVAL INTO v_log_id FROM dual;

    -- ================================================================
    -- PART 1: Scan PL/SQL source for business rule patterns
    -- ================================================================
    FOR r_src IN c_source LOOP

        -- Reset state tracking when we move to a new object
        IF r_src.name != NVL(v_prev_object, '~') OR
           r_src.type != NVL(v_prev_type, '~') THEN
            -- Close any pending multi-line patterns from previous object
            IF v_if_pending = 'Y' THEN
                insert_rule(
                    'Conditional logic (IF without THEN on same line)',
                    v_prev_object, v_prev_type, v_obj_status,
                    v_if_start_line, v_if_start_line, 'IF_THEN');
            END IF;
            IF v_case_pending = 'Y' THEN
                insert_rule(
                    'Case expression (CASE without WHEN on same line)',
                    v_prev_object, v_prev_type, v_obj_status,
                    v_case_start_line, v_case_start_line, 'CASE_WHEN');
            END IF;

            v_prev_object   := r_src.name;
            v_prev_type     := r_src.type;
            v_obj_status    := r_src.status;
            v_in_exception  := 'N';
            v_if_pending    := 'N';
            v_case_pending  := 'N';
        END IF;

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
                    r_src.name, r_src.type, r_src.status,
                    v_if_start_line, r_src.line, 'IF_THEN');
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
                    r_src.name, r_src.type, r_src.status,
                    v_case_start_line, r_src.line, 'CASE_WHEN');
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
                    r_src.name, r_src.type, r_src.status,
                    r_src.line, r_src.line, 'IF_THEN');
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
                r_src.name, r_src.type, r_src.status,
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
                    r_src.name, r_src.type, r_src.status,
                    r_src.line, r_src.line, 'CASE_WHEN');
            ELSE
                -- CASE without WHEN - multi-line
                v_case_pending := 'Y';
                v_case_start_line := r_src.line;
            END IF;
            GOTO next_line;
        END IF;

        -- ---- Pattern: RAISE_APPLICATION_ERROR ----
        IF INSTR(v_upper_text, 'RAISE_APPLICATION_ERROR') > 0 THEN
            insert_rule(
                'Application error raised: ' || SUBSTR(v_trimmed, 1, 200),
                r_src.name, r_src.type, r_src.status,
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
                    r_src.name, r_src.type, r_src.status,
                    r_src.line, r_src.line, 'RAISE_EXCEPTION');
            ELSE
                insert_rule(
                    'Named exception raised: ' || SUBSTR(v_trimmed, 1, 200),
                    r_src.name, r_src.type, r_src.status,
                    r_src.line, r_src.line, 'RAISE_EXCEPTION');
            END IF;
            GOTO next_line;
        END IF;

        -- ---- Pattern: EXIT WHEN ----
        IF INSTR(v_upper_text, 'EXIT WHEN') > 0 OR
           INSTR(v_upper_text, 'EXIT  WHEN') > 0 THEN
            insert_rule(
                'Loop exit condition: ' || SUBSTR(v_trimmed, 1, 200),
                r_src.name, r_src.type, r_src.status,
                r_src.line, r_src.line, 'LOOP_EXIT');
            GOTO next_line;
        END IF;

        -- ---- Pattern: EXCEPTION block ----
        IF v_upper_text = 'EXCEPTION' OR
           SUBSTR(v_upper_text, 1, 10) = 'EXCEPTION ' THEN
            v_in_exception := 'Y';
            insert_rule(
                'Exception handler block begins',
                r_src.name, r_src.type, r_src.status,
                r_src.line, r_src.line, 'EXCEPTION_HANDLER');
            GOTO next_line;
        END IF;

        -- ---- Pattern: WHEN in exception handler (specific handler) ----
        IF v_in_exception = 'Y' AND INSTR(v_upper_text, 'WHEN ') > 0 THEN
            insert_rule(
                'Exception handler: ' || SUBSTR(v_trimmed, 1, 200),
                r_src.name, r_src.type, r_src.status,
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
                r_src.name, r_src.type, r_src.status,
                r_src.line, r_src.line, 'RETURN_VALUE');
        END IF;

        <<next_line>>
        NULL;
    END LOOP;

    -- Handle any trailing multi-line patterns
    IF v_if_pending = 'Y' THEN
        insert_rule(
            'Conditional logic (IF without matching THEN)',
            v_prev_object, v_prev_type, v_obj_status,
            v_if_start_line, v_if_start_line, 'IF_THEN');
    END IF;
    IF v_case_pending = 'Y' THEN
        insert_rule(
            'Case expression (CASE without matching WHEN)',
            v_prev_object, v_prev_type, v_obj_status,
            v_case_start_line, v_case_start_line, 'CASE_WHEN');
    END IF;

    COMMIT;

    -- ================================================================
    -- PART 2: Trigger metadata as business rules
    -- ================================================================
    FOR r_trg IN c_trigger_meta LOOP
        insert_rule(
            'Trigger ' || r_trg.trigger_name || ': ' ||
            r_trg.trigger_type || ' ' || r_trg.triggering_event ||
            ' ON ' || r_trg.table_owner || '.' || r_trg.table_name,
            r_trg.trigger_name, 'TRIGGER', r_trg.status,
            NULL, NULL, 'TRIGGER_RULE');
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
