-- ============================================================================
-- 19_extract_constraints.sql
-- Extracts constraint definitions and columns to OS file via UTL_FILE.
-- Oracle 7 compatible. Handles LONG column (SEARCH_CONDITION).
-- ============================================================================

CREATE OR REPLACE PROCEDURE extract_constraints (
    p_owner     IN VARCHAR2,
    p_status    IN VARCHAR2,
    p_directory IN VARCHAR2,
    p_run_id    IN NUMBER
) IS
    v_file           UTL_FILE.FILE_TYPE;
    v_filename       VARCHAR2(200);
    v_count          NUMBER := 0;
    v_line           VARCHAR2(2000);
    v_log_id         NUMBER;
    v_start          DATE;
    v_search_cond    VARCHAR2(2000);
    v_ref_owner      VARCHAR2(30);
    v_ref_table      VARCHAR2(30);
    v_ref_constraint VARCHAR2(30);
    v_type_desc      VARCHAR2(30);

    CURSOR c_constraints IS
        SELECT constraint_name,
               constraint_type,
               table_name,
               r_owner,
               r_constraint_name,
               status,
               delete_rule
        FROM   all_constraints
        WHERE  owner = p_owner
        ORDER BY table_name, constraint_type, constraint_name;

    CURSOR c_cons_columns (cp_constraint VARCHAR2) IS
        SELECT column_name, position
        FROM   all_cons_columns
        WHERE  owner           = p_owner
        AND    constraint_name = cp_constraint
        ORDER BY position;

    CURSOR c_ref_table (cp_owner VARCHAR2, cp_constraint VARCHAR2) IS
        SELECT table_name
        FROM   all_constraints
        WHERE  owner           = cp_owner
        AND    constraint_name = cp_constraint;

    CURSOR c_ref_columns (cp_owner VARCHAR2, cp_constraint VARCHAR2) IS
        SELECT column_name, position
        FROM   all_cons_columns
        WHERE  owner           = cp_owner
        AND    constraint_name = cp_constraint
        ORDER BY position;

BEGIN
    v_start := SYSDATE;
    SELECT seq_meta_log_id.NEXTVAL INTO v_log_id FROM dual;

    v_filename := p_owner || '_CONSTRAINTS_' || p_status || '_' ||
                  TO_CHAR(SYSDATE, 'YYYYMMDD') || '.txt';

    v_file := UTL_FILE.FOPEN(p_directory, v_filename, 'W');

    UTL_FILE.PUT_LINE(v_file, RPAD('=', 80, '='));
    UTL_FILE.PUT_LINE(v_file, 'METADATA EXTRACT: CONSTRAINTS (' || p_status || ')');
    UTL_FILE.PUT_LINE(v_file, 'OWNER: ' || p_owner);
    UTL_FILE.PUT_LINE(v_file, 'DATE: ' || TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS'));
    UTL_FILE.PUT_LINE(v_file, RPAD('=', 80, '='));
    UTL_FILE.NEW_LINE(v_file);

    FOR r_con IN c_constraints LOOP
        v_count := v_count + 1;

        -- Decode constraint type
        IF r_con.constraint_type = 'P' THEN
            v_type_desc := 'PRIMARY KEY';
        ELSIF r_con.constraint_type = 'U' THEN
            v_type_desc := 'UNIQUE';
        ELSIF r_con.constraint_type = 'R' THEN
            v_type_desc := 'FOREIGN KEY';
        ELSIF r_con.constraint_type = 'C' THEN
            v_type_desc := 'CHECK';
        ELSE
            v_type_desc := r_con.constraint_type;
        END IF;

        UTL_FILE.PUT_LINE(v_file, '--- CONSTRAINT: ' || r_con.constraint_name || ' ---');
        UTL_FILE.PUT_LINE(v_file, '  Type:   ' || v_type_desc);
        UTL_FILE.PUT_LINE(v_file, '  Table:  ' || r_con.table_name);
        UTL_FILE.PUT_LINE(v_file, '  Status: ' || r_con.status);

        -- Columns
        UTL_FILE.PUT_LINE(v_file, '  Columns:');
        FOR r_col IN c_cons_columns(r_con.constraint_name) LOOP
            UTL_FILE.PUT_LINE(v_file, '    ' ||
                NVL(TO_CHAR(r_col.position), '-') || '. ' || r_col.column_name);
        END LOOP;

        -- Check constraint: show search condition (LONG column)
        IF r_con.constraint_type = 'C' THEN
            BEGIN
                SELECT search_condition INTO v_search_cond
                FROM   all_constraints
                WHERE  owner           = p_owner
                AND    constraint_name = r_con.constraint_name;

                IF v_search_cond IS NOT NULL THEN
                    UTL_FILE.PUT_LINE(v_file, '  Condition: ' ||
                        SUBSTR(v_search_cond, 1, 900));
                END IF;
            EXCEPTION
                WHEN VALUE_ERROR THEN
                    UTL_FILE.PUT_LINE(v_file,
                        '  Condition: (truncated - exceeds 2000 characters)');
                WHEN NO_DATA_FOUND THEN
                    NULL;
            END;
        END IF;

        -- Foreign key: show referenced table and columns
        IF r_con.constraint_type = 'R' AND r_con.r_constraint_name IS NOT NULL THEN
            v_ref_owner := NVL(r_con.r_owner, p_owner);

            v_ref_table := NULL;
            OPEN c_ref_table(v_ref_owner, r_con.r_constraint_name);
            FETCH c_ref_table INTO v_ref_table;
            CLOSE c_ref_table;

            IF v_ref_table IS NOT NULL THEN
                UTL_FILE.PUT_LINE(v_file, '  References: ' ||
                    v_ref_owner || '.' || v_ref_table);

                UTL_FILE.PUT_LINE(v_file, '  Ref Columns:');
                FOR r_ref IN c_ref_columns(v_ref_owner, r_con.r_constraint_name) LOOP
                    UTL_FILE.PUT_LINE(v_file, '    ' ||
                        NVL(TO_CHAR(r_ref.position), '-') || '. ' ||
                        r_ref.column_name);
                END LOOP;
            END IF;

            IF r_con.delete_rule IS NOT NULL THEN
                UTL_FILE.PUT_LINE(v_file, '  Delete Rule: ' || r_con.delete_rule);
            END IF;
        END IF;

        UTL_FILE.NEW_LINE(v_file);
    END LOOP;

    UTL_FILE.NEW_LINE(v_file);
    UTL_FILE.PUT_LINE(v_file, 'TOTAL CONSTRAINTS: ' || TO_CHAR(v_count));
    UTL_FILE.PUT_LINE(v_file, RPAD('=', 80, '='));

    UTL_FILE.FCLOSE(v_file);

    INSERT INTO meta_extract_log (log_id, run_id, procedure_name, p_owner,
        p_status, object_count, output_file, start_time, end_time)
    VALUES (v_log_id, p_run_id, 'EXTRACT_CONSTRAINTS', p_owner,
        p_status, v_count, v_filename, v_start, SYSDATE);
    COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        IF UTL_FILE.IS_OPEN(v_file) THEN
            UTL_FILE.FCLOSE(v_file);
        END IF;
        INSERT INTO meta_extract_log (log_id, run_id, procedure_name, p_owner,
            p_status, object_count, output_file, start_time, end_time, error_message)
        VALUES (v_log_id, p_run_id, 'EXTRACT_CONSTRAINTS', p_owner,
            p_status, v_count, v_filename, v_start, SYSDATE,
            SUBSTR(SQLERRM, 1, 2000));
        COMMIT;
        RAISE;
END extract_constraints;
/

PROMPT Procedure EXTRACT_CONSTRAINTS created.
