-- ============================================================================
-- 02_create_sequences.sql
-- Creates sequences for the Oracle 7 metadata capture system.
-- ============================================================================

CREATE SEQUENCE seq_meta_rule_id START WITH 1 INCREMENT BY 1 NOCACHE;
CREATE SEQUENCE seq_meta_run_id  START WITH 1 INCREMENT BY 1 NOCACHE;
CREATE SEQUENCE seq_meta_log_id  START WITH 1 INCREMENT BY 1 NOCACHE;

PROMPT Sequences created successfully.
