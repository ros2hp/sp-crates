-- ============================================================================
-- 01_create_tables.sql
-- Creates metadata staging tables for the Oracle 7 metadata capture system.
-- ============================================================================

-- Run tracking table
CREATE TABLE meta_run_log (
    run_id          NUMBER(10)    NOT NULL,
    run_date        DATE          DEFAULT SYSDATE NOT NULL,
    p_owner         VARCHAR2(30)  NOT NULL,
    p_directory     VARCHAR2(200) NOT NULL,
    status          VARCHAR2(20)  DEFAULT 'RUNNING',
    CONSTRAINT pk_meta_run_log PRIMARY KEY (run_id)
);

-- Business rules table
CREATE TABLE meta_business_rules (
    rule_id          NUMBER(10)     NOT NULL,
    run_id           NUMBER(10)     NOT NULL,
    rule_description VARCHAR2(2000) NOT NULL,
    source_owner     VARCHAR2(30)   NOT NULL,
    source_object    VARCHAR2(30)   NOT NULL,
    source_type      VARCHAR2(30)   NOT NULL,
    source_status    VARCHAR2(7),
    line_number_from NUMBER(10),
    line_number_to   NUMBER(10),
    pattern_type     VARCHAR2(30)   NOT NULL,
    created_date     DATE           DEFAULT SYSDATE,
    CONSTRAINT pk_meta_business_rules PRIMARY KEY (rule_id),
    CONSTRAINT fk_mbr_run FOREIGN KEY (run_id)
        REFERENCES meta_run_log (run_id)
);

CREATE INDEX idx_mbr_run_id ON meta_business_rules (run_id);
CREATE INDEX idx_mbr_owner_obj ON meta_business_rules (source_owner, source_object);
CREATE INDEX idx_mbr_pattern ON meta_business_rules (pattern_type);

-- Extraction log table
CREATE TABLE meta_extract_log (
    log_id          NUMBER(10)     NOT NULL,
    run_id          NUMBER(10)     NOT NULL,
    procedure_name  VARCHAR2(60)   NOT NULL,
    p_owner         VARCHAR2(30)   NOT NULL,
    p_status        VARCHAR2(7),
    object_count    NUMBER(10)     DEFAULT 0,
    output_file     VARCHAR2(200),
    start_time      DATE,
    end_time        DATE,
    error_message   VARCHAR2(2000),
    CONSTRAINT pk_meta_extract_log PRIMARY KEY (log_id),
    CONSTRAINT fk_mel_run FOREIGN KEY (run_id)
        REFERENCES meta_run_log (run_id)
);

CREATE INDEX idx_mel_run_id ON meta_extract_log (run_id);

COMMIT;

PROMPT Tables created successfully.
