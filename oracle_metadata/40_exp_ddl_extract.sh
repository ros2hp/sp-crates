#!/bin/sh
# ============================================================================
# 40_exp_ddl_extract.sh
# Extracts DDL for a schema using Oracle 7 EXP/IMP SHOW=Y approach,
# plus DBMS_JOBS extraction via SQL*Plus.
#
# Usage:
#   ./40_exp_ddl_extract.sh <owner> <output_dir> <connect_string>
#
# Example:
#   ./40_exp_ddl_extract.sh HR /oracle/ddl_output system/manager
#   ./40_exp_ddl_extract.sh SCOTT /tmp/ddl_out scott/tiger
#
# Prerequisites:
#   - Oracle EXP and IMP utilities on PATH
#   - SQL*Plus on PATH
#   - User must have DBA or EXP_FULL_DATABASE role for full schema export
#   - Output directory must exist
# ============================================================================

# --- Validate arguments ---
if [ $# -ne 3 ]; then
    echo "Usage: $0 <owner> <output_dir> <connect_string>"
    echo "  owner          - Schema owner to extract (e.g. HR)"
    echo "  output_dir     - Directory for output files"
    echo "  connect_string - Oracle connect string (e.g. system/manager)"
    exit 1
fi

OWNER=$1
OUTPUT_DIR=$2
CONNECT=$3
DATE_STAMP=`date +%Y%m%d`
OWNER_UPPER=`echo ${OWNER} | tr '[a-z]' '[A-Z]'`

# --- Validate output directory ---
if [ ! -d "${OUTPUT_DIR}" ]; then
    echo "Creating output directory: ${OUTPUT_DIR}"
    mkdir -p "${OUTPUT_DIR}"
    if [ $? -ne 0 ]; then
        echo "ERROR: Cannot create directory ${OUTPUT_DIR}"
        exit 1
    fi
fi

echo "============================================================"
echo " Oracle 7 DDL Extraction"
echo " Owner:     ${OWNER_UPPER}"
echo " Output:    ${OUTPUT_DIR}"
echo " Date:      ${DATE_STAMP}"
echo "============================================================"
echo ""

# --- File names ---
DMP_FILE="${OUTPUT_DIR}/${OWNER_UPPER}_export_${DATE_STAMP}.dmp"
EXP_LOG="${OUTPUT_DIR}/${OWNER_UPPER}_export_${DATE_STAMP}.log"
DDL_FILE="${OUTPUT_DIR}/${OWNER_UPPER}_ddl_${DATE_STAMP}.sql"
IMP_LOG="${OUTPUT_DIR}/${OWNER_UPPER}_imp_show_${DATE_STAMP}.log"
JOBS_FILE="${OUTPUT_DIR}/${OWNER_UPPER}_dbms_jobs_${DATE_STAMP}.sql"
JOBS_LOG="${OUTPUT_DIR}/${OWNER_UPPER}_dbms_jobs_${DATE_STAMP}.log"

# ============================================================
# Step 1: Export schema (structure only, no data)
# ============================================================
echo "--- Step 1: Exporting schema ${OWNER_UPPER} (structure only) ---"

exp ${CONNECT} \
    owner=${OWNER_UPPER} \
    file=${DMP_FILE} \
    log=${EXP_LOG} \
    rows=N \
    grants=Y \
    indexes=Y \
    constraints=Y \
    triggers=Y

EXP_RC=$?
if [ ${EXP_RC} -ne 0 ]; then
    echo "WARNING: EXP returned code ${EXP_RC}. Check ${EXP_LOG}"
    echo "  (Non-zero may still be OK if only warnings)"
fi
echo "  Dump file: ${DMP_FILE}"
echo "  Export log: ${EXP_LOG}"
echo ""

# ============================================================
# Step 2: Extract DDL using IMP SHOW=Y
# ============================================================
echo "--- Step 2: Extracting DDL via IMP SHOW=Y ---"

imp ${CONNECT} \
    file=${DMP_FILE} \
    log=${IMP_LOG} \
    show=Y \
    full=Y

IMP_RC=$?
if [ ${IMP_RC} -ne 0 ]; then
    echo "WARNING: IMP returned code ${IMP_RC}. Check ${IMP_LOG}"
fi

# The IMP log contains the DDL mixed with IMP messages.
# Extract just the SQL statements.
echo "  Filtering DDL statements from IMP output..."

# Write header
echo "-- ============================================================" > ${DDL_FILE}
echo "-- DDL Extract for ${OWNER_UPPER}" >> ${DDL_FILE}
echo "-- Generated: ${DATE_STAMP}" >> ${DDL_FILE}
echo "-- Source: Oracle EXP/IMP SHOW=Y" >> ${DDL_FILE}
echo "-- ============================================================" >> ${DDL_FILE}
echo "" >> ${DDL_FILE}

# Filter: keep lines starting with SQL keywords, skip IMP messages
# IMP messages typically start with ". " or "Import:" or "IMP-"
grep -v "^\. " ${IMP_LOG} \
    | grep -v "^Import:" \
    | grep -v "^IMP-" \
    | grep -v "^Connected" \
    | grep -v "^Export file" \
    | grep -v "^importing" \
    | grep -v "^$" \
    >> ${DDL_FILE}

echo "  DDL file: ${DDL_FILE}"
echo "  IMP log:  ${IMP_LOG}"
echo ""

# ============================================================
# Step 3: Extract DBMS_JOBS via SQL*Plus
# ============================================================
echo "--- Step 3: Extracting DBMS_JOBS definitions ---"

sqlplus -S ${CONNECT} <<EOSQL > ${JOBS_LOG} 2>&1
SET LINESIZE 2000
SET PAGESIZE 0
SET LONG 2000
SET TRIMSPOOL ON
SET FEEDBACK OFF
SET HEADING OFF
SET VERIFY OFF
SPOOL ${JOBS_FILE}

PROMPT -- ============================================================
PROMPT -- DBMS_JOBS Extract for ${OWNER_UPPER}
PROMPT -- Generated: ${DATE_STAMP}
PROMPT -- ============================================================
PROMPT

-- Job summary as comments
SELECT '-- Job ' || TO_CHAR(job) ||
       ': ' || SUBSTR(what, 1, 200) ||
       ' (Interval: ' || NVL(interval, 'NULL') || ')'
FROM   dba_jobs
WHERE  schema_user = '${OWNER_UPPER}'
ORDER BY job;

PROMPT
PROMPT -- Recreate scripts:
PROMPT

-- Generate DBMS_JOB.SUBMIT calls
SELECT 'DECLARE' || CHR(10) ||
       '  v_job NUMBER;' || CHR(10) ||
       'BEGIN' || CHR(10) ||
       '  DBMS_JOB.SUBMIT(' || CHR(10) ||
       '    job       => v_job,' || CHR(10) ||
       '    what      => ''' || REPLACE(what, '''', '''''') || ''',' || CHR(10) ||
       '    next_date => TO_DATE(''' ||
           TO_CHAR(next_date, 'YYYY-MM-DD HH24:MI:SS') ||
           ''', ''YYYY-MM-DD HH24:MI:SS''),' || CHR(10) ||
       '    interval  => ''' || NVL(interval, 'NULL') || '''' || CHR(10) ||
       '  );' || CHR(10) ||
       '  COMMIT;' || CHR(10) ||
       '  DBMS_OUTPUT.PUT_LINE(''Job created: '' || TO_CHAR(v_job));' || CHR(10) ||
       'END;' || CHR(10) ||
       '/'
FROM   dba_jobs
WHERE  schema_user = '${OWNER_UPPER}'
ORDER BY job;

-- Also extract broken jobs info
PROMPT
PROMPT -- Broken jobs:
SELECT '-- Job ' || TO_CHAR(job) || ' is BROKEN' ||
       ' (failures: ' || TO_CHAR(failures) || ')'
FROM   dba_jobs
WHERE  schema_user = '${OWNER_UPPER}'
AND    broken = 'Y'
ORDER BY job;

-- Job count summary
PROMPT
SELECT '-- Total jobs for ${OWNER_UPPER}: ' || TO_CHAR(COUNT(*))
FROM   dba_jobs
WHERE  schema_user = '${OWNER_UPPER}';

SELECT '-- Broken jobs: ' || TO_CHAR(COUNT(*))
FROM   dba_jobs
WHERE  schema_user = '${OWNER_UPPER}'
AND    broken = 'Y';

SPOOL OFF
EXIT
EOSQL

JOBS_RC=$?
if [ ${JOBS_RC} -ne 0 ]; then
    echo "WARNING: SQL*Plus returned code ${JOBS_RC}."
    echo "  You may not have access to DBA_JOBS. Check ${JOBS_LOG}"
    echo "  If not a DBA, try with USER_JOBS (for current user's jobs only)."
fi
echo "  Jobs file: ${JOBS_FILE}"
echo "  Jobs log:  ${JOBS_LOG}"
echo ""

# ============================================================
# Step 4: Summary
# ============================================================
echo "============================================================"
echo " Extraction Complete"
echo "============================================================"
echo ""
echo " Output files:"
echo "   ${DMP_FILE}"
echo "   ${EXP_LOG}"
echo "   ${DDL_FILE}"
echo "   ${IMP_LOG}"
echo "   ${JOBS_FILE}"
echo "   ${JOBS_LOG}"
echo ""

# File sizes
echo " File sizes:"
ls -la ${OUTPUT_DIR}/${OWNER_UPPER}_*_${DATE_STAMP}.* 2>/dev/null \
    | awk '{print "   " $5 "\t" $9}'

echo ""
echo "============================================================"
echo " Next steps:"
echo "   1. Review ${DDL_FILE} for DDL statements"
echo "   2. Review ${JOBS_FILE} for DBMS_JOB definitions"
echo "   3. Run the metadata capture scripts for business rules:"
echo "      SQL> @99_run_all.sql ${OWNER_UPPER} <utl_file_dir_path>"
echo "============================================================"
