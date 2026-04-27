#!/bin/sh

# Oracle Database Initialization Script
# This script sets up Oracle database for Change Data Capture (CDC) using Debezium
# It performs the following key operations:
# 1. Enables archive log mode
# 2. Configures LogMiner required settings
# 3. Creates LogMiner tablespace
# 4. Sets up CDC user with necessary privileges

# Create recovery area directory and set permissions
mkdir -p /opt/oracle/oradata/recovery_area
chown -R oracle:dba /opt/oracle/oradata/recovery_area
chmod -R 775 /opt/oracle/oradata/recovery_area

# Set Oracle System ID and enable archive log mode
# This is required for CDC to track database changes
ORACLE_SID=FREE
export ORACLE_SID
sqlplus /nolog <<- EOF
	CONNECT sys/test AS SYSDBA
	-- Configure recovery area
	alter system set db_recovery_file_dest_size = 2G scope=both;
	alter system set db_recovery_file_dest = '/opt/oracle/oradata/recovery_area' scope=both;

	-- Shutdown and restart database in mount mode to enable archive logging
	shutdown immediate
	startup mount
	alter database archivelog;
	alter database open;
  -- Verify archive log mode is enabled
	archive log list
	exit;
EOF

# Configure LogMiner prerequisites
# These settings are required for Oracle LogMiner to function properly
sqlplus sys/test@//localhost:1521/FREE as sysdba <<- EOF
  -- Enable supplemental logging for tracking changes
  ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
  -- Remove login attempt restrictions for CDC user
  ALTER PROFILE DEFAULT LIMIT FAILED_LOGIN_ATTEMPTS UNLIMITED;
  exit;
EOF

# Create LogMiner tablespace in root container
# This tablespace will store LogMiner metadata and temporary data
sqlplus sys/test@//localhost:1521/FREE as sysdba <<- EOF
  CREATE TABLESPACE LOGMINER_TBS DATAFILE '/opt/oracle/oradata/FREE/logminer_tbs.dbf' SIZE 25M REUSE AUTOEXTEND ON MAXSIZE UNLIMITED;
  exit;
EOF

# Create LogMiner tablespace in PDB container
# Required for multi-tenant architecture support
sqlplus sys/test@//localhost:1521/FREEPDB1 as sysdba <<- EOF
  CREATE TABLESPACE LOGMINER_TBS DATAFILE '/opt/oracle/oradata/FREE/FREEPDB1/logminer_tbs.dbf' SIZE 25M REUSE AUTOEXTEND ON MAXSIZE UNLIMITED;
  exit;
EOF

# Create and configure CDC user with necessary privileges
# This user will be used by Debezium to access and process database changes
sqlplus sys/test@//localhost:1521/FREE as sysdba <<- EOF
  -- Create CDC user with unlimited tablespace quota
  CREATE USER c##dbzuser IDENTIFIED BY dbz DEFAULT TABLESPACE LOGMINER_TBS QUOTA UNLIMITED ON LOGMINER_TBS CONTAINER=ALL;

  GRANT CREATE SESSION TO c##dbzuser CONTAINER=ALL;
  GRANT SET CONTAINER TO c##dbzuser CONTAINER=ALL;
  GRANT SELECT ON V_$DATABASE to c##dbzuser CONTAINER=ALL;
  GRANT FLASHBACK ANY TABLE TO c##dbzuser CONTAINER=ALL;
  GRANT SELECT ANY TABLE TO c##dbzuser CONTAINER=ALL;
  GRANT SELECT_CATALOG_ROLE TO c##dbzuser CONTAINER=ALL;
  GRANT EXECUTE_CATALOG_ROLE TO c##dbzuser CONTAINER=ALL;
  GRANT SELECT ANY TRANSACTION TO c##dbzuser CONTAINER=ALL;
  GRANT LOGMINING TO c##dbzuser CONTAINER=ALL;

  GRANT CREATE TABLE TO c##dbzuser CONTAINER=ALL;
  GRANT LOCK ANY TABLE TO c##dbzuser CONTAINER=ALL;
  GRANT CREATE SEQUENCE TO c##dbzuser CONTAINER=ALL;

  GRANT EXECUTE ON DBMS_LOGMNR TO c##dbzuser CONTAINER=ALL;
  GRANT EXECUTE ON DBMS_LOGMNR_D TO c##dbzuser CONTAINER=ALL;

  GRANT SELECT ON V_$LOG TO c##dbzuser CONTAINER=ALL;
  GRANT SELECT ON V_$LOG_HISTORY TO c##dbzuser CONTAINER=ALL;
  GRANT SELECT ON V_$LOGMNR_LOGS TO c##dbzuser CONTAINER=ALL;
  GRANT SELECT ON V_$LOGMNR_CONTENTS TO c##dbzuser CONTAINER=ALL;
  GRANT SELECT ON V_$LOGMNR_PARAMETERS TO c##dbzuser CONTAINER=ALL;
  GRANT SELECT ON V_$LOGFILE TO c##dbzuser CONTAINER=ALL;
  GRANT SELECT ON V_$ARCHIVED_LOG TO c##dbzuser CONTAINER=ALL;
  GRANT SELECT ON V_$ARCHIVE_DEST_STATUS TO c##dbzuser CONTAINER=ALL;
  GRANT SELECT ON V_$TRANSACTION TO c##dbzuser CONTAINER=ALL;

  GRANT SELECT ON V_$MYSTAT TO c##dbzuser CONTAINER=ALL;
  GRANT SELECT ON V_$STATNAME TO c##dbzuser CONTAINER=ALL;

  exit;
EOF