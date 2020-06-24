package internal

import (
	"fmt"
	"github.com/wal-g/wal-g/internal/walparser"
	"log"

	"github.com/jackc/pgx"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
)

type NoPostgresVersionError struct {
	error
}

func newNoPostgresVersionError() NoPostgresVersionError {
	return NoPostgresVersionError{errors.New("Postgres version not set, cannot determine backup query")}
}

func (err NoPostgresVersionError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type UnsupportedPostgresVersionError struct {
	error
}

func newUnsupportedPostgresVersionError(version int) UnsupportedPostgresVersionError {
	return UnsupportedPostgresVersionError{errors.Errorf("Could not determine backup query for version %d", version)}
}

func (err UnsupportedPostgresVersionError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

// The QueryRunner interface for controlling database during backup
type QueryRunner interface {
	// This call should inform the database that we are going to copy cluster's contents
	// Should fail if backup is currently impossible
	StartBackup(backup string) (string, string, bool, error)
	// Inform database that contents are copied, get information on backup
	StopBackup() (string, string, string, error)
	// get pg_stat_all_all_tables data
	GetStatistics()
}

// PgQueryRunner is implementation for controlling PostgreSQL 9.0+
type PgQueryRunner struct {
	connection       *pgx.Conn
	Version          int
	SystemIdentifier *uint64
}

// BuildGetVersion formats a query to retrieve PostgreSQL numeric version
func (queryRunner *PgQueryRunner) buildGetVersion() string {
	return "select (current_setting('server_version_num'))::int"
}

// BuildStartBackup formats a query that starts backup according to server features and version
func (queryRunner *PgQueryRunner) BuildStartBackup() (string, error) {
	// TODO: rewrite queries for older versions to remove pg_is_in_recovery()
	// where pg_start_backup() will fail on standby anyway
	switch {
	case queryRunner.Version >= 100000:
		return "SELECT case when pg_is_in_recovery() then '' else (pg_walfile_name_offset(lsn)).file_name end, lsn::text, pg_is_in_recovery() FROM pg_start_backup($1, true, false) lsn", nil
	case queryRunner.Version >= 90600:
		return "SELECT case when pg_is_in_recovery() then '' else (pg_xlogfile_name_offset(lsn)).file_name end, lsn::text, pg_is_in_recovery() FROM pg_start_backup($1, true, false) lsn", nil
	case queryRunner.Version >= 90000:
		return "SELECT case when pg_is_in_recovery() then '' else (pg_xlogfile_name_offset(lsn)).file_name end, lsn::text, pg_is_in_recovery() FROM pg_start_backup($1, true) lsn", nil
	case queryRunner.Version == 0:
		return "", newNoPostgresVersionError()
	default:
		return "", newUnsupportedPostgresVersionError(queryRunner.Version)
	}
}

// BuildStopBackup formats a query that stops backup according to server features and version
func (queryRunner *PgQueryRunner) BuildStopBackup() (string, error) {
	switch {
	case queryRunner.Version >= 90600:
		return "SELECT labelfile, spcmapfile, lsn FROM pg_stop_backup(false)", nil
	case queryRunner.Version >= 90000:
		return "SELECT (pg_xlogfile_name_offset(lsn)).file_name, lpad((pg_xlogfile_name_offset(lsn)).file_offset::text, 8, '0') AS file_offset, lsn::text FROM pg_stop_backup() lsn", nil
	case queryRunner.Version == 0:
		return "", newNoPostgresVersionError()
	default:
		return "", newUnsupportedPostgresVersionError(queryRunner.Version)
	}
}

// NewPgQueryRunner builds QueryRunner from available connection
func newPgQueryRunner(conn *pgx.Conn) (*PgQueryRunner, error) {
	r := &PgQueryRunner{connection: conn}
	err := r.getVersion()
	if err != nil {
		return nil, err
	}
	err = r.getSystemIdentifier()
	if err != nil {
		tracelog.WarningLogger.Printf("Couldn't get system identifier because of error: '%v'\n", err)
	}

	return r, nil
}

func (queryRunner *PgQueryRunner) buildGetSystemIdentifier() string {
	return "select system_identifier from pg_control_system();"
}

// Retrieve PostgreSQL numeric version
func (queryRunner *PgQueryRunner) getVersion() (err error) {
	conn := queryRunner.connection
	err = conn.QueryRow(queryRunner.buildGetVersion()).Scan(&queryRunner.Version)
	return errors.Wrap(err, "GetVersion: getting Postgres version failed")
}

func (queryRunner *PgQueryRunner) getSystemIdentifier() (err error) {
	conn := queryRunner.connection
	err = conn.QueryRow(queryRunner.buildGetSystemIdentifier()).Scan(&queryRunner.SystemIdentifier)
	return errors.Wrap(err, "System Identifier: getting identifier of DB failed")
}

// StartBackup informs the database that we are starting copy of cluster contents
func (queryRunner *PgQueryRunner) startBackup(backup string) (backupName string, lsnString string, inRecovery bool, dataDir string, err error) {
	tracelog.InfoLogger.Println("Calling pg_start_backup()")
	startBackupQuery, err := queryRunner.BuildStartBackup()
	conn := queryRunner.connection
	if err != nil {
		return "", "", false, "", errors.Wrap(err, "QueryRunner StartBackup: Building start backup query failed")
	}

	if err = conn.QueryRow(startBackupQuery, backup).Scan(&backupName, &lsnString, &inRecovery); err != nil {
		return "", "", false, "", errors.Wrap(err, "QueryRunner StartBackup: pg_start_backup() failed")
	}

	if err = conn.QueryRow("show data_directory").Scan(&dataDir); err != nil {
		return "", "", false, "", errors.Wrap(err, "QueryRunner StartBackup: show data_directory failed")
	}

	return backupName, lsnString, inRecovery, dataDir, nil
}

// StopBackup informs the database that copy is over
func (queryRunner *PgQueryRunner) stopBackup() (label string, offsetMap string, lsnStr string, err error) {
	tracelog.InfoLogger.Println("Calling pg_stop_backup()")
	conn := queryRunner.connection

	tx, err := conn.Begin()
	if err != nil {
		return "", "", "", errors.Wrap(err, "QueryRunner StopBackup: transaction begin failed")
	}
	defer tx.Rollback()

	_, err = tx.Exec("SET statement_timeout=0;")
	if err != nil {
		return "", "", "", errors.Wrap(err, "QueryRunner StopBackup: failed setting statement timeout in transaction")
	}

	stopBackupQuery, err := queryRunner.BuildStopBackup()
	if err != nil {
		return "", "", "", errors.Wrap(err, "QueryRunner StopBackup: Building stop backup query failed")
	}

	err = tx.QueryRow(stopBackupQuery).Scan(&label, &offsetMap, &lsnStr)
	if err != nil {
		return "", "", "", errors.Wrap(err, "QueryRunner StopBackup: stop backup failed")
	}

	err = tx.Commit()
	if err != nil {
		return "", "", "", errors.Wrap(err, "QueryRunner StopBackup: commit failed")
	}

	return label, offsetMap, lsnStr, nil
}

// todo desc
func (queryRunner *PgQueryRunner) BuildStatisticsQuery() (string, error) {
	switch {
	case queryRunner.Version >= 100000:
		return "SELECT c.relfilenode, c.reltablespace, s.n_tup_ins, s.n_tup_upd, s.n_tup_del " +
			"FROM pg_class c LEFT OUTER JOIN pg_stat_all_tables s ON c.oid = s.relid " +
			"WHERE relfilenode != 0 AND n_tup_ins IS NOT NULL", nil
	case queryRunner.Version >= 90600:
		return "SELECT c.relfilenode, c.reltablespace, s.n_tup_ins, s.n_tup_upd, s.n_tup_del " +
			"FROM pg_class c LEFT OUTER JOIN pg_stat_all_tables s ON c.oid = s.relid " +
			"WHERE relfilenode != 0 AND n_tup_ins IS NOT NULL", nil
	case queryRunner.Version >= 90000:
		return "SELECT c.relfilenode, c.reltablespace, s.n_tup_ins, s.n_tup_upd, s.n_tup_del " +
			"FROM pg_class c LEFT OUTER JOIN pg_stat_all_tables s ON c.oid = s.relid " +
			"WHERE relfilenode != 0 AND n_tup_ins IS NOT NULL", nil
	case queryRunner.Version == 0:
		return "", newNoPostgresVersionError()
	default:
		return "", newUnsupportedPostgresVersionError(queryRunner.Version)
	}
}

// todo desc
func (queryRunner *PgQueryRunner) getStatistics(dbInfo *PgDatabaseInfo) (map[walparser.RelFileNode]PgStatRow, error) {
	tracelog.InfoLogger.Println("Querying pg_stat_all_tables")
	getStatQuery, err := queryRunner.BuildStatisticsQuery()
	conn := queryRunner.connection
	if err != nil {
		return nil, errors.Wrap(err, "QueryRunner GetStatistics: Building get statistics query failed")
	}

	rows, err := conn.Query(getStatQuery)
	if err != nil {
		return nil, errors.Wrap(err, "QueryRunner GetStatistics: pg_stat_all_tables query failed")
	}

	defer rows.Close()
	pgStatRows := make(map[walparser.RelFileNode]PgStatRow)
	for rows.Next() {
		var statRow PgStatRow
		var relFileNodeId uint32
		var spcNode uint32
		if err := rows.Scan(&relFileNodeId, &spcNode, &statRow.nTupleInserted, &statRow.nTupleUpdated,
			&statRow.nTupleDeleted); err != nil {
			log.Println(err.Error())
		}
		relFileNode := walparser.RelFileNode{DBNode: dbInfo.oid,
			RelNode: walparser.Oid(relFileNodeId), SpcNode: walparser.Oid(spcNode)}
		// if tablespace id is zero, use the default database tablespace id
		if relFileNode.SpcNode == walparser.Oid(0) {
			relFileNode.SpcNode = dbInfo.tblSpcOid
		}
		pgStatRows[relFileNode] = statRow
	}

	if rows.Err() != nil {
		return nil, rows.Err()
	}

	return pgStatRows, nil
}

func (queryRunner *PgQueryRunner) BuildGetDatabasesQuery() (string, error) {
	switch {
	case queryRunner.Version >= 90600:
		return "SELECT oid, datname, dattablespace FROM pg_database WHERE datallowconn", nil
	case queryRunner.Version >= 90000:
		return "SELECT oid, datname, dattablespace FROM pg_database WHERE datallowconn", nil
	case queryRunner.Version == 0:
		return "", newNoPostgresVersionError()
	default:
		return "", newUnsupportedPostgresVersionError(queryRunner.Version)
	}
}

// todo desc
func (queryRunner *PgQueryRunner) getDatabases() ([]PgDatabaseInfo, error) {
	tracelog.InfoLogger.Println("Querying pg_database")
	getDbInfoQuery, err := queryRunner.BuildGetDatabasesQuery()
	conn := queryRunner.connection
	if err != nil {
		return nil, errors.Wrap(err, "QueryRunner GetDatabases: Building db names query failed")
	}

	rows, err := conn.Query(getDbInfoQuery)
	if err != nil {
		return nil, errors.Wrap(err, "QueryRunner GetDatabases: pg_database query failed")
	}

	defer rows.Close()
	databases := make([]PgDatabaseInfo, 0)
	for rows.Next() {
		dbInfo := PgDatabaseInfo{}
		var dbOid uint32
		var dbTblSpcOid uint32
		if err := rows.Scan(&dbOid, &dbInfo.name, &dbTblSpcOid); err != nil {
			log.Println(err.Error())
		}
		dbInfo.oid = walparser.Oid(dbOid)
		dbInfo.tblSpcOid = walparser.Oid(dbTblSpcOid)
		databases = append(databases, dbInfo)
	}

	if rows.Err() != nil {
		return nil, rows.Err()
	}

	return databases, nil
}