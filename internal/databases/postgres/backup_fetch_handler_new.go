package postgres

import (
	"fmt"
	"github.com/wal-g/wal-g/internal"

	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/utility"
)

func GetPgFetcherNew(dbDataDirectory, fileMask, restoreSpecPath string, skipRedundantTars bool) func(folder storage.Folder, backup Backup) {
	return func(folder storage.Folder, backup Backup) {
		filesToUnwrap, err := backup.GetFilesToUnwrap(fileMask)
		tracelog.ErrorLogger.FatalfOnError("Failed to fetch backup: %v\n", err)

		var spec *TablespaceSpec
		if restoreSpecPath != "" {
			spec = &TablespaceSpec{}
			err := readRestoreSpec(restoreSpecPath, spec)
			errMessage := fmt.Sprintf("Invalid restore specification path %s\n", restoreSpecPath)
			tracelog.ErrorLogger.FatalfOnError(errMessage, err)
		}

		// directory must be empty before starting a deltaFetch
		isEmpty, err := isDirectoryEmpty(dbDataDirectory)
		tracelog.ErrorLogger.FatalfOnError("Failed to fetch backup: %v\n", err)

		if !isEmpty {
			tracelog.ErrorLogger.FatalfOnError("Failed to fetch backup: %v\n",
				NewNonEmptyDbDataDirectoryError(dbDataDirectory))
		}
		config := NewFetchConfig(backup.Name,
			utility.ResolveSymlink(dbDataDirectory), folder, spec, filesToUnwrap, skipRedundantTars)
		err = deltaFetchRecursionNew(config)
		tracelog.ErrorLogger.FatalfOnError("Failed to fetch backup: %v\n", err)
	}
}

// TODO : unit tests
// deltaFetchRecursion function composes Backup object and recursively searches for necessary base backup
func deltaFetchRecursionNew(cfg *FetchConfig) error {
	backup, err := internal.GetBackupByName(cfg.backupName, utility.BaseBackupPath, cfg.folder)
	if err != nil {
		return err
	}
	pgBackup := ToPgBackup(backup)
	sentinelDto, err := pgBackup.GetSentinel()
	if err != nil {
		return err
	}
	cfg.tablespaceSpec = chooseTablespaceSpecification(sentinelDto.TablespaceSpec, cfg.tablespaceSpec)
	sentinelDto.TablespaceSpec = cfg.tablespaceSpec

	if sentinelDto.IsIncremental() {
		tracelog.InfoLogger.Printf("Delta %v at LSN %x \n", cfg.backupName, *(sentinelDto.BackupStartLSN))
		baseFilesToUnwrap, err := GetBaseFilesToUnwrap(sentinelDto.Files, cfg.filesToUnwrap)
		if err != nil {
			return err
		}
		unwrapResult, err := pgBackup.unwrapNew(cfg.dbDataDirectory, sentinelDto, cfg.filesToUnwrap,
			false, cfg.skipRedundantTars)
		if err != nil {
			return err
		}
		cfg.filesToUnwrap = baseFilesToUnwrap
		cfg.backupName = *sentinelDto.IncrementFrom
		if cfg.skipRedundantTars {
			// if we skip redundant tars we should exclude files that
			// no longer need any additional information (completed ones)
			cfg.SkipRedundantFiles(unwrapResult)
		}
		tracelog.InfoLogger.Printf("%v fetched. Downgrading from LSN %x to LSN %x \n",
			cfg.backupName, *(sentinelDto.BackupStartLSN), *(sentinelDto.IncrementFromLSN))
		err = deltaFetchRecursionNew(cfg)
		if err != nil {
			return err
		}

		return nil
	}

	tracelog.InfoLogger.Printf("%x reached. Applying base backup... \n", *(sentinelDto.BackupStartLSN))
	_, err = pgBackup.unwrapNew(cfg.dbDataDirectory, sentinelDto, cfg.filesToUnwrap,
		false, cfg.skipRedundantTars)
	return err
}
