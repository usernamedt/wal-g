package gp

import (
	"github.com/wal-g/wal-g/cmd/pg"
	"github.com/wal-g/wal-g/internal/databases/greenplum"
	"github.com/wal-g/wal-g/internal/databases/postgres"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
)

const (
	segmentFetchShortDescription = "Fetches a backup of single segment from storage"
)

var segFileMask string
var segRestoreSpec string
var segReverseDeltaUnpack bool
var segSkipRedundantTars bool
var segFetchTargetUserData string
var segDstDataDir string

var backupFetchCmd = &cobra.Command{
	Use:   "segment-fetch content_id destination_directory [backup_name | --target-user-data <data>]",
	Short: segmentFetchShortDescription,
	Args:  cobra.RangeArgs(2,3),
	Run: func(cmd *cobra.Command, args []string) {
		contentId, err := greenplum.ParseContentId(args[0])
		tracelog.ErrorLogger.FatalOnError(err)

		segDstDataDir = args[1]

		if segFetchTargetUserData == "" {
			segFetchTargetUserData = viper.GetString(internal.FetchTargetUserDataSetting)
		}
		// cut the first (content_id) argument to make args Postgres-compatible
		targetBackupSelector, err := pg.CreateTargetFetchBackupSelector(cmd, args[1:], segFetchTargetUserData)
		tracelog.ErrorLogger.FatalOnError(err)

		folder, err := internal.ConfigureFolder()
		tracelog.ErrorLogger.FatalOnError(err)

		var pgFetcher func(folder storage.Folder, backup internal.Backup)
		segReverseDeltaUnpack = segReverseDeltaUnpack || viper.GetBool(internal.UseReverseUnpackSetting)
		segSkipRedundantTars = segSkipRedundantTars || viper.GetBool(internal.SkipRedundantTarsSetting)
		if segReverseDeltaUnpack {
			pgFetcher = postgres.GetPgFetcherNew(segDstDataDir, segFileMask, segRestoreSpec, segSkipRedundantTars)
		} else {
			pgFetcher = postgres.GetPgFetcherOld(segDstDataDir, segFileMask, segRestoreSpec)
		}

		internal.HandleBackupFetch(folder, targetBackupSelector, greenplum.FormatSegmentBackupPath(contentId), pgFetcher)
	},
}

func init() {
	backupFetchCmd.Flags().StringVar(&segFileMask, pg.MaskFlag, "", pg.MaskFlagDescription)
	backupFetchCmd.Flags().StringVar(&segRestoreSpec, pg.RestoreSpecFlag, "", pg.RestoreSpecDescription)
	backupFetchCmd.Flags().BoolVar(&segReverseDeltaUnpack, pg.ReverseDeltaUnpackFlag, false, pg.ReverseDeltaUnpackDescription)
	backupFetchCmd.Flags().BoolVar(&segSkipRedundantTars, pg.SkipRedundantTarsFlag, false, pg.SkipRedundantTarsDescription)
	backupFetchCmd.Flags().StringVar(&segFetchTargetUserData, pg.TargetUserDataFlag, "", pg.TargetUserDataDescription)
	cmd.AddCommand(backupFetchCmd)
}
