package gp

import (
	"github.com/wal-g/wal-g/cmd/pg"
	"github.com/wal-g/wal-g/internal/databases/greenplum"
	"github.com/wal-g/wal-g/internal/databases/postgres"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
)

const (
	segmentPushShortDescription = "Makes segment backup and uploads it to storage (should be used by backup-push only)"
)

var (
	// segmentPushCmd is an internal WAL-G command
	// that is designed to be called remotely by backup-push from Greenplum coordinator host
	segmentPushCmd = &cobra.Command{
		Use:   "segment-push content_id db_directory",
		Short: segmentPushShortDescription,
		Args: cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			contentId, err := greenplum.ParseContentId(args[0])
			tracelog.ErrorLogger.FatalOnError(err)
			dataDir := args[0]

			verifyPageChecksums := viper.GetBool(internal.VerifyPageChecksumsSetting)
			storeAllCorruptBlocks := viper.GetBool(internal.StoreAllCorruptBlocksSetting)

			tarBallComposerType := postgres.RegularComposer
			if viper.GetBool(internal.UseRatingComposerSetting) {
				tarBallComposerType = postgres.RatingComposer
			}

			if segUserData == "" {
				segUserData = viper.GetString(internal.SentinelUserDataSetting)
			}

			deltaBaseSelector := internal.NewLatestBackupSelector()
			backupPath := greenplum.FormatSegmentBackupPath(contentId)

			arguments := postgres.NewBackupArguments(dataDir, backupPath,
				permanent, verifyPageChecksums, segFullBackup, storeAllCorruptBlocks,
				tarBallComposerType, deltaBaseSelector, segUserData)

			backupHandler, err := postgres.NewBackupHandler(arguments)
			tracelog.ErrorLogger.FatalOnError(err)
			backupHandler.HandleBackupPush()
		},
	}
	segFullBackup            = false
	segUserData              = ""
)

func init() {
	cmd.AddCommand(segmentPushCmd)
	// mark this command as hidden since it is not designed to be used by the end user
	segmentPushCmd.Hidden = true
	segmentPushCmd.Flags().BoolVarP(&permanent, pg.PermanentFlag, pg.PermanentShorthand, false, pg.PermanentDesc)
	segmentPushCmd.Flags().BoolVarP(&segFullBackup, pg.FullBackupFlag, pg.FullBackupShorthand, false, pg.FullBackupDesc)
	segmentPushCmd.Flags().StringVar(&userData, pg.AddUserDataFlag, "", pg.AddUserDataDesc)
}
