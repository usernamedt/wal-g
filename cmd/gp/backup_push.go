package gp

import (
	"github.com/wal-g/wal-g/cmd/pg"
	"strconv"

	"github.com/wal-g/wal-g/internal/databases/greenplum"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
)

const (
	backupPushShortDescription = "Makes backup and uploads it to storage"
	segmentCfgDirFlag = "seg-cfg-dir"

	permanentShorthand  = "p"
	fullBackupShorthand = "f"

	segmentCfgDirDesc = "Path to the directory containing config file (must be the same on all segments)"
)

var (
	// backupPushCmd represents the backupPush command
	backupPushCmd = &cobra.Command{
		Use:   "backup-push",
		Short: backupPushShortDescription, // TODO : improve description
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			if userData == "" {
				userData = viper.GetString(internal.SentinelUserDataSetting)
			}

			//verifyPageChecksums = verifyPageChecksums || viper.GetBool(internal.VerifyPageChecksumsSetting)
			//storeAllCorruptBlocks = storeAllCorruptBlocks || viper.GetBool(internal.StoreAllCorruptBlocksSetting)
			//tarBallComposerType := postgres.RegularComposer
			//
			//useRatingComposer = useRatingComposer || viper.GetBool(internal.UseRatingComposerSetting)
			//if useRatingComposer {
			//	tarBallComposerType = postgres.RatingComposer
			//}
			//if deltaFromName == "" {
			//	deltaFromName = viper.GetString(internal.DeltaFromNameSetting)
			//}
			//if deltaFromUserData == "" {
			//	deltaFromUserData = viper.GetString(internal.DeltaFromUserDataSetting)
			//}
			//deltaBaseSelector, err := createDeltaBaseSelector(cmd, deltaFromName, deltaFromUserData)
			//tracelog.ErrorLogger.FatalOnError(err)

			if userData == "" {
				userData = viper.GetString(internal.SentinelUserDataSetting)
			}
			arguments := greenplum.NewBackupArguments(permanent, userData, prepareSegmentFwdArgs(), segmentCfgDir, fullBackup)
			backupHandler, err := greenplum.NewBackupHandler(arguments)
			tracelog.ErrorLogger.FatalOnError(err)
			backupHandler.HandleBackupPush()
		},
	}
	permanent     = false
	userData      = ""
	segmentCfgDir = ""
	fullBackup    = false
)

// prepare arguments that are going to be forwarded to segments
func prepareSegmentFwdArgs() []greenplum.SegmentFwdArg {
	return []greenplum.SegmentFwdArg{
		{Name: fullBackupFlag, Value: strconv.FormatBool(fullBackup)},
	}
}

func init() {
	cmd.AddCommand(backupPushCmd)

	backupPushCmd.Flags().BoolVarP(&permanent, pg.PermanentFlag, pg.PermanentShorthand,false, pg.PermanentDesc)
	backupPushCmd.Flags().BoolVarP(&fullBackup, pg.FullBackupFlag, pg.FullBackupShorthand,false, pg.FullBackupDesc)
	backupPushCmd.Flags().StringVar(&userData, pg.AddUserDataFlag, "", pg.AddUserDataDesc)
	backupPushCmd.Flags().StringVar(&segmentCfgDir, segmentCfgDirFlag, "", segmentCfgDirDesc)
	_ = backupPushCmd.MarkFlagRequired(segmentCfgDirFlag)
}
