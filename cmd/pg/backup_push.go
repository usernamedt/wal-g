package pg

import (
	"fmt"
	"github.com/wal-g/wal-g/internal/databases/postgres"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
)

const (
	backupPushShortDescription = "Makes backup and uploads it to storage"

	permanentFlag             = "permanent"
	fullBackupFlag            = "full"
	verifyPagesFlag           = "verify"
	storeAllCorruptBlocksFlag = "store-all-corrupt"
	useRatingComposerFlag     = "rating-composer"
	deltaFromUserDataFlag     = "delta-from-user-data"
	deltaFromNameFlag         = "delta-from-name"
	addUserDataFlag           = "add-user-data"

	permanentShorthand             = "p"
	fullBackupShorthand            = "f"
	verifyPagesShorthand           = "v"
	storeAllCorruptBlocksShorthand = "s"
	useRatingComposerShorthand     = "r"
)

var (
	// backupPushCmd represents the backupPush command
	backupPushCmd = &cobra.Command{
		Use:   "backup-push db_directory",
		Short: backupPushShortDescription, // TODO : improve description
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			uploader, err := postgres.ConfigureWalUploader()
			tracelog.ErrorLogger.FatalOnError(err)
			verifyPageChecksums = verifyPageChecksums || viper.GetBool(internal.VerifyPageChecksumsSetting)
			storeAllCorruptBlocks = storeAllCorruptBlocks || viper.GetBool(internal.StoreAllCorruptBlocksSetting)
			tarBallComposerType := postgres.RegularComposer
			useRatingComposer = useRatingComposer || viper.GetBool(internal.UseRatingComposerSetting)
			if useRatingComposer {
				tarBallComposerType = postgres.RatingComposer
			}
			if deltaFromName == "" {
				deltaFromName = viper.GetString(internal.DeltaFromNameSetting)
			}
			if deltaFromUserData == "" {
				deltaFromUserData = viper.GetString(internal.DeltaFromUserDataSetting)
			}
			deltaBaseSelector, err := createDeltaBaseSelector(cmd, deltaFromName, deltaFromUserData)
			tracelog.ErrorLogger.FatalOnError(err)

			if userData == "" {
				userData = viper.GetString(internal.SentinelUserDataSetting)
			}

			postgres.HandleBackupPush(uploader, args[0], permanent, fullBackup, verifyPageChecksums,
				storeAllCorruptBlocks, tarBallComposerType, deltaBaseSelector, userData)
		},
	}
	permanent             = false
	fullBackup            = false
	verifyPageChecksums   = false
	storeAllCorruptBlocks = false
	useRatingComposer     = false
	deltaFromName         = ""
	deltaFromUserData     = ""
	userData              = ""
)

// create the BackupSelector for delta backup base according to the provided flags
func createDeltaBaseSelector(cmd *cobra.Command, targetBackupName, targetUserData string) (internal.BackupSelector, error) {
	backupSelector, err := internal.NewTargetBackupSelector(targetUserData, targetBackupName, postgres.NewGenericMetaFetcher())
	if err != nil {
		fmt.Println(cmd.UsageString())
		return nil, err
	}
	return backupSelector, nil
}

func init() {
	cmd.AddCommand(backupPushCmd)

	backupPushCmd.Flags().BoolVarP(&permanent, permanentFlag, permanentShorthand, false, "Pushes permanent backup")
	backupPushCmd.Flags().BoolVarP(&fullBackup, fullBackupFlag, fullBackupShorthand, false, "Make full backup-push")
	backupPushCmd.Flags().BoolVarP(&verifyPageChecksums, verifyPagesFlag, verifyPagesShorthand, false, "Verify page checksums")
	backupPushCmd.Flags().BoolVarP(&storeAllCorruptBlocks, storeAllCorruptBlocksFlag, storeAllCorruptBlocksShorthand,
		false, "Store all corrupt blocks found during page checksum verification")
	backupPushCmd.Flags().BoolVarP(&useRatingComposer, useRatingComposerFlag, useRatingComposerShorthand, false, "Use rating tar composer (beta)")
	backupPushCmd.Flags().StringVar(&deltaFromName, deltaFromNameFlag, "", "Select the backup specified by name as the target for the delta backup")
	backupPushCmd.Flags().StringVar(&deltaFromUserData, deltaFromUserDataFlag, "", "Select the backup specified by UserData as the target for the delta backup")
	backupPushCmd.Flags().StringVar(&userData, addUserDataFlag, "", "Write the provided user data to the backup sentinel and metadata files.")
}
