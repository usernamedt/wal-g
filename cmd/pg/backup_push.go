package pg

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
)

const (
	backupPushShortDescription     = "Makes backup and uploads it to storage"
	permanentFlag                  = "permanent"
	fullBackupFlag                 = "full"
	verifyPagesFlag                = "verify"
	storeAllCorruptBlocksFlag      = "store-all-corrupt"
	useRatingComposerFlag          = "rating-composer"
	incrementFromFlag              = "increment-from"
	permanentShorthand             = "p"
	fullBackupShorthand            = "f"
	verifyPagesShorthand           = "v"
	storeAllCorruptBlocksShorthand = "s"
	useRatingComposerShorthand     = "r"
	incrementFromShorthand		   = "i"
)

var (
	// backupPushCmd represents the backupPush command
	backupPushCmd = &cobra.Command{
		Use:   "backup-push db_directory",
		Short: backupPushShortDescription, // TODO : improve description
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			uploader, err := internal.ConfigureWalUploader()
			tracelog.ErrorLogger.FatalOnError(err)
			verifyPageChecksums = verifyPageChecksums || viper.GetBool(internal.VerifyPageChecksumsSetting)
			storeAllCorruptBlocks = storeAllCorruptBlocks || viper.GetBool(internal.StoreAllCorruptBlocksSetting)
			tarBallComposerType := internal.RegularComposer
			useRatingComposer = useRatingComposer || viper.GetBool(internal.UseRatingComposerSetting)
			if useRatingComposer {
				tarBallComposerType = internal.RatingComposer
			}
			internal.HandleBackupPush(uploader, args[0], permanent, fullBackup, verifyPageChecksums,
				storeAllCorruptBlocks, tarBallComposerType, incrementFrom)
		},
	}
	permanent             = false
	fullBackup            = false
	verifyPageChecksums   = false
	storeAllCorruptBlocks = false
	useRatingComposer     = false
	incrementFrom 		  = ""
)

func init() {
	cmd.AddCommand(backupPushCmd)

	backupPushCmd.Flags().BoolVarP(&permanent, permanentFlag, permanentShorthand, false, "Pushes permanent backup")
	backupPushCmd.Flags().BoolVarP(&fullBackup, fullBackupFlag, fullBackupShorthand, false, "Make full backup-push")
	backupPushCmd.Flags().BoolVarP(&verifyPageChecksums, verifyPagesFlag, verifyPagesShorthand, false, "Verify page checksums")
	backupPushCmd.Flags().BoolVarP(&storeAllCorruptBlocks, storeAllCorruptBlocksFlag, storeAllCorruptBlocksShorthand,
		false, "Store all corrupt blocks found during page checksum verification")
	backupPushCmd.Flags().BoolVarP(&useRatingComposer, useRatingComposerFlag, useRatingComposerShorthand, false, "Use rating tar composer (beta)")
	backupPushCmd.Flags().StringVarP(&incrementFrom, incrementFromFlag, incrementFromShorthand, "", "Increment from specific backup specified by name or UserData")
}
