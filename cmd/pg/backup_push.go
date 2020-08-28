package pg

import (
	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"

	"github.com/spf13/cobra"
)

const (
	BackupPushShortDescription = "Makes backup and uploads it to storage"
	PermanentFlag              = "permanent"
	FullBackupFlag             = "full"
	UseRatingComposer          = "rating-composer"
	PermanentShorthand         = "p"
	FullBackupShorthand        = "f"
	UseRatingComposerShortHand = "r"
)

var (
	// backupPushCmd represents the backupPush command
	backupPushCmd = &cobra.Command{
		Use:   "backup-push db_directory",
		Short: BackupPushShortDescription, // TODO : improve description
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			uploader, err := internal.ConfigureWalUploader()
			tracelog.ErrorLogger.FatalOnError(err)
			tarBallComposerType := internal.RegularComposer
			useRatingComposer = useRatingComposer || viper.GetBool(internal.UseRatingComposerSetting)
			if useRatingComposer {
				tarBallComposerType = internal.RatingComposer
			}
			internal.HandleBackupPush(uploader, args[0], permanent, fullBackup, tarBallComposerType)
		},
	}
	permanent  = false
	fullBackup = false
	useRatingComposer = false
)

func init() {
	Cmd.AddCommand(backupPushCmd)

	backupPushCmd.Flags().BoolVarP(&permanent, PermanentFlag, PermanentShorthand, false, "Pushes permanent backup")
	backupPushCmd.Flags().BoolVarP(&fullBackup, FullBackupFlag, FullBackupShorthand, false, "Make full backup-push")
	backupPushCmd.Flags().BoolVarP(&useRatingComposer, UseRatingComposer, UseRatingComposerShortHand, false, "Use rating tar composer (beta)")
}
