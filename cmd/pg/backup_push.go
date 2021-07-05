package pg

import (
	"fmt"

	"github.com/wal-g/wal-g/utility"

	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal/databases/postgres"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
)

const (
	BackupPushShortDescription = "Makes backup and uploads it to storage"

	PermanentFlag             = "permanent"
	FullBackupFlag            = "full"
	VerifyPagesFlag           = "verify"
	StoreAllCorruptBlocksFlag = "store-all-corrupt"
	UseRatingComposerFlag     = "rating-composer"
	DeltaFromUserDataFlag     = "delta-from-user-data"
	DeltaFromNameFlag         = "delta-from-name"
	AddUserDataFlag           = "add-user-data"

	PermanentShorthand             = "p"
	FullBackupShorthand            = "f"
	VerifyPagesShorthand           = "v"
	StoreAllCorruptBlocksShorthand = "s"
	UseRatingComposerShorthand     = "r"

	PermanentDesc = "Pushes permanent backup"
	FullBackupDesc = "Make full backup-push"
	VerifyPagesDesc = "Verify page checksums"
	StoreAllCorruptBlocksDesc = "Store all corrupt blocks found during page checksum verification"
	UseRatingComposerDesc     = "Use rating tar composer (beta)"
	DeltaFromUserDataDesc    = "Select the backup specified by UserData as the target for the delta backup"
	DeltaFromNameDesc         = "Select the backup specified by name as the target for the delta backup"
	AddUserDataDesc           = "Write the provided user data to the backup sentinel and metadata files"
)

var (
	// BackupPushCmd represents the backupPush command
	BackupPushCmd = &cobra.Command{
		Use:   "backup-push db_directory",
		Short: BackupPushShortDescription, // TODO : improve description
		Args:  cobra.MaximumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var dataDirectory string

			if len(args) > 0 {
				dataDirectory = args[0]
			}

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
			arguments := postgres.NewBackupArguments(dataDirectory, utility.BaseBackupPath,
				permanent, verifyPageChecksums || viper.GetBool(internal.VerifyPageChecksumsSetting),
				fullBackup, storeAllCorruptBlocks || viper.GetBool(internal.StoreAllCorruptBlocksSetting),
				tarBallComposerType, deltaBaseSelector, userData)

			backupHandler, err := postgres.NewBackupHandler(arguments)
			tracelog.ErrorLogger.FatalOnError(err)
			backupHandler.HandleBackupPush()
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
func createDeltaBaseSelector(cmd *cobra.Command,
	targetBackupName, targetUserData string) (internal.BackupSelector, error) {
	switch {
	case targetUserData != "" && targetBackupName != "":
		fmt.Println(cmd.UsageString())
		return nil, errors.New("only one delta target should be specified")

	case targetBackupName != "":
		tracelog.InfoLogger.Printf("Selecting the backup with name %s as the base for the current delta backup...\n",
			targetBackupName)
		return internal.NewBackupNameSelector(targetBackupName)

	case targetUserData != "":
		tracelog.InfoLogger.Println(
			"Selecting the backup with specified user data as the base for the current delta backup...")
		return internal.NewUserDataBackupSelector(targetUserData, postgres.NewGenericMetaFetcher()), nil

	default:
		tracelog.InfoLogger.Println("Selecting the latest backup as the base for the current delta backup...")
		return internal.NewLatestBackupSelector(), nil
	}
}

func init() {
	cmd.AddCommand(BackupPushCmd)

	BackupPushCmd.Flags().BoolVarP(&permanent, PermanentFlag, PermanentShorthand,
		false, PermanentDesc)
	BackupPushCmd.Flags().BoolVarP(&fullBackup, FullBackupFlag, FullBackupShorthand,
		false, FullBackupDesc)
	BackupPushCmd.Flags().BoolVarP(&verifyPageChecksums, VerifyPagesFlag, VerifyPagesShorthand,
		false, VerifyPagesDesc)
	BackupPushCmd.Flags().BoolVarP(&storeAllCorruptBlocks, StoreAllCorruptBlocksFlag, StoreAllCorruptBlocksShorthand,
		false, StoreAllCorruptBlocksDesc)
	BackupPushCmd.Flags().BoolVarP(&useRatingComposer, UseRatingComposerFlag, UseRatingComposerShorthand,
		false, UseRatingComposerDesc)
	BackupPushCmd.Flags().StringVar(&deltaFromName, DeltaFromNameFlag,
		"", DeltaFromNameDesc)
	BackupPushCmd.Flags().StringVar(&deltaFromUserData, DeltaFromUserDataFlag,
		"", DeltaFromUserDataDesc)
	BackupPushCmd.Flags().StringVar(&userData, AddUserDataFlag,
		"", AddUserDataDesc)
}
