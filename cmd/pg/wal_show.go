package pg

import (
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"os"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
)

const (
	WalShowUsage            = "wal-show"
	WalShowShortDescription = "Show storage WAL segments info grouped by timelines."
	WalShowLongDescription  = "Show information such as missing segments for each timeline found in storage. " +
		"Optionally, show available backups for each timeline."

	detailedOutputFlag        = "detailed-json"
	detailedOutputDescription = "Output detailed information in JSON format."

	disableBackupsLookupFlag        = "without-backups"
	disableBackupsLookupDescription = "Disable backups lookup for each timeline."
)

var (
	// walShowCmd represents the walShow command
	walShowCmd = &cobra.Command{
		Use:   WalShowUsage,
		Short: WalShowShortDescription,
		Long:  WalShowLongDescription,
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			folder, err := internal.ConfigureFolder()
			tracelog.ErrorLogger.FatalOnError(err)
			outputType := postgres.TableOutput
			if detailedJsonOutput {
				outputType = postgres.JsonOutput
			}
			outputWriter := postgres.NewWalShowOutputWriter(outputType, os.Stdout, !disableBackupsLookup)
			postgres.HandleWalShow(folder, !disableBackupsLookup, outputWriter)
		},
	}
	detailedJsonOutput   bool
	disableBackupsLookup bool
)

func init() {
	cmd.AddCommand(walShowCmd)
	walShowCmd.Flags().BoolVar(&detailedJsonOutput, detailedOutputFlag, false, detailedOutputDescription)
	walShowCmd.Flags().BoolVar(&disableBackupsLookup, disableBackupsLookupFlag, false, disableBackupsLookupDescription)
}
