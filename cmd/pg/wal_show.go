package pg

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"os"
)

const (
	WalShowUsage            = "wal-show"
	WalShowShortDescription = "TODO"
	WalShowLongDescription  = "TO DO"
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
			outputWriter := internal.NewWalShowOutputWriter(internal.JsonOutput, os.Stdout, true)
			internal.HandleWalShow(folder, true, outputWriter)
		},
	}
)

func init() {
	Cmd.AddCommand(walShowCmd)
}
