package pg

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
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
			internal.HandleWalShow(folder, true, internal.NewWalShowOutputFormatter(internal.JsonOutput))
		},
	}
)

func init() {
	Cmd.AddCommand(walShowCmd)
}
