package pg

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
)

const WalVerifyShortDescription = "Prints available range to perform PITR"

var (
	// walVerifyCmd represents the walVerify command
	walVerifyCmd = &cobra.Command{
		Use:   "wal-verify",
		Short: WalVerifyShortDescription,
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			folder, err := internal.ConfigureFolder()
			tracelog.ErrorLogger.FatalOnError(err)
			internal.HandleWalVerify(folder)
		},
	}
)

func init() {
	Cmd.AddCommand(walVerifyCmd)
}
