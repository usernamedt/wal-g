package pg

import (
	"github.com/spf13/cobra"
)

const WalPrefetchShortDescription = `Used for prefetching process forking
and should not be called by user.`

// walPrefetchCmd represents the walPrefetch command
var walPrefetchCmd = &cobra.Command{
	Use:    "wal-prefetch wal_name prefetch_location",
	Short:  WalPrefetchShortDescription,
	Args:   cobra.ExactArgs(2),
	Hidden: true,
	Run: func(cmd *cobra.Command, args []string) {
		//uploader, err := internal.ConfigureWalUploaderWithoutCompressMethod()
		//tracelog.ErrorLogger.FatalOnError(err)
		//internal.HandleWALPrefetch(uploader, args[0], args[1])
	},
}

func init() {
	Cmd.AddCommand(walPrefetchCmd)
}
