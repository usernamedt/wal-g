package pg

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/databases/postgres"
)

const (
	catchupPushShortDescription = "Creates incremental backup from lsn"
)

var (
	// catchupPushCmd represents the catchup-push command
	catchupPushCmd = &cobra.Command{
		Use:   "catchup-push PGDATA --from-lsn LSN",
		Short: catchupPushShortDescription,
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			uploader, err := postgres.ConfigureWalUploader()
			tracelog.ErrorLogger.FatalOnError(err)
			postgres.HandleCatchupPush(uploader, args[0], fromLSN)
		},
	}
	fromLSN uint64
)

func init() {
	cmd.AddCommand(catchupPushCmd)

	catchupPushCmd.Flags().Uint64Var(&fromLSN, "from-lsn", 0, "LSN to start incremental backup")
}
