package gp

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/cmd/pg"
	"github.com/wal-g/wal-g/internal/databases/greenplum"
	"github.com/wal-g/wal-g/internal/databases/postgres"
)

const WalPushShortDescription = "Uploads a WAL file to storage"

// walPushCmd represents the walPush command
var walPushCmd = &cobra.Command{
	Use:   "wal-push content_id wal_filepath",
	Short: WalPushShortDescription, // TODO : improve description
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		contentId, err := greenplum.ParseContentId(args[0])
		tracelog.ErrorLogger.FatalOnError(err)

		uploader, err := postgres.ConfigureWalUploader()
		tracelog.ErrorLogger.FatalOnError(err)

		pg.ConfigureWalPushASM(uploader)

		uploader.UploadingFolder = uploader.UploadingFolder.GetSubFolder(greenplum.FormatSegmentWalPath(contentId))
		postgres.HandleWALPush(uploader, args[0])
	},
}

func init() {
	cmd.AddCommand(walPushCmd)
}
