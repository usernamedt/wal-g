package gp

import (
"github.com/spf13/cobra"
"github.com/wal-g/tracelog"
"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/greenplum"
	"github.com/wal-g/wal-g/internal/databases/postgres"
)

const SegWalFetchShortDescription = "Fetches a segment WAL file from storage"

// walFetchCmd represents the walFetch command
var walFetchCmd = &cobra.Command{
	Use:   "wal-fetch content_id wal_name destination_filename",
	Short: SegWalFetchShortDescription, // TODO : improve description
	Args:  cobra.ExactArgs(3),
	Run: func(cmd *cobra.Command, args []string) {
		contentId, err := greenplum.ParseContentId(args[0])
		tracelog.ErrorLogger.FatalOnError(err)

		folder, err := internal.ConfigureFolder()
		tracelog.ErrorLogger.FatalOnError(err)

		segmentWalPath := greenplum.FormatSegmentWalPath(contentId)
		postgres.HandleWALFetch(folder.GetSubFolder(segmentWalPath), args[1], args[2], true)
	},
}

func init() {
	cmd.AddCommand(walFetchCmd)
}
