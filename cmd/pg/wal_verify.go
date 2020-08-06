package pg

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
)

const (
	WalVerifyUsage            = "wal-verify"
	WalVerifyShortDescription = "Verify WAL segment files consistency in storage."
	WalVerifyLongDescription  = "Walk through WAL segments in storage in a reversed chronological order and check for missing segments."

	StartWalSegmentFlag        = "start-segment"
	StartWalSegmentShorthand   = "s"
	StartWalSegmentDescription = "Name of WAL segment to start walk from."
)

var (
	// walVerifyCmd represents the walVerify command
	walVerifyCmd = &cobra.Command{
		Use:   WalVerifyUsage,
		Short: WalVerifyShortDescription,
		Long:  WalVerifyLongDescription,
		Args:  cobra.RangeArgs(0,1),
		Run: func(cmd *cobra.Command, args []string) {
			folder, err := internal.ConfigureFolder()
			tracelog.ErrorLogger.FatalOnError(err)
			if startWalSegment == "" {
				internal.HandleWalVerify(folder, internal.QueryCurrentWalSegment())
			} else {
				internal.HandleWalVerify(folder, internal.ParseStartWalSegment(startWalSegment))
			}
		},
	}
	startWalSegment string
)

func init() {
	Cmd.AddCommand(walVerifyCmd)
	walVerifyCmd.Flags().StringVarP(&startWalSegment, StartWalSegmentFlag, StartWalSegmentShorthand, "", StartWalSegmentDescription)
}
