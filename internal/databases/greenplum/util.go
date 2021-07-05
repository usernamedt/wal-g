package greenplum

import (
	"fmt"
	"github.com/wal-g/wal-g/utility"
	"path"
	"strconv"
)

func FormatSegmentStoragePrefix(contentID int) string {
	return fmt.Sprintf("seg%d", contentID)
}

func FormatSegmentBackupPath(contentID int) string {
	return path.Join(FormatSegmentStoragePrefix(contentID), utility.BaseBackupPath)
}

func FormatSegmentWalPath(contentID int) string {
	return path.Join(FormatSegmentStoragePrefix(contentID), utility.WalPath)
}

func ParseContentId(contentIdStr string) (int, error) {
	contentID, err := strconv.Atoi(contentIdStr)
	if err != nil {
		return 0, err
	}
	if contentID < -1 {
		return 0, fmt.Errorf("invalid Greenplum content ID: %s", contentIdStr)
	}
	return contentID, nil
}