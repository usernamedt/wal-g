package internal_test

import (
	"bytes"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/testtools"
	"github.com/wal-g/wal-g/utility"
)

