package mysql

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"time"

	"github.com/wal-g/wal-g/utility"
)

var BinlogMagic = [...]byte{0xfe, 0x62, 0x69, 0x6e}

const BinlogMagicLength = 4

const BinlogEventHeaderSize = 13

func time2uint32(t time.Time) uint32 {
	ts := t.Unix()
	if ts > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(ts)
}

func minInt(i, j int) int {
	if i < j {
		return i
	}
	return j
}

// https://dev.mysql.com/doc/internals/en/event-structure.html
// First 4 fields are the same in all versions
type BinlogEventHeader struct {
	Timestamp   uint32
	TypeCode    uint8
	ServerId    uint32
	EventLength uint32
}

func ParseEventHeader(buf []byte) (header BinlogEventHeader) {
	if len(buf) < BinlogEventHeaderSize {
		panic("failed to parse binlog event header: buffer is too short")
	}
	le := binary.LittleEndian
	header.Timestamp = le.Uint32(buf[0:])
	header.TypeCode = buf[4]
	header.ServerId = le.Uint32(buf[5:])
	header.EventLength = le.Uint32(buf[9:])
	return header
}

type BinlogReader struct {
	reader          *bufio.Reader
	startTs         uint32
	endTs           uint32
	headerBuf       []byte
	headerSaved     bool
	intervalEntered bool
	intervalLeft    bool
	tail            int
}

func NewBinlogReader(reader io.Reader, startTs time.Time, endTs time.Time) *BinlogReader {
	return &BinlogReader{
		reader:  bufio.NewReaderSize(reader, 10*utility.Mebibyte),
		startTs: time2uint32(startTs),
		endTs:   time2uint32(endTs),
	}
}

func (bl *BinlogReader) saveMagicAndHeaderEvent() error {
	var magic [4]byte
	_, err := io.ReadFull(bl.reader, magic[:])
	if err != nil {
		return err
	}
	if magic != BinlogMagic {
		return fmt.Errorf("incorrect binlog magic: %v", magic)
	}
	hbuf, err := bl.reader.Peek(BinlogEventHeaderSize)
	if err != nil {
		return err
	}
	header := ParseEventHeader(hbuf)
	bl.headerBuf = make([]byte, 4+header.EventLength)
	copy(bl.headerBuf[:4], magic[:])
	_, err = io.ReadFull(bl.reader, bl.headerBuf[4:])
	return err
}

func (bl *BinlogReader) readMagicAndHeaderEvent(buf []byte) int {
	limit := minInt(len(bl.headerBuf), len(buf))
	copy(buf, bl.headerBuf[:limit])
	bl.headerBuf = bl.headerBuf[limit:]
	return limit
}

func (bl *BinlogReader) readEvent(buf []byte) (int, error) {
	limit := minInt(bl.tail, len(buf))
	read, err := bl.reader.Read(buf[:limit])
	bl.tail -= read
	return read, err
}

func (bl *BinlogReader) Read(buf []byte) (int, error) {
	blen := len(buf)
	// save magic and first event (aka header) into the temporary buffer
	// and keep them until first appropriate event
	if !bl.headerSaved {
		err := bl.saveMagicAndHeaderEvent()
		if err != nil {
			return 0, err
		}
		bl.headerSaved = true
	}
	// read events, checking timestamps
	offset := 0
	for offset < blen {
		// pass magic and header event to client with first appropriate event
		if bl.intervalEntered && len(bl.headerBuf) > 0 {
			read := bl.readMagicAndHeaderEvent(buf[offset:])
			offset += read
			if len(bl.headerBuf) > 0 {
				return offset, nil
			}
		}
		// pass next event to client
		if bl.tail > 0 {
			read, err := bl.readEvent(buf[offset:])
			offset += read
			if err != nil || bl.tail > 0 {
				return offset, err
			}
		}
		// parse next event
		hbuf, err := bl.reader.Peek(BinlogEventHeaderSize)
		if err != nil {
			// may return EOF here
			return offset, err
		}
		header := ParseEventHeader(hbuf)
		evlen := int(header.EventLength)
		if header.Timestamp < bl.startTs {
			_, err := bl.reader.Discard(evlen)
			if err != nil {
				return offset, err
			}
			continue
		}
		bl.intervalEntered = true
		if header.Timestamp >= bl.endTs {
			bl.intervalLeft = true
			return offset, io.EOF
		}
		// set event to be read
		bl.tail = evlen
	}
	return offset, nil
}

func (bl *BinlogReader) NeedAbort() bool {
	return bl.intervalLeft
}

func GetBinlogStartTimestamp(path string) (time.Time, error) {
	file, err := os.Open(path)
	if err != nil {
		return time.Time{}, err
	}
	defer file.Close()
	buf := make([]byte, BinlogMagicLength+BinlogEventHeaderSize)
	_, err = io.ReadFull(file, buf)
	if err != nil {
		return time.Time{}, err
	}
	if bytes.Compare(BinlogMagic[:], buf[:BinlogMagicLength]) != 0 {
		return time.Time{}, fmt.Errorf("incorrect binlog magic: %v", buf[:BinlogMagicLength])
	}
	header := ParseEventHeader(buf[BinlogMagicLength:])
	return time.Unix(int64(header.Timestamp), 0), nil
}
