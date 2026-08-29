package storage

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/RiverPhillips/raft/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLogEntryRoundTrips(t *testing.T) {
	tests := map[string]struct {
		logEntry raft.LogEntry
		size     uint32
	}{
		"Simple": {
			logEntry: raft.LogEntry{
				Term:    1,
				Command: raft.Command("test"),
			},
			size: 20,
		},
		"Sentinel": {
			logEntry: raft.LogEntry{
				Term:    0,
				Command: nil,
			},
			size: 16,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			var b bytes.Buffer

			rec := &WALRecord{Entry: tc.logEntry}
			err := WriteLogEntry(&b, rec)
			require.NoError(t, err)

			res, err := ReadLogEntry(bytes.NewReader(b.Bytes()))
			require.NoError(t, err)

			assert.Equal(t, tc.logEntry, res.Entry)
			assert.Equal(t, tc.size, res.FrameLen)
		})
	}
}

func TestReturnAnErroryWhenLogEntryIsTruncated(t *testing.T) {
	var b bytes.Buffer

	err := WriteLogEntry(&b, &WALRecord{Entry: raft.LogEntry{Term: 1, Command: raft.Command("test")}})
	require.NoError(t, err)

	trunc := b.Bytes()[:4]

	_, err = ReadLogEntry(bytes.NewReader(trunc))
	assert.Error(t, err)
}

func TestReturnAnErroryWhenLogEntryHasAByteChanged(t *testing.T) {
	var b bytes.Buffer

	err := WriteLogEntry(&b, &WALRecord{Entry: raft.LogEntry{Term: 1, Command: raft.Command("test")}})
	require.NoError(t, err)

	buf := b.Bytes()
	buf[4] = 0x1

	_, err = ReadLogEntry(bytes.NewReader(buf))
	assert.ErrorIs(t, err, ErrCorruptedWAL)
}

func TestMetadataRoundTrips(t *testing.T) {
	dir := t.TempDir()
	fmt.Println(dir)
	storage, err := NewOnDiskStorage(dir)
	require.NoError(t, err)

	err = storage.WriteMetadata(t.Context(), 10, 1)
	require.NoError(t, err)

	state, err := storage.LoadState(t.Context())
	require.NoError(t, err)
	assert.Equal(t, state.CurrentTerm, raft.Term(10))
	assert.Equal(t, state.VotedFor, raft.MemberId(1))
}

func TestLoadsLogs(t *testing.T) {
	dir := t.TempDir()
	storage, err := NewOnDiskStorage(dir)
	require.NoError(t, err)

	err = storage.WriteMetadata(t.Context(), 10, 1)
	require.NoError(t, err)

	err = storage.AppendToLog(t.Context(), raft.LogEntry{Term: raft.Term(10), Command: raft.Command("test")})
	require.NoError(t, err)
	require.NoError(t, storage.Close(t.Context()))

	storage, err = NewOnDiskStorage(dir)
	require.NoError(t, err)

	state, err := storage.LoadState(t.Context())
	require.NoError(t, err)
	assert.Equal(t, state.Log, []raft.LogEntry{{Term: raft.Term(10), Command: raft.Command("test")}})

}
