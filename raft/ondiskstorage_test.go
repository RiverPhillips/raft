package raft

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLogEntryRoundTrips(t *testing.T) {
	tests := map[string]struct {
		logEntry LogEntry
		size     uint32
	}{
		"Simple": {
			logEntry: LogEntry{
				Term:    1,
				Command: Command("test"),
			},
			size: 20,
		},
		"Sentinel": {
			logEntry: LogEntry{
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

	err := WriteLogEntry(&b, &WALRecord{Entry: LogEntry{Term: 1, Command: Command("test")}})
	require.NoError(t, err)

	trunc := b.Bytes()[:4]

	_, err = ReadLogEntry(bytes.NewReader(trunc))
	assert.Error(t, err)
}

func TestReturnAnErroryWhenLogEntryHasAByteChanged(t *testing.T) {
	var b bytes.Buffer

	err := WriteLogEntry(&b, &WALRecord{Entry: LogEntry{Term: 1, Command: Command("test")}})
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
	assert.Equal(t, state.CurrentTerm, Term(10))
	assert.Equal(t, state.VotedFor, MemberId(1))
}

func TestLoadsLogs(t *testing.T) {
	dir := t.TempDir()
	storage, err := NewOnDiskStorage(dir)
	require.NoError(t, err)

	err = storage.WriteMetadata(t.Context(), 10, 1)
	require.NoError(t, err)

	err = storage.AppendToLog(t.Context(), LogEntry{Term: Term(10), Command: Command("test")})
	require.NoError(t, err)
	require.NoError(t, storage.Close(t.Context()))

	storage, err = NewOnDiskStorage(dir)
	require.NoError(t, err)

	state, err := storage.LoadState(t.Context())
	require.NoError(t, err)
	assert.Equal(t, state.Log, []LogEntry{{Term: Term(10), Command: Command("test")}})

}

func TestTruncateLog(t *testing.T) {
	dir := t.TempDir()
	storage, err := NewOnDiskStorage(dir)
	require.NoError(t, err)

	require.NoError(t, storage.WriteMetadata(t.Context(), 1, 1))
	err = storage.AppendToLog(t.Context(),
		LogEntry{Term: Term(1), Command: Command("test1")},
		LogEntry{Term: Term(2), Command: Command("test2")},
	)
	require.NoError(t, err)

	require.NoError(t, storage.Close(t.Context()))

	storage, err = NewOnDiskStorage(dir)
	require.NoError(t, err)

	state, err := storage.LoadState(t.Context())
	require.NoError(t, err)

	assert.Equal(t, []LogEntry{
		{Term: Term(1), Command: Command("test1")},
		{Term: Term(2), Command: Command("test2")},
	}, state.Log)

	err = storage.TruncateLog(t.Context(), uint64(2))
	require.NoError(t, err)

	require.NoError(t, storage.AppendToLog(t.Context(), LogEntry{Term: 1, Command: Command("test3")}))

	require.NoError(t, storage.Close(t.Context()))

	storage, err = NewOnDiskStorage(dir)
	require.NoError(t, err)

	state, err = storage.LoadState(t.Context())
	require.NoError(t, err)

	assert.Equal(t, []LogEntry{
		{Term: Term(1), Command: Command("test1")},
		{Term: Term(1), Command: Command("test3")},
	}, state.Log)
}
