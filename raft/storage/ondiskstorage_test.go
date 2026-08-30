package storage

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
				Command: []byte("test"),
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

			rec := &walRecord{LogEntry: tc.logEntry}
			err := WritelogEntry(&b, rec)
			require.NoError(t, err)

			res, err := ReadlogEntry(bytes.NewReader(b.Bytes()))
			require.NoError(t, err)

			assert.Equal(t, tc.logEntry, res.LogEntry)
			assert.Equal(t, tc.size, res.FrameLen)
		})
	}
}

func TestReturnAnErroryWhenLogEntryIsTruncated(t *testing.T) {
	var b bytes.Buffer

	err := WritelogEntry(&b, &walRecord{LogEntry: LogEntry{Term: 1, Command: []byte("test")}})
	require.NoError(t, err)

	trunc := b.Bytes()[:4]

	_, err = ReadlogEntry(bytes.NewReader(trunc))
	assert.Error(t, err)
}

func TestReturnAnErroryWhenLogEntryHasAByteChanged(t *testing.T) {
	var b bytes.Buffer

	err := WritelogEntry(&b, &walRecord{LogEntry: LogEntry{Term: 1, Command: []byte("test")}})
	require.NoError(t, err)

	buf := b.Bytes()
	buf[4] = 0x1

	_, err = ReadlogEntry(bytes.NewReader(buf))
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
	assert.Equal(t, state.CurrentTerm, uint64(10))
	assert.Equal(t, state.VotedFor, uint32(1))
}

func TestLoadsLogs(t *testing.T) {
	dir := t.TempDir()
	storage, err := NewOnDiskStorage(dir)
	require.NoError(t, err)

	err = storage.WriteMetadata(t.Context(), 10, 1)
	require.NoError(t, err)

	err = storage.AppendToLog(t.Context(), LogEntry{Term: (10), Command: []byte("test")})
	require.NoError(t, err)
	require.NoError(t, storage.Close(t.Context()))

	storage, err = NewOnDiskStorage(dir)
	require.NoError(t, err)

	state, err := storage.LoadState(t.Context())
	require.NoError(t, err)
	assert.Equal(t, state.Log, []LogEntry{{Term: (10), Command: []byte("test")}})

}

func TestTruncateLog(t *testing.T) {
	dir := t.TempDir()
	storage, err := NewOnDiskStorage(dir)
	require.NoError(t, err)

	require.NoError(t, storage.WriteMetadata(t.Context(), 1, 1))
	err = storage.AppendToLog(t.Context(),
		LogEntry{Term: (1), Command: []byte("test1")},
		LogEntry{Term: (2), Command: []byte("test2")},
	)
	require.NoError(t, err)

	require.NoError(t, storage.Close(t.Context()))

	storage, err = NewOnDiskStorage(dir)
	require.NoError(t, err)

	state, err := storage.LoadState(t.Context())
	require.NoError(t, err)

	assert.Equal(t, []LogEntry{
		{Term: (1), Command: []byte("test1")},
		{Term: (2), Command: []byte("test2")},
	}, state.Log)

	err = storage.TruncateLog(t.Context(), uint64(2))
	require.NoError(t, err)

	require.NoError(t, storage.AppendToLog(t.Context(), LogEntry{Term: 1, Command: []byte("test3")}))

	require.NoError(t, storage.Close(t.Context()))

	storage, err = NewOnDiskStorage(dir)
	require.NoError(t, err)

	state, err = storage.LoadState(t.Context())
	require.NoError(t, err)

	assert.Equal(t, []LogEntry{
		{Term: (1), Command: []byte("test1")},
		{Term: (1), Command: []byte("test3")},
	}, state.Log)
}
