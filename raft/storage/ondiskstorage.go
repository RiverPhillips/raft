package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

const (
	METADATA_MAGIC = "RAFT"
	WAL_MAGIC      = "WAL"

	METADATA_FILE    = "raft.metadata"
	METADTA_TMP_FILE = "raft.metatadata.tmp"
	LOCK_FILE        = "raft.lock"
)

var (
	ErrNotDir       = errors.New("path is not for a directory")
	ErrCorruptedWAL = errors.New("corrupted WAL entry. CRC did not match")
)

// Todo flock for process isolation
type OnDiskStorage struct {
	mu sync.Mutex

	walFile  *os.File
	lockFile *os.File

	dirPath       string
	uninitialized bool
}

type LogEntry struct {
	Term    uint64
	Command []byte
}

type walRecord struct {
	LogEntry LogEntry
	FrameLen uint32
}

func NewOnDiskStorage(dirPath string) (*OnDiskStorage, error) {
	_, statErr := os.Stat(dirPath)
	err := os.MkdirAll(dirPath, 0700)
	if err != nil {
		return nil, err
	}

	lockFile, err := os.OpenFile(filepath.Join(dirPath, LOCK_FILE), os.O_RDONLY|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}

	if err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return nil, fmt.Errorf("failed to acquire lock: %w", err)
	}

	walFile, err := os.OpenFile(dirPath+"/raft.log", os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}

	return &OnDiskStorage{
		walFile:       walFile,
		dirPath:       dirPath,
		uninitialized: errors.Is(statErr, os.ErrNotExist),
		lockFile:      lockFile,
	}, nil
}

func (s *OnDiskStorage) AppendToLog(ctx context.Context, logEntries ...LogEntry) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, l := range logEntries {
		if err := WritelogEntry(s.walFile, &walRecord{LogEntry: l}); err != nil {
			return err
		}
	}
	return s.walFile.Sync()
}

func (s *OnDiskStorage) LoadState(ctx context.Context) (StoredState, error) {
	if err := ctx.Err(); err != nil {
		return StoredState{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	metdataFile, err := os.OpenFile(filepath.Join(s.dirPath, METADATA_FILE), os.O_RDONLY, 0)
	if err != nil {
		if os.IsNotExist(err) {
			return StoredState{}, nil
		}
		return StoredState{}, err
	}
	defer func() {
		_ = metdataFile.Close()
	}()

	magicBuf := make([]byte, 4)
	if _, err := io.ReadFull(metdataFile, magicBuf); err != nil {
		return StoredState{}, nil
	}
	if !bytes.Equal(magicBuf, []byte(METADATA_MAGIC)) {
		return StoredState{}, fmt.Errorf("invalid magic number in mesdata file. Expected: %x. Read: %x", []byte(METADATA_MAGIC), magicBuf)
	}

	termBuf := make([]byte, 8)
	votedBuf := make([]byte, 4)
	crcBuf := make([]byte, 4)

	if _, err := io.ReadFull(metdataFile, termBuf); err != nil {
		return StoredState{}, err
	}

	if _, err := io.ReadFull(metdataFile, votedBuf); err != nil {
		return StoredState{}, err
	}

	if _, err := io.ReadFull(metdataFile, crcBuf); err != nil {
		return StoredState{}, err
	}

	crcHasher := crc32.NewIEEE()
	crcHasher.Write(termBuf)
	crcHasher.Write(votedBuf)

	if !bytes.Equal(crcBuf, crcHasher.Sum(nil)) {
		return StoredState{}, errors.New("CORRUPTED METADATA: Checksum did not match")
	}

	voted := binary.BigEndian.Uint32(votedBuf)
	term := binary.BigEndian.Uint64(termBuf)

	var log []LogEntry
	var offset int64

	if _, err := s.walFile.Seek(0, io.SeekStart); err != nil {
		return StoredState{}, err
	}
	for {
		start := offset
		rec, err := ReadlogEntry(s.walFile)

		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			if err = s.walFile.Truncate(start); err != nil {
				return StoredState{}, err
			}
			if _, err = s.walFile.Seek(start, io.SeekStart); err != nil {
				return StoredState{}, err
			}
			break
		}

		log = append(log, rec.LogEntry)
		offset = start + int64(rec.FrameLen)
	}

	return StoredState{
		VotedFor:    voted,
		CurrentTerm: term,
		Log:         log,
	}, nil

}

func (s *OnDiskStorage) WriteMetadata(ctx context.Context, term uint64, votedFor uint32) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	tmpPath := filepath.Join(s.dirPath, METADTA_TMP_FILE)
	metadataPath := filepath.Join(s.dirPath, METADATA_FILE)
	file, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0600)
	if err != nil {
		return err
	}

	hasher := crc32.NewIEEE()

	termBuf := make([]byte, 8)
	memberBuf := make([]byte, 4)
	binary.BigEndian.PutUint64(termBuf, uint64(term))
	binary.BigEndian.PutUint32(memberBuf, uint32(votedFor))

	hasher.Write(termBuf)
	hasher.Write(memberBuf)

	crc := hasher.Sum(nil)

	_, err = file.Write([]byte(METADATA_MAGIC))
	if err != nil {
		return err
	}

	_, err = file.Write(termBuf)
	if err != nil {
		return err
	}

	_, err = file.Write(memberBuf)
	if err != nil {
		return err
	}

	_, err = file.Write(crc)
	if err != nil {
		return err
	}

	if err := file.Sync(); err != nil {
		return err
	}

	err = file.Close()
	if err != nil {
		return err
	}

	err = os.Rename(tmpPath, metadataPath)
	if err != nil {
		return err
	}
	return nil
}

func WritelogEntry(w io.Writer, rec *walRecord) error {
	log := rec.LogEntry
	if len(log.Command) == 0 && log.Term != 0 {
		return errors.New("empty command not allowed")
	}
	hasher := crc32.NewIEEE()

	termBuf := make([]byte, 8)
	cmdLen := uint32(len(log.Command))
	cmdLenBuf := make([]byte, 4)

	binary.BigEndian.PutUint64(termBuf, uint64(log.Term))

	if _, err := hasher.Write(termBuf); err != nil {
		return err
	}
	if _, err := w.Write(termBuf); err != nil {
		return err
	}

	binary.BigEndian.PutUint32(cmdLenBuf, cmdLen)

	if _, err := hasher.Write(cmdLenBuf); err != nil {
		return err
	}
	if _, err := w.Write(cmdLenBuf); err != nil {
		return err
	}

	if _, err := hasher.Write(log.Command); err != nil {
		return err
	}
	if _, err := w.Write(log.Command); err != nil {
		return err
	}

	if _, err := w.Write(hasher.Sum(nil)); err != nil {
		return err
	}
	rec.FrameLen = cmdLen + 16
	return nil
}

func ReadlogEntry(r io.Reader) (walRecord, error) {
	hasher := crc32.NewIEEE()
	res := walRecord{}

	termBuf := make([]byte, 8)
	cmdLenBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, termBuf); err != nil {
		return res, err
	}
	hasher.Write(termBuf)

	res.LogEntry.Term = binary.BigEndian.Uint64(termBuf)

	if _, err := io.ReadFull(r, cmdLenBuf); err != nil {
		return res, err
	}

	hasher.Write(cmdLenBuf)
	cmdLen := binary.BigEndian.Uint32(cmdLenBuf)

	if cmdLen == 0 {
		res.LogEntry.Command = nil
	} else {
		res.LogEntry.Command = make([]byte, cmdLen)
		if _, err := io.ReadFull(r, res.LogEntry.Command); err != nil {
			return res, err
		}

		hasher.Write(res.LogEntry.Command)
	}

	checkSumBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, checkSumBuf); err != nil {
		return res, err
	}

	checksum := hasher.Sum(nil)
	if !bytes.Equal(checkSumBuf, checksum) {
		return res, ErrCorruptedWAL
	}

	res.FrameLen = cmdLen + 16
	return res, nil
}

func (s *OnDiskStorage) Close(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.walFile == nil {
		return nil
	}
	err := s.walFile.Close()
	if err != nil {
		return err
	}
	s.walFile = nil

	err = syscall.Flock(int(s.lockFile.Fd()), syscall.LOCK_UN)
	if err != nil {
		return err
	}
	err = s.lockFile.Close()
	if err != nil {
		return err
	}

	s.lockFile = nil

	return err
}

// Truncates log removing all entries from index, 1 based indexing
// i.e. Truncate(ctx, 2) removes all entries from position 2 onwards
func (s *OnDiskStorage) TruncateLog(ctx context.Context, idx uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// Find the start of idx
	// We're starting at position i
	var curr uint64 = 1
	// Reuse this buffer
	cmdLenBuf := make([]byte, 4)

	// Move back to the start
	if _, err := s.walFile.Seek(0, io.SeekStart); err != nil {
		return err
	}

	for curr < idx {
		// Skip the first 8 bytes - we don't care about the term
		if _, err := s.walFile.Seek(8, io.SeekCurrent); err != nil {
			return err
		}

		if _, err := io.ReadFull(s.walFile, cmdLenBuf); err != nil {
			return err
		}
		cmdLen := binary.BigEndian.Uint32(cmdLenBuf)

		// Advance past the cmd
		if _, err := s.walFile.Seek(int64(cmdLen), io.SeekCurrent); err != nil {
			return err
		}

		// Skip the checksum
		if _, err := s.walFile.Seek(4, io.SeekCurrent); err != nil {
			return err
		}
		curr += 1
	}

	currentOffset, err := s.walFile.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}

	if err := s.walFile.Truncate(currentOffset); err != nil {
		return err
	}

	if err := s.walFile.Sync(); err != nil {
		return err
	}
	return nil
}
