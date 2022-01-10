package kvs

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

const CompactionThreshold uint64 = 1024 * 1024

// KvStore The `KvStore` stores string key/value pairs.
//
// Key/value pairs are persisted to disk in log files. Log files are named after
// monotonically increasing generation numbers with a `log` extension name.
// A map in memory stores the keys and the value locations for fast query.
type KvStore struct {
	path     string    // directory for the log and other data
	index    *sync.Map // map generation number to the file reader // TODO instead of rw lock
	reader   *KvStoreReader
	writer   *SyncKvStoreWriter
	refCount int32
}

// Clone the cloned KvStore shared the `path`,`index`,`writer`,but not `reader`
// We need to close the reader and writer when we shut down the KvStore, which
// contains some file descriptors. When Clone KvStore we increase the reference
// count of KvStore, when we execute the Shutdown method we decrease its reference
// count, and when the reference count goes to zero we really close the writer
func (s *KvStore) Clone() KvsEngine {
	return &KvStore{
		path:     s.path,
		index:    s.index,
		reader:   s.reader.clone(), // 每一个连接一个store实例，reader是各自独立的，其他的共享
		writer:   s.writer,
		refCount: atomic.AddInt32(&s.refCount, 1),
	}
}

func Open(path string) (*KvStore, error) {
	fmt.Printf("store path: %s\n", path)
	err := os.MkdirAll(path, os.ModePerm)
	if err != nil {
		return nil, err
	}

	readers := make(map[uint64]*FileReaderWithPos)
	var index sync.Map

	var genList []uint64
	if genList, err = sortedGenList(path); err != nil {
		return nil, err
	}

	uncompacted := uint64(0)

	var curGen uint64 = 1
	if len(genList) > 0 {
		for _, gen := range genList {
			f, err := os.Open(logPath(path, gen))
			if err != nil {
				return nil, err
			}
			reader, err := newFileReaderWithPos(f)
			if i, err := load(gen, reader, &index); err != nil {
				return nil, err
			} else {
				uncompacted += i
				readers[gen] = reader
			}
		}
		curGen = genList[len(genList)-1] + 1
	}

	writer, err := newLogFile(path, curGen)
	if err != nil {
		return nil, err
	}

	safePoint := uint64(0)

	r := &KvStoreReader{
		path:      path,
		safePoint: &safePoint,
		readers:   readers,
	}

	w := &KvStoreWriter{
		reader:      r.clone(),
		writer:      writer,
		currentGen:  curGen,
		uncompacted: uncompacted,
		path:        path,
		index:       &index,
	}

	syncW := &SyncKvStoreWriter{
		writer: w,
		stop:   false,
	}

	return &KvStore{
		path:     path,
		reader:   r,
		writer:   syncW,
		index:    &index,
		refCount: 1,
	}, nil
}

type SyncKvStoreWriter struct {
	writer *KvStoreWriter
	lock   sync.Mutex
	stop   bool
}

func (s *SyncKvStoreWriter) Set(key string, value string) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if !s.stop {
		return s.writer.set(key, value)
	} else {
		return errors.New("already Shutdown")
	}
}

func (s *SyncKvStoreWriter) Remove(key string) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if !s.stop {
		return s.writer.remove(key)
	} else {
		return errors.New("already Shutdown")
	}
}

func (s *SyncKvStoreWriter) shutdown() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if !s.stop {
		s.stop = true
		err := s.writer.shutdown()
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *KvStore) Set(key string, value string) error {
	return s.writer.Set(key, value)
}

func (s *KvStore) Get(key string) (string, error) {
	cmdPos, ok := s.index.Load(key)
	if ok {
		cmd, err := s.reader.readCommand(cmdPos.(*CommandPos))
		if err != nil {
			return "", err
		}
		if cmd.Type == Set {
			return cmd.Value, nil
		} else {
			return "", errors.New("unexpect command type")
		}
	} else {
		return "", nil
	}
}

func (s *KvStore) Remove(key string) error {
	return s.writer.Remove(key)
}

func (s *KvStore) Shutdown() error {
	// only Shutdown reader
	if err := s.reader.shutdown(); err != nil {
		return err
	}
	// ref count == 0 Shutdown writer
	if atomic.AddInt32(&s.refCount, -1) == 0 {
		fmt.Println("shutdown writer...")
		if err := s.writer.shutdown(); err != nil {
			return err
		}
	}
	return nil
}

// KvStoreReader A single thread reader.
//
// Each `KvStore` instance has its own `KvStoreReader` and
// `KvStoreReader`s open the same files separately. So the user
// can read concurrently through multiple `KvStore`s in different
// threads.
type KvStoreReader struct {
	path      string
	safePoint *uint64 // generation of the latest compaction file
	readers   map[uint64]*FileReaderWithPos
}

func (r *KvStoreReader) shutdown() error {
	for _, v := range r.readers {
		err := v.file.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// closeStaleHandles Close file handles with generation number less than safe_point.
//
// `safe_point` is updated to the latest compaction gen after a compaction finishes.
// The compaction generation contains the sum of all operations before it and the
// in-memory index contains no entries with generation number less than safe_point.
// So we can safely close those file handles and the stale files can be deleted.
func (r *KvStoreReader) closeStaleHandles() {
	// TODO replace with TreeMap
	var staleGens []uint64
	for gen, _ := range r.readers {
		if gen < atomic.LoadUint64(r.safePoint) {
			staleGens = append(staleGens, gen)
		}
	}
	for _, gen := range staleGens {
		reader, _ := r.readers[gen]
		delete(r.readers, gen)
		err := reader.file.Close()
		if err != nil {
			fmt.Println(err)
		}
	}
}

/// Read the log file at the given `CommandPos`.
func (r *KvStoreReader) readAnd(cmdPos *CommandPos, f func(reader io.Reader) (interface{}, error)) (interface{}, error) {
	r.closeStaleHandles()

	reader, ok := r.readers[cmdPos.Gen]
	// Open the file if we haven't opened it in this `KvStoreReader`.
	if !ok {
		f, err := os.Open(logPath(r.path, cmdPos.Gen))
		if err != nil {
			return nil, err
		}
		newReader, err := newFileReaderWithPos(f)
		if err != nil {
			return nil, err
		}
		r.readers[cmdPos.Gen] = newReader
		reader = newReader
	}
	_, err := reader.Seek(int64(cmdPos.Pos), 0)
	if err != nil {
		return nil, err
	}
	limitRd := io.LimitReader(reader, int64(cmdPos.Len))
	return f(limitRd)
}

// Read the log file at the given `CommandPos` and deserialize it to `Command`.
func (r *KvStoreReader) readCommand(cmdPos *CommandPos) (*Command, error) {
	v, err := r.readAnd(cmdPos, func(reader io.Reader) (interface{}, error) {
		dec := json.NewDecoder(reader)
		var cmd Command
		err := dec.Decode(&cmd)
		if err != nil {
			return nil, err
		}
		return &cmd, nil
	})
	return v.(*Command), err
}

func (r *KvStoreReader) clone() *KvStoreReader {
	return &KvStoreReader{
		path:      r.path,
		safePoint: r.safePoint,
		readers:   make(map[uint64]*FileReaderWithPos),
	}
}

type KvStoreWriter struct {
	reader      *KvStoreReader
	writer      *FileWriterWithPos
	currentGen  uint64
	uncompacted uint64 // the number of bytes representing "stale" commands that could be deleted during a compaction
	path        string
	index       *sync.Map
}

func (w *KvStoreWriter) shutdown() error {
	fmt.Println("shutdown KvStoreWriter...")
	// Shutdown reader
	if err := w.reader.shutdown(); err != nil {
		return nil
	}
	if err := w.writer.Flush(); err != nil {
		return nil
	}
	err := w.writer.file.Sync()
	if err != nil {
		return err
	}
	err = w.writer.file.Close()
	if err != nil {
		return err
	}
	return nil
}

func (w *KvStoreWriter) set(key string, value string) error {
	cmd := Command{
		Set, key, value,
	}
	pos := w.writer.pos
	enc := json.NewEncoder(w.writer)
	err := enc.Encode(&cmd)
	if err != nil {
		return err
	}
	err = w.writer.Flush()
	if err != nil {
		return err
	}

	old, ok := w.index.Load(key)
	if ok {
		w.uncompacted += old.(*CommandPos).Len
	}
	w.index.Store(key, newCommandPos(w.currentGen, uint64(pos), uint64(w.writer.pos-pos)))

	if w.uncompacted > CompactionThreshold {
		err := w.compact()
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *KvStoreWriter) remove(key string) error {
	if v, ok := w.index.Load(key); ok {
		cmd := Command{
			Remove, key, "",
		}
		pos := w.writer.pos

		if err := jsonToWriter(w.writer, &cmd); err != nil {
			return err
		}
		if err := w.writer.Flush(); err != nil {
			return err
		}

		w.index.Delete(key)
		w.uncompacted += v.(*CommandPos).Len
		// the "remove" command itself can be deleted in the next compaction
		// so we add its length to `uncompacted`
		w.uncompacted += uint64(w.writer.pos - pos)

		if w.uncompacted > CompactionThreshold {
			err := w.compact()
			if err != nil {
				return err
			}
		}
		return nil
	} else {
		return nil
	}
}

// Clears stale entries in the log.
func (w *KvStoreWriter) compact() error {
	// fmt.Println("compact...")
	compactionGen := w.currentGen + 1
	w.currentGen += 2
	if newWriter, err := newLogFile(w.path, w.currentGen); err != nil {
		return err
	} else {
		// close the old writer
		err := w.writer.close()
		if err != nil {
			return err
		}
		w.writer = newWriter
	}

	compactionWriter, err := newLogFile(w.path, compactionGen)
	if err != nil {
		return err
	}
	newPos := uint64(0) // pos in the new log file.
	var rangeErr error = nil
	w.index.Range(func(key, value interface{}) bool {
		n, readErr := w.reader.readAnd(value.(*CommandPos), func(reader io.Reader) (interface{}, error) {
			return io.Copy(compactionWriter, reader)
		})
		if readErr != nil {
			rangeErr = readErr
			return false
		}
		w.index.Store(key, newCommandPos(compactionGen, newPos, uint64(n.(int64))))
		newPos += uint64(n.(int64))
		return true
	})
	if rangeErr != nil {
		return rangeErr
	}
	err = compactionWriter.Flush()
	if err != nil {
		return err
	}

	// we need to close compactionWriter
	err = compactionWriter.close()
	if err != nil {
		return err
	}

	atomic.StoreUint64(w.reader.safePoint, compactionGen)
	w.reader.closeStaleHandles()

	// remove stale log files.
	var genList []uint64
	if genList, err = sortedGenList(w.path); err != nil {
		return err
	}
	var staleGens []uint64
	for _, gen := range genList {
		if gen < compactionGen {
			staleGens = append(staleGens, gen)
		}
	}

	for _, gen := range staleGens {
		err = os.Remove(logPath(w.path, gen))
		if err != nil {
			fmt.Println(err)
		}
	}
	w.uncompacted = 0
	return nil
}

func newLogFile(path string, gen uint64) (*FileWriterWithPos, error) {
	p := logPath(path, gen)
	f, err := os.OpenFile(p, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	w := newFileWriterWithPos(f)
	return w, nil
}

func sortedGenList(path string) ([]uint64, error) {
	dirEntryList, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	var res = make([]uint64, 0, len(dirEntryList))
	for _, v := range dirEntryList {
		if !v.IsDir() && strings.HasSuffix(v.Name(), ".log") {
			i, err := strconv.Atoi(strings.TrimSuffix(v.Name(), ".log"))
			if err == nil {
				res = append(res, uint64(i))
			}
		}
	}
	sort.Slice(res, func(i, j int) bool { return res[i] < res[j] })
	return res, nil
}

// Load the whole log file and store value locations in the index map.
//
// Returns how many bytes can be saved after a compaction.
func load(gen uint64, reader *FileReaderWithPos, index *sync.Map) (uint64, error) {
	uncompacted := uint64(0) // number of bytes that can be saved after a compaction.
	// To make sure we read from the beginning of the file.
	pos, err := reader.Seek(0, 0)
	if err != nil {
		return 0, err
	}
	dec := json.NewDecoder(reader)
	for {
		var cmd Command
		if err := dec.Decode(&cmd); err == io.EOF {
			break
		} else if err != nil {
			return 0, err
		} else {
			newPos := dec.InputOffset()
			if cmd.Type == Set {
				old, ok := index.Load(cmd.Key)
				// if already contain key, add its length to `uncompacted`
				if ok {
					uncompacted += old.(*CommandPos).Len
				}
				index.Store(cmd.Key, newCommandPos(gen, uint64(pos), uint64(newPos-pos)))
			} else if cmd.Type == Remove {
				old, ok := index.Load(cmd.Key)
				if ok {
					uncompacted += old.(*CommandPos).Len
				}
				// the "remove" command itself can be deleted in the next compaction.
				// so we add its length to `uncompacted`.
				uncompacted += uint64(newPos - pos)
			}
			pos = newPos
		}
	}
	return uncompacted, err
}

func jsonToWriter(w io.Writer, v interface{}) error {
	enc := json.NewEncoder(w)
	err := enc.Encode(v)
	if err != nil {
		return err
	}
	return nil
}

func logPath(path string, gen uint64) string {
	return path + strconv.FormatUint(gen, 10) + ".log"
}

type FileReaderWithPos struct {
	file   *os.File
	reader *bufio.Reader
	pos    int64
}

func newFileReaderWithPos(f *os.File) (*FileReaderWithPos, error) {
	_, err := f.Seek(0, 0)
	if err != nil {
		return nil, err
	}
	return &FileReaderWithPos{
		file:   f,
		reader: nil,
		pos:    0,
	}, nil
}

func (r *FileReaderWithPos) Read(buf []byte) (int, error) {
	// l := len(buf)
	n, err := r.file.Read(buf)
	if err != nil {
		return 0, err
	}
	//if l != n {
	//	return 0, errors.New(fmt.Sprintf("expect read %d bytes, only read %d", l, n))
	//}
	r.pos += int64(n)
	return n, nil
}

func (r *FileReaderWithPos) Seek(offset int64, whence int) (int64, error) {
	p, err := r.file.Seek(offset, whence)
	if err != nil {
		return 0, err
	}
	if p != offset {
		return 0, errors.New(fmt.Sprintf("seek to %d, ret %d", offset, p))
	}
	r.pos = p
	return p, nil
}

type FileWriterWithPos struct {
	file   *os.File
	writer *bufio.Writer
	pos    int64
}

func newFileWriterWithPos(f *os.File) *FileWriterWithPos {
	return &FileWriterWithPos{
		file:   f,
		writer: bufio.NewWriter(f),
		pos:    0,
	}
}

func (w *FileWriterWithPos) close() error {
	if err := w.writer.Flush(); err != nil {
		return nil
	}
	err := w.file.Sync()
	if err != nil {
		return err
	}
	err = w.file.Close()
	if err != nil {
		return err
	}
	return nil
}

func (w *FileWriterWithPos) Write(buf []byte) (int, error) {
	lens := len(buf)
	n, err := w.writer.Write(buf)
	if err != nil {
		return 0, err
	}
	if n != lens {
		return 0, errors.New(fmt.Sprintf("expect write %d bytes, but only %d", lens, n))
	}
	w.pos += int64(n)
	return n, nil
}

func (w *FileWriterWithPos) Flush() error {
	return w.writer.Flush()
}

type CommandType = uint16

const (
	Set CommandType = iota
	Remove
)

type Command struct {
	Type  CommandType `json:"type"`
	Key   string      `json:"key"`
	Value string      `json:"value"`
}

type CommandPos struct {
	Gen uint64 `json:"gen"`
	Pos uint64 `json:"pos"`
	Len uint64 `json:"len"`
}

func newCommandPos(gen uint64, pos uint64, len uint64) *CommandPos {
	return &CommandPos{
		gen, pos, len,
	}
}
