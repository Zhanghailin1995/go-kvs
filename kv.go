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
	index    *sync.Map // map generation number to the file reader
	reader   *KvStoreReader
	writer   *SyncKvStoreWriter
	refCount int32
}

func (s *KvStore) clone() KvsEngine {
	return &KvStore{
		path:     s.path,
		index:    s.index,
		reader:   s.reader.Clone(), // 每一个连接一个store实例，reader是各自独立的，其他的共享
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
		reader:      r.Clone(),
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
		return s.writer.Set(key, value)
	} else {
		return errors.New("already shutdown")
	}
}

func (s *SyncKvStoreWriter) Remove(key string) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if !s.stop {
		return s.writer.Remove(key)
	} else {
		return errors.New("already shutdown")
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

func (s *KvStore) shutdown() error {
	// only shutdown reader
	if err := s.reader.shutdown(); err != nil {
		return err
	}
	// ref count == 0 shutdown writer
	if atomic.AddInt32(&s.refCount, -1) == 0 {
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

func (r *KvStoreReader) closeStaleHandles() error {
	// TODO replace with TreeMap
	var staleGens []uint64
	for gen := range r.readers {
		if gen < atomic.LoadUint64(r.safePoint) {
			staleGens = append(staleGens, gen)
		}
	}
	for _, gen := range staleGens {
		reader, ok := r.readers[gen]
		if !ok {
			return errors.New("can not find reader")
		}
		err := reader.file.Close()
		if err != nil {
			return err
		}
		delete(r.readers, gen)
		//err = os.Remove(logPath(s.path, gen))
		//if err != nil {
		//	fmt.Println(err)
		//}
	}
	return nil
}

/// Read the log file at the given `CommandPos`.
func (r *KvStoreReader) readAnd(cmdPos *CommandPos, f func(reader io.Reader) (*Command, error)) (*Command, error) {
	err := r.closeStaleHandles()
	if err != nil {
		return nil, err
	}

	reader, ok := r.readers[cmdPos.Gen]
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
	_, err = reader.Seek(int64(cmdPos.Pos), 0)
	if err != nil {
		return nil, err
	}
	limitRd := io.LimitReader(reader, int64(cmdPos.Len))
	return f(limitRd)
}

// Read the log file at the given `CommandPos` and deserialize it to `Command`.
func (r *KvStoreReader) readCommand(cmdPos *CommandPos) (*Command, error) {
	return r.readAnd(cmdPos, func(reader io.Reader) (*Command, error) {
		dec := json.NewDecoder(reader)
		var cmd Command
		err := dec.Decode(&cmd)
		if err != nil {
			return nil, err
		}
		return &cmd, nil
	})
}

func (r *KvStoreReader) Clone() *KvStoreReader {
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
	uncompacted uint64
	path        string
	index       *sync.Map
}

func (w *KvStoreWriter) shutdown() error {
	// shutdown reader
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

func (w *KvStoreWriter) Set(key string, value string) error {
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
		// TODO compact
	}
	return nil
}

func (w *KvStoreWriter) Remove(key string) error {
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
		// the "Remove" command itself can be deleted in the next compaction
		// so we add its length to `uncompacted`
		w.uncompacted += uint64(w.writer.pos - pos)

		if w.uncompacted > CompactionThreshold {
			// TODO compact
		}
		return nil
	} else {
		return nil
	}
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
				// the "Remove" command itself can be deleted in the next compaction.
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
	l, err := r.file.Read(buf)
	if err != nil {
		return 0, err
	}
	r.pos += int64(l)
	return l, nil
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
