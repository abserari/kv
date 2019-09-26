package buntdb

import (
	"errors"
	"io"
	"os"
	"sync"

	"github.com/tidwall/btree"
)

var (
	ErrTxNotWritable = errors.New("tx not writable")

	ErrTxClosed = errors.New("tx closed")

	ErrNotFound = errors.New("not found")

	ErrInvalid = errors.New("invalid database")

	ErrDatabaseClosed = errors.New("database closed")

	ErrIndexExists = errors.New("index exists")

	ErrInvalidOperation = errors.New("invalid operation")

	ErrInvalidSyncPolicy = errors.New("invalid sync policy")

	ErrShrinkInProcess = errors.New("shrink is in-process")

	ErrpersistenceActive = errors.New("persistence active")

	ErrTxIterating = errors.New("tx is iterating")
)

type DB struct {
	mu        sync.RWMutex
	file      *os.File
	buf       []byte
	keys      *btree.BTree
	exps      *btree.BTree
	idxs      map[string]*index
	exmgr     bool
	flushes   int
	closed    bool
	config    Config
	persist   bool
	shrinking bool
	lastaofsz int // the size of the last shrink aof size
}

type SyncPolicy int

const (
	Never SyncPolicy = iota

	EverySecond

	Always
)

type Config struct {
	SyncPolicy SyncPolicy

	AutoShrinkPercentage int

	AutoShrinkMinSize int

	AutoShrinkDisabled bool

	OnExpired func(keys []string)

	OnExpiredSync func(key, value string, tx *Tx) error
}

type exctx struct {
	db *DB
}

const btreeDegrees = 64

func Open(path string) (*DB, error) {
	db := &DB{}
	db.keys = btree.New(btreeDegrees, nil)
	db.exps = btree.New(btreeDegrees, &exctx{db})
	db.idxs = make(map[string]*index)

	db.config = Config{
		SyncPolicy:           EverySecond,
		AutoShrinkPercentage: 100,
		AutoShrinkMinSize:    32 * 1024 * 1024,
	}

	db.persist = path != ":memory:"
	if db.persist {
		var err error

		db.file, err = os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			return nil, err
		}
		if err := db.load(); err != nil {
			_ = db.file.Close()
			return nil, err
		}
	}

	go db.backgroundManager()
	return db, nil
}

func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrDatabaseClosed
	}
	db.closed = true
	if db.persist {
		db.file.Sync()
		if err := db.file.Close(); err != nil {
			return err
		}
	}

	db.keys, db.exps, db.idxs, db.file = nil, nil, nil, nil
	return nil
}

func (db *DB) Save(wr io.Writer) error {
	var err error
	db.mu.Lock()
	defer db.mu.Unlock()

	var buf []byte
	db.keys.Ascend(func(item btree.Item) bool {
		dbi := item.(*dbItem)
		buf = dbi.writeSetTo(buf)
		if len(buf) > 1024*1024*4 {
			_, err = wr.Write(buf)
			if err != nil {
				return false
			}
			buf = buf[:0]
		}
		return true
	})
	if err != nil {
		return err
	}

	if len(buf) > 0 {
		_, err = wr.Write(buf)
		if err != nil {
			return err
		}
	}
	return nil
}
