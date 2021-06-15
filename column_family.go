package broton

import (
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
)

type ColumnFamily struct {
	Store *Store
	Db    *pebble.DB
	Name  string

	closed      chan struct{}
	isScheduled uint32
	timer       *time.Timer
}

func NewColumnFamily(store *Store, name string) *ColumnFamily {
	cf := &ColumnFamily{
		Store:       store,
		Name:        name,
		closed:      make(chan struct{}),
		isScheduled: 0,
		timer:       time.NewTimer(time.Second * 10),
	}

	cf.timer.Stop()

	return cf
}

func (cf *ColumnFamily) sync() {

	cf.timer.Reset(time.Second * 10)

	for {

		select {
		case <-cf.timer.C:
			cf.Db.LogData(nil, pebble.Sync)

			cf.timer.Stop()
			cf.timer.Reset(time.Second * 10)

			atomic.StoreUint32(&cf.isScheduled, 0)
		case <-cf.closed:
			cf.timer.Stop()
			close(cf.closed)
			return
		}
	}
}

func (cf *ColumnFamily) requestSync() {

	if atomic.LoadUint32(&cf.isScheduled) != 0 {
		return
	}

	atomic.StoreUint32(&cf.isScheduled, 1)

	cf.timer.Stop()
	cf.timer.Reset(time.Millisecond * 100)
}

func (cf *ColumnFamily) Open() error {

	dbPath := filepath.Join(cf.Store.dbPath, cf.Name)
	db, err := pebble.Open(dbPath, &pebble.Options{})
	if err != nil {
		return err
	}

	cf.Db = db

	go cf.sync()

	return nil
}

func (cf *ColumnFamily) Close() error {
	cf.closed <- struct{}{}
	cf.Db.LogData(nil, pebble.Sync)
	return cf.Db.Close()
}

func (cf *ColumnFamily) Write(key []byte, data []byte) error {

	err := cf.Db.Set(key, data, pebble.NoSync)
	if err != nil {
		return err
	}

	cf.requestSync()

	return nil
}
