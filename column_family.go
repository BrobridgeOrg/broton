package broton

import (
	"path/filepath"

	"github.com/cockroachdb/pebble"
)

type ColumnFamily struct {
	Store *Store
	Db    *pebble.DB
	Name  string
}

func NewColumnFamily(store *Store, name string) *ColumnFamily {
	return &ColumnFamily{
		Store: store,
		Name:  name,
	}
}

func (cf *ColumnFamily) Open() error {

	dbPath := filepath.Join(cf.Store.dbPath, cf.Name)
	db, err := pebble.Open(dbPath, &pebble.Options{})
	if err != nil {
		return err
	}

	cf.Db = db

	return nil
}

func (cf *ColumnFamily) Close() error {
	return cf.Db.Close()
}
