package broton

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/cockroachdb/pebble"
)

type Store struct {
	broton         *Broton
	options        *Options
	name           string
	dbPath         string
	columnFamilies map[string]*ColumnFamily
}

func NewStore(broton *Broton, storeName string) (*Store, error) {

	store := &Store{
		broton:         broton,
		options:        broton.options,
		name:           storeName,
		dbPath:         filepath.Join(broton.options.DatabasePath, storeName),
		columnFamilies: make(map[string]*ColumnFamily),
	}

	err := store.openDatabase()
	if err != nil {
		return nil, err
	}

	return store, nil
}

func (store *Store) openDatabase() error {

	err := os.MkdirAll(store.dbPath, os.ModePerm)
	if err != nil {
		return err
	}

	return nil
}

func (store *Store) Close() {

	for _, cf := range store.columnFamilies {
		cf.Close()
	}

	store.broton.UnregisterStore(store.name)
}

func (store *Store) assertColumnFamily(name string) (*ColumnFamily, error) {

	cf, ok := store.columnFamilies[name]
	if !ok {
		cf := NewColumnFamily(store, name)
		err := cf.Open()
		if err != nil {
			return nil, err
		}

		store.columnFamilies[name] = cf

		return cf, nil
	}

	return cf, nil
}

func (store *Store) GetColumnFamailyHandle(name string) (*ColumnFamily, error) {
	return store.getColumnFamailyHandle(name)
}

func (store *Store) RegisterColumns(names []string) error {

	for _, name := range names {

		// Assert column family
		_, err := store.assertColumnFamily(name)
		if err != nil {
			return err
		}
	}

	return nil
}

func (store *Store) getColumnFamailyHandle(name string) (*ColumnFamily, error) {

	cf, ok := store.columnFamilies[name]
	if !ok {
		return nil, fmt.Errorf("Not found \"%s\" column family", name)
	}

	return cf, nil
}

func (store *Store) getValue(column string, key []byte) ([]byte, io.Closer, error) {

	cfHandle, err := store.getColumnFamailyHandle(column)
	if err != nil {
		return nil, nil, errors.New("Not found \"" + column + "\" column family")
	}

	return cfHandle.Db.Get(key)
}

func (store *Store) Delete(column string, key []byte) error {

	cfHandle, err := store.getColumnFamailyHandle(column)
	if err != nil {
		return errors.New("Not found \"" + column + "\" column family")
	}

	return cfHandle.Db.Delete(key, pebble.Sync)
}

func (store *Store) Put(column string, key []byte, data []byte) error {

	cfHandle, err := store.getColumnFamailyHandle(column)
	if err != nil {
		return errors.New("Not found \"" + column + "\" column family")
	}

	return cfHandle.Db.Set(key, data, pebble.Sync)
}

func (store *Store) PutInt64(column string, key []byte, value int64) error {

	cfHandle, err := store.getColumnFamailyHandle(column)
	if err != nil {
		return errors.New("Not found \"" + column + "\" column family")
	}

	data := Int64ToBytes(value)

	return cfHandle.Db.Set(key, data, pebble.Sync)
}

func (store *Store) GetInt64(column string, key []byte) (int64, error) {

	value, closer, err := store.getValue(column, key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return 0, nil
		}

		return 0, err
	}

	data := BytesToInt64(value)

	closer.Close()

	return data, nil
}

func (store *Store) PutUint64(column string, key []byte, value uint64) error {

	cfHandle, err := store.getColumnFamailyHandle(column)
	if err != nil {
		return errors.New("Not found \"" + column + "\" column family")
	}

	data := Uint64ToBytes(value)

	return cfHandle.Db.Set(key, data, pebble.Sync)
}

func (store *Store) GetUint64(column string, key []byte) (uint64, error) {

	value, closer, err := store.getValue(column, key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return 0, nil
		}

		return 0, err
	}

	data := BytesToUint64(value)

	closer.Close()

	return data, nil
}

func (store *Store) PutFloat64(column string, key []byte, value float64) error {

	cfHandle, err := store.getColumnFamailyHandle(column)
	if err != nil {
		return errors.New("Not found \"" + column + "\" column family")
	}

	data := Float64ToBytes(value)

	return cfHandle.Db.Set(key, data, pebble.Sync)
}

func (store *Store) GetFloat64(column string, key []byte) (float64, error) {

	value, closer, err := store.getValue(column, key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return 0, nil
		}

		return 0, err
	}

	data := BytesToFloat64(value)

	closer.Close()

	return data, nil
}

func (store *Store) GetBytes(column string, key []byte) ([]byte, error) {

	value, closer, err := store.getValue(column, key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return []byte(""), nil
		}

		return []byte(""), err
	}

	data := make([]byte, len(value))
	copy(data, value)

	closer.Close()

	return data, nil
}

func (store *Store) PutString(column string, key []byte, value string) error {

	cfHandle, err := store.getColumnFamailyHandle(column)
	if err != nil {
		return errors.New("Not found \"" + column + "\" column family")
	}

	data := StrToBytes(value)

	return cfHandle.Db.Set(key, data, pebble.Sync)
}

func (store *Store) GetString(column string, key []byte) (string, error) {

	value, closer, err := store.getValue(column, key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return "", nil
		}

		return "", err
	}

	data := make([]byte, len(value))
	copy(data, value)

	closer.Close()

	return string(data), nil
}

func (store *Store) List(column string, targetKey []byte, callback func(key []byte, value []byte) bool) error {

	cfHandle, err := store.getColumnFamailyHandle(column)
	if err != nil {
		return errors.New("Not found \"" + column + "\" column family")
	}

	iter := cfHandle.Db.NewIter(nil)
	for iter.SeekGE(targetKey); iter.Valid(); iter.Next() {
		isContinuous := callback(iter.Key(), iter.Value())

		if !isContinuous {
			break
		}
	}

	return iter.Close()
}
