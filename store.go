package broton

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/tecbot/gorocksdb"
)

type Store struct {
	broton    *Broton
	options   *Options
	name      string
	db        *gorocksdb.DB
	cfHandles map[string]*gorocksdb.ColumnFamilyHandle
	ro        *gorocksdb.ReadOptions
	wo        *gorocksdb.WriteOptions

	subscriptions sync.Map
}

func NewStore(broton *Broton, storeName string) (*Store, error) {

	// Initializing options
	ro := gorocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)
	//	ro.SetTailing(true)
	wo := gorocksdb.NewDefaultWriteOptions()

	store := &Store{
		broton:    broton,
		options:   broton.options,
		name:      storeName,
		cfHandles: make(map[string]*gorocksdb.ColumnFamilyHandle),
		ro:        ro,
		wo:        wo,
	}

	err := store.openDatabase()
	if err != nil {
		return nil, err
	}

	return store, nil
}

func (store *Store) openDatabase() error {

	dbpath := filepath.Join(store.options.DatabasePath, store.name)
	err := os.MkdirAll(dbpath, os.ModePerm)
	if err != nil {
		return err
	}

	// List column families
	cfNames, _ := gorocksdb.ListColumnFamilies(store.options.RocksdbOptions, dbpath)

	if len(cfNames) == 0 {
		cfNames = []string{"default"}
	}

	// Preparing options for column families
	cfOpts := make([]*gorocksdb.Options, len(cfNames))
	for i := range cfNames {
		cfOpts[i] = store.options.RocksdbOptions
	}

	// Open database
	db, cfHandles, err := gorocksdb.OpenDbColumnFamilies(store.options.RocksdbOptions, dbpath, cfNames, cfOpts)
	if err != nil {
		return err
	}

	for i, name := range cfNames {
		store.cfHandles[name] = cfHandles[i]
	}

	store.db = db

	return nil
}

func (store *Store) Close() {
	store.db.Close()
	store.broton.UnregisterStore(store.name)
}

func (store *Store) assertColumnFamily(name string) (*gorocksdb.ColumnFamilyHandle, error) {

	handle, ok := store.cfHandles[name]
	if !ok {
		handle, err := store.db.CreateColumnFamily(store.options.RocksdbOptions, name)
		if err != nil {
			return nil, err
		}

		store.cfHandles[name] = handle

		return handle, nil
	}

	return handle, nil
}

func (store *Store) GetDb() *gorocksdb.DB {
	return store.db
}

func (store *Store) GetColumnFamailyHandle(name string) (*gorocksdb.ColumnFamilyHandle, error) {
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

func (store *Store) getColumnFamailyHandle(name string) (*gorocksdb.ColumnFamilyHandle, error) {

	cfHandle, ok := store.cfHandles[name]
	if !ok {
		return nil, fmt.Errorf("Not found \"%s\" column family", name)
	}

	return cfHandle, nil
}

func (store *Store) getValue(column string, key []byte) (*gorocksdb.Slice, error) {

	cfHandle, err := store.getColumnFamailyHandle(column)
	if err != nil {
		return nil, errors.New("Not found \"" + column + "\" column family")
	}

	value, err := store.db.GetCF(store.ro, cfHandle, key)
	if err != nil {
		return nil, err
	}

	return value, nil
}

func (store *Store) Delete(column string, key []byte) error {

	cfHandle, err := store.getColumnFamailyHandle(column)
	if err != nil {
		return errors.New("Not found \"" + column + "\" column family")
	}

	// Write
	err = store.db.DeleteCF(store.wo, cfHandle, key)
	if err != nil {
		return err
	}

	return nil
}

func (store *Store) Put(column string, key []byte, data []byte) error {

	cfHandle, err := store.getColumnFamailyHandle(column)
	if err != nil {
		return errors.New("Not found \"" + column + "\" column family")
	}

	// Write
	err = store.db.PutCF(store.wo, cfHandle, key, data)
	if err != nil {
		return err
	}

	return nil
}

func (store *Store) PutInt64(column string, key []byte, value int64) error {

	cfHandle, err := store.getColumnFamailyHandle(column)
	if err != nil {
		return errors.New("Not found \"" + column + "\" column family")
	}

	data := Int64ToBytes(value)

	// Write
	err = store.db.PutCF(store.wo, cfHandle, key, data)
	if err != nil {
		return err
	}

	return nil
}

func (store *Store) GetInt64(column string, key []byte) (int64, error) {

	value, err := store.getValue(column, key)
	if err != nil {
		return 0, err
	}

	if !value.Exists() {
		return 0, nil
	}

	data := BytesToInt64(value.Data())
	value.Free()

	return data, nil
}

func (store *Store) PutUint64(column string, key []byte, value uint64) error {

	cfHandle, err := store.getColumnFamailyHandle(column)
	if err != nil {
		return errors.New("Not found \"" + column + "\" column family")
	}

	data := Uint64ToBytes(value)

	// Write
	err = store.db.PutCF(store.wo, cfHandle, key, data)
	if err != nil {
		return err
	}

	return nil
}

func (store *Store) GetUint64(column string, key []byte) (uint64, error) {

	value, err := store.getValue(column, key)
	if err != nil {
		return 0, err
	}

	if !value.Exists() {
		return 0, nil
	}

	data := BytesToUint64(value.Data())
	value.Free()

	return data, nil
}

func (store *Store) PutFloat64(column string, key []byte, value float64) error {

	cfHandle, err := store.getColumnFamailyHandle(column)
	if err != nil {
		return errors.New("Not found \"" + column + "\" column family")
	}

	data := Float64ToBytes(value)

	// Write
	err = store.db.PutCF(store.wo, cfHandle, key, data)
	if err != nil {
		return err
	}

	return nil
}

func (store *Store) GetFloat64(column string, key []byte) (float64, error) {

	value, err := store.getValue(column, key)
	if err != nil {
		return 0, err
	}

	if !value.Exists() {
		return 0, nil
	}

	data := BytesToFloat64(value.Data())

	value.Free()

	return data, nil
}

func (store *Store) GetBytes(column string, key []byte) ([]byte, error) {

	value, err := store.getValue(column, key)
	if err != nil {
		return nil, err
	}

	if !value.Exists() {
		return nil, nil
	}

	data := value.Data()
	value.Free()

	return data, nil
}

func (store *Store) PutString(column string, key []byte, value string) error {

	cfHandle, err := store.getColumnFamailyHandle(column)
	if err != nil {
		return errors.New("Not found \"" + column + "\" column family")
	}

	data := StrToBytes(value)

	// Write
	err = store.db.PutCF(store.wo, cfHandle, key, data)
	if err != nil {
		return err
	}

	return nil
}

func (store *Store) GetString(column string, key []byte) (string, error) {

	value, err := store.getValue(column, key)
	if err != nil {
		return "", err
	}

	if !value.Exists() {
		return "", nil
	}

	data := string(value.Data())
	value.Free()

	return data, nil
}

func (store *Store) List(column string, targetKey []byte, callback func(key []byte, value []byte) bool) error {

	cfHandle, err := store.getColumnFamailyHandle(column)
	if err != nil {
		return errors.New("Not found \"" + column + "\" column family")
	}

	// Initializing iterator
	ro := gorocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)
	ro.SetTailing(true)
	iter := store.db.NewIteratorCF(ro, cfHandle)
	if iter.Err() != nil {
		return iter.Err()
	}

	for iter.Seek(targetKey); iter.Valid(); iter.Next() {
		key := iter.Key()
		value := iter.Value()

		isContinuous := callback(key.Data(), value.Data())

		key.Free()
		value.Free()

		if !isContinuous {
			break
		}
	}

	return nil
}
