package broton

import (
	"os"
)

type Broton struct {
	options *Options
	dbPath  string
	stores  map[string]*Store
}

func NewBroton(options *Options) (*Broton, error) {

	err := os.MkdirAll(options.DatabasePath, os.ModePerm)
	if err != nil {
		return nil, err
	}

	eventStore := &Broton{
		stores:  make(map[string]*Store),
		options: options,
	}

	return eventStore, nil
}

func (bt *Broton) UnregisterStore(name string) {
	delete(bt.stores, name)
}

func (bt *Broton) Close() {
	for _, store := range bt.stores {
		store.Close()
	}

	bt.stores = make(map[string]*Store)
}

func (bt *Broton) GetStore(storeName string) (*Store, error) {

	if store, ok := bt.stores[storeName]; ok {
		return store, nil
	}

	store, err := NewStore(bt, storeName)
	if err != nil {
		return nil, err

	}

	bt.stores[storeName] = store

	return store, nil
}
