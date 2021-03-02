package broton

import (
	"fmt"
	"os"
	"sync/atomic"
)

var testBroton *Broton
var testCounter int32

func createTestBroton(name string) {

	err := os.RemoveAll("./" + name)
	if err != nil {
		panic(err)
	}

	options := NewOptions()
	options.DatabasePath = "./" + name

	broton, err := NewBroton(options)
	if err != nil {
		panic(err)
	}

	testBroton = broton
}

func closeTestBroton() {
	testBroton.Close()
}

func createTestStore() *Store {

	counter := atomic.AddInt32(&testCounter, 1)
	name := fmt.Sprintf("bench-%d", counter)

	// Create a new store for benchmark
	store, err := testBroton.GetStore(name)
	if err != nil {
		panic(err)
	}

	err = store.RegisterColumns([]string{name})
	if err != nil {
		panic(err)
	}

	return store
}
