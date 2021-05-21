package broton

import (
	"bytes"
	"fmt"
	"testing"
)

func TestPut(t *testing.T) {

	createTestBroton("testing")
	defer closeTestBroton()

	store := createTestStore()

	column := fmt.Sprintf("bench-%d", testCounter)

	if err := store.Put(column, []byte("Benchmark"), []byte("value")); err != nil {
		t.Error(err)
	}

	value, err := store.GetBytes(column, []byte("Benchmark"))
	if err != nil {
		t.Error(err)
	}

	if bytes.Compare(value, []byte("value")) != 0 {
		t.Fail()
	}
}

func TestPutInt64(t *testing.T) {

	createTestBroton("testing")
	defer closeTestBroton()

	store := createTestStore()

	column := fmt.Sprintf("bench-%d", testCounter)

	if err := store.PutInt64(column, []byte("Benchmark"), int64(999999)); err != nil {
		t.Error(err)
	}

	value, err := store.GetInt64(column, []byte("Benchmark"))
	if err != nil {
		t.Error(err)
	}

	if value != int64(999999) {
		t.Fail()
	}
}

func TestPutUint64(t *testing.T) {

	createTestBroton("testing")
	defer closeTestBroton()

	store := createTestStore()

	column := fmt.Sprintf("bench-%d", testCounter)

	if err := store.PutUint64(column, []byte("Benchmark"), uint64(999999)); err != nil {
		t.Error(err)
	}

	value, err := store.GetUint64(column, []byte("Benchmark"))
	if err != nil {
		t.Error(err)
	}

	if value != uint64(999999) {
		t.Fail()
	}
}

func TestPutFloat64(t *testing.T) {

	createTestBroton("testing")
	defer closeTestBroton()

	store := createTestStore()

	column := fmt.Sprintf("bench-%d", testCounter)

	if err := store.PutFloat64(column, []byte("Benchmark"), float64(999.999)); err != nil {
		t.Error(err)
	}

	value, err := store.GetFloat64(column, []byte("Benchmark"))
	if err != nil {
		t.Error(err)
	}

	if value != float64(999.999) {
		t.Fail()
	}
}

func TestPutString(t *testing.T) {

	createTestBroton("testing")
	defer closeTestBroton()

	store := createTestStore()

	column := fmt.Sprintf("bench-%d", testCounter)

	if err := store.PutString(column, []byte("Benchmark"), "test"); err != nil {
		t.Error(err)
	}

	value, err := store.GetString(column, []byte("Benchmark"))
	if err != nil {
		t.Error(err)
	}

	if value != "test" {
		t.Fail()
	}
}

func TestDelete(t *testing.T) {

	createTestBroton("testing")
	defer closeTestBroton()

	store := createTestStore()

	column := fmt.Sprintf("bench-%d", testCounter)

	if err := store.Put(column, []byte("Benchmark"), []byte("value")); err != nil {
		t.Error(err)
	}

	if err := store.Delete(column, []byte("Benchmark")); err != nil {
		t.Error(err)
	}

	// It should get nothing
	value, err := store.GetBytes(column, []byte("Benchmark"))
	if value != nil {
		t.Error(err)
	}
}

func TestList(t *testing.T) {

	createTestBroton("testing")
	defer closeTestBroton()

	store := createTestStore()

	column := fmt.Sprintf("bench-%d", testCounter)

	if err := store.Put(column, []byte("Benchmark"), []byte("value")); err != nil {
		t.Error(err)
	}

	for i := 1; i <= 10; i++ {
		key := Int64ToBytes(int64(i))

		if err := store.PutInt64(column, key, int64(i)); err != nil {
			t.Error(err)
		}
	}

	var counter int64 = 0
	targetKey := Int64ToBytes(int64(1))
	store.List(column, targetKey, func(key []byte, value []byte) bool {

		if counter == 10 {
			return false
		}

		counter++

		if BytesToInt64(value) != counter {
			t.Fail()
			return false
		}

		return true
	})
}
