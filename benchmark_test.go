package broton

import (
	"encoding/binary"
	"fmt"
	"testing"
)

var benchmarkStore *Store
var benchmarkColumn string

func BenchmarkWrite(b *testing.B) {

	createTestBroton("testing")

	benchmarkStore = createTestStore()

	benchmarkColumn = fmt.Sprintf("bench-%d", testCounter)

	key := make([]byte, 4)
	value := []byte("value")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint32(key, uint32(i))
		if err := benchmarkStore.Put(benchmarkColumn, key, value); err != nil {
			panic(err)
		}
	}
}

func BenchmarkRead(b *testing.B) {

	key := make([]byte, 4)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint32(key, uint32(i))
		_, err := benchmarkStore.GetBytes(benchmarkColumn, key)
		if err != nil {
			break
		}
	}
}
