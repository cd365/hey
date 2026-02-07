package all

import (
	"net/http"
	_ "net/http/pprof"
	"testing"
)

func TestAll(t *testing.T) {
	All()
}

func BenchmarkAll(b *testing.B) {
	go func() {
		err := http.ListenAndServe("localhost:9090", nil)
		if err != nil {
			b.Log(err.Error())
		}
	}()
	for i := 0; i < b.N; i++ {
		All()
	}
}
