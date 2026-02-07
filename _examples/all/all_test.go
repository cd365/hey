package all

import (
	"testing"
)

func TestAll(t *testing.T) {
	All()
}

func BenchmarkAll(b *testing.B) {
	for i := 0; i < b.N; i++ {
		All()
	}
}
