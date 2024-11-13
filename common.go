package hey

import (
	"strings"
	"sync"
)

var (
	stringBuilder = &sync.Pool{
		New: func() interface{} { return &strings.Builder{} },
	}
)

func getStringBuilder() *strings.Builder {
	return stringBuilder.Get().(*strings.Builder)
}

func putStringBuilder(b *strings.Builder) {
	b.Reset()
	stringBuilder.Put(b)
}
