// Common methods.

package common

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"strings"
	"time"

	"github.com/cd365/hey/v6"
	"github.com/cd365/hey/v6/cst"
)

const (
	heyV6 = "github.com/cd365/hey/v6"
)

type FileLineMethod struct {
	file   string // file name
	line   int    // file line
	method string // method name
}

func MyCaller() string {
	callers := make([]*FileLineMethod, 0)
	for skip := 1; true; skip++ {
		pc, file, line, ok := runtime.Caller(skip)
		if !ok {
			break
		}
		tmp := &FileLineMethod{
			file:   file,
			line:   line,
			method: runtime.FuncForPC(pc).Name(),
		}
		callers = append(callers, tmp)
	}
	latest := 0
	for index, value := range callers {
		if strings.Contains(value.method, heyV6) {
			latest = index
		}
	}
	if length := len(callers); latest+1 == length {
		v := callers[latest]
		return fmt.Sprintf("%s:%d %s", v.file, v.line, v.method)
	}
	v := callers[latest+1]
	return fmt.Sprintf("%s:%d %s", v.file, v.line, v.method)
}

type MyTrack struct{}

func (s *MyTrack) Track(ctx context.Context, track any) {
	tmp, ok := track.(*hey.MyTrack)
	if !ok || tmp == nil {
		return
	}
	switch tmp.Type {
	case hey.TrackDebug:
		log.Println(MyCaller(), "<<DEBUG>>", hey.SQLToString(hey.NewSQL(tmp.Prepare, tmp.Args...)))
	case hey.TrackSQL:
		log.Println(MyCaller(), "<<SQL>>", hey.SQLToString(hey.NewSQL(tmp.Prepare, tmp.Args...)))
	case hey.TrackTransaction:
		if tmp.TxState == cst.BEGIN {
			log.Println(MyCaller(), "<<TRANSACTION>>", fmt.Sprintf("%s %s %s %s", tmp.TxId, tmp.TxMsg, tmp.TxState, tmp.TimeStart.Format(time.DateTime)))
		} else {
			log.Println(MyCaller(), "<<TRANSACTION>>", fmt.Sprintf("%s %s %s %s", tmp.TxId, tmp.TxMsg, tmp.TxState, tmp.TimeEnd.Sub(tmp.TimeStart).String()))
		}
	default:

	}
}
