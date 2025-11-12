package hey

import (
	"context"
	"time"
)

type MyTrackType string

const (
	TrackDebug       MyTrackType = "DEBUG"
	TrackSQL         MyTrackType = "SQL"
	TrackTransaction MyTrackType = "TRANSACTION"
)

type Track interface {
	Track(ctx context.Context, track any)
}

type MyTrack struct {
	// Context Value of context.
	Context context.Context

	// Type Track type.
	Type MyTrackType

	// TimeStart Start time.
	TimeStart time.Time

	// TimeEnd End time.
	TimeEnd time.Time

	// Err Error value.
	Err error

	// TxId Transaction id.
	TxId string

	// TxMsg Transaction message.
	TxMsg string

	// TxState Transaction state.
	TxState string

	// Prepare Original SQL statement.
	Prepare string

	// Script SQL Script.
	Script string

	// Args The parameter list corresponding to the original SQL statement.
	Args []any
}

func (s *MyTrack) track(way *Way) {
	track := way.track
	if track == nil {
		return
	}
	if s.TimeEnd.IsZero() {
		s.TimeEnd = time.Now()
	}
	s.Script = SQLToString(NewSQL(s.Prepare, s.Args...))
	if way.IsInTransaction() {
		way.transaction.script = append(way.transaction.script, s)
	} else {
		track.Track(s.Context, s)
	}
}

func newTrack(typeValue MyTrackType) *MyTrack {
	return &MyTrack{
		Type: typeValue,
	}
}

func (s *Way) trackDebug(ctx context.Context, maker Maker) *MyTrack {
	script := maker.ToSQL()
	tmp := newTrack(TrackDebug)
	tmp.Context = ctx
	tmp.Prepare = script.Prepare
	tmp.Args = script.Args
	tmp.Script = SQLToString(script)
	tmp.TimeStart = time.Now()
	return tmp
}

func (s *Way) trackSQL(ctx context.Context, prepare string, args []any) *MyTrack {
	tmp := newTrack(TrackSQL)
	tmp.Context = ctx
	tmp.Prepare = prepare
	tmp.Args = args
	tmp.TimeStart = time.Now()
	return tmp
}

func (s *Way) trackTransaction(ctx context.Context, startTime time.Time) *MyTrack {
	tmp := newTrack(TrackTransaction)
	tmp.Context = ctx
	tmp.TimeStart = startTime
	return tmp
}
