package hey

import (
	"database/sql"
	"time"
)

// transaction Information for transaction.
type transaction struct {
	way *Way
	tx  *sql.Tx

	startAt time.Time

	id      string
	message string

	err error

	logCmd []*CmdLog

	endAt time.Time

	state string
}

// start Logging when starting a transaction.
func (s *transaction) start() {
	if s.way.log == nil {
		return
	}
	lg := s.way.log.Info()
	lg.Str("id", s.id)
	lg.Int64("start_at", s.startAt.UnixMilli())
	lg.Str("state", "BEGIN")
	lg.Send()
}

// write Recording transaction logs.
func (s *transaction) write() {
	if s.way.log == nil {
		return
	}
	if s.endAt.IsZero() {
		s.endAt = time.Now()
	}
	for _, v := range s.logCmd {
		lg := s.way.log.Info()
		if v.err != nil {
			lg = s.way.log.Error()
			lg.Str("error", v.err.Error())
			lg.Str("script", PrepareString(s.way.cfg.Helper, v.prepare, v.args.args))
		} else {
			if v.args.endAt.Sub(v.args.startAt) > s.way.cfg.WarnDuration {
				lg = s.way.log.Warn()
				lg.Str("script", PrepareString(s.way.cfg.Helper, v.prepare, v.args.args))
			}
		}
		lg.Str("id", s.id).Str("msg", s.message)
		lg.Str("prepare", v.prepare)
		lg.Any("args", v.args.args)
		lg.Int64("start_at", v.args.startAt.UnixMilli())
		lg.Int64("end_at", v.args.endAt.UnixMilli())
		lg.Str("cost", v.args.endAt.Sub(v.args.startAt).String())
		lg.Send()
	}
	lg := s.way.log.Info()
	if s.err != nil {
		lg = s.way.log.Error()
		lg.Str("error", s.err.Error())
	}
	lg.Str("id", s.id)
	lg.Int64("start_at", s.startAt.UnixMilli())
	lg.Str("state", s.state)
	lg.Int64("end_at", s.endAt.UnixMilli())
	lg.Str("cost", s.endAt.Sub(s.startAt).String())
	lg.Send()
}
