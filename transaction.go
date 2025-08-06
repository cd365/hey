package hey

import (
	"context"
	"database/sql"
	"time"
)

const (
	logTxBegin    = "BEGIN"
	logTxCommit   = "COMMIT"
	logTxRollback = "ROLLBACK"

	logId      = "id"
	logStartAt = "start_at"
	logEndAt   = "end_at"
	logState   = "state"
	logError   = "error"
	logScript  = "script"
	logMsg     = "msg"
	logPrepare = "prepare"
	logArgs    = "args"
	logCost    = "cost"
)

// transaction Information for transaction.
type transaction struct {
	startAt time.Time
	endAt   time.Time

	ctx context.Context

	err error

	way *Way

	tx *sql.Tx

	id      string
	message string

	state string

	sqlLog []*sqlLog
}

// start Logging when starting a transaction.
func (s *transaction) start() {
	if s.way.log == nil {
		return
	}
	lg := s.way.log.Info()
	lg.Str(logId, s.id)
	lg.Int64(logStartAt, s.startAt.UnixMilli())
	lg.Str(logState, logTxBegin)
	lg.Msg(EmptyString)
}

// write Recording transaction logs.
func (s *transaction) write() {
	if s.way.log == nil {
		return
	}
	if s.endAt.IsZero() {
		s.endAt = time.Now()
	}
	for _, v := range s.sqlLog {
		lg := s.way.log.Info()
		if v.err != nil {
			lg = s.way.log.Error()
			lg.Str(logError, v.err.Error())
			lg.Str(logScript, prepareArgsToString(v.prepare, v.args.args))
		} else {
			if v.args.endAt.Sub(v.args.startAt) > s.way.cfg.WarnDuration {
				lg = s.way.log.Warn()
				lg.Str(logScript, prepareArgsToString(v.prepare, v.args.args))
			}
		}
		lg.Str(logId, s.id).Str(logMsg, s.message)
		lg.Str(logPrepare, v.prepare)
		lg.Any(logArgs, v.args.handleArgs())
		lg.Int64(logStartAt, v.args.startAt.UnixMilli())
		lg.Int64(logEndAt, v.args.endAt.UnixMilli())
		lg.Str(logCost, v.args.endAt.Sub(v.args.startAt).String())
		lg.Msg(EmptyString)
	}
	lg := s.way.log.Info()
	if s.err != nil {
		lg = s.way.log.Error()
		lg.Str(logError, s.err.Error())
	}
	lg.Str(logId, s.id)
	lg.Int64(logStartAt, s.startAt.UnixMilli())
	lg.Str(logState, s.state)
	lg.Int64(logEndAt, s.endAt.UnixMilli())
	lg.Str(logCost, s.endAt.Sub(s.startAt).String())
	lg.Msg(EmptyString)
}
