package logger

import (
	"github.com/rs/zerolog"
	"io"
	"os"
)

type Logger struct {
	logger      *zerolog.Logger
	customEvent func(event *zerolog.Event) *zerolog.Event
}

func NewLogger(writer io.Writer) *Logger {
	if writer == nil {
		writer = os.Stdout
	}
	logger := zerolog.New(writer).With().Caller().Timestamp().Logger()
	logger.Level(zerolog.TraceLevel)
	return &Logger{
		logger:      &logger,
		customEvent: func(event *zerolog.Event) *zerolog.Event { return event },
	}
}

func (s *Logger) GetLevel() zerolog.Level {
	return s.logger.GetLevel()
}

func (s *Logger) SetLevel(lvl zerolog.Level) *Logger {
	logger := s.logger.Level(lvl)
	s.logger = &logger
	return s
}

// CustomContext Set common log properties.
func (s *Logger) CustomContext(custom func(ctx zerolog.Context) zerolog.Logger) *Logger {
	if custom != nil {
		ctx := s.logger.With()
		logger := custom(ctx)
		s.logger = &logger
	}
	return s
}

// CustomEvent Set custom properties before calling output log.
func (s *Logger) CustomEvent(customEvent func(event *zerolog.Event) *zerolog.Event) *Logger {
	if customEvent != nil {
		s.customEvent = customEvent
	}
	return s
}

func (s *Logger) GetLogger() *zerolog.Logger {
	return s.logger
}

func (s *Logger) SetLogger(logger *zerolog.Logger) *Logger {
	s.logger = logger
	return s
}

// SetOutput Duplicates the current logger and sets writer as its output.
func (s *Logger) SetOutput(writer io.Writer) *Logger {
	logger := s.logger.Output(writer)
	s.logger = &logger
	return s
}

func (s *Logger) Trace() *zerolog.Event {
	return s.customEvent(s.logger.Trace())
}

func (s *Logger) Debug() *zerolog.Event {
	return s.customEvent(s.logger.Debug())
}

func (s *Logger) Info() *zerolog.Event {
	return s.customEvent(s.logger.Info())
}

func (s *Logger) Warn() *zerolog.Event {
	return s.customEvent(s.logger.Warn())
}

func (s *Logger) Error() *zerolog.Event {
	return s.customEvent(s.logger.Error())
}

func (s *Logger) Fatal() *zerolog.Event {
	return s.customEvent(s.logger.Fatal())
}

func (s *Logger) Panic() *zerolog.Event {
	return s.customEvent(s.logger.Panic())
}

var defaultLogger = NewLogger(nil)

func Default() *Logger {
	return defaultLogger
}

func Trace() *zerolog.Event {
	return defaultLogger.Trace()
}

func Debug() *zerolog.Event {
	return defaultLogger.Debug()
}

func Info() *zerolog.Event {
	return defaultLogger.Info()
}

func Warn() *zerolog.Event {
	return defaultLogger.Warn()
}

func Error() *zerolog.Event {
	return defaultLogger.Error()
}

func Fatal() *zerolog.Event {
	return defaultLogger.Fatal()
}

func Panic() *zerolog.Event {
	return defaultLogger.Panic()
}
