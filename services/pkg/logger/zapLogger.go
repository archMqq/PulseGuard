package logger

import "go.uber.org/zap"

type ZapLogger struct {
	l *zap.SugaredLogger
}

func NewProductionLogger() (Logger, error) {
	logger, err := zap.NewProduction(zap.AddCaller())
	if err != nil {
		return nil, err
	}

	sugar := logger.Sugar()

	return &ZapLogger{
		l: sugar,
	}, nil
}

func (z *ZapLogger) Info(msg string, fields ...any) {
	z.l.Info(msg, fields)
}

func (z *ZapLogger) Warn(msg string, fields ...any) {
	z.l.Warn(msg, fields)
}

func (z *ZapLogger) Error(msg string, fields ...any) {
	z.l.Error(msg, fields)
}
