package database

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/sirupsen/logrus"
	"gorm.io/gorm/logger"
)

// Colors
const (
	Reset       = "\033[0m"
	Red         = "\033[31m"
	Green       = "\033[32m"
	Yellow      = "\033[33m"
	Blue        = "\033[34m"
	Magenta     = "\033[35m"
	Cyan        = "\033[36m"
	White       = "\033[37m"
	BlueBold    = "\033[34;1m"
	MagentaBold = "\033[35;1m"
	RedBold     = "\033[31;1m"
	YellowBold  = "\033[33;1m"
)

// NewLog initialize logger
func NewLog(config logger.Config, logDir string) logger.Interface {

	var (
		traceStr     = "%s\n[%.3fms] [rows:%v] %s"
		traceWarnStr = "%s %s\n[%.3fms] [rows:%v] %s"
		traceErrStr  = "%s %s\n[%.3fms] [rows:%v] %s"
	)

	if config.Colorful {
		traceStr = Yellow + "[%.3fms] " + BlueBold + "[rows:%v]" + Reset + " %s"
		traceWarnStr = Green + "%s\n" + Reset + RedBold + "[%.3fms] " + Yellow + "[rows:%v]" + Magenta + " %s" + Reset
		traceErrStr = RedBold + "%s\n" + Reset + Yellow + "[%.3fms] " + BlueBold + "[rows:%v]" + Reset + " %s"
	}
	log := logrus.New()
	log.SetLevel(logrus.InfoLevel)
	log.SetReportCaller(true)

	if logDir == "" {
		logDir = "./logs"
	}

	hook, err := NewLogrusFileLoggerHook(logDir, 10<<20)
	if err == nil {
		log.AddHook(hook)
	}

	return &gormLog{
		Config:       config,
		log:          log.WithField("module", "GORM"),
		traceStr:     traceStr,
		traceWarnStr: traceWarnStr,
		traceErrStr:  traceErrStr,
	}
}

type gormLog struct {
	logger.Config
	log                                 *logrus.Entry
	traceStr, traceErrStr, traceWarnStr string
}

// LogMode log mode
func (l *gormLog) LogMode(level logger.LogLevel) logger.Interface {
	newlogger := *l
	newlogger.LogLevel = level
	return &newlogger
}

// Info print info
func (l *gormLog) Info(ctx context.Context, msg string, data ...interface{}) {
	if l.LogLevel >= logger.Info {
		l.log.Infof(msg, data...)
	}
}

// Warn print warn messages
func (l *gormLog) Warn(ctx context.Context, msg string, data ...interface{}) {
	if l.LogLevel >= logger.Warn {
		l.log.Warnf(msg, data...)
	}
}

// Error print error messages
func (l *gormLog) Error(ctx context.Context, msg string, data ...interface{}) {
	if l.LogLevel >= logger.Error {
		l.log.Errorf(msg, data...)
	}
}

// Trace print sql message
//
//nolint:cyclop
func (l *gormLog) Trace(ctx context.Context, begin time.Time, fc func() (string, int64), err error) {
	if l.LogLevel <= logger.Silent {
		return
	}

	elapsed := time.Since(begin)
	switch {
	case err != nil && l.LogLevel >= logger.Error && (!errors.Is(err, logger.ErrRecordNotFound) || !l.IgnoreRecordNotFoundError):
		sql, rows := fc()
		if rows == -1 {
			l.log.Errorf(l.traceErrStr, err, float64(elapsed.Nanoseconds())/1e6, "-", sql)
		} else {
			l.log.Errorf(l.traceErrStr, err, float64(elapsed.Nanoseconds())/1e6, rows, sql)
		}
	case elapsed > l.SlowThreshold && l.SlowThreshold != 0 && l.LogLevel >= logger.Warn:
		sql, rows := fc()
		slowLog := fmt.Sprintf("SLOW SQL >= %v", l.SlowThreshold)
		if rows == -1 {
			l.log.Warnf(l.traceWarnStr, slowLog, float64(elapsed.Nanoseconds())/1e6, "-", sql)
		} else {
			l.log.Warnf(l.traceWarnStr, slowLog, float64(elapsed.Nanoseconds())/1e6, rows, sql)
		}
	case l.LogLevel == logger.Info:
		sql, rows := fc()
		if rows == -1 {
			l.log.Infof(l.traceStr, float64(elapsed.Nanoseconds())/1e6, "-", sql)
		} else {
			l.log.Infof(l.traceStr, float64(elapsed.Nanoseconds())/1e6, rows, sql)
		}
	}
}

// ParamsFilter filter params
func (l *gormLog) ParamsFilter(ctx context.Context, sql string, params ...interface{}) (string, []interface{}) {
	if l.Config.ParameterizedQueries {
		return sql, nil
	}
	return sql, params
}

// LogrusFileLoggerHook /文件日志Hook
type LogrusFileLoggerHook struct {
	maxLogSize int64
	logDir     string
	logPath    string
	logFile    *os.File
}

// NewLogrusFileLoggerHook /工厂方法
func NewLogrusFileLoggerHook(logDir string, maxLogSize int64) (hook *LogrusFileLoggerHook, err error) {
	object := &LogrusFileLoggerHook{
		maxLogSize: maxLogSize,
		logDir:     logDir,
	}
	return object, object.makeLogFile()
}

// /创建日志文件
func (object *LogrusFileLoggerHook) makeLogFile() error {
	var err error
	if filepath.IsAbs(object.logDir) {
		object.logPath = object.logDir
	} else {
		dir, err := os.Getwd()
		if nil != err {
			panic(err)
		}
		object.logPath = filepath.Join(dir, object.logDir)
	}
	err = os.MkdirAll(object.logDir, 0777)
	if nil != err {
		panic(err)
	}
	logTAG := "GORM"

	now := time.Now()
	object.logPath += fmt.Sprintf("%s%s-%04d-%02d-%02dT%02d-%02d-%02d.%d.log",
		string(filepath.Separator),
		logTAG,
		now.Year(),
		now.Month(),
		now.Day(),
		now.Hour(),
		now.Minute(),
		now.Second(),
		os.Getpid())
	if nil != object.logFile {
		err = object.logFile.Close()
		if nil != err {
			return err
		}
	}
	object.logFile, err = os.OpenFile(object.logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
	if nil != err {
		return err
	}
	return nil
}

// Levels /日志等级回调
func (object *LogrusFileLoggerHook) Levels() []logrus.Level {
	return logrus.AllLevels
}

// Fire /激发
func (object *LogrusFileLoggerHook) Fire(entry *logrus.Entry) error {
	if size, err := FileSize(object.logPath); nil != err {
		return err
	} else if size > object.maxLogSize {
		if err = object.makeLogFile(); nil != err {
			return err
		}
	}
	content, err := entry.String()
	if nil != err {
		return err
	}
	_, err = object.logFile.Write([]byte(content))
	if nil != err {
		return err
	}
	return nil
}

// FileSize 文件大小
func FileSize(path string) (size int64, err error) {
	if 0 >= len(path) {
		err = errors.New("invalid path")
		return
	}

	var fi os.FileInfo
	fi, err = os.Stat(path)
	if nil == err {
		size = fi.Size()
	}

	return
}
