package logger

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

// Level 日志级别
type Level int

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
)

var (
	// 当前日志级别
	currentLevel = LevelInfo
	// 标准输出日志
	stdLogger = log.New(os.Stdout, "", 0)
)

// SetLevel 设置日志级别
func SetLevel(level Level) {
	currentLevel = level
}

// shouldLog 判断是否应该记录该级别的日志
func shouldLog(level Level) bool {
	return level >= currentLevel
}

// formatLine 格式化日志行
func formatLine(level Level, message string) string {
	timestamp := time.Now().Format("2006-01-02 15:04:05.000")

	levelStr := ""
	switch level {
	case LevelDebug:
		levelStr = "DEBUG"
	case LevelInfo:
		levelStr = "INFO "
	case LevelWarn:
		levelStr = "WARN "
	case LevelError:
		levelStr = "ERROR"
	}

	return fmt.Sprintf("[%s] [%s] %s", timestamp, levelStr, message)
}

func formatMessage(level Level, format string, args ...interface{}) string {
	message := format
	if len(args) > 0 {
		message = fmt.Sprintf(format, args...)
	}
	return formatLine(level, message)
}

func formatFieldValue(value interface{}) string {
	switch v := value.(type) {
	case string:
		if strings.ContainsAny(v, " \t\r\n") || v == "" {
			return strconv.Quote(v)
		}
		return v
	case error:
		return strconv.Quote(v.Error())
	default:
		return fmt.Sprint(value)
	}
}

func formatFields(fields ...interface{}) string {
	if len(fields) == 0 {
		return ""
	}

	parts := make([]string, 0, len(fields)/2)
	for i := 0; i < len(fields); i += 2 {
		key := fmt.Sprint(fields[i])
		value := "<missing>"
		if i+1 < len(fields) {
			value = formatFieldValue(fields[i+1])
		}
		parts = append(parts, fmt.Sprintf("%s=%s", key, value))
	}
	return strings.Join(parts, " ")
}

func logEvent(level Level, event string, fields ...interface{}) {
	if !shouldLog(level) {
		return
	}

	message := fmt.Sprintf("event=%s", event)
	if extras := formatFields(fields...); extras != "" {
		message += " " + extras
	}
	stdLogger.Println(formatLine(level, message))
}

// Debug 调试日志
func Debug(format string, args ...interface{}) {
	if shouldLog(LevelDebug) {
		stdLogger.Println(formatMessage(LevelDebug, format, args...))
	}
}

// DebugE 带emoji的调试日志
func DebugE(emoji string, format string, args ...interface{}) {
	if shouldLog(LevelDebug) {
		stdLogger.Println(formatMessage(LevelDebug, format, args...))
	}
}

// Info 信息日志
func Info(format string, args ...interface{}) {
	if shouldLog(LevelInfo) {
		stdLogger.Println(formatMessage(LevelInfo, format, args...))
	}
}

// InfoE 带emoji的信息日志
func InfoE(emoji string, format string, args ...interface{}) {
	if shouldLog(LevelInfo) {
		stdLogger.Println(formatMessage(LevelInfo, format, args...))
	}
}

// Warn 警告日志
func Warn(format string, args ...interface{}) {
	if shouldLog(LevelWarn) {
		stdLogger.Println(formatMessage(LevelWarn, format, args...))
	}
}

// WarnE 带emoji的警告日志
func WarnE(emoji string, format string, args ...interface{}) {
	if shouldLog(LevelWarn) {
		stdLogger.Println(formatMessage(LevelWarn, format, args...))
	}
}

// Error 错误日志
func Error(format string, args ...interface{}) {
	if shouldLog(LevelError) {
		stdLogger.Println(formatMessage(LevelError, format, args...))
	}
}

// ErrorE 带emoji的错误日志
func ErrorE(emoji string, format string, args ...interface{}) {
	if shouldLog(LevelError) {
		stdLogger.Println(formatMessage(LevelError, format, args...))
	}
}

// Fatal 致命错误日志并退出
func Fatal(format string, args ...interface{}) {
	stdLogger.Fatalln(formatMessage(LevelError, format, args...))
}

// FatalE 带emoji的致命错误日志并退出
func FatalE(emoji string, format string, args ...interface{}) {
	stdLogger.Fatalln(formatMessage(LevelError, format, args...))
}

func DebugEvent(event string, fields ...interface{}) { logEvent(LevelDebug, event, fields...) }
func InfoEvent(event string, fields ...interface{})  { logEvent(LevelInfo, event, fields...) }
func WarnEvent(event string, fields ...interface{})  { logEvent(LevelWarn, event, fields...) }
func ErrorEvent(event string, fields ...interface{}) { logEvent(LevelError, event, fields...) }
func FatalEvent(event string, fields ...interface{}) {
	message := fmt.Sprintf("event=%s", event)
	if extras := formatFields(fields...); extras != "" {
		message += " " + extras
	}
	stdLogger.Fatalln(formatLine(LevelError, message))
}
