package logger

import (
	"csvreader/pkg/constants"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"runtime"

	"github.com/petermattis/goid"
)

var (
	Async   *log.Logger
	logCh   chan logMessage
	closeCh chan struct{}
	flushCh chan struct{}
)

type logMessage struct {
	level string
	msg   string
}

// nota che in GO l'init viene chiamato automaticamente ogni volta che importi il pacchetto non serve richiamarlo
func init() {
	file, err := os.OpenFile(constants.LOGFILE, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatalf("Errore nell'apertura del file di log: %v", err)
	}
	Async = log.New(io.MultiWriter(os.Stdout, file), constants.PREFIX, log.Ldate|log.Ltime|log.Lmicroseconds)
	logCh = make(chan logMessage, 100)
	closeCh = make(chan struct{})
	flushCh = make(chan struct{})

	go logWorker()
}

func logWorker() {
	for {
		select {
		case logMsg := <-logCh:
			Async.SetPrefix(logMsg.level)
			Async.Println(logMsg.msg)
		case <-closeCh:
			for len(logCh) > 0 {
				logMsg := <-logCh
				Async.SetPrefix(logMsg.level)
				Async.Println(logMsg.msg)
			}
			flushCh <- struct{}{}
			return
		}
	}
}

// Funzione per ottenere il file e la linea da cui è stato chiamato il logger, solo il nome del file e l'ID della goroutine
func logLocationAsync() string {
	_, file, line, ok := runtime.Caller(2)
	if !ok {
		return constants.WTF
	}
	return fmt.Sprintf("[%s] [line:%d] [GoroutineID: %d] ", path.Base(file), line, goid.Get())
}

func logAsync(level string, v ...interface{}) {
	logCh <- logMessage{
		level: level,
		msg:   fmt.Sprintln(append([]interface{}{logLocation()}, v...)...),
	}
}

func DebugAsync(v ...interface{}) {
	logAsync(constants.DEBUG, v...)
}

func InfoAsync(v ...interface{}) {
	logAsync(constants.INFO, v...)
}

func WarningAsync(v ...interface{}) {
	logAsync(constants.WARNING, v...)
}

func ErrorAsync(v ...interface{}) {
	logAsync(constants.ERROR, v...)
}

func Close() {
	close(closeCh)
	<-flushCh
}
