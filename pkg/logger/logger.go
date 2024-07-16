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

var Logger *log.Logger

// nota che in GO l'init viene chiamato automaticamente ogni volta che importi il pacchetto non serve richiamarlo
func init() {
	file, err := os.OpenFile(constants.LOGFILE, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatalf("Errore nell'apertura del file di log: %v", err)
	}
	Logger = log.New(io.MultiWriter(os.Stdout, file), constants.PREFIX, log.Ldate|log.Ltime|log.Lmicroseconds)
}

// Funzione per ottenere il file e la linea da cui è stato chiamato il logger, solo il nome del file e l'ID della goroutine
func logLocation() string {
	_, file, line, ok := runtime.Caller(2)
	if !ok {
		return constants.WTF
	}
	return fmt.Sprintf("[%s] [line:%d] [GoroutineID: %d] ", path.Base(file), line, goid.Get())
}

func Debug(v ...interface{}) {
	Logger.SetPrefix(constants.DEBUG)
	Logger.Println(append([]interface{}{logLocation()}, v...)...)
}

func Info(v ...interface{}) {
	Logger.SetPrefix(constants.INFO)
	Logger.Println(append([]interface{}{logLocation()}, v...)...)
}

func Warning(v ...interface{}) {
	Logger.SetPrefix(constants.WARNING)
	Logger.Println(append([]interface{}{logLocation()}, v...)...)
}

func Error(v ...interface{}) {
	Logger.SetPrefix(constants.ERROR)
	Logger.Println(append([]interface{}{logLocation()}, v...)...)
}
