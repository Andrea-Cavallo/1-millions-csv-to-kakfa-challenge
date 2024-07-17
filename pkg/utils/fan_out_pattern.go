package utils

import (
	"sync"
)

// Il pattern Fan-Out in Go permette di distribuire il lavoro tra vari worker (consumer) usando i canali di Go.
// Vediamo come possiamo utilizzare la funzione Split che hai definito nel tuo programma principale per distribuire i task.
// Prima di tutto, possiamo definire una funzione di worker che consumerà i dati dai canali generati dalla funzione Split.
// Successivamente, possiamo impostare il canale principale, chiamare Split per ottenere i canali suddivisi
// e infine avviare i worker per elaborare i dati.
// see -> https://github.com/tmrts/go-patterns/blob/master/messaging/fan_out.md
// Split a channel into n channels that receive messages in a round-robin fashion.
func Split(ch <-chan func(), n int) []<-chan func() {

	// crea un array di canali che verranno utilizzati per distribuire i task tra i vari worker
	cs := make([]chan func(), n)
	for i := 0; i < n; i++ {
		cs[i] = make(chan func())
	}

	// Distribuisce il lavoro in modo round-robin tra i canali indicati
	// fino a quando il canale principale non viene chiuso. In tal caso,
	// chiude tutti i canali e ritorna.
	distributeToChannels := func(ch <-chan func(), cs []chan func()) {
		// Chiude ogni canale quando l'esecuzione termina.
		defer func(cs []chan func()) {
			for _, c := range cs {
				close(c)
			}
		}(cs)

		for {
			for _, c := range cs {
				select {
				case val, ok := <-ch:
					if !ok {
						return
					}
					c <- val
				}
			}
		}
	}

	go distributeToChannels(ch, cs)

	result := make([]<-chan func(), n)
	for i := 0; i < n; i++ {
		result[i] = cs[i]
	}
	return result
}

// Worker è una funzione che processa i dati da un canale

func Worker(ch <-chan func(), wg *sync.WaitGroup) {
	defer wg.Done()
	for task := range ch {
		task()
	}
}
