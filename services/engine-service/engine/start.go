package engine

import (
	// "fmt" - the only print in this file was a debug banner, now commented out
	"log"
)

func start() {
	engine := InitOrderEngine()
	// fmt.Println("Starting the engine")
	log.Println("Started the cancellation processing")
	go engine.ProcessCancellationQueue()
	log.Println("starting the order Processing ")
	engine.ProcessOrderQueue()
}
