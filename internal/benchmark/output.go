package benchmark

import (
	"log"
	"os"
)

// Silence redirects os.Stdout, os.Stderr and the standard logger to the null device and
// returns a function that restores them.
//
// It exists because the matching engine logs every order it processes (orderbook and
// ledger use fmt.Println/log.Println on the hot path), which floods the terminal and
// dominates any timing measured while those writes are live. Benchmarks should silence
// output around the measured region and restore it before printing the report.
func Silence() func() {
	devnull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		// Leaving the streams untouched is better than failing the benchmark over log
		// redirection.
		return func() {}
	}
	origStdout, origStderr := os.Stdout, os.Stderr
	origLogWriter := log.Writer()
	os.Stdout, os.Stderr = devnull, devnull
	log.SetOutput(devnull)

	return func() {
		os.Stdout, os.Stderr = origStdout, origStderr
		log.SetOutput(origLogWriter)
		_ = devnull.Close()
	}
}
