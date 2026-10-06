// Package heaporderbook is a backup/reference package.
//
// It contains the original heap/map-based order book (orderbook.go) and its data
// structures (struct.go) as they existed before the ladder-based, integer-priced
// matcher replaced them. Those two files carry a `//go:build ignore` tag so they are
// not compiled as part of the module; this file keeps the directory a valid Go package
// so `go build ./...` does not fail on it.
package heaporderbook
