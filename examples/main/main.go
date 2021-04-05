package main

import (
	"time"

	"github.com/birdayz/gstreams"
)

func main() {

	g := gstreams.NewStreamThread("my-group", "abc")
	g.Start()
	time.Sleep(10 * time.Minute)
}
