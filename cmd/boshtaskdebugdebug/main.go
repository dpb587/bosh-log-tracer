package main

import (
	"bufio"
	"os"

	"github.com/dpb587/boshdebugtracer/log"
	"github.com/dpb587/boshdebugtracer/log/taskdebug"
	"github.com/dpb587/boshdebugtracer/observer/debug"
)

func main() {
	var err error

	parsers := taskdebug.Parser

	observer := debug.NewObserver()
	observer.Begin()
	defer observer.Commit()

	var offset int64

	scanner := bufio.NewScanner(os.Stdin)
	buf := make([]byte, 1024*1024)
	scanner.Buffer(buf, 1024*1024)
	for scanner.Scan() {
		offset += 1

		var l log.Line = log.RawLine{
			RawLineOffset: offset,
			RawLineData:   scanner.Text(),
		}

		for _, p := range parsers {
			l, err = p.Parse(l)
			if err != nil {
				panic(err)
			}
		}

		err := observer.Handle(l)
		if err != nil {
			panic(err)
		}
	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}
}
