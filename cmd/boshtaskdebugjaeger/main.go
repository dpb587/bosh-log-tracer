package main

import (
	"bufio"
	"os"

	"github.com/dpb587/boshdebugtracer/log"
	"github.com/dpb587/boshdebugtracer/log/taskdebug"
	"github.com/dpb587/boshdebugtracer/log/taskdebug/jaeger"
	"github.com/dpb587/boshdebugtracer/observer/context"
)

func main() {
	var err error

	ctx := &context.Context{}
	parsers := taskdebug.Parser

	observer := jaeger.NewObserver(ctx, jaeger.ObserverOptions{
		IncludeLogReferences: false,
	})
	observer.Begin()
	defer observer.Commit()

	var offset int64

	scanner := bufio.NewScanner(os.Stdin)
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
