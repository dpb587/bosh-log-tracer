package parser

import (
	"regexp"
	"strconv"
	"time"

	"github.com/dpb587/bosh-log-tracer/log"
	"github.com/dpb587/bosh-log-tracer/log/taskdebug"
)

var SequelParser = sequelParser{}

type sequelParser struct{}

// (0.000175s) (conn: 47432699065800) SELECT * FROM "tasks" WHERE "id" = 50995
var sequelOneRE = regexp.MustCompile(`^\(([\d\.]+)s\)\s\(conn:\s(\d+)\)\s(.+)$`)

func (p sequelParser) Parse(inU log.Line) (log.Line, error) {
	in, ok := inU.(taskdebug.RawMessage)
	if !ok {
		return inU, nil
	}

	if in.Component != "DirectorJobRunner" {
		return inU, nil
	}

	if m := sequelOneRE.FindStringSubmatch(in.Message); len(m) > 0 {
		msg := taskdebug.SequelMessage{
			RawMessage: in,
			Connection: m[2],
			Query:      m[3],
		}

		if res, err := strconv.ParseFloat(m[1], 64); err == nil {
			msg.Duration = time.Duration(int64(res * 1000000))
		}

		return msg, nil
	}

	return inU, nil
}
