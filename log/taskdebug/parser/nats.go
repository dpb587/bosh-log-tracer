package parser

import (
	"regexp"

	"github.com/dpb587/bosh-log-tracer/log"
	"github.com/dpb587/bosh-log-tracer/log/taskdebug"
)

var NATSMessageParser = natsMessageParser{}

type natsMessageParser struct{}

// SENT: agent.0e2a1093-0ace-4685-a361-a6f40a11f7ed {"protocol":3,"method":"get_state",...
var natsMessageOneRE = regexp.MustCompile(`^(SENT|RECEIVED): ([^ ]+) (.+)$`)

func (p natsMessageParser) Parse(inU log.Line) (log.Line, error) {
	in, ok := inU.(taskdebug.RawMessage)
	if !ok {
		return inU, nil
	}

	if in.Component != "DirectorJobRunner" {
		return inU, nil
	}

	if m := natsMessageOneRE.FindStringSubmatch(in.Message); len(m) > 0 {
		out := taskdebug.NATSMessageMessage{
			RawMessage: in,
			Event:      m[1],
			Channel:    m[2],
			Payload:    m[3],
		}

		return out, nil
	}

	return inU, nil
}
