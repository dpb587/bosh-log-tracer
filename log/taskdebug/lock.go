package taskdebug

import (
	"regexp"

	"github.com/dpb587/boshdebugtracer/log"
)

var LockParser = lockParser{}

type lockParser struct{}

type LockMessage struct {
	RawMessage

	Event string
	Name  string
	UID   string
}

var _ log.Line = &LockMessage{}

// Acquiring lock: lock:deployment:concourse
// Acquired lock: lock:deployment:concourse
// Renewing lock: lock:deployment:concourse
// Deleted lock: lock:deployment:concourse uid: 3366af32-333e-453c-a73d-e2d7730071ba
var lockMessageOneRE = regexp.MustCompile(`^(Acquiring|Acquired|Renewing|Deleted) lock: ([^\s]+)( uid: (.+))?$`)

func (p lockParser) Parse(inU log.Line) (log.Line, error) {
	in, ok := inU.(RawMessage)
	if !ok {
		return inU, nil
	}

	if in.Component != "DirectorJobRunner" {
		return inU, nil
	}

	if m := lockMessageOneRE.FindStringSubmatch(in.Message); len(m) > 0 {
		msg := LockMessage{
			RawMessage: in,
			Event:      m[1],
			Name:       m[2],
			UID:        m[4],
		}

		return msg, nil
	}

	return inU, nil
}
