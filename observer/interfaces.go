package observer

import "github.com/dpb587/bosh-log-tracer/log"

type Observer interface {
	Begin() error
	Commit() error
	Handle(log.Line) error
}
