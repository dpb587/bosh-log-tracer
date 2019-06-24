package observer

import "github.com/dpb587/boshdebugtracer/log"

type Observer interface {
	Begin() error
	Commit() error
	Handle(log.Line) error
}
