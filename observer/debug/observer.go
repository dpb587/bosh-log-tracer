package debug

import (
	"fmt"

	"github.com/dpb587/bosh-log-tracer/log"
	"github.com/dpb587/bosh-log-tracer/log/taskdebug"
	"github.com/dpb587/bosh-log-tracer/observer"
)

type Observer struct{}

var _ observer.Observer = &Observer{}

func NewObserver() *Observer {
	return &Observer{}
}

func (l *Observer) Begin() error {
	return nil
}

func (l *Observer) Commit() error {
	return nil
}

func (l *Observer) Handle(msg log.Line) error {
	switch m := msg.(type) {
	case taskdebug.InstanceAspectChangedMessage:
	case taskdebug.ExternalCPIMessage, taskdebug.ExternalCPIRequestMessage:
		return l.print(m)
	}

	return nil
}

func (l *Observer) print(msg log.Line) error {
	fmt.Printf("%#+v\n", msg)

	return nil
}
