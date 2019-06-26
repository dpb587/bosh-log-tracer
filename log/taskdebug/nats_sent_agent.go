package taskdebug

import (
	"encoding/json"
	"fmt"

	"github.com/dpb587/bosh-log-tracer/log"
)

type NATSMessageSentAgentMessage struct {
	NATSMessageMessage

	AgentID         string
	PayloadProtocol int
	PayloadMethod   string
	PayloadReplyTo  string
}

var _ log.Line = &NATSMessageSentAgentMessage{}

func (m NATSMessageSentAgentMessage) GetArgument0String() string {
	var payload struct {
		Arguments []interface{} `json:"arguments"`
	}

	err := json.Unmarshal([]byte(m.Payload), &payload)
	if err != nil {
		fmt.Println(m.Payload)
		panic(err)
	}

	if len(payload.Arguments) < 1 {
		panic("logical inconsistency: expected argument")
	}

	return payload.Arguments[0].(string)
}
