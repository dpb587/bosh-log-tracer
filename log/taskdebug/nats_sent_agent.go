package taskdebug

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/dpb587/bosh-log-tracer/log"
)

var NATSMessageSentAgentParser = natsMessageSentAgentParser{}

type natsMessageSentAgentParser struct{}

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

func (p natsMessageSentAgentParser) Parse(inU log.Line) (log.Line, error) {
	inU, err := NATSMessageParser.Parse(inU)
	if inU == nil || err != nil {
		return inU, err
	}

	in, ok := inU.(NATSMessageMessage)
	if !ok {
		return inU, nil
	}

	if !strings.HasPrefix(in.Channel, "agent.") {
		return inU, nil
	}

	out := NATSMessageSentAgentMessage{
		NATSMessageMessage: in,
		AgentID:            strings.TrimPrefix(in.Channel, "agent."),
	}

	var payload struct {
		Protocol int    `json:"protocol"`
		Method   string `json:"method"`
		ReplyTo  string `json:"reply_to"`
	}

	err = json.Unmarshal([]byte(out.Payload), &payload)
	if err != nil {
		panic(err)
	}

	out.PayloadProtocol = payload.Protocol
	out.PayloadMethod = payload.Method
	out.PayloadReplyTo = payload.ReplyTo

	return out, nil
}
