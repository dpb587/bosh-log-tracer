package parser

import (
	"encoding/json"
	"strings"

	"github.com/dpb587/bosh-log-tracer/log"
	"github.com/dpb587/bosh-log-tracer/log/taskdebug"
)

var NATSMessageSentAgentParser = natsMessageSentAgentParser{}

type natsMessageSentAgentParser struct{}

func (p natsMessageSentAgentParser) Parse(inU log.Line) (log.Line, error) {
	inU, err := NATSMessageParser.Parse(inU)
	if inU == nil || err != nil {
		return inU, err
	}

	in, ok := inU.(taskdebug.NATSMessageMessage)
	if !ok {
		return inU, nil
	}

	if !strings.HasPrefix(in.Channel, "agent.") {
		return inU, nil
	}

	out := taskdebug.NATSMessageSentAgentMessage{
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
