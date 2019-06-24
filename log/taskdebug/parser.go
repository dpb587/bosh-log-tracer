package taskdebug

import "github.com/dpb587/boshdebugtracer/log"

var Parser = []log.LineParser{
	RawParser,

	ProcessParser,
	SequelParser,

	NATSMessageSentAgentParser,
	NATSMessageParser,

	ExternalCPIRequestParser,
	ExternalCPIParser,

	CPIAWSRPCParser,

	LockParser,
}
