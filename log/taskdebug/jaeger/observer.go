package jaeger

import (
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/dpb587/bosh-log-tracer/log"
	"github.com/dpb587/bosh-log-tracer/log/taskdebug"
	"github.com/dpb587/bosh-log-tracer/observer"
	"github.com/dpb587/bosh-log-tracer/observer/context"

	opentracing "github.com/opentracing/opentracing-go"
	opentracinglog "github.com/opentracing/opentracing-go/log"
	jaeger "github.com/uber/jaeger-client-go"
	jaegercfg "github.com/uber/jaeger-client-go/config"
)

type tracer struct {
	t opentracing.Tracer
	c io.Closer
}

type Observer struct {
	ctx     *context.Context
	tracers map[string]tracer

	rootSpan               opentracing.Span
	lastMessage            taskdebug.RawMessage
	emulatedStage          string
	updatingInstanceGroups []string

	includeLogReferences bool
}

type ObserverOptions struct {
	IncludeLogReferences bool
}

var _ observer.Observer = &Observer{}

func NewObserver(ctx *context.Context, o ObserverOptions) *Observer {
	return &Observer{
		ctx:                  ctx,
		tracers:              map[string]tracer{},
		includeLogReferences: o.IncludeLogReferences,
	}
}

func (l *Observer) getTracer(service string) opentracing.Tracer {
	tracerTuple, exists := l.tracers[service]
	if !exists {
		t, c, err := jaegercfg.Configuration{
			ServiceName: service,
			Sampler: &jaegercfg.SamplerConfig{
				Type:  jaeger.SamplerTypeConst,
				Param: 1,
			},
			Reporter: &jaegercfg.ReporterConfig{
				LogSpans: true,
			},
		}.NewTracer()
		if err != nil {
			panic(err)
		}

		tracerTuple = tracer{
			t: t,
			c: c,
		}

		l.tracers[service] = tracerTuple
	}

	return tracerTuple.t
}

func (l *Observer) Begin() error {
	return nil
}

func (l *Observer) Commit() error {
	for _, group := range l.updatingInstanceGroups {
		ctx := l.ctx.Open(
			context.Annotation{Key: "updater", Value: "instance_group"},
			context.Annotation{Key: "updater.instance_group", Value: group},
		)

		lastMessage, ok := ctx.Get("last_message")
		if !ok {
			continue // because of no-op instance groups?
			panic("logical inconsistnecy: expected instance group last message")
		}

		igspU, ok := ctx.Get("tracing.span")
		if !ok {
			panic("logical inconsistency: expected instance group start span")
		}

		igspU.(opentracing.Span).FinishWithOptions(opentracing.FinishOptions{FinishTime: lastMessage.(taskdebug.NATSMessageMessage).LogTime})
	}

	if l.rootSpan != nil {
		l.endEmulatedStage(l.lastMessage)
		l.rootSpan.FinishWithOptions(
			opentracing.FinishOptions{FinishTime: l.lastMessage.LogTime},
		)
	}

	for _, tracer := range l.tracers {
		err := tracer.c.Close()
		if err != nil {
			panic(err)
		}
	}

	if l.rootSpan != nil {
		fmt.Printf("http://localhost:16686/trace/%s\n", l.rootSpan.Context().(jaeger.SpanContext).TraceID())
	}

	return nil
}

func (l *Observer) Handle(msg log.Line) error {
	if v, ok := msg.(taskdebug.RawMessage); ok {
		// for closing our final span at the end
		l.lastMessage = v
	}

	switch m := msg.(type) {
	case taskdebug.ProcessMessage:
		return l.process(m)

	case taskdebug.SequelMessage:
		// shouldn't these be redacted?

		if m.Tags["action"] == "compile_package" && strings.Contains(m.Query, `INSERT INTO "events" `) && strings.Contains(m.Query, `'create', 'instance',`) {
			// hacky hacky correlate the future vm name to a package
			// the create_vm calls do not have any package-specific details
			// TODO mysql; or just a better way
			instance := strings.SplitN(strings.SplitN(m.Query, `'create', 'instance', '`, 2)[1], `', `, 2)[0]

			// should already exist
			ctx := l.ctx.Open(
				context.Annotation{Key: "compilation.package", Value: m.Tags["package"]},
				context.Annotation{Key: "compilation.stemcell", Value: m.Tags["stemcell"]},
			)
			ctx.AddAnnotation(context.Annotation{Key: "expected_compilation_instance", Value: instance})
		}

		// noisy; ignore for now
		// return l.sequel(m)

	case taskdebug.NATSMessageSentAgentMessage:
		return l.natsSentAgent(m)
	case taskdebug.NATSMessageMessage:
		if m.Event == "RECEIVED" {
			return l.natsReceived(m)
		} else if m.Event == "SENT" && strings.HasPrefix(m.Channel, "hm.") {
			return l.natsSentHM(m)
		}

	case taskdebug.ExternalCPIRequestMessage:
		return l.externalCPIRequest(m)
	case taskdebug.ExternalCPIMessage:
		if m.Event == "response" {
			return l.externalCPIResponse(m)
		}

	case taskdebug.CPIAWSRPCMessage:
		return l.cpiAWSRPC(m)

	case taskdebug.LockMessage:
		return l.lock(m)

	case taskdebug.InstanceAspectChangedMessage:
		return l.instanceAspectChanged(m)

	case taskdebug.RawMessage:
		if m.Message == "Creating job" {
			return l.creatingJob(m)
		} else if m.Message == "Creating deployment plan" {
			return l.startEmulatedStage(m, "preparing")
		} else if m.Message == "Generating a list of compile tasks" {
			return l.startEmulatedStage(m, "compilation")
		} else if m.Message == "Updating deployment" {
			return l.startEmulatedStage(m, "updating")
		} else if m.Message == "Finished updating deployment" {
			// seems this is actually logged twice
			if l.emulatedStage == "updating" {
				_ = l.endEmulatedStage(m)
				return l.startEmulatedStage(m, "finishing") // kinda implied
			}
		} else if strings.HasPrefix(m.Message, "Compiling package '") {
			// msg.Tags["action"] == "compile_package"
			return l.startPackageCompilation(m)
		} else if strings.HasPrefix(m.Message, "Finished compiling package '") {
			// msg.Tags["action"] == "compile_package"
			return l.finishPackageCompilation(m)
		} else if strings.HasPrefix(m.Message, "Updating instance ") {
			return l.startUpdateInstance(m)
		} else if strings.HasPrefix(m.Message, "Creating missing VM") {
			return l.startCreateInstance(m)
		} else if strings.HasPrefix(m.Message, "Agenda step Bosh::Director::DeploymentPlan::Steps::RenderInstanceJobTemplatesStep finished") {
			// TODO formal agenda step
			return l.finishCreateInstance(m)
		}
	}

	return nil
}

func (l *Observer) instanceAspectChanged(msg taskdebug.InstanceAspectChangedMessage) error {
	ctx := l.ctx.Open(
		context.Annotation{Key: "aggregation", Value: "instance_aspect_changed"},
		context.Annotation{Key: "instance_group", Value: msg.InstanceGroup},
		context.Annotation{Key: "instance_id", Value: msg.InstanceID},
	)

	// should only ever be one per aspect
	// weirdly, seems like stemcell_changed? can appear multiple times
	ctx.Set(msg.Aspect, msg)

	return nil
}

func (l *Observer) startUpdateInstance(msg taskdebug.RawMessage) error {
	var igsp, sp opentracing.Span

	ctx := l.ctx.Open(
		context.Annotation{Key: "updater", Value: "instance_group"},
		context.Annotation{Key: "updater.instance_group", Value: msg.Tags["instance_group"]},
	)
	igspU, ok := ctx.Get("tracing.span")
	if !ok {
		igsp = l.getTracer("updater").StartSpan(
			fmt.Sprintf("group: %s", msg.Tags["instance_group"]),
			opentracing.StartTime(msg.LogTime),
			opentracing.ChildOf(l.findParentSpan().Context()),
			opentracing.Tag{Key: "instance_group", Value: msg.Tags["instance_group"]},
		)

		ctx.Set("tracing.span", igsp)

		l.updatingInstanceGroups = append(l.updatingInstanceGroups, msg.Tags["instance_group"])
	} else {
		igsp = igspU.(opentracing.Span)
	}

	ctx = l.ctx.Open(
		context.Annotation{Key: "updater", Value: "instance_id"},
		context.Annotation{Key: "updater.instance_group", Value: msg.Tags["instance_group"]},
		context.Annotation{Key: "updater.instance_id", Value: msg.Tags["instance_id"]},
	)
	spU, ok := ctx.Get("tracing.span")
	if !ok {
		sp = l.getTracer("updater").StartSpan(
			fmt.Sprintf("id: %s", msg.Tags["instance_id"]),
			opentracing.StartTime(msg.LogTime),
			opentracing.ChildOf(igsp.Context()),
			opentracing.Tag{Key: "instance_group", Value: msg.Tags["instance_group"]},
			opentracing.Tag{Key: "instance_id", Value: msg.Tags["instance_id"]},
		)

		l.addSpanLogReference(sp, "start", msg)

		aspectsCtx := l.ctx.Open(
			context.Annotation{Key: "aggregation", Value: "instance_aspect_changed"},
			context.Annotation{Key: "instance_group", Value: msg.Tags["instance_group"]},
			context.Annotation{Key: "instance_id", Value: msg.Tags["instance_id"]},
		)

		aspectChanges := aspectsCtx.Keys()
		sort.Strings(aspectChanges)

		for _, k := range aspectChanges {
			msgAspectU, _ := aspectsCtx.Get(k)
			msgAspect := msgAspectU.(taskdebug.InstanceAspectChangedMessage)

			for k, v := range msgAspect.GetChangedFromTags() {
				sp.SetTag(strings.TrimSuffix(fmt.Sprintf("updater.change.%s.old.%s", msgAspect.Aspect, k), "."), v)
			}

			for k, v := range msgAspect.GetChangedToTags() {
				sp.SetTag(strings.TrimSuffix(fmt.Sprintf("updater.change.%s.new.%s", msgAspect.Aspect, k), "."), v)
			}

			if msgAspect.Aspect == "packages" {
				sp.SetTag("updater.change.packages", msgAspect.GetChangedPackages())
			}

			l.addSpanLogReference(sp, "changed", msgAspect)
		}

		if len(aspectChanges) > 0 {
			sp.SetTag("updater.changes", aspectChanges)
		}

		ctx.Set("tracing.span", sp)
	} else {
		sp = spU.(opentracing.Span)
	}

	return nil
}

func (l *Observer) startCreateInstance(msg taskdebug.RawMessage) error {
	var sp opentracing.Span

	if l.emulatedStage != "updating" {
		// don't do this for compilations/preparations
		return nil
	}

	// if there's an updater scope, just use that
	updaterCtx := l.ctx.Open(
		context.Annotation{Key: "updater", Value: "instance_id"},
		context.Annotation{Key: "updater.instance_group", Value: msg.Tags["instance_group"]},
		context.Annotation{Key: "updater.instance_id", Value: msg.Tags["instance_id"]},
	)
	_, ok := updaterCtx.Get("tracing.span")
	if ok {
		return nil
	}

	ctx := l.ctx.Open(
		context.Annotation{Key: "creator", Value: "instance_id"},
		context.Annotation{Key: "creator.instance_group", Value: msg.Tags["instance_group"]},
		context.Annotation{Key: "creator.instance_id", Value: msg.Tags["instance_id"]},
	)
	spU, ok := ctx.Get("tracing.span")
	if !ok {
		sp = l.getTracer("creator").StartSpan(
			fmt.Sprintf("%s/%s", msg.Tags["instance_group"], msg.Tags["instance_id"]),
			opentracing.StartTime(msg.LogTime),
			opentracing.ChildOf(l.findParentSpan().Context()),
			opentracing.Tag{Key: "instance_group", Value: msg.Tags["instance_group"]},
			opentracing.Tag{Key: "instance_id", Value: msg.Tags["instance_id"]},
		)

		l.addSpanLogReference(sp, "start", msg)

		ctx.Set("tracing.span", sp)
	} else {
		sp = spU.(opentracing.Span)
	}

	return nil
}

func (l *Observer) finishCreateInstance(msg taskdebug.RawMessage) error {
	var sp opentracing.Span

	ctx := l.ctx.Open(
		context.Annotation{Key: "creator", Value: "instance_id"},
		context.Annotation{Key: "creator.instance_group", Value: msg.Tags["instance_group"]},
		context.Annotation{Key: "creator.instance_id", Value: msg.Tags["instance_id"]},
	)
	spU, ok := ctx.Get("tracing.span")
	if !ok {
		return nil
	}

	sp = spU.(opentracing.Span)
	sp.FinishWithOptions(opentracing.FinishOptions{FinishTime: msg.LogTime})
	l.addSpanLogReference(sp, "finish", msg)

	return nil
}

func (l *Observer) finishUpdateInstance(start taskdebug.NATSMessageSentAgentMessage, end taskdebug.NATSMessageMessage) error {
	// original sending message has the metadata we need to correlate

	ctx := l.ctx.Open(
		context.Annotation{Key: "updater", Value: "instance_id"},
		context.Annotation{Key: "updater.instance_group", Value: start.Tags["instance_group"]},
		context.Annotation{Key: "updater.instance_id", Value: start.Tags["instance_id"]},
	)
	spU, ok := ctx.Get("tracing.span")
	if !ok {
		panic("logical inconsistency: expected instance start span")
	}

	sp := spU.(opentracing.Span)
	sp.FinishWithOptions(opentracing.FinishOptions{FinishTime: end.LogTime})
	l.addSpanLogReference(sp, "finish", end)

	ctx = l.ctx.Open(
		context.Annotation{Key: "updater", Value: "instance_group"},
		context.Annotation{Key: "updater.instance_group", Value: start.Tags["instance_group"]},
	)

	ctx.Set("last_message", end)

	return nil
}

func (l *Observer) startPackageCompilation(msg taskdebug.RawMessage) error {
	sp := l.getTracer("compiler").StartSpan(
		fmt.Sprintf("compile: %s", msg.Tags["package_name"]),
		opentracing.StartTime(msg.LogTime),
		opentracing.ChildOf(l.findParentSpan().Context()),
		opentracing.Tag{Key: "package_name", Value: msg.Tags["package_name"]},
		opentracing.Tag{Key: "package_fingerprint", Value: msg.Tags["package_fingerprint"]},
		opentracing.Tag{Key: "stemcell_os", Value: msg.Tags["stemcell_os"]},
		opentracing.Tag{Key: "stemcell_version", Value: msg.Tags["stemcell_version"]},
	)

	l.addSpanLogReference(sp, "start", msg)

	ctx := l.ctx.Open(
		context.Annotation{Key: "compilation.package", Value: msg.Tags["package"]},
		context.Annotation{Key: "compilation.stemcell", Value: msg.Tags["stemcell"]},
	)
	ctx.Set("tracing.span", sp)

	return nil
}

func (l *Observer) finishPackageCompilation(msg taskdebug.RawMessage) error {
	ctx := l.ctx.Open(
		context.Annotation{Key: "compilation.package", Value: msg.Tags["package"]},
		context.Annotation{Key: "compilation.stemcell", Value: msg.Tags["stemcell"]},
	)
	spU, ok := ctx.Get("tracing.span")
	if !ok {
		panic("logical inconsistency: expected original task span")
	}

	sp := spU.(opentracing.Span)
	l.addSpanLogReference(sp, "finish", msg)
	sp.FinishWithOptions(
		opentracing.FinishOptions{FinishTime: msg.LogTime},
	)

	return nil
}

func (l *Observer) getDefaultAnnotations(msg taskdebug.RawMessage) []context.Annotations {
	var res []context.Annotations

	action, ok := msg.Tags["action"]
	if ok {
		if action == "compile_package" {
			res = append(
				res,
				context.Annotations{
					{Key: "compilation.package", Value: msg.Tags["package"]},
					{Key: "compilation.stemcell", Value: msg.Tags["stemcell"]},
				},
			)
		} else if action == "canary_update" || action == "instance_update" || action == "create_missing_vm" {
			ig, ok1 := msg.Tags["instance_group"]
			igid, ok2 := msg.Tags["instance_id"]
			if ok1 && ok2 {
				res = append(
					res,
					context.Annotations{
						{Key: "updater", Value: "instance_id"},
						{Key: "updater.instance_group", Value: ig},
						{Key: "updater.instance_id", Value: igid},
					},
				)

				if strings.HasPrefix(ig, "compilation-") {
					res = append(
						res,
						context.Annotations{
							{Key: "expected_compilation_instance", Value: fmt.Sprintf("%s/%s", ig, igid)},
						},
					)
				}

				res = append(
					res,
					context.Annotations{
						{Key: "creator", Value: "instance_id"},
						{Key: "creator.instance_group", Value: ig},
						{Key: "creator.instance_id", Value: igid},
					},
				)
			}
		}
	}

	return res
}

func (l *Observer) findParentSpan(priorities ...context.Annotations) opentracing.Span {
	merged := priorities

	if l.emulatedStage != "" {
		merged = append(
			merged,
			context.Annotations{
				{
					Key:   "emulated_stage",
					Value: l.emulatedStage,
				},
			},
		)
	}

	for _, annotations := range merged {
		scope := l.ctx.Find(annotations...)
		if scope != nil {
			span, ok := scope.Get("tracing.span")
			if !ok {
				// error?
				continue
			}

			return span.(opentracing.Span)
		}
	}

	return l.rootSpan
}

func (l *Observer) startEmulatedStage(msg taskdebug.RawMessage, stage string) error {
	err := l.endEmulatedStage(msg)
	if err != nil {
		panic(err)
	}

	l.emulatedStage = stage

	sp := l.getTracer("stage").StartSpan(
		stage,
		opentracing.ChildOf(l.rootSpan.Context()),
		opentracing.StartTime(msg.LogTime),
	)
	l.addSpanLogReference(sp, "start", msg)

	ctx := l.ctx.Open(context.Annotation{Key: "emulated_stage", Value: stage})
	ctx.Set("tracing.span", sp)

	return nil
}

func (l *Observer) endEmulatedStage(msg taskdebug.RawMessage) error {
	if l.emulatedStage == "" {
		return nil
	}

	ctx := l.ctx.Open(context.Annotation{Key: "emulated_stage", Value: l.emulatedStage})
	spU, ok := ctx.Get("tracing.span")
	if !ok {
		panic("logical inconsistency: expected original task span")
	}

	sp := spU.(opentracing.Span)
	l.addSpanLogReference(sp, "start", msg)
	sp.FinishWithOptions(
		opentracing.FinishOptions{FinishTime: msg.LogTime},
	)

	l.emulatedStage = ""

	return nil
}

func (l *Observer) process(msg taskdebug.ProcessMessage) error {
	sp := l.getTracer("worker").StartSpan(
		"update_deployment",
		opentracing.StartTime(msg.LogTime),
		opentracing.Tag{Key: "ref", Value: "thirteen"}, // dev search correlation
		opentracing.Tag{Key: "director.worker", Value: msg.WorkerName},
		opentracing.Tag{Key: "director.instance.name", Value: msg.InstanceName},
		opentracing.Tag{Key: "director.instance.id", Value: msg.InstanceID},
		opentracing.Tag{Key: "host.ip", Value: msg.IP},
	)
	l.addSpanLogReference(sp, "start", msg)

	l.rootSpan = sp

	return nil
}

func (l *Observer) creatingJob(msg taskdebug.RawMessage) error {
	if l.rootSpan == nil {
		panic("logical inconsistency: expected root span by this time")
	}

	l.rootSpan.SetTag("task", msg.Tags["task"])

	return nil
}

func (l *Observer) sequel(msg taskdebug.SequelMessage) error {
	if l.rootSpan == nil {
		// debug queries show up before the startup "process" message
		return nil
	}

	operation := strings.SplitN(msg.Query, " ", 2)[0]
	if operation == "BEGIN" || operation == "COMMIT" {
		// ignore these for simplicity for now
		// TODO consider a transaction span
		return nil
	}

	sp := l.getTracer("db").StartSpan(
		operation,
		opentracing.ChildOf(l.findParentSpan(l.getDefaultAnnotations(msg.RawMessage)...).Context()),
		opentracing.StartTime(msg.LogTime.Add(-1*msg.Duration)),
	)
	l.addSpanLogReference(sp, "start", msg)

	l.addSpanLogReference(sp, "finish", msg)
	sp.FinishWithOptions(
		opentracing.FinishOptions{FinishTime: msg.LogTime},
	)

	return nil
}

func (l *Observer) natsSentHM(msg taskdebug.NATSMessageMessage) error {
	var parentSpan opentracing.Span = l.findParentSpan(l.getDefaultAnnotations(msg.RawMessage)...)

	sp := l.getTracer("nats").StartSpan(
		fmt.Sprintf("hm: %s", strings.TrimPrefix(msg.Channel, "hm.director.")),
		opentracing.StartTime(msg.LogTime),
		opentracing.ChildOf(parentSpan.Context()),
	)
	l.addSpanLogReference(sp, "start", msg)

	// no response expected, so finish immediately
	l.addSpanLogReference(sp, "finish", msg)
	sp.FinishWithOptions(
		opentracing.FinishOptions{FinishTime: msg.LogTime},
	)

	return nil
}

func (l *Observer) natsSentAgent(msg taskdebug.NATSMessageSentAgentMessage) error {
	var parentSpan opentracing.Span = l.findParentSpan(l.getDefaultAnnotations(msg.RawMessage)...)

	if msg.PayloadMethod == "get_task" {
		ctx := l.ctx.Open(context.Annotation{Key: "agent.task_id", Value: msg.GetArgument0String()})
		res, ok := ctx.Get("tracing.span")
		if !ok {
			panic("logical inconsistency: expected original task span")
		}

		parentSpan = res.(opentracing.Span)
	} else if msg.PayloadMethod == "ping" {
		// ping is like other methods, except it only ever happens once and involves
		// spamming multiple SENTs; only span one outer set
		// this is weird tracing because those other ping attempts are not really
		// surfaced so they're just dangling spans; might be better to show the
		// attempt as a 0ms [failed] span?

		ctx := l.ctx.Open(
			context.Annotation{Key: "agent.id", Value: msg.AgentID},
			context.Annotation{Key: "agent.method", Value: msg.PayloadMethod},
		)
		if sp, started := ctx.Get("tracing.span"); started {
			parentSpan = sp.(opentracing.Span)
		} else {
			sp := l.getTracer("nats").StartSpan(
				fmt.Sprintf("agent: %s", msg.PayloadMethod),
				opentracing.StartTime(msg.LogTime),
				opentracing.ChildOf(parentSpan.Context()),
				opentracing.Tag{Key: "nats.agent.agent_id", Value: msg.AgentID},
				opentracing.Tag{Key: "nats.agent.method", Value: msg.PayloadMethod},
			)
			l.addSpanLogReference(sp, "start", msg)

			ctx.Set("tracing.span", sp)
			ctx.Set("nats.sent", msg)

			parentSpan = sp
		}

	} else if msg.PayloadMethod != "get_state" && msg.PayloadMethod != "start" { // for all other methods which will result in subsequent get_task calls
		// outer span = method
		// inner spans = [initial message, get_state...]

		operation := msg.PayloadMethod

		if operation == "run_script" {
			operation = msg.GetArgument0String()
		}

		sp := l.getTracer("nats").StartSpan(
			fmt.Sprintf("agent: %s", operation),
			opentracing.StartTime(msg.LogTime),
			opentracing.ChildOf(parentSpan.Context()),
			opentracing.Tag{Key: "nats.agent.agent_id", Value: msg.AgentID},
			opentracing.Tag{Key: "nats.agent.method", Value: msg.PayloadMethod},
		)
		l.addSpanLogReference(sp, "start", msg)

		ctx := l.ctx.Open(
			context.Annotation{Key: "agent.id", Value: msg.AgentID},
			context.Annotation{Key: "agent.method", Value: msg.PayloadMethod},
			context.Annotation{Key: "agent.pending_task_id", Value: msg.PayloadReplyTo},
		)
		ctx.Set("tracing.span", sp)
		ctx.Set("nats.sent", msg)

		parentSpan = sp
	}

	sp := l.getTracer("nats").StartSpan(
		fmt.Sprintf("agent: %s", msg.PayloadMethod),
		opentracing.StartTime(msg.LogTime),
		opentracing.ChildOf(parentSpan.Context()),
		opentracing.Tag{Key: "nats.agent.agent_id", Value: msg.AgentID},
		opentracing.Tag{Key: "nats.agent.method", Value: msg.PayloadMethod},
	)
	l.addSpanLogReference(sp, "start", msg)

	ctx := l.ctx.Open(context.Annotation{Key: "nats.reply_to", Value: msg.PayloadReplyTo})
	ctx.Set("tracing.span", sp)
	ctx.Set("nats.sent", msg)

	return nil
}

func (l *Observer) natsReceived(msg taskdebug.NATSMessageMessage) error {
	scope := l.ctx.Open(context.Annotation{Key: "nats.reply_to", Value: msg.Channel})
	spU, ok := scope.Get("tracing.span")
	if !ok {
		panic("logical inconsistency: expected sent message span")
	}

	sp := spU.(opentracing.Span)
	l.addSpanLogReference(sp, "finish", msg)
	sp.FinishWithOptions(opentracing.FinishOptions{FinishTime: msg.LogTime})

	sentMsgU, ok := scope.Get("nats.sent")
	if !ok {
		panic("logical inconsistency: expected sent message")
	}

	sentMsg := sentMsgU.(taskdebug.NATSMessageSentAgentMessage)

	switch sentMsg.PayloadMethod {
	case "get_task", "ping":
		if msg.GetReceivedState() != "running" {
			// close the outer task span
			var findAnnotations context.Annotations

			if sentMsg.PayloadMethod == "ping" {
				findAnnotations = context.Annotations{
					{Key: "agent.id", Value: sentMsg.AgentID},
					{Key: "agent.method", Value: sentMsg.PayloadMethod},
				}
			} else {
				findAnnotations = context.Annotations{
					{Key: "agent.task_id", Value: sentMsg.GetArgument0String()},
				}
			}

			ctx := l.ctx.Open(findAnnotations...)
			spU, ok := ctx.Get("tracing.span")
			if !ok {
				panic("logical inconsistency: expected original task span")
			}

			sp := spU.(opentracing.Span)
			l.addSpanLogReference(sp, "finish", msg)
			sp.FinishWithOptions(opentracing.FinishOptions{FinishTime: msg.LogTime})

			{ // cheat and assume this is the last step of updating an instance
				taskMsgU, ok := ctx.Get("nats.sent")
				if !ok {
					panic("logical inconsistency: expected original message")
				}

				taskMsg := taskMsgU.(taskdebug.NATSMessageSentAgentMessage)

				if taskMsg.PayloadMethod == "run_script" && taskMsg.GetArgument0String() == "post-start" {
					l.finishUpdateInstance(taskMsg, msg)
				}
			}
		}
	case "get_state", "start":
		// nop
	default:
		// it should have come back with a task id that we want to annotate for subsequent calls
		scope := l.ctx.Open(context.Annotation{Key: "agent.pending_task_id", Value: msg.Channel})
		scope.AddAnnotation(context.Annotation{Key: "agent.task_id", Value: msg.GetReceivedTaskID()})

		spU, ok := scope.Get("tracing.span")
		if !ok {
			panic("logical inconsistency: expected wrapping task span")
		}

		spU.(opentracing.Span).SetTag("nats.agent.task_id", msg.GetReceivedTaskID())
	}

	return nil
}

func (l *Observer) externalCPIRequest(msg taskdebug.ExternalCPIRequestMessage) error {
	sp := l.getTracer("cpi").StartSpan(
		msg.PayloadMethod,
		opentracing.StartTime(msg.LogTime),
		opentracing.ChildOf(l.findParentSpan(l.getDefaultAnnotations(msg.RawMessage)...).Context()),
		opentracing.Tag{Key: "cpi.method", Value: msg.PayloadMethod},
		opentracing.Tag{Key: "cpi.exec", Value: msg.Command},
	)
	l.addSpanLogReference(sp, "start", msg)

	ctx := l.ctx.Open(context.Annotation{Key: "external_cpi.correlation", Value: msg.Correlation})
	ctx.Set("tracing.span", sp)

	return nil
}

func (l *Observer) externalCPIResponse(msg taskdebug.ExternalCPIMessage) error {
	scope := l.ctx.Open(context.Annotation{Key: "external_cpi.correlation", Value: msg.Correlation})
	spU, ok := scope.Get("tracing.span")
	if !ok {
		panic("logical inconsistency: expected sent message span")
	}

	sp := spU.(opentracing.Span)
	l.addSpanLogReference(sp, "finish", msg)
	sp.FinishWithOptions(opentracing.FinishOptions{FinishTime: msg.LogTime})

	return nil
}

func (l *Observer) cpiAWSRPC(msg taskdebug.CPIAWSRPCMessage) error {
	sp := l.getTracer("aws").StartSpan(
		msg.PayloadMethod,
		opentracing.StartTime(msg.LogTime.Add(-1*msg.Duration)),
		opentracing.ChildOf(l.findParentSpan(context.Annotations{{Key: "external_cpi.correlation", Value: msg.Correlation}}).Context()),
		opentracing.Tag{Key: "aws.method", Value: msg.PayloadMethod},
		opentracing.Tag{Key: "http.status_code", Value: msg.StatusCode},
		opentracing.Tag{Key: "aws.retries", Value: msg.Retries},
	)
	l.addSpanLogReference(sp, "finish", msg)
	sp.FinishWithOptions(opentracing.FinishOptions{FinishTime: msg.LogTime})

	return nil
}

var lockOperationMap = map[string]string{
	"Acquiring": "acquire",
	"Acquired":  "acquired",
	"Renewing":  "renew",
	"Deleted":   "delete",
}

func (l *Observer) lock(msg taskdebug.LockMessage) error {
	if msg.Event == "Acquiring" {
		sp := l.getTracer("lock").StartSpan(
			strings.TrimPrefix(msg.Name, "lock:"),
			opentracing.StartTime(msg.LogTime),
			opentracing.ChildOf(l.findParentSpan(l.getDefaultAnnotations(msg.RawMessage)...).Context()),
		)
		l.addSpanLogReference(sp, "start", msg)

		ctx := l.ctx.Open(context.Annotation{Key: "lock.name", Value: msg.Name})
		ctx.Set("tracing.span", sp)

		return nil
	} else if msg.Event == "Acquired" || msg.Event == "Renewing" {
		// not actually a span?
		sp := l.getTracer("lock").StartSpan(
			lockOperationMap[msg.Event],
			opentracing.StartTime(msg.LogTime),
			opentracing.ChildOf(l.findParentSpan(context.Annotations{{Key: "lock.name", Value: msg.Name}}).Context()),
		)
		l.addSpanLogReference(sp, "finish", msg)
		sp.FinishWithOptions(opentracing.FinishOptions{FinishTime: msg.LogTime})
	} else if msg.Event == "Deleted" {
		scope := l.ctx.Open(context.Annotation{Key: "lock.name", Value: msg.Name})
		parentSpanU, ok := scope.Get("tracing.span")
		if !ok {
			panic("logical inconsistency: expected sent message span")
		}

		parentSpan := parentSpanU.(opentracing.Span)

		// not actually a span?
		sp := l.getTracer("lock").StartSpan(
			lockOperationMap[msg.Event],
			opentracing.StartTime(msg.LogTime),
			opentracing.ChildOf(parentSpan.Context()),
		)
		l.addSpanLogReference(sp, "finish", msg)
		sp.FinishWithOptions(opentracing.FinishOptions{FinishTime: msg.LogTime})

		l.addSpanLogReference(parentSpan, "finish", msg)
		parentSpan.FinishWithOptions(opentracing.FinishOptions{FinishTime: msg.LogTime})
	} else {
		panic(fmt.Sprintf("logical inconsistency: unexpected lock event: %s", msg.Event))
	}

	return nil
}

func (l *Observer) addSpanLogReference(sp opentracing.Span, event string, msg log.Line) {
	if !l.includeLogReferences {
		return
	}

	sp.LogFields(
		opentracinglog.String("event", event),
		opentracinglog.Int64("line", msg.LineOffset()),
		opentracinglog.String("message", msg.LineData()),
	)
}
