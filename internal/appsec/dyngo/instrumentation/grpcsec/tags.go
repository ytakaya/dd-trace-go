// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016 Datadog, Inc.

package grpcsec

import (
	"encoding/json"
	"net"

	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"
	"gopkg.in/DataDog/dd-trace-go.v1/internal/appsec/dyngo/instrumentation/httpsec"
	"gopkg.in/DataDog/dd-trace-go.v1/internal/log"

	"google.golang.org/grpc/metadata"
)

// SetSecurityEventTags sets the AppSec-specific span tags when a security event
// occurred into the service entry span.
func SetSecurityEventTags(span ddtrace.Span, events []json.RawMessage, addr net.Addr, md metadata.MD) {
	setEventSpanTags(span, events)
	var ip string
	switch actual := addr.(type) {
	case *net.UDPAddr:
		ip = actual.IP.String()
	case *net.TCPAddr:
		ip = actual.IP.String()
	}
	span.SetTag("network.client.ip", ip)
	for h, v := range httpsec.NormalizeHTTPHeaders(md) {
		span.SetTag("grpc.metadata."+h, v)
	}
}

// setEventSpanTags sets the security event span tags into the service entry span.
func setEventSpanTags(span ddtrace.Span, events []json.RawMessage) {
	// Set the appsec event span tag
	// TODO(Julio-Guerra): a future libddwaf version should return something
	//   avoiding us the following events concatenation logic which currently
	//   involves unserializing the top-level JSON arrays to concatenate them
	//   together.
	concatenated := make([]json.RawMessage, 0, len(events)) // at least len(events)
	for _, event := range events {
		// Unmarshal the top level array
		var tmp []json.RawMessage
		if err := json.Unmarshal(event, &tmp); err != nil {
			log.Error("appsec: unexpected error while unserializing the appsec event `%s`: %v", string(event), err)
			return
		}
		concatenated = append(concatenated, tmp...)
	}

	// TODO(Julio-Guerra): avoid serializing the json in the request hot path
	// eventTag is the structure to use in the `_dd.appsec.json` span tag.
	type eventTag struct {
		Triggers []json.RawMessage `json:"triggers"`
	}
	event, err := json.Marshal(eventTag{Triggers: concatenated})
	if err != nil {
		log.Error("appsec: unexpected error while serializing the appsec event span tag: %v", err)
		return
	}
	span.SetTag("_dd.appsec.json", string(event))

	// Keep this span due to the security event
	span.SetTag(ext.ManualKeep, true)
	span.SetTag("_dd.origin", "appsec")

	// Set the appsec.event tag needed by the appsec backend
	span.SetTag("appsec.event", true)
}
