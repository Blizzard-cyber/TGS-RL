// Command proto-roundtrip-go decodes a SchedulingIntent from stdin and writes
// the deterministic protobuf bytes back to stdout. It is intentionally tiny:
// the cross-language test owns fixture construction and semantic assertions.
package main

import (
	"fmt"
	"io"
	"os"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"google.golang.org/protobuf/proto"
)

func main() {
	wire, err := io.ReadAll(os.Stdin)
	if err != nil {
		fail(err)
	}
	intent := &tgsrlv1.SchedulingIntent{}
	if err := proto.Unmarshal(wire, intent); err != nil {
		fail(err)
	}
	if intent.GetExecutionId() == "" || intent.GetStageId() == "" || intent.GetVersion() == 0 {
		fail(fmt.Errorf("decoded intent is missing its identity"))
	}
	out, err := proto.MarshalOptions{Deterministic: true}.Marshal(intent)
	if err != nil {
		fail(err)
	}
	if _, err := os.Stdout.Write(out); err != nil {
		fail(err)
	}
}

func fail(err error) {
	_, _ = fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
