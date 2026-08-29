// Command proto-roundtrip-go decodes a length-delimited SchedulingIntent and
// PlacementPlan from stdin and writes both deterministic payloads back. The
// cross-language test owns fixture construction and semantic assertions.
package main

import (
	"encoding/binary"
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
	intentWire, planWire, err := splitFrames(wire)
	if err != nil {
		fail(err)
	}
	intent := &tgsrlv1.SchedulingIntent{}
	if err := proto.Unmarshal(intentWire, intent); err != nil {
		fail(err)
	}
	if intent.GetExecutionId() == "" || intent.GetStageId() == "" || intent.GetVersion() == 0 {
		fail(fmt.Errorf("decoded intent is missing its identity"))
	}
	plan := &tgsrlv1.PlacementPlan{}
	if err := proto.Unmarshal(planWire, plan); err != nil {
		fail(err)
	}
	if plan.GetPlanId() == "" {
		fail(fmt.Errorf("decoded plan is missing its identity"))
	}
	intentOut, err := proto.MarshalOptions{Deterministic: true}.Marshal(intent)
	if err != nil {
		fail(err)
	}
	planOut, err := proto.MarshalOptions{Deterministic: true}.Marshal(plan)
	if err != nil {
		fail(err)
	}
	out := appendFrame(nil, intentOut)
	out = appendFrame(out, planOut)
	if _, err := os.Stdout.Write(out); err != nil {
		fail(err)
	}
}

func splitFrames(wire []byte) ([]byte, []byte, error) {
	firstLength, read := binary.Uvarint(wire)
	if read <= 0 || firstLength > uint64(len(wire)-read) {
		return nil, nil, fmt.Errorf("invalid intent frame")
	}
	firstEnd := read + int(firstLength)
	secondLength, secondRead := binary.Uvarint(wire[firstEnd:])
	if secondRead <= 0 || secondLength != uint64(len(wire)-firstEnd-secondRead) {
		return nil, nil, fmt.Errorf("invalid plan frame")
	}
	return wire[read:firstEnd], wire[firstEnd+secondRead:], nil
}

func appendFrame(out, payload []byte) []byte {
	var prefix [binary.MaxVarintLen64]byte
	length := binary.PutUvarint(prefix[:], uint64(len(payload)))
	out = append(out, prefix[:length]...)
	return append(out, payload...)
}

func fail(err error) {
	_, _ = fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
