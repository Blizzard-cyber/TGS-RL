package state

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func stableID(prefix string, parts ...string) string {
	hash := sha256.New()
	for _, part := range parts {
		_, _ = hash.Write([]byte(strconv.Itoa(len(part))))
		_, _ = hash.Write([]byte{':'})
		_, _ = hash.Write([]byte(part))
	}
	return prefix + "-" + hex.EncodeToString(hash.Sum(nil)[:12])
}

func snapshotID(revision uint64) string {
	return fmt.Sprintf("snapshot-%d", revision)
}

func cloneSnapshot(snapshot *tgsrlv1.ClusterSnapshot) *tgsrlv1.ClusterSnapshot {
	if snapshot == nil {
		return nil
	}
	return proto.Clone(snapshot).(*tgsrlv1.ClusterSnapshot)
}

func cloneIntent(intent *tgsrlv1.SchedulingIntent) *tgsrlv1.SchedulingIntent {
	if intent == nil {
		return nil
	}
	return proto.Clone(intent).(*tgsrlv1.SchedulingIntent)
}

func clonePlan(plan *tgsrlv1.PlacementPlan) *tgsrlv1.PlacementPlan {
	if plan == nil {
		return nil
	}
	return proto.Clone(plan).(*tgsrlv1.PlacementPlan)
}

func cloneTimestamp(timestamp *timestamppb.Timestamp) *timestamppb.Timestamp {
	if timestamp == nil {
		return nil
	}
	return proto.Clone(timestamp).(*timestamppb.Timestamp)
}

func clonePendingUnit(unit *tgsrlv1.PendingUnit) *tgsrlv1.PendingUnit {
	if unit == nil {
		return nil
	}
	return proto.Clone(unit).(*tgsrlv1.PendingUnit)
}

func cloneResourceVector(vector *tgsrlv1.ResourceVector) *tgsrlv1.ResourceVector {
	if vector == nil {
		return nil
	}
	return proto.Clone(vector).(*tgsrlv1.ResourceVector)
}

func cloneCapabilitySet(capabilities *tgsrlv1.CapabilitySet) *tgsrlv1.CapabilitySet {
	if capabilities == nil {
		return nil
	}
	return proto.Clone(capabilities).(*tgsrlv1.CapabilitySet)
}

func cloneResourceVectorMap(in map[string]*tgsrlv1.ResourceVector) map[string]*tgsrlv1.ResourceVector {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]*tgsrlv1.ResourceVector, len(in))
	for key, value := range in {
		out[key] = cloneResourceVector(value)
	}
	return out
}
