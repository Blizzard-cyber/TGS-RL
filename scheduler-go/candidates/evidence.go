package candidates

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strconv"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
)

type evidenceEntry struct {
	candidate *tgsrlv1.PlacementCandidate
	rejection *tgsrlv1.CandidateRejection
	unitID    string
	deviceID  string
	selected  bool
}

type evidenceCollector struct {
	budget     int
	entries    []evidenceEntry
	candidates map[*tgsrlv1.PlacementCandidate]int
}

func newEvidenceCollector(budget, initialCapacity int) *evidenceCollector {
	return &evidenceCollector{
		budget:     budget,
		entries:    make([]evidenceEntry, 0, initialCapacity),
		candidates: make(map[*tgsrlv1.PlacementCandidate]int, initialCapacity),
	}
}

func (c *evidenceCollector) hasRegularCapacity() bool {
	return len(c.entries) < c.budget
}

func (c *evidenceCollector) addCandidate(option Option) {
	if option.Candidate == nil || !c.hasRegularCapacity() {
		return
	}
	if _, exists := c.candidates[option.Candidate]; exists {
		return
	}
	c.entries = append(c.entries, evidenceEntry{candidate: option.Candidate, unitID: option.UnitID, deviceID: option.DeviceID})
	c.candidates[option.Candidate] = len(c.entries) - 1
}

func (c *evidenceCollector) addRejection(unitID, deviceID string, rejection *tgsrlv1.CandidateRejection) {
	if rejection == nil || !c.hasRegularCapacity() {
		return
	}
	c.entries = append(c.entries, evidenceEntry{rejection: rejection, unitID: unitID, deviceID: deviceID})
}

func (c *evidenceCollector) addSelected(option Option) {
	if option.Candidate == nil {
		return
	}
	if index, exists := c.candidates[option.Candidate]; exists {
		c.entries[index].selected = true
		return
	}
	entry := evidenceEntry{candidate: option.Candidate, unitID: option.UnitID, deviceID: option.DeviceID, selected: true}
	if c.hasRegularCapacity() {
		c.entries = append(c.entries, entry)
		c.candidates[option.Candidate] = len(c.entries) - 1
		return
	}
	for index := len(c.entries) - 1; index >= 0; index-- {
		if !c.entries[index].selected {
			delete(c.candidates, c.entries[index].candidate)
			c.entries[index] = entry
			c.candidates[option.Candidate] = index
			return
		}
	}
	// All retained evidence is selected. Growing here makes the effective
	// budget max(configured budget, selected candidate count).
	c.entries = append(c.entries, entry)
	c.candidates[option.Candidate] = len(c.entries) - 1
}

func (c *evidenceCollector) project() ([]*tgsrlv1.PlacementCandidate, []*tgsrlv1.CandidateRejection) {
	candidateEntries := make([]evidenceEntry, 0, len(c.entries))
	rejectionEntries := make([]evidenceEntry, 0, len(c.entries))
	for _, entry := range c.entries {
		if entry.candidate != nil {
			candidateEntries = append(candidateEntries, entry)
		} else if entry.rejection != nil {
			rejectionEntries = append(rejectionEntries, entry)
		}
	}
	sort.SliceStable(candidateEntries, func(i, j int) bool {
		left, right := candidateEntries[i], candidateEntries[j]
		return compareRank(
			left.candidate.GetScore(), left.deviceID, left.unitID, left.candidate.GetCandidateId(),
			right.candidate.GetScore(), right.deviceID, right.unitID, right.candidate.GetCandidateId(),
		) < 0
	})
	sort.SliceStable(rejectionEntries, func(i, j int) bool {
		left, right := rejectionEntries[i], rejectionEntries[j]
		if left.unitID != right.unitID {
			return left.unitID < right.unitID
		}
		if left.deviceID != right.deviceID {
			return left.deviceID < right.deviceID
		}
		if left.rejection.GetCandidateId() != right.rejection.GetCandidateId() {
			return left.rejection.GetCandidateId() < right.rejection.GetCandidateId()
		}
		if left.rejection.GetReason() != right.rejection.GetReason() {
			return left.rejection.GetReason() < right.rejection.GetReason()
		}
		return left.rejection.GetDetail() < right.rejection.GetDetail()
	})

	candidates := make([]*tgsrlv1.PlacementCandidate, len(candidateEntries))
	for index := range candidateEntries {
		candidates[index] = candidateEntries[index].candidate
	}
	rejections := make([]*tgsrlv1.CandidateRejection, len(rejectionEntries))
	for index := range rejectionEntries {
		rejections[index] = rejectionEntries[index].rejection
	}
	return candidates, rejections
}

func compareRank(leftScore float64, leftDevice, leftUnit, leftID string, rightScore float64, rightDevice, rightUnit, rightID string) int {
	if leftScore > rightScore {
		return -1
	}
	if leftScore < rightScore {
		return 1
	}
	if leftDevice < rightDevice {
		return -1
	}
	if leftDevice > rightDevice {
		return 1
	}
	if leftUnit < rightUnit {
		return -1
	}
	if leftUnit > rightUnit {
		return 1
	}
	if leftID < rightID {
		return -1
	}
	if leftID > rightID {
		return 1
	}
	return 0
}

func defaultCandidateID(snapshot *tgsrlv1.ClusterSnapshot, intent *tgsrlv1.SchedulingIntent) CandidateIDFunc {
	revision := strconv.FormatUint(snapshot.GetRevision(), 10)
	version := strconv.FormatUint(intent.GetVersion(), 10)
	return func(unitID, deviceID string) string {
		return stableID("candidate", unitID, deviceID, revision, version)
	}
}

func stableID(prefix string, parts ...string) string {
	hash := sha256.New()
	for _, part := range parts {
		_, _ = hash.Write([]byte(strconv.Itoa(len(part))))
		_, _ = hash.Write([]byte{':'})
		_, _ = hash.Write([]byte(part))
	}
	return prefix + "-" + hex.EncodeToString(hash.Sum(nil)[:12])
}
