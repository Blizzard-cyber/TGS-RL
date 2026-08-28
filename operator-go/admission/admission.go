package admission

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/Blizzard-cyber/TGS-RL/operator-go/api"
)

type QueuePolicy struct {
	Name              string
	QuotaGroup        string
	Capacity          api.ResourceList
	AllowPreemption   bool
	PreemptionVictims []RunningBundle
}

type RunningBundle struct {
	Key       string
	Priority  int32
	Resources api.ResourceList
}

type Decision struct {
	Allowed       bool
	Phase         string
	Reason        string
	ReservedQuota api.ResourceList
	PreemptedKeys []string
}

type Evaluator struct{}

func New() *Evaluator {
	return &Evaluator{}
}

func (e *Evaluator) Evaluate(bundle *api.Bundle, policy QueuePolicy) (Decision, error) {
	if bundle == nil {
		return Decision{}, fmt.Errorf("bundle is required")
	}
	if bundle.Admission.Queue == "" {
		return Decision{}, fmt.Errorf("bundle admission queue is required")
	}
	if policy.Name == "" {
		policy.Name = bundle.Admission.Queue
	}
	if policy.QuotaGroup == "" {
		policy.QuotaGroup = bundle.Admission.QuotaGroup
	}
	if bundle.Admission.Queue != policy.Name {
		return Decision{}, fmt.Errorf("queue mismatch: bundle=%s policy=%s", bundle.Admission.Queue, policy.Name)
	}
	if bundle.Admission.QuotaGroup != policy.QuotaGroup {
		return Decision{}, fmt.Errorf("quota group mismatch: bundle=%s policy=%s", bundle.Admission.QuotaGroup, policy.QuotaGroup)
	}

	missing, ok := fits(policy.Capacity, bundle.Admission.Requests)
	if ok {
		return Decision{
			Allowed:       true,
			Phase:         "admitted",
			Reason:        "quota-available",
			ReservedQuota: api.CloneResourceList(bundle.Admission.Requests),
		}, nil
	}

	if !(policy.AllowPreemption && bundle.Admission.AllowPreemption) {
		return Decision{
			Allowed: false,
			Phase:   "queued",
			Reason:  "insufficient-quota:" + missing,
		}, nil
	}

	victims := append([]RunningBundle(nil), policy.PreemptionVictims...)
	sort.Slice(victims, func(i, j int) bool {
		if victims[i].Priority != victims[j].Priority {
			return victims[i].Priority < victims[j].Priority
		}
		return victims[i].Key < victims[j].Key
	})

	reclaimed := api.ResourceList{}
	preempted := make([]string, 0)
	for _, victim := range victims {
		add(reclaimed, victim.Resources)
		preempted = append(preempted, victim.Key)
		if _, ok := fits(merge(policy.Capacity, reclaimed), bundle.Admission.Requests); ok {
			sort.Strings(preempted)
			return Decision{
				Allowed:       true,
				Phase:         "admitted",
				Reason:        "preempted-lower-priority",
				ReservedQuota: api.CloneResourceList(bundle.Admission.Requests),
				PreemptedKeys: preempted,
			}, nil
		}
	}

	return Decision{
		Allowed: false,
		Phase:   "queued",
		Reason:  "insufficient-quota-after-preemption:" + missing,
	}, nil
}

func fits(capacity, requested api.ResourceList) (string, bool) {
	for resource, wantRaw := range requested {
		want, err := parseQuantity(wantRaw)
		if err != nil {
			return resource, false
		}
		have, err := parseQuantity(capacity[resource])
		if err != nil {
			have = 0
		}
		if have+1e-9 < want {
			return resource, false
		}
	}
	return "", true
}

func add(dst, values api.ResourceList) {
	for key, raw := range values {
		current, _ := parseQuantity(dst[key])
		addition, _ := parseQuantity(raw)
		dst[key] = formatQuantity(current + addition)
	}
}

func merge(left, right api.ResourceList) api.ResourceList {
	out := api.CloneResourceList(left)
	if out == nil {
		out = api.ResourceList{}
	}
	add(out, right)
	return out
}

func formatQuantity(value float64) string {
	if value == float64(int64(value)) {
		return fmt.Sprintf("%.0f", value)
	}
	return fmt.Sprintf("%.3f", value)
}

func parseQuantity(raw string) (float64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, nil
	}
	if strings.HasSuffix(raw, "m") {
		value, err := strconv.ParseFloat(strings.TrimSuffix(raw, "m"), 64)
		if err != nil {
			return 0, err
		}
		return value / 1000.0, nil
	}
	return strconv.ParseFloat(raw, 64)
}
