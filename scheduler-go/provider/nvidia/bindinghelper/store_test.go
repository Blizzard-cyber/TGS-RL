package bindinghelper

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func testBinding() Binding {
	return Binding{SandboxID: "sandbox-a", BindingID: "binding-a", Generation: 4, DeviceIDs: []string{"GPU-b", "GPU-a"}, Share: .5, IdempotencyKey: "bind-key", MPSServerPID: 4242}
}

func testReceipt(key, actionID string, step, expected int) Receipt {
	return Receipt{StepIndex: step, ExpectedActions: expected, PlanDigest: "sha256:plan", ActionID: actionID, PlanID: "plan-a", IdempotencyKey: key, SandboxID: "sandbox-a", TransactionGeneration: 7, ActionGeneration: 4, Succeeded: true, CommandDigest: "sha256:action" + actionID, Recoverable: true}
}

func TestStorePersistsBindingAndReceiptAcrossRestart(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "binding.json")
	store, err := NewStore(path)
	if err != nil {
		t.Fatal(err)
	}
	binding := testBinding()
	if err := store.ApplyBinding(binding, testReceipt(binding.IdempotencyKey, "action-a", 0, 1)); err != nil {
		t.Fatalf("ApplyBinding() error = %v", err)
	}
	restarted, _ := NewStore(path)
	state, err := restarted.Snapshot()
	if err != nil || len(state.Bindings) != 1 || state.Receipts[binding.IdempotencyKey].Committed {
		t.Fatalf("restarted state = %+v error = %v", state, err)
	}
	if state.Receipts[binding.IdempotencyKey].RequestDigest != BindingRequestDigest(binding) {
		t.Fatalf("request digest = %q", state.Receipts[binding.IdempotencyKey].RequestDigest)
	}
	var bindings bytes.Buffer
	if err := WriteBindingsCSV(&bindings, state); err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(bindings.String()); got != "sandbox-a,binding-a,4,GPU-a;GPU-b,0.5,bind-key,4242" {
		t.Fatalf("binding CSV = %q", got)
	}
	if info, err := os.Stat(path); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("state mode = %v error = %v", info.Mode().Perm(), err)
	}
	if info, err := os.Stat(directory); err != nil || info.Mode().Perm() != 0o700 {
		t.Fatalf("state directory mode = %v error = %v", info.Mode().Perm(), err)
	}
}

func TestStoreFencesGenerationAndIdempotency(t *testing.T) {
	store, _ := NewStore(filepath.Join(t.TempDir(), "binding.json"))
	binding := testBinding()
	receipt := testReceipt(binding.IdempotencyKey, "action-a", 0, 1)
	if err := store.ApplyBinding(binding, receipt); err != nil {
		t.Fatal(err)
	}
	if err := store.ApplyBinding(binding, receipt); err != nil {
		t.Fatalf("idempotent retry failed: %v", err)
	}
	stale := binding
	stale.Generation--
	stale.IdempotencyKey = "stale-key"
	staleReceipt := testReceipt(stale.IdempotencyKey, "action-stale", 0, 1)
	staleReceipt.ActionGeneration = stale.Generation
	if err := store.ApplyBinding(stale, staleReceipt); err == nil || !strings.Contains(err.Error(), "stale generation") {
		t.Fatalf("stale binding error = %v", err)
	}
	conflict := binding
	conflict.BindingID = "different"
	if err := store.ApplyBinding(conflict, receipt); err == nil || !strings.Contains(err.Error(), "idempotency key") {
		t.Fatalf("idempotency conflict error = %v", err)
	}
}

func TestStoreRejectsRetryAfterBindingWasSuperseded(t *testing.T) {
	store, _ := NewStore(filepath.Join(t.TempDir(), "binding.json"))
	first := testBinding()
	firstReceipt := testReceipt(first.IdempotencyKey, "action-a", 0, 1)
	if err := store.ApplyBinding(first, firstReceipt); err != nil {
		t.Fatal(err)
	}
	second := first
	second.BindingID = "binding-b"
	second.Generation++
	second.IdempotencyKey = "bind-key-b"
	secondReceipt := testReceipt(second.IdempotencyKey, "action-b", 0, 1)
	secondReceipt.ActionGeneration = second.Generation
	secondReceipt.TransactionGeneration++
	secondReceipt.PlanID = "plan-b"
	secondReceipt.PlanDigest = "sha256:plan-b"
	if err := store.ApplyBinding(second, secondReceipt); err != nil {
		t.Fatal(err)
	}
	if err := store.ApplyBinding(first, firstReceipt); err == nil || !strings.Contains(err.Error(), "superseded") {
		t.Fatalf("superseded bind retry error = %v", err)
	}
}

func TestStoreReleaseIsIdempotentUntilRebound(t *testing.T) {
	store, _ := NewStore(filepath.Join(t.TempDir(), "binding.json"))
	binding := testBinding()
	if err := store.ApplyBinding(binding, testReceipt(binding.IdempotencyKey, "action-a", 0, 1)); err != nil {
		t.Fatal(err)
	}
	release := testReceipt("release-key", "allocation-a", 0, 1)
	release.TransactionGeneration++
	release.PlanID = "plan-release"
	release.PlanDigest = "sha256:plan-release"
	if err := store.ReleaseBinding(binding.SandboxID, "allocation-a", binding.Generation, release); err != nil {
		t.Fatal(err)
	}
	if err := store.ReleaseBinding(binding.SandboxID, "allocation-a", binding.Generation, release); err != nil {
		t.Fatalf("idempotent release retry error = %v", err)
	}
	rebound := binding
	rebound.Generation++
	rebound.BindingID = "binding-b"
	rebound.IdempotencyKey = "bind-key-b"
	rebindReceipt := testReceipt(rebound.IdempotencyKey, "action-b", 0, 1)
	rebindReceipt.ActionGeneration = rebound.Generation
	rebindReceipt.TransactionGeneration++
	rebindReceipt.PlanID = "plan-b"
	rebindReceipt.PlanDigest = "sha256:plan-b"
	if err := store.ApplyBinding(rebound, rebindReceipt); err != nil {
		t.Fatal(err)
	}
	if err := store.ReleaseBinding(binding.SandboxID, "allocation-a", binding.Generation, release); err == nil || !strings.Contains(err.Error(), "superseded") {
		t.Fatalf("superseded release retry error = %v", err)
	}
}

func TestStoreSupersedingMutationReportsStaleRecoveryReceipt(t *testing.T) {
	store, _ := NewStore(filepath.Join(t.TempDir(), "binding.json"))
	binding := testBinding()
	if err := store.ApplyBinding(binding, testReceipt(binding.IdempotencyKey, "action-bind", 0, 1)); err != nil {
		t.Fatal(err)
	}
	release := testReceipt("release-key", "action-release", 0, 1)
	release.Recoverable = false
	release.TransactionGeneration++
	release.PlanID = "plan-release"
	release.PlanDigest = "sha256:plan-release"
	if err := store.ReleaseBinding(binding.SandboxID, binding.BindingID, binding.Generation, release); err != nil {
		t.Fatal(err)
	}
	state, err := store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := state.Receipts[binding.IdempotencyKey]; !exists {
		t.Fatal("superseded receipt history was lost")
	}
	var output bytes.Buffer
	if err := WriteReceiptsCSV(&output, state); err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(output.String()); !strings.Contains(got, ",false,SUPERSEDED,binding state no longer matches this transaction,") {
		t.Fatalf("superseded receipt = %q", got)
	}
}

func TestStoreLeavesCompleteReceiptSetUncommitted(t *testing.T) {
	store, _ := NewStore(filepath.Join(t.TempDir(), "binding.json"))
	first := testBinding()
	first.SandboxID, first.BindingID, first.IdempotencyKey = "sandbox-a", "binding-a", "key-a"
	second := testBinding()
	second.SandboxID, second.BindingID, second.IdempotencyKey = "sandbox-b", "binding-b", "key-b"
	if err := store.ApplyBinding(first, testReceipt("key-a", "action-a", 0, 2)); err != nil {
		t.Fatal(err)
	}
	state, _ := store.Snapshot()
	if state.Receipts["key-a"].Committed {
		t.Fatal("partial receipt set marked committed")
	}
	secondReceipt := testReceipt("key-b", "action-b", 1, 2)
	secondReceipt.SandboxID = "sandbox-b"
	if err := store.ApplyBinding(second, secondReceipt); err != nil {
		t.Fatal(err)
	}
	state, _ = store.Snapshot()
	if state.Receipts["key-a"].Committed || state.Receipts["key-b"].Committed {
		t.Fatalf("helper must not infer provider commit: %+v", state.Receipts)
	}
}

func TestStoreSerializesConcurrentUpdates(t *testing.T) {
	store, _ := NewStore(filepath.Join(t.TempDir(), "binding.json"))
	var group sync.WaitGroup
	for index := 0; index < 16; index++ {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			binding := testBinding()
			binding.SandboxID = "sandbox-" + string(rune('a'+index))
			binding.BindingID = "binding-" + string(rune('a'+index))
			binding.IdempotencyKey = "key-" + string(rune('a'+index))
			receipt := testReceipt(binding.IdempotencyKey, "action-"+string(rune('a'+index)), 0, 1)
			receipt.PlanID = "plan-" + string(rune('a'+index))
			receipt.PlanDigest = "digest-" + string(rune('a'+index))
			receipt.SandboxID = binding.SandboxID
			if err := store.ApplyBinding(binding, receipt); err != nil {
				t.Errorf("ApplyBinding(%d) error = %v", index, err)
			}
		}(index)
	}
	group.Wait()
	state, err := store.Snapshot()
	if err != nil || len(state.Bindings) != 16 || len(state.Receipts) != 16 {
		t.Fatalf("concurrent state bindings=%d receipts=%d error=%v", len(state.Bindings), len(state.Receipts), err)
	}
}

func TestStoreFailedMutationDoesNotReplaceDurableState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "binding.json")
	store, _ := NewStore(path)
	binding := testBinding()
	if err := store.ApplyBinding(binding, testReceipt(binding.IdempotencyKey, "action-a", 0, 1)); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	stale := binding
	stale.Generation--
	stale.IdempotencyKey = "stale-key"
	receipt := testReceipt(stale.IdempotencyKey, "action-stale", 0, 1)
	receipt.ActionGeneration = stale.Generation
	if err := store.ApplyBinding(stale, receipt); err == nil {
		t.Fatal("stale mutation succeeded")
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("failed mutation changed durable state")
	}
	var state State
	if err := json.Unmarshal(after, &state); err != nil || len(state.Bindings) != 1 || len(state.Receipts) != 1 {
		t.Fatalf("durable state = %+v error = %v", state, err)
	}
}

func TestReplaceBindingRetrySucceedsAfterTargetWasPersisted(t *testing.T) {
	store, _ := NewStore(filepath.Join(t.TempDir(), "binding.json"))
	source := testBinding()
	source.DeviceIDs = []string{"MIG-source/1/0"}
	if err := store.ApplyBinding(source, testReceipt(source.IdempotencyKey, "action-source", 0, 1)); err != nil {
		t.Fatal(err)
	}
	target := source
	target.BindingID = "binding-new"
	target.Generation++
	target.DeviceIDs = []string{"MIG-target/2/0"}
	target.Share = 1
	target.IdempotencyKey = "replace-key"
	receipt := testReceipt(target.IdempotencyKey, "action-replace", 0, 1)
	receipt.ActionGeneration = target.Generation
	receipt.TransactionGeneration++
	receipt.PlanID = "plan-replace"
	receipt.PlanDigest = "sha256:plan-replace"
	request := ReplaceRequest{SandboxID: source.SandboxID, SourceBindingID: source.BindingID, SourceDeviceID: source.DeviceIDs[0], SourceGeneration: source.Generation, Target: target}
	if err := store.ReplaceBinding(request, receipt); err != nil {
		t.Fatal(err)
	}
	if err := store.ReplaceBinding(request, receipt); err != nil {
		t.Fatalf("idempotent replacement retry failed after source was replaced: %v", err)
	}
	conflict := receipt
	conflict.CommandDigest = "sha256:different"
	if err := store.ReplaceBinding(request, conflict); err == nil || !strings.Contains(err.Error(), "idempotency key") {
		t.Fatalf("replacement receipt conflict error = %v", err)
	}
}
