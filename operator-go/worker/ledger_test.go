package worker

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestFileDeliveryRepositoryRegistrationProtoJSONRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "delivery.json")
	want := semanticRegistrationForLedgerTest()
	if err := NewFileDeliveryRepository(path).SaveRegistration(want); err != nil {
		t.Fatal(err)
	}

	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(payload, []byte(`"uint64Value":"18446744073709551615"`)) {
		t.Fatalf("ledger does not contain protojson uint64: %s", payload)
	}
	if bytes.Contains(payload, []byte(`"Kind"`)) {
		t.Fatalf("ledger contains legacy oneof wrapper: %s", payload)
	}

	registrations, err := NewFileDeliveryRepository(path).ListRegistrations()
	if err != nil {
		t.Fatal(err)
	}
	assertRegistrationProtosEqual(t, registrations, want)
}

func TestFileDeliveryRepositoryReplacesRegistrationsAtomically(t *testing.T) {
	path := filepath.Join(t.TempDir(), "delivery.json")
	repository := NewFileDeliveryRepository(path)
	stale := observationRegistration(decisionForSequence(40))
	current := observationRegistration(decisionForSequence(41))
	if err := repository.Save(DeliveryRecord{DecisionID: "decision-active", Sequence: 42}); err != nil {
		t.Fatal(err)
	}
	if err := repository.SaveRegistration(stale); err != nil {
		t.Fatal(err)
	}
	if err := repository.ReplaceRegistrations([]ObservationRegistration{current}); err != nil {
		t.Fatal(err)
	}

	registrations, err := repository.ListRegistrations()
	if err != nil {
		t.Fatal(err)
	}
	assertRegistrationProtosEqual(t, registrations, current)
	delivery, err := repository.Load()
	if err != nil {
		t.Fatal(err)
	}
	if delivery.DecisionID != "decision-active" || delivery.Sequence != 42 {
		t.Fatalf("delivery = %+v, want preserved active record", delivery)
	}
}

func TestFileDeliveryRepositoryLoadsLegacyRegistrationWithoutOneof(t *testing.T) {
	path := filepath.Join(t.TempDir(), "delivery.json")
	want := observationRegistration(decisionForSequence(42))
	want.Decision.DecidedAt = timestamppb.New(time.Unix(1_700_000_000, 123).UTC())
	want.JobRun.CreatedAt = timestamppb.New(time.Unix(1_700_000_100, 456).UTC())
	want.JobRun.ExecutionContract = &tgsrlv1.ExecutionContract{
		CommitPolicy: &tgsrlv1.CommitPolicy{CommitTimeout: durationpb.New(1500 * time.Millisecond)},
	}
	want.JobRun.Labels = map[string]string{
		"stringValue": "ordinary map value",
		"boolValue":   "also ordinary",
	}
	want.Decision.SemanticContext = &tgsrlv1.SemanticEnvelope{Attributes: map[string]string{
		"stringValue": "attribute",
		"boolValue":   "attribute",
	}}
	writeLegacyLedger(t, path, want)

	registrations, err := NewFileDeliveryRepository(path).ListRegistrations()
	if err != nil {
		t.Fatal(err)
	}
	assertRegistrationProtosEqual(t, registrations, want)
}

func TestFileDeliveryRepositoryRejectsUnknownLegacyProtoField(t *testing.T) {
	path := filepath.Join(t.TempDir(), "delivery.json")
	registration := observationRegistration(decisionForSequence(43))
	writeLegacyLedger(t, path, registration)

	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var ledger map[string]json.RawMessage
	if err := json.Unmarshal(payload, &ledger); err != nil {
		t.Fatal(err)
	}
	var registrations map[string]map[string]json.RawMessage
	if err := json.Unmarshal(ledger["registrations"], &registrations); err != nil {
		t.Fatal(err)
	}
	var decision map[string]json.RawMessage
	if err := json.Unmarshal(registrations[registration.BundleKey]["decision"], &decision); err != nil {
		t.Fatal(err)
	}
	decision["future_field"] = json.RawMessage(`true`)
	registrations[registration.BundleKey]["decision"], err = json.Marshal(decision)
	if err != nil {
		t.Fatal(err)
	}
	ledger["registrations"], err = json.Marshal(registrations)
	if err != nil {
		t.Fatal(err)
	}
	payload, err = json.Marshal(ledger)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, payload, 0o644); err != nil {
		t.Fatal(err)
	}

	_, err = NewFileDeliveryRepository(path).ListRegistrations()
	if err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("ListRegistrations() error = %v, want unknown field rejection", err)
	}
}

func TestFileDeliveryRepositorySyncsDirectoryAfterRemovingLastRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "delivery.json")
	repository := NewFileDeliveryRepository(path)
	if err := repository.Save(DeliveryRecord{DecisionID: "decision-1", Sequence: 1}); err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("directory sync failed")
	var syncedPath string
	repository.syncDirectory = func(path string) error {
		syncedPath = path
		return wantErr
	}

	err := repository.Clear()
	if !errors.Is(err, wantErr) {
		t.Fatalf("Clear() error = %v, want %v", err, wantErr)
	}
	if syncedPath != filepath.Dir(path) {
		t.Fatalf("synced directory = %q, want %q", syncedPath, filepath.Dir(path))
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("ledger remains after Clear(): %v", err)
	}
}

func TestFileDeliveryRepositoryMigratesLegacySemanticValueKinds(t *testing.T) {
	path := filepath.Join(t.TempDir(), "delivery.json")
	want := semanticRegistrationForLedgerTest()
	writeLegacyLedger(t, path, want)

	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(payload, []byte(`"Kind":{"Uint64Value":18446744073709551615}`)) {
		t.Fatalf("fixture does not contain legacy oneof wrapper: %s", payload)
	}
	registrations, err := NewFileDeliveryRepository(path).ListRegistrations()
	if err != nil {
		t.Fatal(err)
	}
	assertRegistrationProtosEqual(t, registrations, want)
}

func semanticRegistrationForLedgerTest() ObservationRegistration {
	registration := observationRegistration(decisionForSequence(41))
	registration.Decision.DecidedAt = timestamppb.New(time.Unix(1_700_000_000, 123).UTC())
	registration.Decision.SemanticContext = &tgsrlv1.SemanticEnvelope{
		TypedFields: []*tgsrlv1.SemanticField{
			{Key: "string", Value: &tgsrlv1.SemanticValue{Kind: &tgsrlv1.SemanticValue_StringValue{StringValue: "value"}}},
			{Key: "int64", Value: &tgsrlv1.SemanticValue{Kind: &tgsrlv1.SemanticValue_Int64Value{Int64Value: -9_223_372_036_854_775_808}}},
			{Key: "uint64", Value: &tgsrlv1.SemanticValue{Kind: &tgsrlv1.SemanticValue_Uint64Value{Uint64Value: 18_446_744_073_709_551_615}}},
			{Key: "double", Value: &tgsrlv1.SemanticValue{Kind: &tgsrlv1.SemanticValue_DoubleValue{DoubleValue: 1.25}}},
			{Key: "bool", Value: &tgsrlv1.SemanticValue{Kind: &tgsrlv1.SemanticValue_BoolValue{BoolValue: true}}},
			{Key: "bytes", Value: &tgsrlv1.SemanticValue{Kind: &tgsrlv1.SemanticValue_BytesValue{BytesValue: []byte{0, 1, 255}}}},
			{Key: "object", Value: &tgsrlv1.SemanticValue{Kind: &tgsrlv1.SemanticValue_ObjectValue{ObjectValue: &tgsrlv1.SemanticObject{Fields: []*tgsrlv1.SemanticField{{Key: "nested", Value: &tgsrlv1.SemanticValue{Kind: &tgsrlv1.SemanticValue_BoolValue{BoolValue: false}}}}}}}},
			{Key: "list", Value: &tgsrlv1.SemanticValue{Kind: &tgsrlv1.SemanticValue_ListValue{ListValue: &tgsrlv1.SemanticList{Values: []*tgsrlv1.SemanticValue{{Kind: &tgsrlv1.SemanticValue_StringValue{StringValue: "nested"}}, {Kind: &tgsrlv1.SemanticValue_Uint64Value{Uint64Value: 7}}}}}}},
		},
	}
	registration.JobRun.CreatedAt = timestamppb.New(time.Unix(1_700_000_100, 456).UTC())
	registration.JobRun.ExecutionContract = &tgsrlv1.ExecutionContract{
		CommitPolicy: &tgsrlv1.CommitPolicy{CommitTimeout: durationpb.New(1500 * time.Millisecond)},
		Conditions:   []*tgsrlv1.Condition{{Operands: []*tgsrlv1.SemanticValue{{Kind: &tgsrlv1.SemanticValue_BytesValue{BytesValue: []byte("job-run")}}}}},
	}
	return registration
}

func writeLegacyLedger(t *testing.T, path string, registration ObservationRegistration) {
	t.Helper()
	payload, err := json.Marshal(deliveryLedgerState{Registrations: map[string]ObservationRegistration{registration.BundleKey: registration}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, payload, 0o644); err != nil {
		t.Fatal(err)
	}
}

func assertRegistrationProtosEqual(t *testing.T, registrations []ObservationRegistration, want ObservationRegistration) {
	t.Helper()
	if len(registrations) != 1 {
		t.Fatalf("registrations = %d, want 1", len(registrations))
	}
	if registrations[0].BundleKey != want.BundleKey {
		t.Fatalf("bundle key = %q, want %q", registrations[0].BundleKey, want.BundleKey)
	}
	if !proto.Equal(registrations[0].Decision, want.Decision) {
		t.Fatalf("decision mismatch\n got: %v\nwant: %v", registrations[0].Decision, want.Decision)
	}
	if !proto.Equal(registrations[0].JobRun, want.JobRun) {
		t.Fatalf("job run mismatch\n got: %v\nwant: %v", registrations[0].JobRun, want.JobRun)
	}
}
