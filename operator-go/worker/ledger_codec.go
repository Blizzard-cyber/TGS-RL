package worker

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/api"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// persistedObservationRegistration keeps protobuf messages behind an explicit
// codec boundary. The surrounding registration remains ordinary JSON while
// Decision and JobRun use protobuf's canonical JSON representation.
type persistedObservationRegistration struct {
	BundleKey               string               `json:"bundleKey"`
	Bundle                  *api.Bundle          `json:"bundle"`
	Decision                json.RawMessage      `json:"decision"`
	JobRun                  json.RawMessage      `json:"jobRun"`
	PublishedEventIDs       map[string]bool      `json:"publishedEventIds,omitempty"`
	LastState               tgsrlv1.RuntimeState `json:"lastState,omitempty"`
	PendingState            tgsrlv1.RuntimeState `json:"pendingState,omitempty"`
	Transition              uint64               `json:"transition,omitempty"`
	PublishedStates         map[string]uint64    `json:"publishedStates,omitempty"`
	PendingObservedAt       time.Time            `json:"pendingObservedAt,omitempty"`
	PendingDetail           string               `json:"pendingDetail,omitempty"`
	PendingControlKey       string               `json:"pendingControlKey,omitempty"`
	PendingControlRevision  uint64               `json:"pendingControlRevision,omitempty"`
	PendingControlRequestID string               `json:"pendingControlRequestId,omitempty"`
}

type persistedDeliveryLedgerState struct {
	Delivery      *DeliveryRecord                             `json:"delivery,omitempty"`
	Registrations map[string]persistedObservationRegistration `json:"registrations,omitempty"`
}

func encodeDeliveryLedgerState(state deliveryLedgerState) (persistedDeliveryLedgerState, error) {
	persisted := persistedDeliveryLedgerState{Delivery: state.Delivery}
	if len(state.Registrations) == 0 {
		return persisted, nil
	}
	persisted.Registrations = make(map[string]persistedObservationRegistration, len(state.Registrations))
	for key, registration := range state.Registrations {
		encoded, err := encodeObservationRegistration(registration)
		if err != nil {
			return persistedDeliveryLedgerState{}, fmt.Errorf("encode observation registration %q: %w", key, err)
		}
		persisted.Registrations[key] = encoded
	}
	return persisted, nil
}

func encodeObservationRegistration(registration ObservationRegistration) (persistedObservationRegistration, error) {
	var decision, jobRun json.RawMessage
	var err error
	if registration.Decision != nil {
		decision, err = protojson.Marshal(registration.Decision)
		if err != nil {
			return persistedObservationRegistration{}, fmt.Errorf("marshal decision: %w", err)
		}
	}
	if registration.JobRun != nil {
		jobRun, err = protojson.Marshal(registration.JobRun)
		if err != nil {
			return persistedObservationRegistration{}, fmt.Errorf("marshal job run: %w", err)
		}
	}
	return persistedObservationRegistration{
		BundleKey: registration.BundleKey, Bundle: registration.Bundle, Decision: decision, JobRun: jobRun,
		PublishedEventIDs: registration.PublishedEventIDs, LastState: registration.LastState, PendingState: registration.PendingState,
		Transition: registration.Transition, PublishedStates: registration.PublishedStates, PendingObservedAt: registration.PendingObservedAt,
		PendingDetail: registration.PendingDetail, PendingControlKey: registration.PendingControlKey,
		PendingControlRevision: registration.PendingControlRevision, PendingControlRequestID: registration.PendingControlRequestID,
	}, nil
}

func decodeDeliveryLedgerState(persisted persistedDeliveryLedgerState) (deliveryLedgerState, error) {
	state := deliveryLedgerState{Delivery: persisted.Delivery}
	if len(persisted.Registrations) == 0 {
		return state, nil
	}
	state.Registrations = make(map[string]ObservationRegistration, len(persisted.Registrations))
	for key, registration := range persisted.Registrations {
		decoded, err := decodeObservationRegistration(registration)
		if err != nil {
			return deliveryLedgerState{}, fmt.Errorf("decode observation registration %q: %w", key, err)
		}
		state.Registrations[key] = decoded
	}
	return state, nil
}

func decodeObservationRegistration(persisted persistedObservationRegistration) (ObservationRegistration, error) {
	var decision *tgsrlv1.DecisionRecord
	if len(persisted.Decision) != 0 && string(persisted.Decision) != "null" {
		decision = &tgsrlv1.DecisionRecord{}
		if err := unmarshalDurableProto(persisted.Decision, decision); err != nil {
			return ObservationRegistration{}, fmt.Errorf("unmarshal decision: %w", err)
		}
	}
	var jobRun *tgsrlv1.JobRun
	if len(persisted.JobRun) != 0 && string(persisted.JobRun) != "null" {
		jobRun = &tgsrlv1.JobRun{}
		if err := unmarshalDurableProto(persisted.JobRun, jobRun); err != nil {
			return ObservationRegistration{}, fmt.Errorf("unmarshal job run: %w", err)
		}
	}
	return ObservationRegistration{
		BundleKey: persisted.BundleKey, Bundle: persisted.Bundle, Decision: decision, JobRun: jobRun,
		PublishedEventIDs: persisted.PublishedEventIDs, LastState: persisted.LastState, PendingState: persisted.PendingState,
		Transition: persisted.Transition, PublishedStates: persisted.PublishedStates, PendingObservedAt: persisted.PendingObservedAt,
		PendingDetail: persisted.PendingDetail, PendingControlKey: persisted.PendingControlKey,
		PendingControlRevision: persisted.PendingControlRevision, PendingControlRequestID: persisted.PendingControlRequestID,
	}, nil
}

func unmarshalDurableProto(payload json.RawMessage, message protoreflect.ProtoMessage) error {
	protoJSONErr := protojson.Unmarshal(payload, message)
	if protoJSONErr == nil {
		return nil
	}

	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	var raw any
	if err := decoder.Decode(&raw); err != nil {
		return err
	}
	if usesProtoJSONRootFields(raw, message.ProtoReflect().Descriptor()) {
		return protoJSONErr
	}
	proto.Reset(message)
	sanitized, err := json.Marshal(stripLegacySemanticKinds(raw, message.ProtoReflect().Descriptor()))
	if err != nil {
		return err
	}
	if err := unmarshalStrictJSON(sanitized, message); err != nil {
		return fmt.Errorf("decode as protojson: %v; decode as legacy JSON: %w", protoJSONErr, err)
	}
	return restoreLegacySemanticValues(message.ProtoReflect(), raw)
}

func unmarshalStrictJSON(payload []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if decoder.More() {
		return fmt.Errorf("multiple JSON values")
	}
	return nil
}

func usesProtoJSONRootFields(raw any, descriptor protoreflect.MessageDescriptor) bool {
	object, ok := raw.(map[string]any)
	if !ok {
		return false
	}
	fields := descriptor.Fields()
	for index := 0; index < fields.Len(); index++ {
		field := fields.Get(index)
		if field.JSONName() == field.TextName() {
			continue
		}
		if _, exists := object[field.JSONName()]; exists {
			return true
		}
	}
	return false
}

func stripLegacySemanticKinds(raw any, descriptor protoreflect.MessageDescriptor) any {
	object, ok := raw.(map[string]any)
	if !ok {
		return raw
	}
	clean := make(map[string]any, len(object))
	for key, value := range object {
		if descriptor.FullName() == "tgsrl.v1.SemanticValue" && key == "Kind" {
			continue
		}
		field := legacyFieldByName(descriptor, key)
		if field == nil {
			clean[key] = value
			continue
		}
		if field.IsMap() {
			if field.MapValue().Kind() == protoreflect.MessageKind {
				if values, ok := value.(map[string]any); ok {
					mapped := make(map[string]any, len(values))
					for mapKey, mapValue := range values {
						mapped[mapKey] = stripLegacySemanticKinds(mapValue, field.MapValue().Message())
					}
					clean[key] = mapped
					continue
				}
			}
		} else if field.IsList() && field.Kind() == protoreflect.MessageKind {
			if values, ok := value.([]any); ok {
				mapped := make([]any, len(values))
				for index, listValue := range values {
					mapped[index] = stripLegacySemanticKinds(listValue, field.Message())
				}
				clean[key] = mapped
				continue
			}
		} else if field.Kind() == protoreflect.MessageKind {
			clean[key] = stripLegacySemanticKinds(value, field.Message())
			continue
		}
		clean[key] = value
	}
	return clean
}

func legacyFieldByName(descriptor protoreflect.MessageDescriptor, name string) protoreflect.FieldDescriptor {
	fields := descriptor.Fields()
	for index := 0; index < fields.Len(); index++ {
		field := fields.Get(index)
		if name == field.TextName() || name == field.JSONName() || name == string(field.Name()) {
			return field
		}
	}
	return nil
}

func restoreLegacySemanticValues(message protoreflect.Message, raw any) error {
	if message.Descriptor().FullName() == "tgsrl.v1.SemanticValue" {
		semantic, ok := message.Interface().(*tgsrlv1.SemanticValue)
		if !ok {
			return fmt.Errorf("unexpected semantic value type %T", message.Interface())
		}
		return restoreLegacySemanticValue(semantic, raw)
	}
	rawObject, ok := raw.(map[string]any)
	if !ok {
		return nil
	}
	fields := message.Descriptor().Fields()
	for index := 0; index < fields.Len(); index++ {
		field := fields.Get(index)
		rawField, exists := legacyRawField(rawObject, field)
		if !exists || rawField == nil {
			continue
		}
		if field.IsMap() {
			if field.MapValue().Kind() != protoreflect.MessageKind {
				continue
			}
			rawMap, ok := rawField.(map[string]any)
			if !ok {
				continue
			}
			values := message.Get(field).Map()
			var restoreErr error
			values.Range(func(key protoreflect.MapKey, value protoreflect.Value) bool {
				rawValue, exists := rawMap[key.String()]
				if exists {
					restoreErr = restoreLegacySemanticValues(value.Message(), rawValue)
				}
				return restoreErr == nil
			})
			if restoreErr != nil {
				return restoreErr
			}
			continue
		}
		if field.IsList() {
			if field.Kind() != protoreflect.MessageKind {
				continue
			}
			rawList, ok := rawField.([]any)
			if !ok {
				continue
			}
			values := message.Get(field).List()
			for item := 0; item < values.Len() && item < len(rawList); item++ {
				if err := restoreLegacySemanticValues(values.Get(item).Message(), rawList[item]); err != nil {
					return err
				}
			}
			continue
		}
		if field.Kind() == protoreflect.MessageKind && message.Has(field) {
			if err := restoreLegacySemanticValues(message.Get(field).Message(), rawField); err != nil {
				return err
			}
		}
	}
	return nil
}

func legacyRawField(raw map[string]any, field protoreflect.FieldDescriptor) (any, bool) {
	for _, name := range []string{field.TextName(), field.JSONName(), string(field.Name())} {
		if value, ok := raw[name]; ok {
			return value, true
		}
	}
	return nil, false
}

func restoreLegacySemanticValue(value *tgsrlv1.SemanticValue, raw any) error {
	rawObject, ok := raw.(map[string]any)
	if !ok {
		return nil
	}
	kind, exists := rawObject["Kind"]
	if !exists || kind == nil {
		return nil
	}
	wrapper, ok := kind.(map[string]any)
	if !ok {
		return fmt.Errorf("legacy SemanticValue Kind must be an object")
	}
	if len(wrapper) == 0 {
		return nil
	}
	if len(wrapper) != 1 {
		return fmt.Errorf("legacy SemanticValue Kind has %d alternatives", len(wrapper))
	}
	for name, rawValue := range wrapper {
		switch name {
		case "StringValue":
			parsed, ok := rawValue.(string)
			if !ok {
				return fmt.Errorf("legacy StringValue must be a string")
			}
			value.Kind = &tgsrlv1.SemanticValue_StringValue{StringValue: parsed}
		case "Int64Value":
			parsed, err := parseLegacyInt64(rawValue)
			if err != nil {
				return fmt.Errorf("legacy Int64Value: %w", err)
			}
			value.Kind = &tgsrlv1.SemanticValue_Int64Value{Int64Value: parsed}
		case "Uint64Value":
			parsed, err := parseLegacyUint64(rawValue)
			if err != nil {
				return fmt.Errorf("legacy Uint64Value: %w", err)
			}
			value.Kind = &tgsrlv1.SemanticValue_Uint64Value{Uint64Value: parsed}
		case "DoubleValue":
			parsed, err := parseLegacyFloat64(rawValue)
			if err != nil {
				return fmt.Errorf("legacy DoubleValue: %w", err)
			}
			value.Kind = &tgsrlv1.SemanticValue_DoubleValue{DoubleValue: parsed}
		case "BoolValue":
			parsed, ok := rawValue.(bool)
			if !ok {
				return fmt.Errorf("legacy BoolValue must be a bool")
			}
			value.Kind = &tgsrlv1.SemanticValue_BoolValue{BoolValue: parsed}
		case "BytesValue":
			encoded, ok := rawValue.(string)
			if !ok {
				return fmt.Errorf("legacy BytesValue must be a base64 string")
			}
			parsed, err := base64.StdEncoding.DecodeString(encoded)
			if err != nil {
				return fmt.Errorf("legacy BytesValue: %w", err)
			}
			value.Kind = &tgsrlv1.SemanticValue_BytesValue{BytesValue: parsed}
		case "ObjectValue":
			parsed := &tgsrlv1.SemanticObject{}
			if err := decodeLegacySemanticContainer(rawValue, parsed); err != nil {
				return fmt.Errorf("legacy ObjectValue: %w", err)
			}
			value.Kind = &tgsrlv1.SemanticValue_ObjectValue{ObjectValue: parsed}
		case "ListValue":
			parsed := &tgsrlv1.SemanticList{}
			if err := decodeLegacySemanticContainer(rawValue, parsed); err != nil {
				return fmt.Errorf("legacy ListValue: %w", err)
			}
			value.Kind = &tgsrlv1.SemanticValue_ListValue{ListValue: parsed}
		default:
			return fmt.Errorf("unknown legacy SemanticValue alternative %q", name)
		}
	}
	return nil
}

func decodeLegacySemanticContainer(raw any, message protoreflect.ProtoMessage) error {
	sanitized, err := json.Marshal(stripLegacySemanticKinds(raw, message.ProtoReflect().Descriptor()))
	if err != nil {
		return err
	}
	if err := unmarshalStrictJSON(sanitized, message); err != nil {
		return err
	}
	return restoreLegacySemanticValues(message.ProtoReflect(), raw)
}

func parseLegacyInt64(value any) (int64, error) {
	switch value := value.(type) {
	case json.Number:
		return strconv.ParseInt(value.String(), 10, 64)
	case string:
		return strconv.ParseInt(value, 10, 64)
	default:
		return 0, fmt.Errorf("must be an integer")
	}
}

func parseLegacyUint64(value any) (uint64, error) {
	switch value := value.(type) {
	case json.Number:
		return strconv.ParseUint(value.String(), 10, 64)
	case string:
		return strconv.ParseUint(value, 10, 64)
	default:
		return 0, fmt.Errorf("must be an unsigned integer")
	}
}

func parseLegacyFloat64(value any) (float64, error) {
	switch value := value.(type) {
	case json.Number:
		return strconv.ParseFloat(value.String(), 64)
	case string:
		return strconv.ParseFloat(value, 64)
	default:
		return 0, fmt.Errorf("must be a number")
	}
}
