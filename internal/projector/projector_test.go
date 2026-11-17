package projector

import (
	"encoding/json"
	"testing"
)

func TestOrderCreatedPayloadDecodesProjectionFields(t *testing.T) {
	payload := []byte(`{"totalMinor":2598,"createdAt":"2026-11-17T09:00:00Z"}`)
	var order orderCreated
	if err := json.Unmarshal(payload, &order); err != nil {
		t.Fatal(err)
	}
	if order.TotalMinor != 2598 || order.CreatedAt == "" {
		t.Fatalf("decoded order = %+v", order)
	}
}

func TestInventoryEventTypesAreProjectionEvents(t *testing.T) {
	for _, eventType := range []string{"InventoryAdjusted", "InventoryReserved", "InventoryReleased"} {
		if !isInventoryProjectionEvent(eventType) {
			t.Fatalf("expected %s to be projected", eventType)
		}
	}
	if isInventoryProjectionEvent("UnknownEvent") {
		t.Fatal("unknown event must not be projected")
	}
}

func TestOrderProjectionRequiresCanonicalTopic(t *testing.T) {
	if !isOrderProjectionEvent(orderEventsTopic, "OrderCreated") {
		t.Fatal("canonical OrderCreated event should be projected")
	}
	for _, event := range [][2]string{{"storemesh.inventory.events", "OrderCreated"}, {orderEventsTopic, "OrderUpdated"}} {
		if isOrderProjectionEvent(event[0], event[1]) {
			t.Fatalf("unexpected order projection for topic=%q type=%q", event[0], event[1])
		}
	}
}
