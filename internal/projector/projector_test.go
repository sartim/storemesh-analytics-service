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
