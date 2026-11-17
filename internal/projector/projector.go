package projector

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

type Event struct {
	ID      string
	Topic   string
	Type    string
	Payload []byte
}

type orderCreated struct {
	TotalMinor int64  `json:"totalMinor"`
	CreatedAt  string `json:"createdAt"`
}

// Apply records the event ID before updating the projection in the same
// transaction. A duplicate Kafka delivery therefore has no effect.
func Apply(ctx context.Context, db *sql.DB, event Event) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	result, err := tx.ExecContext(ctx, `
		INSERT INTO processed_events (event_id, topic) VALUES ($1, $2)
		ON CONFLICT (event_id) DO NOTHING`, event.ID, event.Topic)
	if err != nil {
		return fmt.Errorf("record event: %w", err)
	}
	inserted, err := result.RowsAffected()
	if err != nil || inserted == 0 {
		return err
	}

	if event.Type == "OrderCreated" {
		var order orderCreated
		if err := json.Unmarshal(event.Payload, &order); err != nil {
			return fmt.Errorf("decode OrderCreated: %w", err)
		}
		at, err := time.Parse(time.RFC3339, order.CreatedAt)
		if err != nil {
			return fmt.Errorf("parse order timestamp: %w", err)
		}
		_, err = tx.ExecContext(ctx, `
			INSERT INTO daily_sales (sale_date, order_count, gross_minor)
			VALUES ($1, 1, $2)
			ON CONFLICT (sale_date) DO UPDATE SET
			  order_count = daily_sales.order_count + 1,
			  gross_minor = daily_sales.gross_minor + EXCLUDED.gross_minor`,
			at.UTC().Format("2006-01-02"), order.TotalMinor)
		if err != nil {
			return fmt.Errorf("update daily sales: %w", err)
		}
	} else if event.Topic == "storemesh.inventory.events" && isInventoryProjectionEvent(event.Type) {
		_, err = tx.ExecContext(ctx, `
				INSERT INTO inventory_event_counts (event_type, event_count)
				VALUES ($1, 1)
				ON CONFLICT (event_type) DO UPDATE SET
				  event_count = inventory_event_counts.event_count + 1`, event.Type)
		if err != nil {
			return fmt.Errorf("update inventory event counts: %w", err)
		}
	}

	return tx.Commit()
}

func isInventoryProjectionEvent(eventType string) bool {
	switch eventType {
	case "InventoryAdjusted", "InventoryReserved", "InventoryReleased":
		return true
	default:
		return false
	}
}
