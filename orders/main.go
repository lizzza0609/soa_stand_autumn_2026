// Orders вЂ” РЅР°РёРІРЅР°СЏ СЂРµР°Р»РёР·Р°С†РёСЏ.
//
// РћС„РѕСЂРјР»РµРЅРёРµ Р·Р°РєР°Р·Р°: Р·Р°СЂРµР·РµСЂРІРёСЂРѕРІР°С‚СЊ С‚РѕРІР°СЂ РІ Inventory, СЃРїРёСЃР°С‚СЊ РґРµРЅСЊРіРё РІ
// Payment, Р·Р°РїРёСЃР°С‚СЊ Р·Р°РєР°Р· РІ Р±Р°Р·Сѓ. РџРѕРєР° Payment Рё Inventory СЂР°Р±РѕС‚Р°СЋС‚ Р±РµР· РѕС‚РєР°Р·РѕРІ, РІСЃС‘ СЂР°Р±РѕС‚Р°РµС‚.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const schema = `
CREATE TABLE IF NOT EXISTS orders (
    id           TEXT PRIMARY KEY,
    user_id      TEXT NOT NULL,
    status       TEXT NOT NULL,
    amount_cents BIGINT NOT NULL,
    items        JSONB NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS delivery_events (
    id         TEXT PRIMARY KEY,
    user_id    TEXT NOT NULL,
    points     BIGINT NOT NULL CHECK (points > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS user_bonuses (
    user_id TEXT PRIMARY KEY,
    points  BIGINT NOT NULL DEFAULT 0 CHECK (points >= 0)
);

CREATE TABLE IF NOT EXISTS outbox_events (
    id         TEXT PRIMARY KEY,
    order_id   TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at    TIMESTAMPTZ
)`

type Item struct {
	SKU        string `json:"sku"`
	Qty        int    `json:"qty"`
	PriceCents int64  `json:"price_cents"`
}

type OrderRequest struct {
	UserID string `json:"user_id"`
	Items  []Item `json:"items"`
}

type app struct {
	db           *pgxpool.Pool
	paymentURL   string
	inventoryURL string
	eventsURL    string
}

func main() {
	a := &app{
		paymentURL:   strings.TrimRight(mustEnv("PAYMENT_URL"), "/"),
		inventoryURL: strings.TrimRight(mustEnv("INVENTORY_URL"), "/"),
		eventsURL:    strings.TrimRight(os.Getenv("EVENTS_URL"), "/"),
	}
	db, err := connect(mustEnv("DATABASE_URL"))
	if err != nil {
		slog.Error("Р±Р°Р·Р°", "err", err)
		os.Exit(1)
	}
	a.db = db

	mux := http.NewServeMux()
	go func() {
		a.publishPendingEvents()

		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for range ticker.C {
			a.publishPendingEvents()
		}
	}()
	mux.HandleFunc("POST /webhooks/delivery", a.handleDelivery)
	mux.HandleFunc("GET /users/{id}/bonus", a.getBonus)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("POST /orders", a.createOrder)
	mux.HandleFunc("GET /orders/{id}", a.getOrder)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	slog.Info("orders: СЃР»СѓС€Р°СЋ", "port", port)
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		slog.Error("server", "err", err)
		os.Exit(1)
	}
}

// connect Р¶РґС‘С‚ Р±Р°Р·Сѓ: РѕРЅР° РјРѕР¶РµС‚ РїРѕРґРЅРёРјР°С‚СЊСЃСЏ РґРѕР»СЊС€Рµ СЃРµСЂРІРёСЃР°.
func connect(url string) (*pgxpool.Pool, error) {
	var lastErr error
	for range 60 {
		db, err := pgxpool.New(context.Background(), url)
		if err == nil {
			if _, err = db.Exec(context.Background(), schema); err == nil {
				return db, nil
			}
			db.Close()
		}
		lastErr = err
		slog.Info("Р±Р°Р·Р° РЅРµРґРѕСЃС‚СѓРїРЅР°, Р¶РґСѓ", "err", err)
		time.Sleep(time.Second)
	}
	return nil, lastErr
}

func (a *app) createOrder(w http.ResponseWriter, r *http.Request) {
	var req OrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || !valid(req) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad_request"})
		return
	}

	id := newID()
	var amount int64
	for _, it := range req.Items {
		amount += int64(it.Qty) * it.PriceCents
	}

	items, err := json.Marshal(req.Items)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal_error"})
		return
	}

	ctx := r.Context()

	// РЎРЅР°С‡Р°Р»Р° СЃРѕС…СЂР°РЅСЏРµРј Р·Р°РєР°Р·: РґР°Р¶Рµ РїСЂРё СЃР±РѕРµ РІРЅРµС€РЅРµРіРѕ СЃРµСЂРІРёСЃР°
	// РѕРЅ РѕСЃС‚Р°РЅРµС‚СЃСЏ РІ Р±Р°Р·Рµ СЃРѕ СЃС‚Р°С‚СѓСЃРѕРј pending.
	_, err = a.db.Exec(ctx,
		`INSERT INTO orders (id, user_id, status, amount_cents, items)
		 VALUES ($1, $2, 'pending', $3, $4)`,
		id, req.UserID, amount, items,
	)
	if err != nil {
		slog.Error("save pending order", "id", id, "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal_error"})
		return
	}

	// Р•СЃР»Рё СЂРµР·РµСЂРІРёСЂРѕРІР°РЅРёРµ РІСЂРµРјРµРЅРЅРѕ РЅРµРґРѕСЃС‚СѓРїРЅРѕ, СЃРѕС…СЂР°РЅСЏРµРј pending.
	if err := a.reserve(id, req.Items); err != nil {
		slog.Warn("reserve failed", "id", id, "error", err)
		writeJSON(w, http.StatusCreated, map[string]string{"id": id, "status": "pending"})
		return
	}

	declined, err := a.charge(id, amount)
	if err != nil {
		// РќРµР»СЊР·СЏ СѓРІРµСЂРµРЅРЅРѕ СЃС‡РёС‚Р°С‚СЊ РїР»Р°С‚С‘Р¶ РЅРµСѓСЃРїРµС€РЅС‹Рј:
		// РѕРїРµСЂР°С†РёСЏ РјРѕРіР»Р° Р·Р°РІРµСЂС€РёС‚СЊСЃСЏ, Р° РѕС‚РІРµС‚ РїРѕС‚РµСЂСЏС‚СЊСЃСЏ.
		slog.Warn("payment result unknown", "id", id, "error", err)
		writeJSON(w, http.StatusCreated, map[string]string{"id": id, "status": "pending"})
		return
	}

	status := "paid"
	if declined {
		a.release(id)
		status = "rejected"
	}

	if status == "paid" {
		tx, txErr := a.db.Begin(ctx)
		if txErr != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal_error"})
			return
		}

		_, txErr = tx.Exec(ctx,
			`UPDATE orders SET status = 'paid' WHERE id = $1`,
			id,
		)
		if txErr == nil {
			eventID := newID()
			_, txErr = tx.Exec(ctx,
				`INSERT INTO outbox_events (id, order_id) VALUES ($1, $2)`,
				eventID, id,
			)
		}

		if txErr != nil {
			_ = tx.Rollback(ctx)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal_error"})
			return
		}

		if txErr = tx.Commit(ctx); txErr != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal_error"})
			return
		}
	} else {
		_, err = a.db.Exec(ctx,
			`UPDATE orders SET status = $1 WHERE id = $2`,
			status, id,
		)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal_error"})
			return
		}
	}

	if err != nil {
		slog.Error("update order status", "id", id, "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal_error"})
		return
	}

	slog.Info("order", "id", id, "status", status)
	writeJSON(w, http.StatusCreated, map[string]string{"id": id, "status": status})
}

func (a *app) fail(w http.ResponseWriter, id string, err error) {
	slog.Error("POST /orders", "id", id, "err", err)
	writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal", "message": err.Error()})
}

func (a *app) getOrder(w http.ResponseWriter, r *http.Request) {
	var (
		id, status string
		amount     int64
	)
	err := a.db.QueryRow(r.Context(), `SELECT id, status, amount_cents FROM orders WHERE id = $1`,
		r.PathValue("id")).Scan(&id, &status, &amount)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not_found"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"id": id, "status": status, "amount_cents": amount})
}

type DeliveryEvent struct {
	ID          string `json:"id"`
	Type        string `json:"type"`
	UserID      string `json:"user_id"`
	BonusPoints int    `json:"bonus_points"`
}

func (a *app) handleDelivery(w http.ResponseWriter, r *http.Request) {
	var event DeliveryEvent
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	if event.ID == "" || event.Type != "delivery.confirmed" ||
		event.UserID == "" || event.BonusPoints < 1 {
		http.Error(w, "invalid delivery event", http.StatusBadRequest)
		return
	}

	tx, err := a.db.Begin(r.Context())
	if err != nil {
		http.Error(w, "database error", http.StatusInternalServerError)
		return
	}
	defer tx.Rollback(r.Context())

	tag, err := tx.Exec(r.Context(), `
        INSERT INTO delivery_events (id, user_id, points)
        VALUES ($1, $2, $3)
        ON CONFLICT (id) DO NOTHING
    `, event.ID, event.UserID, event.BonusPoints)
	if err != nil {
		http.Error(w, "database error", http.StatusInternalServerError)
		return
	}

	if tag.RowsAffected() == 0 {
		// РЎРѕР±С‹С‚РёРµ СѓР¶Рµ РѕР±СЂР°Р±РѕС‚Р°РЅРѕ: РїРѕРІС‚РѕСЂРЅРѕРµ РЅР°С‡РёСЃР»РµРЅРёРµ РЅРµ РЅСѓР¶РЅРѕ.
		w.WriteHeader(http.StatusOK)
		return
	}

	_, err = tx.Exec(r.Context(), `
        INSERT INTO user_bonuses (user_id, points)
        VALUES ($1, $2)
        ON CONFLICT (user_id)
        DO UPDATE SET points = user_bonuses.points + EXCLUDED.points
    `, event.UserID, event.BonusPoints)
	if err != nil {
		http.Error(w, "database error", http.StatusInternalServerError)
		return
	}

	if err := tx.Commit(r.Context()); err != nil {
		http.Error(w, "database error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (a *app) getBonus(w http.ResponseWriter, r *http.Request) {
	userID := r.PathValue("id")

	var points int64
	err := a.db.QueryRow(r.Context(), `
        SELECT points FROM user_bonuses WHERE user_id = $1
    `, userID).Scan(&points)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeJSON(w, http.StatusOK, map[string]int64{"points": 0})
			return
		}
		http.Error(w, "database error", http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusOK, map[string]int64{"points": points})
}

func (a *app) publishPendingEvents() {
	if a.eventsURL == "" {
		return
	}

	rows, err := a.db.Query(context.Background(),
		`SELECT id, order_id, created_at
		 FROM outbox_events
		 WHERE sent_at IS NULL
		 ORDER BY created_at
		 LIMIT 100`,
	)
	if err != nil {
		slog.Error("read outbox", "error", err)
		return
	}

	type pendingEvent struct {
		ID        string
		OrderID   string
		CreatedAt time.Time
	}

	var events []pendingEvent
	for rows.Next() {
		var event pendingEvent
		if err := rows.Scan(&event.ID, &event.OrderID, &event.CreatedAt); err != nil {
			slog.Error("read outbox row", "error", err)
			rows.Close()
			return
		}
		events = append(events, event)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		slog.Error("iterate outbox", "error", err)
		return
	}

	for _, event := range events {
		body := map[string]any{
			"id":          event.ID,
			"type":        "order.paid",
			"order_id":    event.OrderID,
			"occurred_at": event.CreatedAt.UTC().Format(time.RFC3339Nano),
		}

		code, err := a.call(http.MethodPost, a.eventsURL+"/events", body)
		if err != nil || code != http.StatusAccepted {
			slog.Warn("publish event failed", "event_id", event.ID, "status", code, "error", err)
			continue
		}

		_, err = a.db.Exec(context.Background(),
			`UPDATE outbox_events SET sent_at = now()
			 WHERE id = $1 AND sent_at IS NULL`,
			event.ID,
		)
		if err != nil {
			slog.Error("mark event sent", "event_id", event.ID, "error", err)
		}
	}
}

func (a *app) reserve(id string, items []Item) error {
	type resvItem struct {
		SKU string `json:"sku"`
		Qty int    `json:"qty"`
	}
	ri := make([]resvItem, 0, len(items))
	for _, it := range items {
		ri = append(ri, resvItem{it.SKU, it.Qty})
	}
	code, err := call(http.MethodPost, a.inventoryURL+"/reservations", map[string]any{"order_id": id, "items": ri})
	if err != nil {
		return err
	}
	if code != http.StatusOK && code != http.StatusCreated {
		return fmt.Errorf("inventory: %d", code)
	}
	return nil
}

func (a *app) release(id string) {
	_, _ = call(http.MethodDelete, a.inventoryURL+"/reservations/"+id, nil)
}

var errPayment = errors.New("payment")

// charge СЃРїРёСЃС‹РІР°РµС‚ РґРµРЅСЊРіРё; declined вЂ” Р±Р°РЅРє РѕС‚РєР°Р·Р°Р».
func (a *app) charge(id string, amount int64) (declined bool, err error) {
	declined, err = a.chargeOnce(id, amount)
	if err == nil {
		return declined, nil
	}

	// Ответ мог потеряться уже после успешного списания.
	// Повторяем запрос с тем же Idempotency-Key.
	slog.Warn("payment result uncertain; retrying idempotently", "id", id, "err", err)

	declined, retryErr := a.chargeOnce(id, amount)
	if retryErr == nil {
		return declined, nil
	}

	// Не утверждаем, что платёж отклонён, если результат неизвестен.
	return false, fmt.Errorf("payment result unknown: first attempt: %v; retry: %w", err, retryErr)
}

func (a *app) chargeOnce(id string, amount int64) (bool, error) {
	code, err := callWithHeaders(http.MethodPost, a.paymentURL+"/payments", map[string]any{
		"order_id":     id,
		"amount_cents": amount,
		"currency":     "RUB",
	}, map[string]string{
		"Idempotency-Key": id,
	})
	if err != nil {
		return false, err
	}
	switch code {
	case http.StatusOK, http.StatusCreated:
		return false, nil
	case http.StatusPaymentRequired:
		return true, nil
	}
	return false, fmt.Errorf("%w: %d", errPayment, code)
}

func call(method, url string, body any) (int, error) {
	return callWithHeaders(method, url, body, nil)
}

func callWithHeaders(method, url string, body any, headers map[string]string) (int, error) {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, err
		}
		rd = bytes.NewReader(b)
	}

	req, err := http.NewRequest(method, url, rd)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	for key, value := range headers {
		req.Header.Set(key, value)
	}

	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, nil
}

func valid(r OrderRequest) bool {
	if r.UserID == "" || len(r.Items) == 0 {
		return false
	}
	for _, it := range r.Items {
		if it.SKU == "" || it.Qty < 1 || it.PriceCents < 0 {
			return false
		}
	}
	return true
}

func newID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func mustEnv(k string) string {
	v := os.Getenv(k)
	if v == "" {
		slog.Error("РЅРµ Р·Р°РґР°РЅР° РїРµСЂРµРјРµРЅРЅР°СЏ РѕРєСЂСѓР¶РµРЅРёСЏ", "name", k)
		os.Exit(1)
	}
	return v
}

func (a *app) call(method, url string, body any) (int, error) {
	var reader io.Reader

	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return 0, err
		}
		reader = bytes.NewReader(data)
	}

	req, err := http.NewRequest(method, url, reader)
	if err != nil {
		return 0, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	return resp.StatusCode, nil
}
