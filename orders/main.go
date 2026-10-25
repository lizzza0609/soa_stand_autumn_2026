// Orders — наивная реализация.
//
// Оформление заказа: зарезервировать товар в Inventory, списать деньги в
// Payment, записать заказ в базу. Пока Payment и Inventory работают без отказов, всё работает.
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
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
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
		slog.Error("база", "err", err)
		os.Exit(1)
	}
	a.db = db
	// Восстанавливаем заказы, оставшиеся pending после перезапуска.
	a.recoverPendingOrders()

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
	slog.Info("orders: слушаю", "port", port)
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		slog.Error("server", "err", err)
		os.Exit(1)
	}
}

// connect ждёт базу: она может подниматься дольше сервиса.
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
		slog.Info("база недоступна, жду", "err", err)
		time.Sleep(time.Second)
	}
	return nil, lastErr
}

func (a *app) recoverPendingOrders() {
	ctx := context.Background()

	rows, err := a.db.Query(ctx, `
        SELECT id, items, amount_cents
        FROM orders
        WHERE status = 'pending'
        ORDER BY created_at
    `)
	if err != nil {
		slog.Error("recover pending orders", "error", err)
		return
	}

	type pendingOrder struct {
		id     string
		items  []byte
		amount int64
	}

	var pending []pendingOrder

	for rows.Next() {
		var order pendingOrder
		if err := rows.Scan(&order.id, &order.items, &order.amount); err != nil {
			slog.Error("read pending order", "error", err)
			rows.Close()
			return
		}
		pending = append(pending, order)
	}

	if err := rows.Err(); err != nil {
		slog.Error("iterate pending orders", "error", err)
		rows.Close()
		return
	}
	rows.Close()

	for _, order := range pending {
		var items []Item
		if err := json.Unmarshal(order.items, &items); err != nil {
			slog.Error("decode pending order", "id", order.id, "error", err)
			continue
		}

		go a.processOrder(order.id, items, order.amount)
	}

	slog.Info("pending orders recovery started", "count", len(pending))
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

	// Сначала сохраняем заказ: даже при сбое внешнего сервиса
	// он останется в базе со статусом pending.
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

	go a.processOrder(id, req.Items, amount)

	writeJSON(w, http.StatusCreated, map[string]string{
		"id":     id,
		"status": "pending",
	})
}

func (a *app) processOrder(id string, items []Item, amount int64) {
	ctx := context.Background()

	// Ограничиваем частоту обращений к Inventory и Payment.
	// Не освобождаем резерв и не помечаем заказ rejected при неизвестном результате оплаты.
	for attempt := 0; attempt < 60; attempt++ {
		if attempt > 0 {
			time.Sleep(2 * time.Second)
		}

		var status string
		err := a.db.QueryRow(ctx,
			`SELECT status FROM orders WHERE id = $1`, id,
		).Scan(&status)
		if err != nil {
			slog.Warn("read order status", "id", id, "error", err)
			continue
		}
		if status != "pending" {
			return
		}

		if err := a.reserve(id, items); err != nil {
			slog.Warn("reserve failed; will retry", "id", id, "attempt", attempt+1, "error", err)
			continue
		}

		declined, err := a.charge(id, amount)
		if err != nil {
			slog.Warn("payment result unknown; will retry", "id", id, "attempt", attempt+1, "error", err)
			continue
		}

		if declined {
			a.release(id)
			_, err = a.db.Exec(ctx,
				`UPDATE orders SET status = 'rejected'
 WHERE id = $1 AND status = 'pending'`, id)
			if err != nil {
				slog.Error("reject order", "id", id, "error", err)
			}
			return
		}

		tx, err := a.db.Begin(ctx)
		if err != nil {
			slog.Error("begin payment transaction", "id", id, "error", err)
			continue
		}

		_, err = tx.Exec(ctx,
			`UPDATE orders SET status = 'paid'
 WHERE id = $1 AND status = 'pending'`, id)
		if err != nil {
			_ = tx.Rollback(ctx)
			slog.Error("mark order paid", "id", id, "error", err)
			continue
		}

		eventID := newID()
		_, err = tx.Exec(ctx,
			`INSERT INTO outbox_events (id, order_id) VALUES ($1, $2)`,
			eventID, id)
		if err != nil {
			_ = tx.Rollback(ctx)
			slog.Error("create outbox event", "id", id, "error", err)
			continue
		}

		if err := tx.Commit(ctx); err != nil {
			slog.Error("commit paid order", "id", id, "error", err)
			continue
		}
		return
	}

	slog.Warn("order remains pending after retry limit", "id", id)
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
		// Событие уже обработано: повторное начисление не нужно.
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

var (
	paymentSlots = make(chan struct{}, 8)
	paymentMu    sync.Mutex
	paymentUntil time.Time
	paymentProbe bool
)

func (a *app) charge(id string, amount int64) (bool, error) {
	paymentMu.Lock()
	now := time.Now()

	if now.Before(paymentUntil) {
		paymentMu.Unlock()
		return false, fmt.Errorf("payment circuit open")
	}

	isProbe := !paymentUntil.IsZero()
	if isProbe {
		if paymentProbe {
			paymentMu.Unlock()
			return false, fmt.Errorf("payment probe in progress")
		}
		paymentProbe = true
	}
	paymentMu.Unlock()

	select {
	case paymentSlots <- struct{}{}:
		defer func() { <-paymentSlots }()
	default:
		if isProbe {
			paymentMu.Lock()
			paymentProbe = false
			paymentMu.Unlock()
		}
		return false, fmt.Errorf("payment concurrency limit reached")
	}

	paymentMu.Lock()
	if time.Now().Before(paymentUntil) && !isProbe {
		paymentMu.Unlock()
		return false, fmt.Errorf("payment circuit open")
	}
	paymentMu.Unlock()

	declined, err := a.chargeOnce(id, amount)
	if err == nil {
		if isProbe {
			paymentMu.Lock()
			paymentUntil = time.Time{}
			paymentProbe = false
			paymentMu.Unlock()
		}
		return declined, nil
	}

	// Сетевой тайм-аут: результат платежа неизвестен.
	// Не создаём немедленную волну повторных запросов.
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		paymentMu.Lock()
		paymentUntil = time.Now().Add(10 * time.Second)
		paymentProbe = false
		paymentMu.Unlock()
		return false, fmt.Errorf("payment timeout; retry later: %w", err)
	}

	// HTTP-ошибки, например 500, повторяем с тем же ключом.
	slog.Warn("payment result uncertain; retrying idempotently",
		"id", id, "err", err)

	declined, retryErr := a.chargeOnce(id, amount)
	if retryErr == nil {
		if isProbe {
			paymentMu.Lock()
			paymentUntil = time.Time{}
			paymentProbe = false
			paymentMu.Unlock()
		}
		return declined, nil
	}

	if errors.As(retryErr, &netErr) && netErr.Timeout() {
		paymentMu.Lock()
		paymentUntil = time.Now().Add(10 * time.Second)
		paymentProbe = false
		paymentMu.Unlock()
	} else if isProbe {
		paymentMu.Lock()
		paymentUntil = time.Time{}
		paymentProbe = false
		paymentMu.Unlock()
	}

	return false, fmt.Errorf(
		"payment result unknown: first attempt: %v; retry: %w",
		err, retryErr,
	)
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

	client := &http.Client{Timeout: 5 * time.Second}
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
		slog.Error("не задана переменная окружения", "name", k)
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
