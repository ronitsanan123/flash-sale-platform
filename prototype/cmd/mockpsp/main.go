package main

import (
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"flashsale-platform/prototype/internal/psp"
)

func main() {
	addr := os.Getenv("HTTP_ADDR")
	if addr == "" {
		addr = ":8090"
	}
	mock := psp.NewMock()
	webhookURL := os.Getenv("PSP_WEBHOOK_URL")
	webhookSecret := os.Getenv("PSP_WEBHOOK_SECRET")
	if webhookURL != "" || webhookSecret != "" {
		if err := mock.SetWebhook(webhookURL, []byte(webhookSecret)); err != nil {
			log.Fatal(err)
		}
	}
	srv := &http.Server{Addr: addr, Handler: mock, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second}
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	go func() { <-stop; _ = srv.Close() }()
	log.Printf("mock PSP listening on %s", addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
