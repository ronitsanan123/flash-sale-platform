package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	base := os.Getenv("FLASHSALE_URL")
	if base == "" {
		base = "http://127.0.0.1:8080"
	}
	token := os.Getenv("ADMIN_TOKEN")
	if token == "" {
		fmt.Fprintln(os.Stderr, "ADMIN_TOKEN is required")
		os.Exit(2)
	}
	client := &http.Client{Timeout: 5 * time.Second}
	cmd := os.Args[1]
	var method, path string
	var body any
	switch cmd {
	case "health":
		method = http.MethodGet
		path = "/healthz"
	case "pause", "resume":
		if len(os.Args) != 3 {
			usage()
			os.Exit(2)
		}
		method = http.MethodPost
		path = "/admin/queue/" + os.Args[2] + "/pause"
		body = map[string]bool{"paused": cmd == "pause"}
	case "rate":
		if len(os.Args) != 4 {
			usage()
			os.Exit(2)
		}
		rate, err := strconv.ParseInt(os.Args[3], 10, 64)
		if err != nil || rate < 0 {
			fmt.Fprintln(os.Stderr, "rate must be a non-negative integer")
			os.Exit(2)
		}
		method = http.MethodPost
		path = "/admin/queue/" + os.Args[2] + "/rate"
		body = map[string]int64{"rate": rate}
	default:
		usage()
		os.Exit(2)
	}
	var reader *bytes.Reader
	if body == nil {
		reader = bytes.NewReader(nil)
	} else {
		b, _ := json.Marshal(body)
		reader = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, strings.TrimRight(base, "/")+path, reader)
	if err != nil {
		panic(err)
	}
	req.Header.Set("X-Admin-Token", token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		fmt.Fprintln(os.Stderr, "operation failed:", resp.Status)
		os.Exit(1)
	}
	var v any
	_ = json.NewDecoder(resp.Body).Decode(&v)
	_ = json.NewEncoder(os.Stdout).Encode(v)
}
func usage() {
	fmt.Fprintln(os.Stderr, "usage: flashctl health | pause ITEM | resume ITEM | rate ITEM RPS")
}
