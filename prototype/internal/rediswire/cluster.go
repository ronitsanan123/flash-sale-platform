// Package rediswire is a small RESP2 Redis Cluster client implemented with the
// Go standard library. It follows MOVED/ASK redirects and is intentionally
// narrow: inventory scripts need string, integer, bulk, array and hash replies.
package rediswire

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Error string

func (e Error) Error() string { return string(e) }

type Cluster struct {
	seeds   []string
	mu      sync.RWMutex
	slots   map[int]string
	next    uint64
	timeout time.Duration
	dialer  net.Dialer
}

func NewCluster(seeds []string, timeout time.Duration) (*Cluster, error) {
	clean := make([]string, 0, len(seeds))
	for _, s := range seeds {
		s = strings.TrimSpace(s)
		if s != "" {
			clean = append(clean, s)
		}
	}
	if len(clean) == 0 {
		return nil, fmt.Errorf("at least one Redis Cluster seed is required")
	}
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	return &Cluster{seeds: clean, slots: map[int]string{}, timeout: timeout, dialer: net.Dialer{Timeout: timeout, KeepAlive: 30 * time.Second}}, nil
}
func (c *Cluster) Do(ctx context.Context, routeKey string, args ...string) (any, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("Redis command is empty")
	}
	slot := HashSlot(routeKey)
	for attempt := 0; attempt < 6; attempt++ {
		addr := c.address(slot)
		value, err := c.doOnce(ctx, addr, args, false)
		if err == nil {
			return value, nil
		}
		var re Error
		if !errors.As(err, &re) {
			return nil, err
		}
		parts := strings.Fields(string(re))
		if len(parts) >= 3 && (parts[0] == "MOVED" || parts[0] == "ASK") {
			redirectSlot, _ := strconv.Atoi(parts[1])
			target := parts[2]
			if parts[0] == "MOVED" {
				c.mu.Lock()
				c.slots[redirectSlot] = target
				c.mu.Unlock()
			}
			value, err = c.doOnce(ctx, target, args, parts[0] == "ASK")
			if err == nil {
				return value, nil
			}
			continue
		}
		return nil, err
	}
	return nil, fmt.Errorf("Redis Cluster redirect limit exceeded for slot %d", slot)
}
func (c *Cluster) address(slot int) string {
	c.mu.RLock()
	addr := c.slots[slot]
	c.mu.RUnlock()
	if addr != "" {
		return addr
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	addr = c.seeds[c.next%uint64(len(c.seeds))]
	c.next++
	return addr
}
func (c *Cluster) doOnce(ctx context.Context, addr string, args []string, asking bool) (any, error) {
	conn, err := c.dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	deadline := time.Now().Add(c.timeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	_ = conn.SetDeadline(deadline)
	br := bufio.NewReader(conn)
	bw := bufio.NewWriter(conn)
	if asking {
		if err := writeCommand(bw, []string{"ASKING"}); err != nil {
			return nil, err
		}
		if _, err := readValue(br); err != nil {
			return nil, err
		}
	}
	if err := writeCommand(bw, args); err != nil {
		return nil, err
	}
	return readValue(br)
}
func writeCommand(w *bufio.Writer, args []string) error {
	if _, err := fmt.Fprintf(w, "*%d\r\n", len(args)); err != nil {
		return err
	}
	for _, a := range args {
		if _, err := fmt.Fprintf(w, "$%d\r\n%s\r\n", len(a), a); err != nil {
			return err
		}
	}
	return w.Flush()
}
func readLine(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	if len(line) < 2 || line[len(line)-2:] != "\r\n" {
		return "", fmt.Errorf("malformed RESP line")
	}
	return line[:len(line)-2], nil
}
func readValue(r *bufio.Reader) (any, error) {
	prefix, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	switch prefix {
	case '+':
		return readLine(r)
	case '-':
		line, e := readLine(r)
		if e != nil {
			return nil, e
		}
		return nil, Error(line)
	case ':':
		line, e := readLine(r)
		if e != nil {
			return nil, e
		}
		n, e := strconv.ParseInt(line, 10, 64)
		return n, e
	case '$':
		line, e := readLine(r)
		if e != nil {
			return nil, e
		}
		n, e := strconv.Atoi(line)
		if e != nil {
			return nil, e
		}
		if n < 0 {
			return nil, nil
		}
		buf := make([]byte, n+2)
		if _, e = io.ReadFull(r, buf); e != nil {
			return nil, e
		}
		if string(buf[n:]) != "\r\n" {
			return nil, fmt.Errorf("malformed RESP bulk string")
		}
		return string(buf[:n]), nil
	case '*':
		line, e := readLine(r)
		if e != nil {
			return nil, e
		}
		n, e := strconv.Atoi(line)
		if e != nil {
			return nil, e
		}
		if n < 0 {
			return nil, nil
		}
		out := make([]any, n)
		for i := 0; i < n; i++ {
			out[i], e = readValue(r)
			if e != nil {
				return nil, e
			}
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unsupported RESP prefix %q", prefix)
	}
}

// HashSlot implements Redis Cluster's CRC16/XMODEM slot algorithm including
// the first non-empty {...} hash tag.
func HashSlot(key string) int {
	tag := key
	if start := strings.IndexByte(key, '{'); start >= 0 {
		if end := strings.IndexByte(key[start+1:], '}'); end > 0 {
			tag = key[start+1 : start+1+end]
		}
	}
	var crc uint16
	for i := 0; i < len(tag); i++ {
		crc ^= uint16(tag[i]) << 8
		for j := 0; j < 8; j++ {
			if crc&0x8000 != 0 {
				crc = (crc << 1) ^ 0x1021
			} else {
				crc <<= 1
			}
		}
	}
	return int(crc % 16384)
}
