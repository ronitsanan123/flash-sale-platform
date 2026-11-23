// Package pgwire is a small libpq-backed PostgreSQL connection pool. It keeps
// the prototype dependency-free while using PostgreSQL's real transaction and
// row-lock semantics. Build requires CGO and libpq development headers.
package pgwire

/*
#cgo pkg-config: libpq
#include <libpq-fe.h>
#include <stdlib.h>
*/
import "C"

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"unsafe"
)

type Error struct {
	Message    string
	SQLState   string
	Constraint string
}

func (e *Error) Error() string { return e.Message }

// Result is a detached text-format result. NULL values are represented by nil.
type Result struct {
	Columns    []string
	Rows       [][]*string
	CommandTag string
}

func (r *Result) RowCount() int {
	if r == nil {
		return 0
	}
	return len(r.Rows)
}
func (r *Result) Value(row int, col string) (string, bool) {
	if r == nil || row < 0 || row >= len(r.Rows) {
		return "", false
	}
	for i, name := range r.Columns {
		if name == col && i < len(r.Rows[row]) && r.Rows[row][i] != nil {
			return *r.Rows[row][i], true
		}
	}
	return "", false
}
func (r *Result) String(row int, col string) string { v, _ := r.Value(row, col); return v }

type conn struct {
	mu  sync.Mutex
	ptr *C.PGconn
}

func connect(dsn string) (*conn, error) {
	cdsn := C.CString(dsn)
	defer C.free(unsafe.Pointer(cdsn))
	p := C.PQconnectdb(cdsn)
	if p == nil {
		return nil, fmt.Errorf("PQconnectdb returned nil")
	}
	if C.PQstatus(p) != C.CONNECTION_OK {
		msg := C.GoString(C.PQerrorMessage(p))
		C.PQfinish(p)
		return nil, fmt.Errorf("postgres connect: %s", strings.TrimSpace(msg))
	}
	return &conn{ptr: p}, nil
}
func (c *conn) close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ptr != nil {
		C.PQfinish(c.ptr)
		c.ptr = nil
	}
}
func (c *conn) exec(ctx context.Context, query string, args ...string) (*Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ptr == nil {
		return nil, fmt.Errorf("postgres connection is closed")
	}
	cq := C.CString(query)
	defer C.free(unsafe.Pointer(cq))
	var vals []*C.char
	for _, arg := range args {
		vals = append(vals, C.CString(arg))
	}
	defer func() {
		for _, v := range vals {
			C.free(unsafe.Pointer(v))
		}
	}()
	var pv **C.char
	if len(vals) > 0 {
		pv = (**C.char)(unsafe.Pointer(&vals[0]))
	}
	res := C.PQexecParams(c.ptr, cq, C.int(len(vals)), nil, pv, nil, nil, 0)
	if res == nil {
		return nil, fmt.Errorf("postgres query returned nil result: %s", strings.TrimSpace(C.GoString(C.PQerrorMessage(c.ptr))))
	}
	defer C.PQclear(res)
	status := C.PQresultStatus(res)
	if status != C.PGRES_COMMAND_OK && status != C.PGRES_TUPLES_OK {
		msg := strings.TrimSpace(C.GoString(C.PQresultErrorMessage(res)))
		state := ""
		constraint := ""
		field := C.PQresultErrorField(res, C.PG_DIAG_SQLSTATE)
		if field != nil {
			state = C.GoString(field)
		}
		field = C.PQresultErrorField(res, C.PG_DIAG_CONSTRAINT_NAME)
		if field != nil {
			constraint = C.GoString(field)
		}
		return nil, &Error{Message: msg, SQLState: state, Constraint: constraint}
	}
	out := &Result{CommandTag: strings.TrimSpace(C.GoString(C.PQcmdStatus(res)))}
	nf := int(C.PQnfields(res))
	nr := int(C.PQntuples(res))
	for i := 0; i < nf; i++ {
		out.Columns = append(out.Columns, C.GoString(C.PQfname(res, C.int(i))))
	}
	for ri := 0; ri < nr; ri++ {
		row := make([]*string, nf)
		for ci := 0; ci < nf; ci++ {
			if C.PQgetisnull(res, C.int(ri), C.int(ci)) == 1 {
				continue
			}
			v := C.GoString(C.PQgetvalue(res, C.int(ri), C.int(ci)))
			row[ci] = &v
		}
		out.Rows = append(out.Rows, row)
	}
	return out, nil
}

type Pool struct {
	available chan *conn
	all       []*conn
	mu        sync.Mutex
	closed    bool
}

func NewPool(dsn string, size int) (*Pool, error) {
	if strings.TrimSpace(dsn) == "" {
		return nil, fmt.Errorf("postgres DSN is required")
	}
	if size <= 0 {
		size = 4
	}
	p := &Pool{available: make(chan *conn, size)}
	for i := 0; i < size; i++ {
		c, err := connect(dsn)
		if err != nil {
			p.Close()
			return nil, err
		}
		p.all = append(p.all, c)
		p.available <- c
	}
	return p, nil
}
func (p *Pool) acquire(ctx context.Context) (*conn, error) {
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()
	if closed {
		return nil, fmt.Errorf("postgres pool is closed")
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case c := <-p.available:
		return c, nil
	}
}
func (p *Pool) release(c *conn) {
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()
	if !closed {
		p.available <- c
	}
}
func (p *Pool) Exec(ctx context.Context, q string, args ...string) (*Result, error) {
	c, e := p.acquire(ctx)
	if e != nil {
		return nil, e
	}
	defer p.release(c)
	return c.exec(ctx, q, args...)
}
func (p *Pool) Query(ctx context.Context, q string, args ...string) (*Result, error) {
	return p.Exec(ctx, q, args...)
}
func (p *Pool) WithTx(ctx context.Context, fn func(*Tx) error) error {
	c, e := p.acquire(ctx)
	if e != nil {
		return e
	}
	defer p.release(c)
	if _, e = c.exec(ctx, "BEGIN"); e != nil {
		return e
	}
	tx := &Tx{ctx: ctx, c: c}
	if e = fn(tx); e != nil {
		_, _ = c.exec(context.Background(), "ROLLBACK")
		return e
	}
	if _, e = c.exec(ctx, "COMMIT"); e != nil {
		_, _ = c.exec(context.Background(), "ROLLBACK")
		return e
	}
	return nil
}
func (p *Pool) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	all := append([]*conn(nil), p.all...)
	p.mu.Unlock()
	for _, c := range all {
		c.close()
	}
	return nil
}

type Tx struct {
	ctx context.Context
	c   *conn
}

func (t *Tx) Exec(q string, args ...string) (*Result, error)  { return t.c.exec(t.ctx, q, args...) }
func (t *Tx) Query(q string, args ...string) (*Result, error) { return t.Exec(q, args...) }
