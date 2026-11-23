package inventory

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

type memoryItem struct {
	all       map[string]struct{}
	available map[string]struct{}
	sold      map[string]struct{}
	holds     map[string]Hold
	events    []Event
}

// MemoryStore is a concurrency-safe local implementation used by unit tests
// and the laptop demo. It is not durable or distributed; RedisStore is the
// production-shaped adapter.
type MemoryStore struct {
	mu       sync.Mutex
	items    map[string]*memoryItem
	holdTTL  time.Duration
	payTTL   time.Duration
	now      func() time.Time
	eventSeq uint64
}

func NewMemoryStore(holdTTL, payTTL time.Duration) *MemoryStore {
	if holdTTL <= 0 {
		holdTTL = 5 * time.Minute
	}
	if payTTL <= 0 {
		payTTL = 2 * time.Minute
	}
	return &MemoryStore{items: make(map[string]*memoryItem), holdTTL: holdTTL, payTTL: payTTL, now: time.Now}
}

func (s *MemoryStore) item(name string) *memoryItem {
	it := s.items[name]
	if it == nil {
		it = &memoryItem{all: map[string]struct{}{}, available: map[string]struct{}{}, sold: map[string]struct{}{}, holds: map[string]Hold{}}
		s.items[name] = it
	}
	return it
}

func (s *MemoryStore) emit(it *memoryItem, typ string, h Hold) {
	s.eventSeq++
	it.events = append(it.events, Event{ID: fmt.Sprintf("%d", s.eventSeq), Type: typ, HoldID: h.ID, ItemID: h.Item, UnitID: h.Unit, UserID: h.User, CreatedAt: s.now().UTC()})
}

func (s *MemoryStore) Seed(ctx context.Context, item string, units []string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if item == "" {
		return fmt.Errorf("item is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	it := s.item(item)
	for _, unit := range units {
		if unit == "" {
			return fmt.Errorf("unit id is required")
		}
		it.all[unit] = struct{}{}
		if _, exists := it.sold[unit]; exists {
			continue
		}
		owned := false
		for _, h := range it.holds {
			if h.Unit == unit && h.State != HoldReleased {
				owned = true
				break
			}
		}
		if !owned {
			it.available[unit] = struct{}{}
		}
	}
	return nil
}

func (s *MemoryStore) Reserve(ctx context.Context, item, holdID, user, unit string) (Hold, error) {
	if err := ctx.Err(); err != nil {
		return Hold{}, err
	}
	if item == "" || holdID == "" || user == "" {
		return Hold{}, fmt.Errorf("item, hold id and user are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	it := s.item(item)
	if prior, ok := it.holds[holdID]; ok {
		if prior.User != user || (unit != "" && prior.Unit != unit) {
			return Hold{}, ErrReplayMismatch
		}
		switch prior.State {
		case HoldReleased:
			return Hold{}, ErrHoldReleased
		case HoldForfeited:
			return Hold{}, ErrHoldForfeited
		case HoldHeld, HoldPaying, HoldConfirmed:
			return prior, nil
		default:
			return Hold{}, ErrInvalidState
		}
	}
	var selected string
	if unit != "" {
		if _, ok := it.available[unit]; !ok {
			return Hold{}, ErrTaken
		}
		selected = unit
	} else {
		units := make([]string, 0, len(it.available))
		for id := range it.available {
			units = append(units, id)
		}
		if len(units) == 0 {
			return Hold{}, ErrSoldOut
		}
		sort.Strings(units)
		selected = units[0]
	}
	delete(it.available, selected)
	h := Hold{ID: holdID, Item: item, Unit: selected, User: user, State: HoldHeld, Deadline: s.now().Add(s.holdTTL)}
	it.holds[holdID] = h
	s.emit(it, "HoldCreated", h)
	return h, nil
}

func (s *MemoryStore) LockForPayment(ctx context.Context, item, holdID string) (Hold, error) {
	if err := ctx.Err(); err != nil {
		return Hold{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	it := s.item(item)
	h, ok := it.holds[holdID]
	if !ok {
		return Hold{}, ErrHoldNotFound
	}
	switch h.State {
	case HoldPaying, HoldConfirmed:
		return h, nil
	case HoldReleased:
		return Hold{}, ErrHoldReleased
	case HoldForfeited:
		return Hold{}, ErrHoldForfeited
	case HoldHeld:
		if !s.now().Before(h.Deadline) {
			return Hold{}, ErrExpired
		}
		h.State = HoldPaying
		h.Deadline = s.now().Add(s.payTTL)
		it.holds[holdID] = h
		s.emit(it, "CheckoutRequested", h)
		return h, nil
	default:
		return Hold{}, ErrInvalidState
	}
}

func (s *MemoryStore) Confirm(ctx context.Context, item, holdID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	it := s.item(item)
	h, ok := it.holds[holdID]
	if !ok {
		return ErrHoldNotFound
	}
	if h.State == HoldConfirmed {
		return nil
	}
	if h.State != HoldPaying {
		return ErrInvalidState
	}
	h.State = HoldConfirmed
	h.Deadline = time.Time{}
	it.holds[holdID] = h
	it.sold[h.Unit] = struct{}{}
	delete(it.available, h.Unit)
	s.emit(it, "HoldConfirmed", h)
	return nil
}

// Release returns a unit only for an expired HELD hold. The payment worker uses
// ReleaseAfterPayment after persisting a definitive decline/void in its fenced
// order repository.
func (s *MemoryStore) Release(ctx context.Context, item, holdID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	it := s.item(item)
	h, ok := it.holds[holdID]
	if !ok {
		return ErrHoldNotFound
	}
	if h.State == HoldReleased {
		return nil
	}
	if h.State != HoldHeld || s.now().Before(h.Deadline) {
		return ErrInvalidState
	}
	s.releaseLocked(it, h)
	return nil
}

func (s *MemoryStore) ReleaseAfterPayment(ctx context.Context, item, holdID string, proof ForfeitProof) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if proof != ForfeitProofPSPDeclined && proof != ForfeitProofPSPVoided {
		return ErrUnsafeForfeit
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	it := s.item(item)
	h, ok := it.holds[holdID]
	if !ok {
		return ErrHoldNotFound
	}
	if h.State == HoldReleased {
		return nil
	}
	if h.State != HoldPaying {
		return ErrInvalidState
	}
	s.releaseLocked(it, h)
	return nil
}

func (s *MemoryStore) releaseLocked(it *memoryItem, h Hold) {
	h.State = HoldReleased
	h.Deadline = time.Time{}
	it.holds[h.ID] = h
	if _, sold := it.sold[h.Unit]; !sold {
		it.available[h.Unit] = struct{}{}
	}
	s.emit(it, "HoldReleased", h)
}

func (s *MemoryStore) Forfeit(ctx context.Context, item, holdID string, proof ForfeitProof) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	it := s.item(item)
	h, ok := it.holds[holdID]
	if !ok {
		return ErrHoldNotFound
	}
	if h.State == HoldForfeited {
		return nil
	}
	if err := ValidateForfeitProof(h.State, proof); err != nil {
		return err
	}
	h.State = HoldForfeited
	h.Deadline = time.Time{}
	it.holds[holdID] = h
	delete(it.available, h.Unit)
	// Preserve a previously confirmed owner. Forfeiture quarantines only a
	// unit that is not already durably represented as sold.
	_ = it.sold[h.Unit] // Preserve any prior confirmed owner.
	s.emit(it, "HoldForfeited", h)
	return nil
}

func (s *MemoryStore) Reap(ctx context.Context, item string, shard, limit int) (released, overdue int, err error) {
	if err = ctx.Err(); err != nil {
		return
	}
	if limit <= 0 {
		limit = 100
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	it := s.item(item)
	now := s.now()
	ids := make([]string, 0)
	for id, h := range it.holds {
		if (h.State == HoldHeld || h.State == HoldPaying) && !h.Deadline.After(now) {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	if len(ids) > limit {
		ids = ids[:limit]
	}
	for _, id := range ids {
		h := it.holds[id]
		if h.State == HoldHeld {
			s.releaseLocked(it, h)
			released++
		} else if h.State == HoldPaying {
			s.emit(it, "HoldPayOverdue", h)
			overdue++
		}
	}
	return
}

func (s *MemoryStore) Get(ctx context.Context, item, holdID string) (Hold, bool, error) {
	if err := ctx.Err(); err != nil {
		return Hold{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	h, ok := s.item(item).holds[holdID]
	return h, ok, nil
}

func (s *MemoryStore) Availability(ctx context.Context, item string) (available, held, paying, confirmed, forfeited, total int, err error) {
	if err = ctx.Err(); err != nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	it := s.item(item)
	available = len(it.available)
	total = len(it.all)
	for _, h := range it.holds {
		switch h.State {
		case HoldHeld:
			held++
		case HoldPaying:
			paying++
		case HoldConfirmed:
			confirmed++
		case HoldForfeited:
			if _, alreadySold := it.sold[h.Unit]; !alreadySold {
				forfeited++
			}
		}
	}
	return
}

func (s *MemoryStore) Events(ctx context.Context, item string, after int, limit int) ([]Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	events := s.item(item).events
	start := after
	if start < 0 {
		start = 0
	}
	if start > len(events) {
		start = len(events)
	}
	end := len(events)
	if limit > 0 && start+limit < end {
		end = start + limit
	}
	out := append([]Event(nil), events[start:end]...)
	return out, nil
}
