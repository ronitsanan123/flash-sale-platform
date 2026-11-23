package orders

import "time"

type OrderStatus string
type PaymentStatus string
type InventoryDisposition string

const (
	OrderPendingPayment OrderStatus          = "PENDING_PAYMENT"
	OrderConfirmed      OrderStatus          = "CONFIRMED"
	OrderCancelled      OrderStatus          = "CANCELLED"
	PaymentNotAttempted PaymentStatus        = "NOT_ATTEMPTED"
	PaymentIntent       PaymentStatus        = "INTENT"
	PaymentAuthorized   PaymentStatus        = "AUTHORIZED"
	PaymentCaptured     PaymentStatus        = "CAPTURED"
	PaymentFailed       PaymentStatus        = "FAILED"
	PaymentVoided       PaymentStatus        = "VOIDED"
	PaymentUnknown      PaymentStatus        = "UNKNOWN"
	DispositionNone     InventoryDisposition = "NONE"
	DispositionRelease  InventoryDisposition = "RELEASE"
	DispositionForfeit  InventoryDisposition = "FORFEIT"
	DispositionSold     InventoryDisposition = "SOLD"
)

type Order struct {
	ID                   string               `json:"order_id"`
	ItemID               string               `json:"item_id"`
	UnitID               string               `json:"unit_id"`
	UserID               string               `json:"user_id"`
	HomeRegion           string               `json:"home_region"`
	Status               OrderStatus          `json:"status"`
	InventoryDisposition InventoryDisposition `json:"inventory_disposition"`
	Version              int64                `json:"version"`
	FencingEpoch         int64                `json:"fencing_epoch"`
	CreatedAt            time.Time            `json:"created_at"`
	UpdatedAt            time.Time            `json:"updated_at"`
}

type Payment struct {
	OrderID                 string        `json:"order_id"`
	Status                  PaymentStatus `json:"status"`
	PSPChargeID             string        `json:"psp_charge_id,omitempty"`
	AmountMinor             int64         `json:"amount_minor"`
	Attempts                int           `json:"attempts"`
	LastAttemptFencingEpoch int64         `json:"last_attempt_fencing_epoch,omitempty"`
	UpdatedAt               time.Time     `json:"updated_at"`
}

type CheckoutEvent struct {
	OrderID      string `json:"order_id"`
	ItemID       string `json:"item_id"`
	UnitID       string `json:"unit_id"`
	UserID       string `json:"user_id"`
	HomeRegion   string `json:"home_region"`
	FencingEpoch int64  `json:"fencing_epoch"`
	AmountMinor  int64  `json:"amount_minor"`
	Currency     string `json:"currency"`
}

type Lease struct {
	HomeRegion string    `json:"home_region"`
	OwnerID    string    `json:"owner_id"`
	Epoch      int64     `json:"epoch"`
	ExpiresAt  time.Time `json:"lease_expires_at"`
}

type LeaseToken struct {
	HomeRegion string
	OwnerID    string
	Epoch      int64
}

type Aggregate struct {
	Order          Order
	Payment        Payment
	Created        bool
	UniqueConflict bool
}
