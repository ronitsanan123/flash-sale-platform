package inventory

import "errors"

var (
	ErrSoldOut        = errors.New("sold out")
	ErrTaken          = errors.New("unit already taken")
	ErrExpired        = errors.New("hold expired")
	ErrReplayMismatch = errors.New("hold replay does not match original request")
	ErrInvalidClaim   = errors.New("invalid shard claim")
	ErrHoldReleased   = errors.New("hold ID is terminally released")
	ErrHoldForfeited  = errors.New("hold ID is terminally forfeited")
	ErrHoldConfirmed  = errors.New("hold ID is already confirmed")
	ErrUnsafeForfeit  = errors.New("forfeit requires a safe, definitive payment disposition")
	ErrHoldNotFound   = errors.New("hold not found")
	ErrInvalidState   = errors.New("invalid hold state transition")
)
