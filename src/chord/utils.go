package chord

import (
	"crypto/sha1"
	"math/big"
	"time"
)

const (
	M           = 160
	B           = 5
	K           = 20
	Alpha       = 3
	pingAttempt = 4
	pingTimeOut = 500 * time.Millisecond
)

var (
	RingSize = pow2(M)
)

type dataPair struct {
	k, v string
}

func hash(key string) *big.Int {
	hasher := sha1.New()
	hasher.Write([]byte(key))
	return new(big.Int).SetBytes(hasher.Sum(nil))
}

func pow2(x int) *big.Int {
	return new(big.Int).Exp(big.NewInt(2), big.NewInt(int64(x)), nil)
}

func getStart(id *big.Int, x int) *big.Int {
	return new(big.Int).Mod(new(big.Int).Add(id, pow2(x)), RingSize)
}

func getInterval(id *big.Int, x int) (*big.Int, *big.Int) {
	return getStart(id, x), getStart(id, x+1)
}

func inside(id, lower, upper *big.Int, bound string) bool {
	if lower.Cmp(upper) <= 0 {
		switch bound {
		case "()":
			return id.Cmp(lower) > 0 && id.Cmp(upper) < 0
		case "(]":
			return id.Cmp(lower) > 0 && id.Cmp(upper) <= 0
		case "[)":
			return id.Cmp(lower) >= 0 && id.Cmp(upper) < 0
		case "[]":
			return id.Cmp(lower) >= 0 && id.Cmp(upper) <= 0
		}
	} else {
		switch bound {
		case "()":
			return id.Cmp(lower) > 0 || id.Cmp(upper) < 0
		case "(]":
			return id.Cmp(lower) > 0 || id.Cmp(upper) <= 0
		case "[)":
			return id.Cmp(lower) >= 0 || id.Cmp(upper) < 0
		case "[]":
			return id.Cmp(lower) >= 0 || id.Cmp(upper) <= 0
		}
	}
	return false
}
