package chord

import (
	"crypto/sha1"
	"math/big"
	"time"
)

const (
	M                  = 160
	succListLen        = 5
	pingAttempt        = 4
	dialAttempt        = 3
	pingTimeOut        = 300 * time.Millisecond
	dialTimeOut        = 300 * time.Millisecond
	stablizePauseTime  = 100 * time.Millisecond
	fixfingerPauseTime = 100 * time.Millisecond
	maintainerNum      = 3
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

func getStart(addr string, x int) *big.Int {
	return new(big.Int).Mod(new(big.Int).Add(hash(addr), pow2(x)), RingSize)
}

func getInterval(addr string, x int) (*big.Int, *big.Int) {
	return getStart(addr, x), getStart(addr, x+1)
}

func contain(id, lower, upper *big.Int, bound string) bool {
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
