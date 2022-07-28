package chord

import (
	"crypto/sha1"
	"math/big"
	"time"
)

const (
	NIL                = ""
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

type (
	Address   = string
	Identifer = *big.Int
	KeyType   = string
	ValueType = string
)

type DataPair struct {
	Key KeyType
	Val ValueType
}

func hash(key string) Identifer {
	hasher := sha1.New()
	hasher.Write([]byte(key))
	return new(big.Int).SetBytes(hasher.Sum(nil))
}

func pow2(x int) Identifer {
	return new(big.Int).Exp(big.NewInt(2), big.NewInt(int64(x)), nil)
}

func getStart(addr string, x int) Identifer {
	return new(big.Int).Mod(new(big.Int).Add(hash(addr), pow2(x)), RingSize)
}

func contain(id, lower, upper Identifer, bound string) bool {
	if lower.Cmp(upper) < 0 {
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

// func simplify(args ...string) []string {
// 	ret := make([]string, 0, len(args))
// 	for _, s := range args {
// 		if s != NIL {
// 			ret = append(ret, strings.Split(s, ":")[1])
// 		}
// 	}
// 	return ret
// }
