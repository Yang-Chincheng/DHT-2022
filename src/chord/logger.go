package chord

import (
	"os"

	log "github.com/sirupsen/logrus"
)

func init() {
	file, _ := os.Create("debug.log")
	log.SetOutput(file)
	log.SetLevel(log.WarnLevel)
}

func logger(addr Address) *log.Entry {
	return log.WithField("addr", addr)
}

func errLogger(addr Address, err error) *log.Entry {
	return log.WithField("addr", addr).WithError(err)
}
