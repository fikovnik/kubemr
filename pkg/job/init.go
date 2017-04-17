package job

import (
	log "github.com/Sirupsen/logrus"
	"github.com/onrik/logrus/filename"
)

func init() {
	filenameHook := filename.NewHook()
	log.AddHook(filenameHook)

}
