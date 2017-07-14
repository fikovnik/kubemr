package job

import (
	"github.com/onrik/logrus/filename"
	log "github.com/sirupsen/logrus"
)

//Init adds log filename hook
func Init() {
	filenameHook := filename.NewHook()
	log.AddHook(filenameHook)
}
