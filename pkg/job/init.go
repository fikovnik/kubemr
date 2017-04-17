package job

import (
	log "github.com/Sirupsen/logrus"
	"github.com/onrik/logrus/filename"
)

//Init adds log filename hook
func Init() {
	filenameHook := filename.NewHook()
	log.AddHook(filenameHook)
}
