package catena

import (
	"log"
	"os"
)

var logFileName = "LOG"

var logger = log.New(os.Stderr, "catena: ", log.Lshortfile|log.Lmicroseconds)
