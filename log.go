package catena

import (
	"log"
	"os"
)

var logger = log.New(os.Stderr, "catena: ", log.Lshortfile)
