package main

//
// start the coordinator process, which is implemented
// in ../mr/coordinator.go
//
// go run mrcoordinator.go pg*.txt
//
// Please do not change this file.
//

import (
	"6.824/mr"
)
import "time"

func main() {
	//if len(os.Args) < 2 {
	//	fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
	//	os.Exit(1)
	//}
	path := []string{"D:/github/6.824/src/main/pg-grimm.txt", "D:\\github\\6.824\\src\\main\\pg-being_ernest.txt"}
	//strings := []string{"./pg-grimm.txt"}
	m := mr.MakeCoordinator(path, 10)
	for m.Done() == false {
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
}
