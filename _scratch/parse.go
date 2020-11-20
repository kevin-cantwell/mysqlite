package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/liquidata-inc/vitess/go/vt/sqlparser"
)

func main() {
	stmt, err := sqlparser.Parse(os.Args[1])
	if err != nil {
		panic(err)
	}
	b, _ := json.Marshal(stmt)
	fmt.Println(string(b))
}

/*
	SELECT * FROM STREAM(foo), bar, baz
*/
