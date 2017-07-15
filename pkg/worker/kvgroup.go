package worker

import (
	"bufio"
	"io"
	"strings"
)

//Group holds individual key/value-chan
type Group struct {
	Key  string
	Vals chan []byte
}

//KVGroup groups continous items by keys in a pre-sorted reader
func KVGroup(input io.Reader, g chan *Group, sep string) {
	scanner := bufio.NewScanner(input)
	//g := make(chan *Groups)
	group := &Group{Key: ""}
	for scanner.Scan() {
		byt := scanner.Text()
		splitted := strings.SplitN(byt, sep, 2)
		k := splitted[0]
		v := []byte(splitted[1])
		if group.Key == "" {
			//First grouping...
			group = &Group{Key: k, Vals: make(chan []byte)}
			g <- group
		} else if group.Key != k {
			//Had previous group, now making new...
			close(group.Vals)
			group = &Group{Key: k, Vals: make(chan []byte)}
			g <- group
		}
		group.Vals <- v
	}
	if group.Key != "" {
		//We ever had a group means last is still unclosed...
		close(group.Vals)
	}
	close(g)
}
