package main

import (
	"DHT-2022/src/chord"
	"fmt"
)

/* In this file, you should implement function "NewNode" and
 * a struct which implements the interface "dhtNode".
 */

func NewNode(port int) dhtNode {
	node := new(chord.ChordNode)
	node.Initialize(GetLocalAddress() + ":" + fmt.Sprint(port))
	return node
}
