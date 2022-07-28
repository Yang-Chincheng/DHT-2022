package main

import (
	"DHT-2022/src/kademlia"
	"fmt"
)

/* In this file, you should implement function "NewNode" and
 * a struct which implements the interface "dhtNode".
 */

func NewNode(port int) dhtNode {
	// Todo: create a node and then return it.
	return kademlia.NewKademliaNode(GetLocalAddress() + ":" + fmt.Sprint(port))
}

// Todo: implement a struct which implements the interface "dhtNode".
