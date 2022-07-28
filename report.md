# DHT-2022 project report

## Task

利用 `golang` 实现一个分布式哈希表 (Distributed Hash Table, DHT)，支持以下API

```go
type dhtNode interface {
	/* "Run" is called after calling "NewNode". */
	Run()

	/* "Create" and "Join" are called after calling "Run". */
	/* For a dhtNode, either "Create" or "Join" will be called, but not both. */
	Create()               /* Create a new network. */
	Join(addr string) bool /* Join an existing network. Return "true" if join succeeded and "false" if not. */

	/* Quit from the network it is currently in.*/
	/* "Quit" will not be called before "Create" or "Join". */
	/* For a dhtNode, "Quit" may be called for many times. */
	/* For a quited node, call "Quit" again should have no effect. */
	Quit()

	/* Chord offers a way of "normal" quitting. */
	/* For "force quit", the node quit the network without informing other nodes. */
	/* "ForceQuit" will be checked by TA manually. */
	ForceQuit()

	/* Check whether the node represented by the IP address is in the network. */
	Ping(addr string) bool

	/* Put a key-value pair into the network (if KEY is already in the network, cover it), or
	 * get a key-value pair from the network, or
	 * remove a key-value pair from the network.
	 */
	Put(key string, value string) bool /* Return "true" if success, "false" otherwise. */
	Get(key string) (bool, string)     /* Return "true" and the value if success, "false" otherwise. */
	Delete(key string) bool            /* Remove the key-value pair represented by KEY from the network. */
	/* Return "true" if remove successfully, "false" otherwise. */
}
```

## Protocol

### Chord

实现带数据备份的chord protocol, 以维护节点的强制下线

基于 `FindSuccessor` 进行节点定位与数据查找

使用 `FixFinger`, `Stablize`, `Notify` 等函数并发维护协议的有效性与鲁棒性

### Kademlia

支持 `FindNode`,  `FindValue`,  `Store`,  `Ping` 等API

实现了包括缓存动态expiration time、放宽kbucket分裂条件以加速查找等若干optimization

## Application

nFDM, a naive File Download Manager

基于DHT实现的小应用，通过Bittorrent协议进行文件分享，支持以下API

+ **bootstrap/quit** 加入或退出一个文件分享网络

+ **upload** 上传文件，并生成 `.torrent` 文件与磁力链接

+ **download** 通过`.torrent` 文件或磁力链接从网络中下载文件

## Reference

chord: [original paper](https://pdos.csail.mit.edu/papers/chord:sigcomm01/chord_sigcomm.pdf)

kademlia: [original paper](https://pdos.csail.mit.edu/~petar/papers/maymounkov-kademlia-lncs.pdf), [specification](http://xlattice.sourceforge.net/components/protocol/kademlia/specs.html), [other instructions](https://www.syncfusion.com/succinctly-free-ebooks/kademlia-protocol-succinctly)

bittorrent: [blog]([Building a BitTorrent client from the ground up in Go | Jesse Li](https://blog.jse.li/posts/torrent/))


