package shardctrler

import (
	"fmt"
	"sort"
)

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

func (cg *Config) Clone() Config {
	newcfg := Config{
		Num:    cg.Num + 1,
		Shards: cg.Shards,
		Groups: make(map[int][]string),
	}
	for k, v := range cg.Groups {
		v2 := make([]string, len(v))
		copy(v2, v)
		newcfg.Groups[k] = v2
	}
	return newcfg
}

func (cfg *Config) GetGroupShards(groupid int) int {
	nums := 0
	for _, val := range cfg.Shards {
		if val == groupid {
			nums++
		}
	}
	return nums
}

// groups 会有 join 和 leave 如果只是hash 操作， shard % len(groups) 取模之后 分配的shardid 变化很大
// shards 到 group 的映射， 在各个raft server上，应该可以同等地复现，否则configuration 就不一致了
func AddConfig(old *Config, groups map[int][]string) Config {
	newcfg := old.Clone()
	defer func() {
		fmt.Printf(" config add %v  \r\n\t\t %v \r\n\t\t %v  \r\n", groups, *old, newcfg)
	}()
	// 怎么公平地分配 group，并且移动最少 尤其当group的id 会join 和 leave， 如果固定的映射，肯定是有问题的
	for k, v := range groups {
		if _, has := newcfg.Groups[k]; has {
			panic(" this can not happen")
		}
		newcfg.Groups[k] = v
	}
	/*if len(newcfg.Groups) > NShards {
		panic(" not good, too much groups")
	}*/
	// 每个一个 从前往后
	var gids []int
	for key, _ := range groups {
		gids = append(gids, key)
	}
	sort.Ints(gids)

	shardspergroup := NShards / len(newcfg.Groups)
	if shardspergroup == 0 {
		shardspergroup = 1
	}
	// 不是那么均衡，会多移动一个，不进行优化了
	newinx := 0
	for inx, gid := range newcfg.Shards {
		if gid == 0 || newcfg.GetGroupShards(gid) > shardspergroup {
			newinx %= len(gids)
			newcfg.Shards[inx] = gids[newinx]
			newinx += 1
		}
	}
	return newcfg
}

func LeaveConfig(old *Config, groups []int) Config {
	newcfg := old.Clone()
	defer func() {
		fmt.Printf(" config leave %v  \r\n\t\t %v \r\n\t\t %v  \r\n", groups, *old, newcfg)
	}()

	// 怎么公平地分配 group，并且移动最少 尤其当group的id 会join 和 leave， 如果固定的映射，肯定是有问题的
	for _, v := range groups {
		if _, has := newcfg.Groups[v]; !has {
			panic(fmt.Sprintf(" %d not in config % v", v, newcfg))
		}
		delete(newcfg.Groups, v)
	}
	if len(newcfg.Groups) == 0 {
		for inx, _ := range newcfg.Shards {
			newcfg.Shards[inx] = 0
		}
		return newcfg
	}

	var gidsTobeAllocated = make(map[int]int) // gid --> shards can be allocated to other gids
	var gidsmoved = make(map[int]int)         // gid --> moved nums

	// newcfg 剩余的groups，统计需要被 allocated的groups
	for key, _ := range newcfg.Groups {
		gidsTobeAllocated[key] = newcfg.GetGroupShards(key)
		gidsmoved[key] = 0
	}

	// 减去的 groups  就是需要被拆分的
	var gidsmore []int
	for _, key := range groups {
		gidsmore = append(gidsmore, key)
	}
	sort.Ints(gidsmore)
	//shardspergroup := NShards / len(newcfg.Groups)

	// 3  3,3,4  变成4  2，2，3，3
	//  0， 3，3，4  10/4 = 2
	//  2,2,2,4 ????
	//  0, 3,3,4   --->   1 3 3 3 ---> 2 3 3 2 or  2 2 3 3
	for _, gidmore := range gidsmore {
		for newcfg.GetGroupShards(gidmore) > 0 {
			// 找到最合适的gid
			fitestgid := -1
			{
				// 找到shards个数最少的； 相同多的，moved 少的优先
				var gids []int
				for gid, nums := range gidsTobeAllocated {
					if len(gids) == 0 {
						gids = append(gids, gid)
					} else {
						firgidnums := gidsTobeAllocated[gids[0]]
						if firgidnums == nums {
							gids = append(gids, gid)
						} else if firgidnums > nums {
							gids = []int{gid}
						}
					}
				}
				sort.Ints(gids)

				// 找到最小的移动次数的
				leastmovednums := 0xfffffff
				for _, gid := range gids {
					// 找到 moved 个数最小的， 次数相同，选第一个
					val := gidsmoved[gid]
					if val < leastmovednums {
						leastmovednums = val
						fitestgid = gid
					}
				}
				gidsTobeAllocated[fitestgid] = gidsTobeAllocated[fitestgid] + 1
				gidsmoved[fitestgid] = gidsmoved[fitestgid] + 1
			}
			// 从后往前进行移动，类似栈的删除，最前面的分配总是不变
			for inx := len(newcfg.Shards) - 1; inx >= 0; inx-- {
				if newcfg.Shards[inx] == gidmore {
					newcfg.Shards[inx] = fitestgid
					break
				}
			}
		}
	}
	return newcfg
}

func MoveConfig(old *Config, shardid int, gid int) Config {
	if shardid >= NShards || shardid < 0 {
		panic(" bad shardid ")
	}
	if _, has := old.Groups[gid]; !has {
		panic(" bad gid")
	}

	newcfg := old.Clone()
	newcfg.Shards[shardid] = gid
	return newcfg
}

const (
	OK = "OK"
)

type Err string

type ClerkSerial struct {
	ClerkId   int
	SerialNum int
}

type JoinArgs struct {
	Servers   map[int][]string // new GID -> servers mappings
	ClerkInfo ClerkSerial
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs      []int
	ClerkInfo ClerkSerial
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard     int
	GID       int
	ClerkInfo ClerkSerial
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num       int // desired config number
	ClerkInfo ClerkSerial
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}
