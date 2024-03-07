package shardkv

import (
	"6.824/shardctrler"
	"fmt"
	"time"
)

func (kv *ShardKV) SendShardInfo(shardid int, configfrom int, configto int) {

	_, _isleader := kv.rf.GetState()
	if kv.inited && _isleader {
		kv.mu.Lock()
		cfgfrom := kv.configs[configfrom]
		gidfrom := cfgfrom.Shards[shardid]

		cfgto := kv.configs[configto]
		gidto := cfgto.Shards[shardid]
		if gidfrom == gidto || gidfrom != kv.gid || gidto == kv.gid {
			panic(" big error")
		}
		kv.mu.Unlock()

		defer func() {
			kv.DPrintf0(" exit  SendShardInfo shardid %d confignum %d ", shardid, configfrom)
		}()

		if servers, has := cfgto.Groups[gidto]; has {
			for si := 0; si < len(servers); si++ {
				for {
					_, _isleader = kv.rf.GetState()
					if kv.killed() || false == _isleader {
						return
					}
					args := kv.GetProposeShard(shardid, configfrom, configto)
					var reply ProposeShardReply
					ok := kv.sendRPC(shardid, configfrom, servers[si], "ShardKV.ProposeShard", args, &reply, Clerk_Server_Timeout)
					if ok && reply.Err == OK {
						break
					} else if ErrWrongGroup == reply.Err {
						panic(" really bad ")
					}
					time.Sleep(300 * time.Millisecond)
				}
			}

			for si := 0; si < len(servers); si++ {
				for {
					_, _isleader = kv.rf.GetState()
					if kv.killed() || false == _isleader {
						// 退出发送
						return
					}
					args := CommitShardArgs{
						ShardId:       shardid,
						Gidfrom:       gidfrom,
						Confignumfrom: configfrom,
						Gidto:         gidto,
						Confignumto:   configto,
					}
					var reply CommitShardReply
					ok := kv.sendRPC(shardid, configfrom, servers[si], "ShardKV.CommitShard", &args, &reply, Clerk_Server_Timeout)
					if ok && reply.Err == OK {
						break
					} else if ErrWrongGroup == reply.Err {
						panic(" really bad ")
					}
					time.Sleep(300 * time.Millisecond)
				}
			}
			kv.DPrintf0(" shardid %d start go delete", shardid)
			kv.mu.Lock()
			kv.DPrintf0(" shardid %d enter go delete", shardid)
			kv.advanceShardInfo(shardid, configfrom, ShardPass)
			// delete shard info
			for k, _ := range kv.kv {
				if key2shard(k) == shardid {
					delete(kv.kv, k)
				}
			}
			// 我屮艸芔茻 这里delete 它干啥，状态全没了
			// delete(kv.mapshard, shardid)
			kv.DPrintf0(" delete shardid %d kvinfo when confignum %d ", shardid, configfrom)
			kv.mu.Unlock()
		}
	}
}

func (kv *ShardKV) GetProposeShard(shardid int, configfrom int, configto int) *ProposeShardArgs {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	val := kv.mapshard[shardid]
	if val.CurConfigNum != configfrom {
		panic(fmt.Sprintf(" shardid %d  config %d differ %d ", shardid, configfrom, val.CurConfigNum))
	}
	var args = ProposeShardArgs{
		ShardId:       shardid,
		Confignumfrom: val.CurConfigNum,
		Gidfrom:       kv.configs[val.CurConfigNum].GetGidfromShard(shardid),
		Confignumto:   configto,
		Gidto:         kv.configs[configto].GetGidfromShard(shardid),
		Kv:            make(map[string]string),
		ClerkSerial:   make(map[int]int),
	}
	for k, v := range kv.kv {
		if key2shard(k) == args.ShardId {
			args.Kv[k] = v
		}
	}
	// 为什么要拷贝，因为直接赋值，源可能会被修改
	for k, v := range val.ClerkToSerialNum {
		args.ClerkSerial[k] = v
	}
	return &args
}

func (kv *ShardKV) ProposeShard(args *ProposeShardArgs, reply *ProposeShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	err, needreconfig := kv.DealMeta(args.Confignumto, args.ShardId, args.Gidto)
	reply.Err = err
	if needreconfig {
		kv.DPrintf0("shardid %d getrpc ProposeShard kv:  %v", args.ShardId, args.Kv)
		// 处理增加的kv
		for k, v := range args.Kv {
			if key2shard(k) != args.ShardId {
				panic(" get bad shardid key")
			}
			kv.kv[k] = v
		}
		val := kv.mapshard[args.ShardId]
		val.ClerkToSerialNum = args.ClerkSerial
	}
	return
}

func (kv *ShardKV) DealMeta(argsconfignum, argsshardid, argsgid int) (Err, bool) {
	val, has := kv.mapshard[argsshardid]
	if has == false || len(kv.configs) <= 0 || kv.configs[len(kv.configs)-1].Num < argsconfignum {
		return " i am not ready ", false
	}
	gid := kv.configs[argsconfignum].GetGidfromShard(argsshardid)
	if gid != argsgid || gid != kv.gid {
		return ErrWrongGroup, false
	}
	if argsconfignum > val.CurConfigNum {
		return Err(fmt.Sprintf("i am not ready %d < %d ", val.CurConfigNum, argsconfignum)), false
		//panic(" 还没有 迁移完呢，你就变大了？")
	}
	if argsconfignum < val.CurConfigNum {
		// 经过种种判断，都是正确的，说明来的是一个 过去config num的消息， 可能迁移的时候，它崩溃了，重启之后leader更新，继续迁移，返回成功
		return OK, false
	}
	return OK, true
}

func (kv *ShardKV) CommitShard(args *CommitShardArgs, reply *CommitShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	err, needreconfig := kv.DealMeta(args.Confignumto, args.ShardId, args.Gidto)
	reply.Err = err
	if needreconfig {
		kv.reconfiginlock(args.ShardId, args.Confignumto, ShardCurrent)
	}
	return
}

func (kv *ShardKV) sendRPC(shardid int, confignum int, server string, rpccall string, args interface{}, reply interface{}, timeout int64) bool {
	retchn := make(chan bool)
	stopchn := make(chan int)
	defer func() {
		close(stopchn)
	}()

	go func(server string) {
		start := time.Now()
		ok := kv.make_end(server).Call(rpccall, args, reply)
		timems := time.Now().UnixMilli() - start.UnixMilli()
		kv.DPrintf0("shardid %d config %d send to server %s rpc %s return %v  reply: %v  speed %d ms isobselete %v", shardid, confignum,
			server, rpccall, ok, reply, timems, timems > timeout)
		select {
		case <-stopchn:
			{
				return
			}
		case retchn <- ok:
			{
				return
			}
		}
	}(server)

	select {
	case ok := <-retchn:
		{
			return ok
		}
	case <-time.After(time.Duration(timeout) * time.Millisecond):
		{
			return false
		}
	}
}

func (kv *ShardKV) advanceShardInfo(shardid int, confignum int, state ShardState) {
	val, _ := kv.mapshard[shardid]

	if nil == val.ClerkToSerialNum {
		val.ClerkToSerialNum = make(map[int]int)
	}

	kv.DPrintf0("advanceShardInfo shardid %d config %d to config %d  state %s to  %s ", shardid, val.CurConfigNum, confignum, val.CurShardState.String(), state.String())
	if confignum < val.CurConfigNum {
		panic(fmt.Sprintf(" config back ??  new %d < %d ", confignum, val.CurConfigNum))
	}
	val.CurConfigNum = confignum
	val.CurShardState = state
	kv.mapshard[shardid] = val
}

func (kv *ShardKV) reconfiginlock(shardid int, confignum int, state ShardState) {
	op := Op{
		Optype:     OpReConfig,
		ShardState: state,
		Meta: MetaInfo{
			ShardId:   shardid,
			ConfigNum: confignum,
		},
		Config: kv.configs[confignum],
	}
	kv.advanceShardInfo(shardid, confignum, ShardWaited)
	kv.mu.Unlock()

	kv.DPrintf0(" start reconfig shardid %d confignum %d   to %s ", shardid, confignum, state.String())
	_index, _, ok := kv.rf.Start(op)
	kv.DPrintf0(" start reconfig shardid %d confignum %d return  index %d ok %v", shardid, confignum, _index, ok)

	kv.mu.Lock()
	if ok {
		// 为什么将 advance 放到上面去，因为又出现了 start 先返回了，状态修改为pass， 然后又被改为waited的情况
		//kv.advanceShardInfo(shardid, confignum, ShardWaited)
	}

}

func (kv *ShardKV) advancedconfig() {
	for inx := 0; inx < shardctrler.NShards; inx++ {
		go func(shardid int) {
			for kv.killed() == false {
				kv.mu.Lock()
				_, isleader := kv.rf.GetState()
				val, _ := kv.mapshard[shardid]

				// 可以根据term，重新发起状态
				if isleader && len(kv.configs) > 0 && kv.configs[len(kv.configs)-1].Num > val.CurConfigNum && val.CurState() != ShardWaited {
					newcfg := kv.configs[val.CurConfigNum+1]
					cfg := kv.configs[val.CurConfigNum]
					// shardid 0 mygid 1
					// gid 序列 0 -->1 --> 1---->2---- 1
					// gid 序列 0 2 3  3
					if newcfg.GetGidfromShard(shardid) == kv.gid {
						// 准备应征入伍
						if val.CurConfigNum == 0 {
							// 0 ---> 1
							kv.reconfiginlock(shardid, newcfg.Num, ShardCurrent)
							// 就怕重新 leader 选举之后，新的leader还是自己 这个 reconfiginlock 消息没有收到，

						} else if cfg.GetGidfromShard(shardid) == kv.gid {
							// 当前gid 也是我   1 --> 1
							if val.CurState() == ShardCurrent {
								kv.reconfiginlock(shardid, newcfg.Num, ShardCurrent)
								//kv.advanceShardInfo(shardid, newcfg.Num, ShardCurrent) // 老的gid current， 新的gid 直接进入current
							} else if val.CurState() == ShardPrePareing {
								// 我还在等待呢，等我 current了， 我再迁移
							} else if val.CurState() == ShardPass {
								panic(" today is me,  tomorrow is me why i passed ?")
							}
						} else {
							// 当前gid 不是我，2 ----->  1
							if val.CurState() == ShardPass {
								kv.reconfiginlock(shardid, newcfg.Num, ShardPrePareing)
								//kv.advanceShardInfo(shardid, newcfg.Num, ShardPrePareing)
							}
						}
					} else {
						// 准备退役了
						if cfg.GetGidfromShard(shardid) == kv.gid {
							// 1----> 2  当前是我，需要放弃
							if val.CurState() == ShardCurrent {
								// 发送kv 迁移给 新的gid send rpc ..........
								// 这里好像不能发送，因为， 有些数据可能没有commit  糊涂啊
								// 当前的状态设置为pass， 但是不能删除，只有 apply pass之后，才能 迁移，和删除
								kv.reconfiginlock(shardid, cfg.Num, ShardPass)

							} else if val.CurState() == ShardPrePareing {
								// 我还在等待呢，等我 current了， 我再迁移
							} else if val.CurState() == ShardPass {
								// advance pass
								kv.reconfiginlock(shardid, newcfg.Num, ShardPass)
							}
						} else {
							// 0 --> 2--->3
							if val.CurConfigNum == 0 || val.CurState() == ShardPass {
								kv.reconfiginlock(shardid, newcfg.Num, ShardPass)
							}
						}
					}
					kv.mu.Unlock()
				} else {
					kv.mu.Unlock()
					time.Sleep(30 * time.Millisecond)
				}
			}
		}(inx)
	}
	i := 1
	for kv.killed() == false {
		cfg := kv.ctrlerck.Query(i)
		kv.mu.Lock()
		if cfg.Num == len(kv.configs) {
			kv.configs = append(kv.configs, cfg)
			kv.mu.Unlock()
			i++
		} else {
			kv.mu.Unlock()
			time.Sleep(30 * time.Millisecond)
		}
	}
}
