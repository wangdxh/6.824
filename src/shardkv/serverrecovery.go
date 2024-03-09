package shardkv

import (
	"6.824/shardctrler"
	"fmt"
	"time"
)

func (kv *ShardKV) SendShardInfo(shardid int, configfrom int, configto int) {
	_, _isleader := kv.rf.GetState()
	if _isleader {
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
			for si := 0; si < len(servers); {
				_, _isleader = kv.rf.GetState()
				if kv.killed() || false == _isleader {
					return
				}
				args, configok := kv.GetProposeShard(shardid, configfrom, configto)
				if configok == false {
					// curconfig 变化了，这里直接返回,
					return
				}
				var reply ProposeShardReply
				ok := kv.sendRPC(shardid, configfrom, servers[si], "ShardKV.ProposeShard", args, &reply, Clerk_Server_Timeout)
				if ok && reply.Err == OK {
					break
				}
				si++
				si %= len(servers)
				if si == 0 {
					time.Sleep(100 * time.Millisecond)
				}
			}

			kv.mu.Lock()
			kv.reconfiginlock(shardid, configfrom, ShardPass)
			kv.mu.Unlock()
		} else {
			panic(" what happen")
		}
	}
}

func (kv *ShardKV) GetProposeShard(shardid int, configfrom int, configto int) (*ProposeShardArgs, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	val := kv.getShardInfo2(shardid)
	if val.CurConfigNum != configfrom {
		kv.DPrintf0(" shardid %d  config %d differ %d ", shardid, configfrom, val.CurConfigNum)
		return nil, false
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
	return &args, true
}

func (kv *ShardKV) ProposeShard(args *ProposeShardArgs, reply *ProposeShardReply) {
	_, isleader := kv.rf.GetState()
	reply.Err = OK
	reply.IsLeader = isleader
	if false == isleader {
		reply.Err = " i am not leader"
		return
	}
	for k, _ := range args.Kv {
		if key2shard(k) != args.ShardId {
			panic(" get bad shardid key")
		}
	}

	shardid := args.ShardId
	confignum := args.Confignumto
	gid := args.Gidto

	kv.mu.Lock()
	defer kv.mu.Unlock()

	val, _ := kv.getShardInfo(shardid)
	if len(kv.configs) <= 0 || kv.configs[len(kv.configs)-1].Num < confignum ||
		confignum > val.CurConfigNum {
		reply.Err = Err(fmt.Sprintf(" i am not ready  argscconfignum %d  myconfignum %d ", confignum, val.CurConfigNum))
		return
	}
	dstgid := kv.configs[confignum].GetGidfromShard(shardid)
	if gid != dstgid || gid != kv.gid {
		panic("ErrWrongGroup 这都能计算错？")
	}

	if confignum < val.CurConfigNum {
		kv.DPrintf0(" ProposeShard return ok old request ProposeShard oldcfg %d now cfg %d  curstate %s ", confignum, val.CurConfigNum, val.CurShardState.String())
		return
	}
	if val.CurState() > ShardPrePareing {
		kv.DPrintf0(" ProposeShard return ok old request ProposeShard oldcfg %d now cfg %d  curstate %s ", confignum, val.CurConfigNum, val.CurShardState.String())
		return
	}

	ok := kv.reconfigwithkvinlock(args.ShardId, args.Confignumto, ShardCurrent, args.Kv, args.ClerkSerial)
	if ok == true {
		reply.Err = OK
	} else {
		reply.Err = " reconfig error " // 等待下一次查询，收到current之后，就变成
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

func (kv *ShardKV) getShardInfo(shardid int) (ShardIdInfo, bool) {
	val, has := kv.mapshard[shardid]
	if val.ClerkToSerialNum == nil {
		val.ClerkToSerialNum = make(map[int]int)
		kv.mapshard[shardid] = val
	}
	return val, has
}
func (kv *ShardKV) getShardInfo2(shardid int) ShardIdInfo {
	val, _ := kv.mapshard[shardid]
	if val.ClerkToSerialNum == nil {
		val.ClerkToSerialNum = make(map[int]int)
		kv.mapshard[shardid] = val
	}
	return val
}

func (kv *ShardKV) advanceShardInfo(shardid int, confignum int, state ShardState) {
	val, _ := kv.getShardInfo(shardid)

	kv.DPrintf0("advanceShardInfo shardid %d config %d to config %d  state %s to  %s ", shardid, val.CurConfigNum, confignum, val.CurShardState.String(), state.String())
	if confignum < val.CurConfigNum {
		panic(fmt.Sprintf(" config back ??  new %d < %d ", confignum, val.CurConfigNum))
	}
	val.CurConfigNum = confignum
	val.CurShardState = state
	kv.mapshard[shardid] = val
}

func (kv *ShardKV) reconfiginlock(shardid int, confignum int, state ShardState) bool {
	return kv.reconfigwithkvinlock(shardid, confignum, state, nil, nil)
}

func (kv *ShardKV) reconfigwithkvinlock(shardid int, confignum int, state ShardState, kvshard map[string]string, clerkinfo map[int]int) bool {
	val := kv.getShardInfo2(shardid)
	if val.CurConfigNum == confignum && val.CurShardState > state {
		panic(fmt.Sprintf(" some thing no right  %d  %d  %s %s", val.CurConfigNum, confignum, val.CurShardState, state))
	}
	op := Op{
		Optype:     OpReConfig,
		ShardState: state,
		SKv:        kvshard,
		SClerk:     clerkinfo,
		Meta: MetaInfo{
			ShardId:   shardid,
			ConfigNum: confignum,
		},
		Config: kv.configs[confignum],
	}
	kv.mu.Unlock()

	_index, _term, ok := kv.rf.Start(op)
	kv.DPrintf0(" start reconfiginlock shardid %d confignum %d return  index %d ok %v", shardid, confignum, _index, ok)
	if !ok {
		kv.mu.Lock()
		return false
	}

	ok = kv.waitforconfigsatechange(shardid, confignum, state, _term)
	kv.mu.Lock()
	return ok
}
func (kv *ShardKV) waitforconfigsatechange(shardid int, confignum int, state ShardState, oldterm int) bool {
	start := time.Now()
	for {
		time.Sleep(30 * time.Millisecond)

		termnow, isleader := kv.rf.GetState()
		if oldterm != termnow || isleader == false {
			return false
		}
		kv.mu.Lock()
		val := kv.getShardInfo2(shardid)
		if val.CurConfigNum > confignum {
			kv.mu.Unlock()
			return true
		}
		if val.CurConfigNum == confignum && val.CurState() >= state {
			kv.mu.Unlock()
			return true
		}

		kv.mu.Unlock()
		if time.Now().Unix()-start.Unix() > 10 {
			kv.DPrintf0(" shardid %d confignum %d startop opreconfig to state %s too long  cursharinfo : %v", shardid, confignum, state.String(), val)
			time.Sleep(1 * time.Second)
		}
	}
}
func (kv *ShardKV) advancedconfig() {
	for inx := 0; inx < shardctrler.NShards; inx++ {
		go func(shardid int) {
			for kv.killed() == false {
				kv.mu.Lock()
				_term, isleader := kv.rf.GetState()
				val := kv.getShardInfo2(shardid)

				if isleader && len(kv.configs) > 0 && kv.configs[len(kv.configs)-1].Num > val.CurConfigNum {
					newcfg := kv.configs[val.CurConfigNum+1]
					cfg := kv.configs[val.CurConfigNum]
					kv.DPrintf0(" advanceconfig shradid %d config %d nextconfig %d curconfig %d  state %s", shardid, cfg.Num, newcfg.Num,
						val.CurConfigNum, val.CurShardState.String())
					if newcfg.Num != cfg.Num+1 {
						panic(" en ????? config num err")
					}
					// shardid 0 mygid 1
					// gid 序列 0 -->1 --> 1---->2---- 1
					// gid 序列 0 2 3  3
					if newcfg.GetGidfromShard(shardid) == kv.gid {
						// 准备应征入伍
						if val.CurConfigNum == 0 {
							// 0 ---> 1
							kv.reconfiginlock(shardid, newcfg.Num, ShardCurrent)
						} else if cfg.GetGidfromShard(shardid) == kv.gid {
							// 当前gid 也是我   1 --> 1
							if val.CurState() == ShardCurrent {
								kv.reconfiginlock(shardid, newcfg.Num, ShardCurrent)
							} else if val.CurState() == ShardPrePareing {
								// 等待
								kv.mu.Unlock()
								kv.waitforconfigsatechange(shardid, val.CurConfigNum, ShardCurrent, _term)
								continue
							} else if val.CurState() == ShardPass || val.CurState() == ShardPrePass {
								panic(" today is me,  tomorrow is me why i passed ?")
							}
						} else {
							// 当前gid 不是我，2 ----->  1
							if val.CurState() == ShardPass {
								kv.reconfiginlock(shardid, newcfg.Num, ShardPrePareing)
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
								kv.reconfiginlock(shardid, cfg.Num, ShardPrePass)
							} else if val.CurState() == ShardPrePareing {
								// 我还在等待呢，等我 current了， 我再迁移
								kv.mu.Unlock()
								kv.waitforconfigsatechange(shardid, val.CurConfigNum, ShardCurrent, _term)
								continue
							} else if val.CurState() == ShardPrePass {
								// leader 执行迁移，先进入waited， 迁移完成，进入pass状态； 非leader 等待进入pass，然后删除状态
								// 如果leader迁移的时候挂了，新的leader 在prepass状态，会继续迁移;
								if isleader {
									kv.mu.Unlock()
									kv.SendShardInfo(shardid, cfg.Num, newcfg.Num)
									continue
								}
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

	kv.DPrintf0(" start advanceconfig ")
	defer func() {
		kv.DPrintf0(" exit advanceconfig ")
	}()

	for kv.killed() == false {
		kv.mu.Lock()
		i := kv.configs[len(kv.configs)-1].Num + 1
		kv.mu.Unlock()

		cfg := kv.ctrlerck.Query(i)
		kv.mu.Lock()
		// 这里不能使用i， 因为有可能测试 kv configs 在snapshot那里被更改
		if cfg.Num == kv.configs[len(kv.configs)-1].Num+1 {
			kv.configs = append(kv.configs, cfg)
			kv.mu.Unlock()
		} else {
			kv.mu.Unlock()
			time.Sleep(30 * time.Millisecond)
		}
	}
}
