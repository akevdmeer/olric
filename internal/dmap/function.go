package dmap

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/protocol"
)

func (dm *DMap) Function(ctx context.Context, key string, function string, arg []byte) ([]byte, error) {
	e := newEnv(ctx)
	e.dmap = dm.name
	e.key = key
	e.timestamp = time.Now().UnixNano()
	e.value = arg
	e.function = function
	return dm.function(e, key, function, arg)
}

func (dm *DMap) function(e *env, key string, function string, arg []byte) ([]byte, error) {
	e.hkey = partitions.HKey(dm.name, e.key)
	member := dm.s.primary.PartitionByHKey(e.hkey).Owner()
	// We are on the partition owner. So we can call the function directly.
	if member.CompareByName(dm.s.rt.This()) {
		return dm.functionOnCluster(e)
	}

	// Redirect to the partition owner.
	cmd := protocol.NewFunction(dm.name, key, function, arg).Command(dm.s.ctx)

	rc := dm.s.client.Get(member.String())
	err := rc.Process(e.ctx, cmd)
	if err != nil {
		return nil, protocol.ConvertError(err)
	}

	value, err := cmd.Bytes()
	if err != nil {
		return nil, protocol.ConvertError(err)
	}

	return value, protocol.ConvertError(cmd.Err())
}

func (dm *DMap) functionOnCluster(f *env) ([]byte, error) {
	atomicKey := f.dmap + f.key
	dm.s.locker.Lock(atomicKey)
	defer func() {
		err := dm.s.locker.Unlock(atomicKey)
		if err != nil {
			dm.s.log.V(3).Printf("[ERROR] Failed to release the fine grained lock for key: %s on DMap: %s: %v", f.key, f.dmap, err)
		}
	}()

	if dm.config.functions[f.function] == nil {
		return nil, fmt.Errorf("function: %s is not registered", f.function)
	}

	var currentState []byte
	var ttl int64
	entry, err := dm.getOnCluster(f.hkey, f.key)
	if err != nil {
		if !errors.Is(err, ErrKeyNotFound) {
			dm.s.log.V(3).Printf("[ERROR] Failed to get key: %s on DMap: %s: %v", f.key, f.dmap, err)
			return nil, err
		}
	} else {
		currentState = entry.Value()
		ttl = entry.TTL()
	}

	newState, result, err := dm.config.functions[f.function](f.key, currentState, f.value)
	if err != nil {
		dm.s.log.V(3).Printf("[ERROR] Failed to call function: %s on DMap: %s: %v", f.function, f.dmap, err)
		return nil, err
	}

	// Put
	p := &env{
		function:  f.function,
		dmap:      dm.name,
		key:       f.key,
		hkey:      f.hkey,
		timestamp: f.timestamp,
		kind:      partitions.PRIMARY,
		value:     newState,
	}
	if ttl != 0 {
		p.timeout = time.Until(time.UnixMilli(ttl))
		p.putConfig = &PutConfig{
			HasPX: true,
			PX:    time.Until(time.UnixMilli(ttl)),
		}
	}
	err = dm.putOnCluster(p)
	if err != nil {
		dm.s.log.V(3).Printf("[ERROR] Failed to put the entry after function call: %v", err)
		return nil, err
	}

	return result, nil
}
