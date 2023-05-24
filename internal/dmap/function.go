package dmap

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/buraksezer/olric/internal/cluster/partitions"
)

func (dm *DMap) Function(ctx context.Context, key string, function string, arg []byte) ([]byte, error) {
	e := newEnv(ctx)
	e.dmap = dm.name
	e.key = key
	e.function = function
	e.value = arg
	return dm.function(e)
}

func (dm *DMap) function(e *env) ([]byte, error) {
	f, ok := dm.config.functions[e.function]
	if !ok {
		return nil, fmt.Errorf("function: %s is not registered", e.function)
	}

	atomicKey := e.dmap + e.key
	dm.s.locker.Lock(atomicKey)
	defer func() {
		err := dm.s.locker.Unlock(atomicKey)
		if err != nil {
			dm.s.log.V(3).Printf("[ERROR] Failed to release the fine grained lock for key: %s on DMap: %s: %v", e.key, e.dmap, err)
		}
	}()

	var currentState []byte
	var ttl int64
	entry, err := dm.getOnCluster(e.hkey, e.key)
	if err != nil {
		if !errors.Is(err, ErrKeyNotFound) {
			dm.s.log.V(3).Printf("[ERROR] Failed to get key: %s on DMap: %s: %v", e.key, e.dmap, err)
			return nil, err
		}
	} else {
		currentState = entry.Value()
		ttl = entry.TTL()
	}

	newState, result, err := f(e.key, currentState, e.value)
	if err != nil {
		dm.s.log.V(3).Printf("[ERROR] Failed to call function: %s on DMap: %s: %v", e.function, e.dmap, err)
		return nil, err
	}

	// Put
	p := &env{
		function:  e.function,
		dmap:      dm.name,
		key:       e.key,
		hkey:      e.hkey,
		timestamp: e.timestamp,
		kind:      partitions.PRIMARY,
		value:     newState,
		putConfig: &PutConfig{},
	}
	if ttl != 0 {
		p.timeout = time.Until(time.UnixMilli(ttl))
		p.putConfig.HasPX = true
		p.putConfig.PX = time.Until(time.UnixMilli(ttl))
	}
	err = dm.putOnCluster(p)
	if err != nil {
		dm.s.log.V(3).Printf("[ERROR] Failed to put the entry after function call: %v", err)
		return nil, err
	}

	return result, nil
}
