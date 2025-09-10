package gTaskMg

import (
	"errors"
	"time"
)

type GQueue chan any

func (obj *GQueue) Init(size int) *GQueue {
	*obj = make(chan any, size)
	return obj
}

func (obj *GQueue) EnQueueSync(itm any) {
	*obj <- itm
}

func (obj *GQueue) EnQueue(itm any, timeout time.Duration) error {
	select {
	case *obj <- itm:
		return nil
	case <-time.After(timeout):
		return errors.New(ERR_GQUEUE_TIMEOUT)
	}
}

func (obj *GQueue) DeQueueSync() any {
	itm := <-*obj
	return itm
}

func (obj *GQueue) DeQueue(timeout time.Duration) (any, error) {
	var itm any
	select {
	case itm = <-*obj:
		return itm, nil
	case <-time.After(timeout):
		return nil, errors.New(ERR_GQUEUE_TIMEOUT)
	}
}

func (obj *GQueue) Close() {
	close(*obj)
}
