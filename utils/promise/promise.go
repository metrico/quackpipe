package promise

import (
	"sync"
	"sync/atomic"
)

type Promise[T any] struct {
	lock    sync.Mutex
	err     error
	res     T
	pending int32
}

func New[T any]() *Promise[T] {
	res := &Promise[T]{
		pending: 1,
	}
	res.lock.Lock()
	return res
}

func Fulfilled[T any](err error, res T) *Promise[T] {
	return &Promise[T]{
		err:     err,
		res:     res,
		pending: 0,
	}
}

func (p *Promise[T]) Get() (T, error) {
	p.lock.Lock()
	defer p.lock.Unlock()
	return p.res, p.err
}

func (p *Promise[T]) Done(res T, err error) {
	if atomic.LoadInt32(&p.pending) == 0 {
		return
	}
	p.pending = 0
	p.res = res
	p.err = err
	p.lock.Unlock()
}
