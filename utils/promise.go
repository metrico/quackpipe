package utils

import (
	"sync"
	"sync/atomic"
)

type Promise[T any] interface {
	Get() (T, error)
	Peek() (int32, T, error)
	Done(res T, err error)
}
type SinglePromise[T any] struct {
	lock    sync.Mutex
	err     error
	res     T
	pending int32
}

func New[T any]() Promise[T] {
	res := &SinglePromise[T]{
		pending: 1,
	}
	res.lock.Lock()
	return res
}

func Fulfilled[T any](err error, res T) Promise[T] {
	return &SinglePromise[T]{
		err:     err,
		res:     res,
		pending: 0,
	}
}

func (p *SinglePromise[T]) Get() (T, error) {
	p.lock.Lock()
	defer p.lock.Unlock()
	return p.res, p.err
}

func (p *SinglePromise[T]) Peek() (int32, T, error) {
	return atomic.LoadInt32(&p.pending), p.res, p.err
}

func (p *SinglePromise[T]) Done(res T, err error) {
	if atomic.LoadInt32(&p.pending) == 0 {
		return
	}
	p.pending = 0
	p.res = res
	p.err = err
	p.lock.Unlock()
}

type WaitForAllPromise[T any] struct {
	promises []Promise[T]
}

func (p *WaitForAllPromise[T]) Get() (T, error) {
	var res T
	for _, p := range p.promises {
		_, _, err := p.Peek()
		if err != nil {
			return res, err
		}
	}
	for _, p := range p.promises {
		res, err := p.Get()
		if err != nil {
			return res, err
		}
	}
	return res, nil
}

func (p *WaitForAllPromise[T]) Peek() (int32, T, error) {
	var res T
	for _, p := range p.promises {
		_, _, err := p.Peek()
		if err != nil {
			return 0, res, err
		}
	}
	return 1, res, nil
}

func NewWaitForAll[T any](promises []Promise[T]) Promise[T] {
	return &WaitForAllPromise[T]{promises: promises}
}

func (p *WaitForAllPromise[T]) Done(res T, err error) {
	// No-op
}

func (p *WaitForAllPromise[T]) Add(promise Promise[T]) {
	p.promises = append(p.promises, promise)
}
