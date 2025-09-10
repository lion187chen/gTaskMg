package gTaskMg

import (
	"sync"
)

type GTask struct {
	GQueue
	sync.WaitGroup
	runner  any
	name    string
	manager *GManager
}

func (obj *GTask) Init(runner any, name string, manager *GManager, qsize int) *GTask {
	obj.runner = runner
	obj.name = name
	obj.manager = manager
	obj.GQueue.Init(qsize)
	if obj.manager != nil {
		// 注册 Task。
		obj.manager.RegistTask(obj, name)
		// 注册默认队列。
		obj.manager.RegistQueue(obj.GQueue, name)
		return obj
	}
	return nil
}

func (obj *GTask) Run() any {
	obj.Enter()
	return obj.runner
}

func (obj *GTask) Runner() any {
	return obj.runner
}

func (obj *GTask) Name() string {
	return obj.name
}

func (obj *GTask) Enter() {
	if obj.manager != nil {
		obj.manager.Enter()
	}
	obj.WaitGroup.Add(1)
}

func (obj *GTask) Exit() {
	obj.WaitGroup.Done()
	if obj.manager != nil {
		obj.manager.Exit()
	}
}

func (obj *GTask) Join() {
	obj.WaitGroup.Wait()
}
