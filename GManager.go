package gTaskMg

import (
	"errors"
	"sync"
	"time"
)

type GManager struct {
	sync.WaitGroup
	tlock  sync.RWMutex
	qlock  sync.RWMutex
	tasks  map[string]*GTask
	queues map[string]GQueue
}

const (
	GTSK_DEFAULT        = "GManager.Default"
	GMSG_EXIT    string = "GManager.Exit"
)

const (
	ERR_GQUEUE_NOFOUND string = "cannot find queue"
	ERR_GQUEUE_TIMEOUT string = "gqueue timeout"
)

func (obj *GManager) Init() *GManager {
	obj.tasks = make(map[string]*GTask)
	obj.queues = make(map[string]GQueue)
	obj.CreateTask(obj.defaultTsk, "GManager.Default", 1)
	return obj
}

func (obj *GManager) defaultTsk() {
	q, e := obj.GetQueue(GTSK_DEFAULT)
	for {
		if e {
			select {
			case msg := <-q:
				switch tmsg := msg.(type) {
				case string:
					switch tmsg {
					case GMSG_EXIT:
						obj.Exit()
						return
					default:
					}
				}
			case <-time.After(10 * time.Minute):
			}
		} else {
			time.Sleep(10 * time.Minute)
		}
	}
}

func (obj *GManager) RegistTask(task *GTask, name string) {
	obj.tlock.Lock()
	defer obj.tlock.Unlock()
	obj.tasks[name] = task
}

func (obj *GManager) DeleteTask(name string) {
	obj.tlock.Lock()
	defer obj.tlock.Unlock()

	_, f := obj.tasks[name]
	if f {
		obj.DeleteQueue(name)
		delete(obj.tasks, name)
	}
}

func (obj *GManager) DeleteAllTask() {
	obj.tlock.Lock()
	defer obj.tlock.Unlock()

	for n := range obj.tasks {
		delete(obj.tasks, n)
	}
	obj.DeleteAllQueue()
}

func (obj *GManager) CreateTask(runner any, name string, qsize int) *GTask {
	t := new(GTask).Init(runner, name, obj, qsize)
	return t
}

func (obj *GManager) RegistQueue(queue GQueue, name string) {
	obj.qlock.Lock()
	defer obj.qlock.Unlock()
	obj.queues[name] = queue
}

func (obj *GManager) DeleteQueue(name string) {
	obj.qlock.Lock()
	defer obj.qlock.Unlock()

	q, f := obj.queues[name]
	if f {
		q.Close()
		delete(obj.queues, name)
	}
}

func (obj *GManager) DeleteAllQueue() {
	obj.qlock.Lock()
	defer obj.qlock.Unlock()

	for n, q := range obj.queues {
		q.Close()
		delete(obj.queues, n)
	}
}

func (obj *GManager) CreateQueue(name string, qsize int) *GQueue {
	q := new(GQueue)
	q.Init(qsize)
	obj.RegistQueue(*q, name)
	return q
}

func (obj *GManager) Enter() {
	obj.WaitGroup.Add(1)
}

func (obj *GManager) Exit() {
	obj.WaitGroup.Done()
}

func (obj *GManager) Join() {
	obj.WaitGroup.Wait()
}

func (obj *GManager) Broadcast(event any) {
	obj.tlock.RLock()
	defer obj.tlock.RUnlock()
	// Last to first.
	for name := range obj.tasks {
		t := obj.tasks[name]
		t.GQueue.EnQueueSync(event)
	}
}

func (obj *GManager) BroadcastWithout(event any, without string) {
	obj.tlock.RLock()
	defer obj.tlock.RUnlock()
	// Last to first.
	for name := range obj.tasks {
		if without == name {
			continue
		}
		t := obj.tasks[name]
		t.GQueue.EnQueueSync(event)
	}
}

func (obj *GManager) ReqExit() {
	obj.Broadcast(GMSG_EXIT)
}

func (obj *GManager) ReqExitWithout(without string) {
	obj.BroadcastWithout(GMSG_EXIT, without)
}

func (obj *GManager) ReqTaskExit(name string) {
	t, f := obj.tasks[name]
	if f {
		t.GQueue.EnQueueSync(GMSG_EXIT)
	}
}

// 由于 Golang 的协程死锁检测，当所有任务都处于阻塞状态时，会导致死锁，进而程序崩溃，有必要提供一个可选的默认协程，以防止死锁 panic。
// 允许使用 GTSK_DEFAULT 替换默认的 Default Task，替换默认协程必须发生在默认协程运行之前。
// 使用 go GetDefaultTsk().Run().(func())() 手动启动默认协程，如果能够保证系统中的协程不存在死锁情况，可不启动默认协程。
// 向默认协程发送 GMSG_EXIT 消息可使其退出。
func (obj *GManager) GetDefaultTsk() *GTask {
	obj.tlock.RLock()
	defer obj.tlock.RUnlock()

	t := obj.tasks[GTSK_DEFAULT]
	return t
}

// 这是一个比较危险的函数，在使用时需要确保对应的任务不会被释放。
// 如果只是向队列或者任务发生消息，应使用：GManager.EnQueueSync() 或者 GManager.EnQueue()
func (obj *GManager) GetGTask(name string) (*GTask, bool) {
	obj.tlock.RLock()
	defer obj.tlock.RUnlock()

	t, e := obj.tasks[name]
	return t, e
}

// 这是一个比较危险的函数，在使用时需要确保对应的队列不会被释放，或对通道是否关闭进行检查。
// 如果只是向队列或者任务发生消息，应使用：GManager.EnQueueSync() 或者 GManager.EnQueue()
func (obj *GManager) GetQueue(name string) (GQueue, bool) {
	obj.qlock.RLock()
	defer obj.qlock.RUnlock()

	q, e := obj.queues[name]
	return q, e
}

func (obj *GManager) EnQueueSync(name string, itm any) error {
	obj.qlock.RLock()
	defer obj.qlock.RUnlock()

	q, e := obj.queues[name]
	if !e {
		return errors.New(ERR_GQUEUE_NOFOUND)
	}
	q.EnQueueSync(itm)
	return nil
}

func (obj *GManager) EnQueue(name string, itm any, timeout time.Duration) error {
	obj.qlock.RLock()
	defer obj.qlock.RUnlock()

	q, e := obj.queues[name]
	if !e {
		return errors.New(ERR_GQUEUE_NOFOUND)
	}
	return q.EnQueue(itm, timeout)
}
