package main

import (
	"fmt"
	"time"

	gtask "github.com/lion187chen/gTaskMg"
)

type MGCore struct {
	*gtask.GManager
}

type MTask struct {
	*gtask.GTask
}

var Core MGCore
var ATask MTask

func main() {
	Core.GManager = new(gtask.GManager).Init()
	go Core.GManager.GetDefaultTsk().Run().(func())()

	ATask.GTask = Core.CreateTask(ATask.demoTask, "Demo.Task", 16)

	Core.GManager.EnQueueSync("Demo.Task", "Message 0")           // Send message to a named queue
	ATask.GTask.GQueue.EnQueue("Message 1", 100*time.Millisecond) // OR use my.GQueue
	Core.GManager.BroadcastWithout("Message 2", "Demo.Task")

	go ATask.Run().(func(string))("A Demo Task")

	time.Sleep(10 * time.Second)
	Core.GManager.Broadcast(gtask.GMSG_EXIT)
	Core.GManager.Join()
	Core.GManager.DeleteTask("Demo.Task")

	fmt.Println("All task done!")
}

func (my *MTask) demoTask(name string) {
	defer ATask.GTask.Exit()

	fmt.Println(name)
	for {
		msg := <-my.GQueue // OR use my.DeQueue()/my.DeQueueSync()
		switch tmsg := msg.(type) {
		case string:
			switch tmsg {
			case gtask.GMSG_EXIT:
				return
			default:
				fmt.Println(tmsg)
			}
		}
	}
}
