package main

import (
	"fmt"
	"time"

	gtask "github.com/lion187chen/gTaskMg"
)

type MGCore struct {
	*gtask.GManager
	*gtask.GTask
}

var Core MGCore

func main() {
	Core.GManager = new(gtask.GManager).Init()
	go Core.GManager.GetDefaultTsk().Run().(func())()

	Core.GTask = Core.CreateTask(Core.demoTask, "Demo.Task", 16)

	Core.GManager.EnQueueSync("Demo.Task", "Message 0")          // Send message to a named queue
	Core.GTask.GQueue.EnQueue("Message 1", 100*time.Millisecond) // OR use my.GQueue
	Core.GManager.BroadcastWithout("Message 2", "Demo.Task")

	go Core.Run().(func(string))("A Demo Task")

	time.Sleep(10 * time.Second)
	Core.GManager.Broadcast(gtask.GMSG_EXIT)
	Core.GManager.Join()
	Core.GManager.DeleteTask("Demo.Task")

	fmt.Println("All task done!")
}

func (my *MGCore) demoTask(name string) {
	fmt.Println(name)
	for {
		msg := <-my.GQueue // OR use my.DeQueue()/my.DeQueueSync()
		switch tmsg := msg.(type) {
		case string:
			switch tmsg {
			case gtask.GMSG_EXIT:
				Core.GTask.Exit()
				return
			default:
				fmt.Println(tmsg)
			}
		}
	}
}
