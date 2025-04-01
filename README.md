# GTaskMg

GTaskMg is a extension for Goroutine and Channel Management. We extend the Goroutine by GTask, and the Channel by GQueue.

GManager is used to manage GTasks and GQueues.

GManager manage GTasks and GQueues by name. So users can send message to any GTasks from anywhere by name. Users can broadcast messages to all GTasks(with or without sender). If we want to stop all the GTasks but must wait for them to finish the job, we can broadcast a gtask.GMSG_EXIT message(GManager.ReqExit() or GManager.ReqExitWithout(string)) then use GManager.Join() to wait.

GManager provides a default Goroutine to prevent deadlock. Users can start it if necessary or not.

Enjoy it!

## Install

```bash
go get github.com/lion187chen/gTaskMg
```

## Example

```go
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

func (obj *MGCore) demoTask(name string) {
    fmt.Println(name)
    for {
        msg := <-obj.GQueue // OR use obj.DeQueue()/obj.DeQueueSync()
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
```
