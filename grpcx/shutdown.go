package grpcx

import (
	"github.com/sirupsen/logrus"

	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"
)

var (
	InstanceShutdown *Shutdown
	once             sync.Once
)

const (
	WorkerPoolPriority = iota
	RoutinePoolPriority
	LogPriority
	CronPriority
	GrpcServerPriority //grpc server 優先關閉避免request不斷進來
	HttpServerShutdownPriority
)

// ShutdownHook /关闭钩子
type ShutdownHook interface {
	// Name /名字
	Name() string
	// ShutdownPriority /优先级
	ShutdownPriority() int
	// BeforeShutdown /应用程序退出前
	BeforeShutdown()
	// AfterShutdown /应用程序退出后
	//AfterShutdown()
}

// ShutdownHooks /排序
type shutdownHooks []ShutdownHook

func (object shutdownHooks) Len() int {
	return len(object)
}
func (object shutdownHooks) Less(i, j int) bool {
	return object[i].ShutdownPriority() > object[j].ShutdownPriority()
}
func (object shutdownHooks) Swap(i, j int) {
	object[i], object[j] = object[j], object[i]
}

type ImplementShutdown struct {
	Priority  int
	EventName string
}

func (imp *ImplementShutdown) Name() string {
	return imp.EventName
}

func (imp *ImplementShutdown) ShutdownPriority() int {
	return imp.Priority
}

// Shutdown /应用
type Shutdown struct {
	isShutdown    bool
	log           *logrus.Entry
	signalCh      chan os.Signal
	shutdownHooks shutdownHooks
	sync.RWMutex
}

// NewShutdown /工厂方法
func NewShutdown() *Shutdown {
	once.Do(func() {
		InstanceShutdown = &Shutdown{
			log:           logrus.WithField("module", "Shutdown"),
			signalCh:      make(chan os.Signal, 1),
			shutdownHooks: make([]ShutdownHook, 0),
		}
	})
	return InstanceShutdown
}

func GetShutdown() *Shutdown {
	if InstanceShutdown == nil {
		NewShutdown()
	}
	return InstanceShutdown
}

// /通知关闭
func (object *Shutdown) notifyShutdown() {
	object.Lock()
	sort.Sort(object.shutdownHooks)
	for _, v := range object.shutdownHooks {
		object.log.Infof("app before shutdown: %s", v.Name())
		v.BeforeShutdown()
	}
	object.Unlock()
}

// IsShutdown /是否已关闭
func (object *Shutdown) IsShutdown() bool {
	return object.isShutdown
}

// InstallShutdownHook /安装关闭钩子
func (object *Shutdown) InstallShutdownHook(hook ShutdownHook) *Shutdown {
	object.Lock()
	if hook != nil {
		object.shutdownHooks = append(object.shutdownHooks, hook)
	}
	object.Unlock()
	return object
}

// WaitShutdown /等待关闭
func (object *Shutdown) WaitShutdown() {
	signal.Notify(object.signalCh)
	for s := range object.signalCh {
		switch s {
		case syscall.SIGHUP, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGABRT, syscall.SIGTERM:
			object.log.Infof("Signal: %v, Begin Shutdown", s)
			object.isShutdown = true
			// timeout暫時由docker-compose 控制
			//go object.TimeOut()
			object.notifyShutdown()
			close(object.signalCh)
			object.log.Info("Shutdown Finished")
			os.Exit(0)
		default:
			//object.log.Infof("signal: %v, not handled", s)
		}
	}
}

// TimeOut 超時強制關閉
func (object *Shutdown) TimeOut() {
	for {
		select {
		case <-time.After(time.Second * 60):
			object.log.Error("Shutdown Timeout!!!!!!!!")
			os.Exit(0)
		}
	}
}
