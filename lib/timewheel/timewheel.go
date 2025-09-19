package timewheel

import (
	"container/list"
	"go-redis/logger"
	"time"
)

type task struct {
	delay  time.Duration
	circle int
	key    string    // 任务唯一标识，用于取消或查找
	job    func()
}

// TimeWheel can execute job after waiting given duration
type TimeWheel struct {
	interval time.Duration  // 每一格（slot）的时间跨度
	ticker   *time.Ticker   // 全局时钟，按 interval 周期性触发
	slots    []*list.List   // 槽数组，每个槽是一个链表，存放多个定时任务

	timer             map[string]int  // 任务ID → 所在槽下标，用于快速定位和删除任务
	currentPos        int           // 当前时间轮指针指向的位置（槽下标）
	slotNum           int           // 时间轮一共有多少个槽
	addTaskChannel    chan task     // 新增任务的通道
	removeTaskChannel chan string   // 移除任务的通道（通过任务ID）
	stopChannel       chan bool     // 停止时间轮的通道
}

// New create a new time wheel
func New(interval time.Duration, slotsNum int) *TimeWheel {
	if interval <= 0 || slotsNum <= 0 {
		return nil
	}
	tw := &TimeWheel{
		interval: interval,
		slots: make([]*list.List, slotsNum),
		timer: make(map[string]int),
		currentPos: 0,
		slotNum: slotsNum,
		addTaskChannel: make(chan task),
		removeTaskChannel: make(chan string),
		stopChannel: make(chan bool),
	}
	tw.initSlots()
	return tw
}

func (tw *TimeWheel) initSlots() {
	for i := 0; i < tw.slotNum; i ++ {
		tw.slots[i] = list.New()
	}
}

// Start starts ticker for time wheel
func (tw *TimeWheel) Start() {
	tw.ticker = time.NewTicker(tw.interval)
	go tw.start()
}

// Stop stops the time wheel
func (tw *TimeWheel) Stop() {
	tw.stopChannel <- true
}

// AddJob add new job into pending queue
func (tw *TimeWheel) AddJob(delay time.Duration, key string, job func()) {
	if delay < 0 {
		return
	}
	tw.addTaskChannel <- task{delay: delay, key: key, job: job}
}

// RemoveJob add remove job from pending queue
// if job is done or not found, then nothing happened
func (tw *TimeWheel) RemoveJob(key string) {
	if key == "" {
		return
	}
	tw.removeTaskChannel <- key
}

func (tw *TimeWheel) start() {
	for {
		select {
		case <-tw.ticker.C:
			tw.tickHandler()
		case task := <-tw.addTaskChannel:
			tw.addTask(&task)
		case key := <-tw.removeTaskChannel:
			tw.removeTask(key)
		case <-tw.stopChannel:
			tw.ticker.Stop()
			return
		}
	}
}

func (tw *TimeWheel) tickHandler() {
	l := tw.slots[tw.currentPos]
	tw.scanAndRunTask(l)
	if tw.currentPos == tw.slotNum - 1 {
		tw.currentPos = 0
	} else {
		tw.currentPos ++
	}
}

func (tw *TimeWheel) scanAndRunTask(l *list.List) {
	for e := l.Front(); e != nil; {
		t := e.Value.(*task)
		if t.circle > 0 {
			t.circle --
			e = e.Next()
			continue
		}
		go func() {
			defer func() {
				if err := recover(); err != nil {
					logger.Error(err)
				}
			}()
			job := t.job
			job()
		}()
		next := e.Next()
		l.Remove(e)
		if t.key != "" {
			delete(tw.timer, t.key)
		}
		e = next
	}
}

func (tw *TimeWheel) addTask(task *task) {
	pos, circle := tw.getPositionAndCircle(task.delay)
	task.circle = circle
	tw.slots[pos].PushBack(task)
	if task.key != "" {
		tw.timer[task.key] = pos
	}
}

func (tw *TimeWheel) getPositionAndCircle(d time.Duration) (pos int, circle int)  {
	delaySeconds := int(d.Seconds())
	intervalSeconds := int(tw.interval.Seconds())
	circle = int(delaySeconds / intervalSeconds / tw.slotNum)
	pos = (tw.currentPos + delaySeconds / intervalSeconds) % tw.slotNum
	return
}

func (tw *TimeWheel) removeTask(key string) {
	pos, ok := tw.timer[key]
	if !ok {
		return
	}
	l := tw.slots[pos]
	for e := l.Front(); e != nil; {
		t := e.Value.(*task)
		if t.key == key {
			delete(tw.timer, key)
			l.Remove(e)
		}
		e = e.Next()
	}
}










