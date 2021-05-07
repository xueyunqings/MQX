package mq

import (
	"errors"
	"sync"
	"time"
)

type Broker interface {
	//消息推送。 topic、msg，分别是订阅的主题、要传递的消息
	publish(topic string, msg interface{}) error

	//消息的订阅。 传入订阅的主题，即可完成订阅，并返回对应的channel通道用来接收数据
	subscribe(topic string) (<-chan interface{}, error)

	//取消订阅，传入订阅的主题和对应的通道
	unsubscribe(topic string, sub <-chan interface{}) error

	//关闭消息队列
	close()

	//对推送的消息进行广播，保证每一个订阅者都可以收到
	broadcast(msg interface{}, subscribers []chan interface{})

	//设置消息队列的容量
	setConditions(capacity int)
}

type BrokerImpl struct {
	exit chan bool
	capacity int
	topics map[string][]chan interface{} // key： topic  value ： queue
	sync.RWMutex // 同步锁
}

func NewBroker() *BrokerImpl {
	return &BrokerImpl{
		exit: make(chan bool),
		topics: make(map[string][]chan interface{}),
	}
}

func (b *BrokerImpl) setConditions(capacity int)  {
	b.capacity = capacity
}

func (b *BrokerImpl) subscribe(topic string) (<-chan interface{}, error){
	select {
	case <-b.exit:
		return nil, errors.New("broker closed")
	default:
	}
	ch := make(chan interface{}, b.capacity)
	b.Lock()
	b.topics[topic] = append(b.topics[topic], ch)
	b.Unlock()
	return ch, nil
}

func (b *BrokerImpl) unsubscribe(topic string, sub <-chan interface{}) error {
	select {
	case <-b.exit:
		return errors.New("broker closed")
	default:
	}
	b.RLock()
	subscribers, ok := b.topics[topic]
	b.RUnlock()

	if !ok {
		return nil
	}

	//删除订阅
	var newSubs []chan interface{}
	for _, subscriber := range subscribers {
		if subscriber == sub {
			continue
		}
		newSubs = append(newSubs, subscriber)
	}
	b.Lock()
	b.topics[topic] = newSubs
	b.Unlock()
	return nil
}




func (b *BrokerImpl) publish(topic string, pub interface{}) error {
	select {
	case <-b.exit:
		return errors.New("broker closed")
	default:
	}

	b.RLock()
	subscribers, ok := b.topics[topic]
	b.RUnlock()
	if !ok {
		return nil
	}

	b.broadcast(pub, subscribers)
	return nil
}

func (b *BrokerImpl) broadcast(msg interface{}, subscribers []chan interface{}) {
	count := len(subscribers)
	concurrency := 1

	switch {
	case count > 1000: concurrency = 3
	case count >100 : concurrency = 2
	default: concurrency = 1
	}

	pub := func(start int){
		for j:= start; j<count; j+= concurrency {
			select {
			case subscribers[j] <-msg:
			case <-time.After(time.Millisecond * 5):
			case <-b.exit:
				return
			}
		}
	}
	for i:=0 ;i<concurrency; i++{
		go pub(i)
	}
}

func (b *BrokerImpl) close() {
	select {
	case <- b.exit:
		return
	default:
		close(b.exit)
		b.Lock()
		b.topics = make(map[string][]chan interface{})
		b.Unlock()
	}
	return
}

