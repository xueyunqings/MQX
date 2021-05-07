package main

import (
	"./mq"
	"fmt"
	"time"
)
var (
	topic = "simpleMQTest"
)

func main() {
	//OnceTopic()
	ManyTopic()
}

//------------------------------------------------------------------

func OnceTopic() {
	m := mq.NewClient()
	m.SetConditions(10)
	ch,err := m.Subscribe(topic)
	if err != nil {
		fmt.Println("订阅失败")
		return
	}
	//推送
	go OncePub(m)

	//接受订阅
	OnceSub(ch,m)

	defer m.Close()
}

// OncePub 定时推送
func OncePub(c *mq.Client){
	t := time.NewTimer(10 * time.Second)
	defer t.Stop()
	for {
		select {
		case <- t.C:
			err := c.Publish(topic,"xuexue真好！！！！")
			if err != nil{
				fmt.Println("推送消息失败")
			}
		default:
		}
	}
}

// 接受订阅
func OnceSub(m <-chan interface{},c *mq.Client){
	for{
		val := c.GetPayLoad(m)
		fmt.Printf("收到消息%s\n",val)
	}
}

//------------------------------------------------------------------

func ManyTopic() {
	m := mq.NewClient()
	defer m.Close()
	m.SetConditions(10)
	top := ""
	for i:=0;i<10;i++{
		top = fmt.Sprintf("xuexue_%02d",i)
		go Sub(m,top)
	}
	ManyPub(m)
}

func ManyPub(c *mq.Client) {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			for i := 0; i < 10; i++ {
				//多个topic 推送不同的消息
				top := fmt.Sprintf("xuexue_%02d", i)
				payload := fmt.Sprintf("xuexue棒_%02d", i)
				err := c.Publish(top, payload)
				if err != nil {
					fmt.Println("推送消息失败")
				}
			}
		default:
		}
	}
}

func Sub(c *mq.Client,top string)  {
	ch,err := c.Subscribe(top)
	if err != nil{
		fmt.Printf("订阅消息 top:%s 失败\n",top)
	}
	for  {
		val := c.GetPayLoad(ch)
		if val != nil{
			fmt.Printf("%s 获取的消息是 %s\n",top,val)
		}
	}
}