package mq

import (
	"fmt"
	"sync"
	"testing"
)

//单元测试
func TestClient(t *testing.T) {
	b := NewClient()
	b.SetConditions(100)
	var wg sync.WaitGroup

	println("start")

	for i:=0; i<100; i++ {

		topic := fmt.Sprintf("单元测试%d",i)
		payload := fmt.Sprintf("xuexue%d",i)

		ch, err := b.Subscribe(topic)
		if err != nil {
			t.Fatal(err)
		}
		wg.Add(1)
		go func() {
			e := b.GetPayLoad(ch)
			if e!= payload {
				t.Fatalf("%s expected %s but get %s", topic, payload, e)
			}
			if err := b.Unsubscribe(topic, ch); err != nil {
				t.Fatal(err)
			}
			wg.Done()
		}()
		if err := b.Publish(topic, payload); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
}
