package main

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestQueue(t *testing.T) {
	queue := &Queue{
		byid: map[string]*message{},
	}

	for i := 0; i < 5; i++ {
		s := strconv.Itoa(i)
		queue.Push(s, []byte(s))
	}

	queue.remove("2")
	queue.print()

	queue.remove("0")
	queue.print()

	queue.remove("4")
	queue.print()
	queue.remove("1")
	queue.print()
	queue.remove("3")
	queue.print()

	queue.remove("6")
	queue.print()

	for i := 0; i < 5; i++ {
		s := strconv.Itoa(i)
		queue.Push(s, []byte(s))
	}

	for i := 0; i < 6; i++ {
		fmt.Println(queue.take())
		queue.print()
	}
}

func TestBlockingPush(t *testing.T) {
	queue := NewQueue()

	{
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*2)
		defer cancel()

		_, ok := queue.Take(ctx)
		if ok {
			t.Fatalf("expected timeout before messages")
		}
	}

	go func() {
		time.Sleep(time.Millisecond)

	}()

	{
		recv := make(chan *message, 12)
		wg := sync.WaitGroup{}

		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*5)
				defer cancel()
				msg, ok := queue.Take(ctx)
				if ok {
					recv <- msg
				}
				wg.Done()
			}()
		}

		time.Sleep(time.Millisecond)
		for i := 0; i < 5; i++ {
			s := strconv.Itoa(i)
			queue.Push(s, []byte(s))
		}
		wg.Wait()

		close(recv)

		for msg := range recv {
			fmt.Println(string(msg.payload))
		}

	}
}
