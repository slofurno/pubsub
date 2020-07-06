package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	pb "github.com/slofurno/pubsub/pb"

	"github.com/golang/protobuf/ptypes"
	empty "github.com/golang/protobuf/ptypes/empty"
	"github.com/google/uuid"
	"google.golang.org/grpc"
)

var defaultWaitTimeout = time.Second * 5
var defaultAckDeadline = time.Second * 10

type PubSub struct {
	mu            sync.Mutex
	subscriptions map[string]*Queue
	topics        map[string][]string

	*pb.UnimplementedSubscriberServer
	*pb.UnimplementedPublisherServer
}

func (s *PubSub) String() string {
	buf := &bytes.Buffer{}

	for topic, subs := range s.topics {
		for _, sub := range subs {
			fmt.Fprintf(buf, "%s -> %s\n", topic, sub)
		}
	}

	return buf.String()
}

func lastName(s string) string {
	i := strings.LastIndex(s, "/")
	return s[i+1:]
}

type Queue struct {
	mu sync.Mutex

	head *message
	tail *message

	byid    map[string]*message
	waiting chan *message
}

func NewQueue() *Queue {
	return &Queue{
		byid:    map[string]*message{},
		waiting: make(chan *message),
	}
}

type message struct {
	next     *message
	prev     *message
	lent     bool
	received time.Time
	deadline time.Time

	payload []byte
	id      string
}

func (n *message) ready() bool {
	return !n.lent || time.Now().After(n.deadline)
}

func (m *message) take() *message {
	m.lent = true
	m.deadline = time.Now().Add(defaultAckDeadline)
	return m
}

func (q *Queue) print() {
	var r []string
	for cur := q.head; cur != nil; cur = cur.next {
		x := cur.id
		if !cur.ready() {
			x = "*" + x
		}
		r = append(r, x)
	}
	fmt.Println(strings.Join(r, ","))
}

func (q *Queue) Take(ctx context.Context) (*message, bool) {
	if ret := q.take(); ret != nil {
		return ret, true
	}

	select {
	case <-ctx.Done():
		return nil, false
	case ret := <-q.waiting:
		return ret, true
	}
}

func (q *Queue) take() *message {
	q.mu.Lock()
	defer q.mu.Unlock()

	for cur := q.head; cur != nil; cur = cur.next {
		if cur.ready() {
			return cur.take()
		}
	}

	return nil
}

func (q *Queue) remove(id string) {
	q.mu.Lock()
	defer q.mu.Unlock()

	n, ok := q.byid[id]
	if !ok {
		return
	}

	delete(q.byid, id)
	if q.head == n && q.tail == n {
		q.head = nil
		q.tail = nil
		return
	}

	if q.head == n {
		q.head = n.next
		n.next.prev = nil
		return
	}

	if q.tail == n {
		q.tail = n.prev
		n.prev.next = nil
		return
	}

	n.prev.next = n.next
	n.next.prev = n.prev
}

func (q *Queue) Push(id string, payload []byte) {
	msg := &message{
		id:       id,
		payload:  payload,
		received: time.Now(),
	}

	select {
	case q.waiting <- msg:
		msg.take()
	default:
	}

	q.push(msg)
}

func (q *Queue) push(cur *message) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.byid[cur.id] = cur

	if q.head == nil {
		q.head = cur
		q.tail = cur
		return
	}

	q.tail.next = cur
	cur.prev = q.tail
	q.tail = cur
}

func (q *Queue) setDeadline(id string, deadline time.Time) {
	//TODO track multiple ack ids for each msg
	q.mu.Lock()
	defer q.mu.Unlock()

	n, ok := q.byid[id]
	if !ok {
		return
	}

	n.deadline = deadline
}

func (s *PubSub) Pull(ctx context.Context, req *pb.PullRequest) (*pb.PullResponse, error) {
	sub := lastName(req.Subscription)
	fmt.Printf("Pull: %s\n", sub)

	queue, ok := s.subscriptions[sub]
	if !ok {
		return nil, fmt.Errorf("unknown subscription: %s", sub)
	}

	count := int32(5)
	if req.MaxMessages < count {
		count = req.MaxMessages
	}

	ctx, cancel := context.WithTimeout(ctx, defaultWaitTimeout)
	defer cancel()

	var msgs []*pb.ReceivedMessage
	for i := int32(0); i < count; i++ {
		msg, ok := queue.Take(ctx)
		if !ok {
			break
		}

		ts, _ := ptypes.TimestampProto(msg.received)
		msgs = append(msgs, &pb.ReceivedMessage{
			AckId: msg.id,
			Message: &pb.PubsubMessage{
				Data:        msg.payload,
				MessageId:   msg.id,
				PublishTime: ts,
			},
		})
	}

	return &pb.PullResponse{
		ReceivedMessages: msgs,
	}, nil
}
func (s *PubSub) Publish(ctx context.Context, req *pb.PublishRequest) (*pb.PublishResponse, error) {
	topic := lastName(req.GetTopic())
	fmt.Printf("Publish: %s\n", topic)

	var ids []string
	for _, msg := range req.GetMessages() {
		id := uuid.New().String()
		ids = append(ids, id)

		for _, sub := range s.topics[topic] {
			s.subscriptions[sub].Push(id, msg.Data)
		}
	}

	return &pb.PublishResponse{MessageIds: ids}, nil
}

func (s *PubSub) Acknowledge(ctx context.Context, req *pb.AcknowledgeRequest) (*empty.Empty, error) {
	sub := lastName(req.Subscription)
	fmt.Printf("Acknowledge: %s\n", sub)

	queue, ok := s.subscriptions[sub]
	if !ok {
		return nil, fmt.Errorf("unknown subscription: %s", sub)
	}

	for _, id := range req.GetAckIds() {
		queue.remove(id)
	}

	return &empty.Empty{}, nil
}

func (s *PubSub) ModifyAckDeadline(ctx context.Context, req *pb.ModifyAckDeadlineRequest) (*empty.Empty, error) {
	seconds := req.GetAckDeadlineSeconds()
	deadline := time.Now().Add(time.Second * time.Duration(seconds))
	sub := lastName(req.Subscription)
	fmt.Printf("ModifyAckDeadline: %s\n", sub)

	queue, ok := s.subscriptions[sub]
	if !ok {
		return nil, fmt.Errorf("unknown subscription: %s", sub)
	}

	for _, id := range req.GetAckIds() {
		queue.setDeadline(id, deadline)
	}

	return &empty.Empty{}, nil
}

func (s *PubSub) StreamingPull(srv pb.Subscriber_StreamingPullServer) error {
	done := make(chan struct{})
	req, err := srv.Recv()
	if err != nil {
		return err
	}

	if len(req.AckIds) != 0 || len(req.ModifyDeadlineAckIds) != 0 {
		log.Fatalf("unexpected: %#v\n", req)
	}

	sub := lastName(req.Subscription)
	fmt.Printf("StreamingPull: %s\n", sub)
	queue, ok := s.subscriptions[sub]
	if !ok {
		return fmt.Errorf("unknown subscription: %s", sub)
	}

	go func() {
		for {
			select {
			case <-done:
				return
			case <-srv.Context().Done():
			default:
			}

			var msgs []*pb.ReceivedMessage
			ctx, cancel := context.WithTimeout(srv.Context(), defaultWaitTimeout)
			defer cancel()

			for i := 0; i < 5; i++ {
				msg := queue.take()
				if msg == nil {
					break
				}

				ts, _ := ptypes.TimestampProto(msg.received)
				msgs = append(msgs, &pb.ReceivedMessage{
					AckId: msg.id,
					Message: &pb.PubsubMessage{
						Data:        msg.payload,
						MessageId:   msg.id,
						PublishTime: ts,
					},
				})
			}

			if len(msgs) == 0 {
				msg, ok := queue.Take(ctx)
				if !ok {
					break
				}

				ts, _ := ptypes.TimestampProto(msg.received)
				msgs = append(msgs, &pb.ReceivedMessage{
					AckId: msg.id,
					Message: &pb.PubsubMessage{
						Data:        msg.payload,
						MessageId:   msg.id,
						PublishTime: ts,
					},
				})
			}

			err := srv.Send(&pb.StreamingPullResponse{
				ReceivedMessages: msgs,
			})
			if err != nil {
				fmt.Println(err)
			}
		}
	}()

	for {
		req, err := srv.Recv()
		if err != nil {
			close(done)
			return err
		}

		for _, id := range req.GetAckIds() {
			queue.remove(id)
		}

		for i, id := range req.GetModifyDeadlineAckIds() {
			seconds := req.ModifyDeadlineSeconds[i]
			deadline := time.Now().Add(time.Second * time.Duration(seconds))
			queue.setDeadline(id, deadline)
		}
	}
}

func New(args []string) (*PubSub, string) {
	topics := map[string][]string{}
	address := "127.0.0.1:4004"

	for i := 0; i < len(args); i += 2 {
		switch args[i] {
		case "-sub", "--sub":
			parts := strings.Split(args[i+1], ":")
			if len(parts) != 2 {
				log.Fatalf("expected format topic:subscriber, got: %s\n", args[i+1])
			}
			topics[parts[0]] = append(topics[parts[0]], parts[1])
		case "-address", "--address":
			address = args[i+1]
		default:
			log.Fatalf("unknown arg: %s\n", args[i])
		}
	}

	subscriptions := map[string]*Queue{}

	for _, sub := range topics {
		for _, s := range sub {
			if _, ok := subscriptions[s]; ok {
				log.Fatalf("invalid config, duplicate subscriber: %s\n", s)
			}

			subscriptions[s] = NewQueue()
		}
	}

	svc := &PubSub{
		subscriptions: subscriptions,
		topics:        topics,
	}

	return svc, address
}

func main() {
	args := os.Args[1:]

	svc, address := New(args)
	fmt.Printf("listening on: %s\n%s\n", address, svc)

	lis, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()

	pb.RegisterSubscriberServer(s, svc)
	pb.RegisterPublisherServer(s, svc)

	if err := s.Serve(lis); err != nil {
		fmt.Printf("failed to serve: %v", err)
	}
}
