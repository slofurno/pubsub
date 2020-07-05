package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	pb "github.com/slofurno/pubsub/pb"

	"github.com/golang/protobuf/ptypes"
	empty "github.com/golang/protobuf/ptypes/empty"
	"github.com/google/uuid"
	"google.golang.org/grpc"
)

type Server struct {
	mu    sync.Mutex
	queue *Queue

	*pb.UnimplementedSubscriberServer
	*pb.UnimplementedPublisherServer
}

var defaultWaitTimeout = time.Second * 5
var defaultAckDeadline = time.Second * 10

type PubSub struct {
	mu     sync.Mutex
	topics map[string]*Queue
}

func NewPubSub() *PubSub {
	return &PubSub{
		topics: map[string]*Queue{},
	}
}

func (p *PubSub) GetTopic(name string) *Queue {
	p.mu.Lock()
	defer p.mu.Unlock()

	topic, ok := p.topics[name]
	if !ok {
		queue := NewQueue()
		p.topics[name] = queue
		topic = queue
	}

	return topic
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

func (s *Server) Pull(ctx context.Context, req *pb.PullRequest) (*pb.PullResponse, error) {
	fmt.Println("Pull")
	count := int32(5)
	if req.MaxMessages < count {
		count = req.MaxMessages
	}

	ctx, cancel := context.WithTimeout(ctx, defaultWaitTimeout)
	defer cancel()

	var msgs []*pb.ReceivedMessage
	for i := int32(0); i < count; i++ {
		msg, ok := s.queue.Take(ctx)
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
func (s *Server) Publish(ctx context.Context, req *pb.PublishRequest) (*pb.PublishResponse, error) {
	fmt.Println("Publish")
	var ids []string
	for _, msg := range req.GetMessages() {
		id := uuid.New().String()
		ids = append(ids, id)
		s.queue.Push(id, msg.Data)
	}

	return &pb.PublishResponse{MessageIds: ids}, nil
}

func (s *Server) Acknowledge(ctx context.Context, req *pb.AcknowledgeRequest) (*empty.Empty, error) {
	fmt.Println("Acknowledge", req)
	for _, id := range req.GetAckIds() {
		s.queue.remove(id)
	}

	return &empty.Empty{}, nil
}

func (s *Server) ModifyAckDeadline(ctx context.Context, req *pb.ModifyAckDeadlineRequest) (*empty.Empty, error) {
	fmt.Println("ModifyAckDeadline")
	seconds := req.GetAckDeadlineSeconds()
	deadline := time.Now().Add(time.Second * time.Duration(seconds))
	for _, id := range req.GetAckIds() {
		s.queue.setDeadline(id, deadline)
	}

	return &empty.Empty{}, nil
}

func (s *Server) StreamingPull(srv pb.Subscriber_StreamingPullServer) error {
	done := make(chan struct{})
	fmt.Println("StreamingPull")

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
				msg := s.queue.take()
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
				msg, ok := s.queue.Take(ctx)
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
			s.queue.remove(id)
		}

		for i, id := range req.GetModifyDeadlineAckIds() {
			seconds := req.ModifyDeadlineSeconds[i]
			deadline := time.Now().Add(time.Second * time.Duration(seconds))
			s.queue.setDeadline(id, deadline)
		}
	}
}

func main() {
	lis, err := net.Listen("tcp", "127.0.0.1:4004")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	fmt.Println(lis.Addr())
	s := grpc.NewServer()
	svr := &Server{
		queue: NewQueue(),
	}

	pb.RegisterSubscriberServer(s, svr)
	pb.RegisterPublisherServer(s, svr)

	if err := s.Serve(lis); err != nil {
		fmt.Printf("failed to serve: %v", err)
	}
}
