package emulator

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	pb "google.golang.org/genproto/googleapis/pubsub/v1"

	"github.com/golang/protobuf/ptypes"
	empty "github.com/golang/protobuf/ptypes/empty"
	"github.com/google/uuid"
	"google.golang.org/grpc"
)

var defaultWaitTimeout = time.Second * 5
var defaultAckDeadline = time.Second * 10
var defaultNumAttempts = 4

type PubSub struct {
	mu            sync.Mutex
	subscriptions map[string]*Queue
	topics        map[string][]string

	maxNumAttempts int
	waitTimeout    time.Duration
	verbose        bool

	*pb.UnimplementedSubscriberServer
	*pb.UnimplementedPublisherServer
}

func (s *PubSub) printf(format string, a ...interface{}) {
	if s.verbose {
		fmt.Printf(format, a...)
	}
}

func (s *PubSub) getSubscription(name string) (*Queue, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sub, ok := s.subscriptions[name]
	return sub, ok
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

	maxNumAttempts int
}

func NewQueue(maxNumAttempts int) *Queue {
	return &Queue{
		byid:    map[string]*message{},
		waiting: make(chan *message),

		maxNumAttempts: maxNumAttempts,
	}
}

type message struct {
	next     *message
	prev     *message
	lent     bool
	received time.Time
	deadline time.Time
	attempts int

	payload []byte
	id      string
}

func (n *message) ready() bool {
	return !n.lent || time.Now().After(n.deadline)
}

func (m *message) take() *message {
	m.lent = true
	m.deadline = time.Now().Add(defaultAckDeadline)
	m.attempts++
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

func (q *Queue) quickTake(ctx context.Context) (*message, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if ret := q.take(); ret != nil {
		return ret, true
	}

	return nil, false
}

func (q *Queue) Take(ctx context.Context) (*message, bool) {
	if ret, ok := q.quickTake(ctx); ok {
		return ret, ok
	}

	select {
	case <-ctx.Done():
		return nil, false
	case ret := <-q.waiting:
		return ret, true
	}
}

func (q *Queue) take() *message {

	for cur := q.head; cur != nil; cur = cur.next {
		if cur.ready() {
			if cur.attempts < q.maxNumAttempts {
				return cur.take()
			}

			q.remove(cur.id)
		}
	}

	return nil
}

func (q *Queue) Remove(id string) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.remove(id)
}

func (q *Queue) remove(id string) {
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
	q.mu.Lock()
	defer q.mu.Unlock()

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

func (q *Queue) SetDeadline(id string, deadline time.Time) {
	//TODO track multiple ack ids for each msg
	q.mu.Lock()
	defer q.mu.Unlock()

	msg, ok := q.byid[id]
	if !ok {
		return
	}

	msg.deadline = deadline
	// msg was nacked, could remove it here
	if msg.ready() && msg.attempts < q.maxNumAttempts {
		select {
		case q.waiting <- msg:
			msg.take()
		default:
		}
	}
}

func (s *PubSub) Pull(ctx context.Context, req *pb.PullRequest) (*pb.PullResponse, error) {
	sub := lastName(req.Subscription)
	s.printf("Pull: %s\n", sub)

	queue, ok := s.getSubscription(sub)
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
	s.printf("Publish: %s\n", topic)

	var ids []string
	for _, msg := range req.GetMessages() {
		id := uuid.New().String()
		ids = append(ids, id)

		for _, sub := range s.topics[topic] {
			queue, ok := s.getSubscription(sub)
			if !ok {
				return nil, fmt.Errorf("unknown subscription: %s", sub)
			}
			queue.Push(id, msg.Data)
		}
	}

	return &pb.PublishResponse{MessageIds: ids}, nil
}

func (s *PubSub) Acknowledge(ctx context.Context, req *pb.AcknowledgeRequest) (*empty.Empty, error) {
	sub := lastName(req.Subscription)
	s.printf("Acknowledge: %s\n", sub)

	queue, ok := s.getSubscription(sub)
	if !ok {
		return nil, fmt.Errorf("unknown subscription: %s", sub)
	}

	for _, id := range req.GetAckIds() {
		queue.Remove(id)
	}

	return &empty.Empty{}, nil
}

func (s *PubSub) ModifyAckDeadline(ctx context.Context, req *pb.ModifyAckDeadlineRequest) (*empty.Empty, error) {
	seconds := req.GetAckDeadlineSeconds()
	deadline := time.Now().Add(time.Second * time.Duration(seconds))
	sub := lastName(req.Subscription)

	queue, ok := s.getSubscription(sub)
	if !ok {
		return nil, fmt.Errorf("unknown subscription: %s", sub)
	}

	acked := []string{}
	for _, id := range req.GetAckIds() {
		acked = append(acked, id)
		queue.SetDeadline(id, deadline)
	}

	s.printf("ModifyAckDeadline: %s (%v) = %d\n", sub, acked, seconds)
	return &empty.Empty{}, nil
}

func (s *PubSub) streamingSend(srv pb.Subscriber_StreamingPullServer, queue *Queue) error {
	for {
		err := func() error {

			var msgs []*pb.ReceivedMessage
			ctx, cancel := context.WithTimeout(srv.Context(), defaultWaitTimeout)
			defer cancel()

			i := 0

			//TODO: canceling receives context does not propagate here
			for ; i < 1; i++ {
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

			return srv.Send(&pb.StreamingPullResponse{
				ReceivedMessages: msgs,
			})
		}()
		if err != nil {
			return err
		}
	}
}

func (s *PubSub) streamingRecv(srv pb.Subscriber_StreamingPullServer, queue *Queue) error {

	for {
		req, err := srv.Recv()
		if err != nil {
			return err
		}

		for _, id := range req.GetAckIds() {
			queue.Remove(id)
		}

		for i, id := range req.GetModifyDeadlineAckIds() {
			seconds := req.ModifyDeadlineSeconds[i]
			deadline := time.Now().Add(time.Second * time.Duration(seconds))
			queue.SetDeadline(id, deadline)
		}
	}
}

func (s *PubSub) StreamingPull(srv pb.Subscriber_StreamingPullServer) error {
	req, err := srv.Recv()
	if err != nil {
		return err
	}

	if len(req.AckIds) != 0 || len(req.ModifyDeadlineAckIds) != 0 {
		log.Fatalf("unexpected: %#v\n", req)
	}

	sub := lastName(req.Subscription)
	s.printf("StreamingPull: %s\n", sub)
	queue, ok := s.getSubscription(sub)
	if !ok {
		return fmt.Errorf("unknown subscription: %s", sub)
	}

	go s.streamingSend(srv, queue)
	return s.streamingRecv(srv, queue)
}

func (s *PubSub) CreateSubscription(ctx context.Context, sub *pb.Subscription) (*pb.Subscription, error) {
	topic := lastName(sub.Topic)
	name := lastName(sub.Name)
	s.printf("CreateSubscription: %s -> %s\n", topic, name)

	s.mu.Lock()
	defer s.mu.Unlock()

	s.topics[topic] = append(s.topics[topic], name)
	s.subscriptions[name] = NewQueue(s.maxNumAttempts)

	return sub, nil
	//return nil, status.Errorf(codes.Unimplemented, "method CreateSubscription not implemented")
}

func (s *PubSub) CreateTopic(ctx context.Context, topic *pb.Topic) (*pb.Topic, error) {
	name := lastName(topic.Name)
	s.printf("CreateTopic: %s\n", name)
	return topic, nil
}

type TopicSubscription struct {
	Topic        string
	Subscription string
}

type Config struct {
	TopicsSubscriptions []TopicSubscription

	MaxNumAttempts int

	Verbose bool
}

func New(cfg *Config) *PubSub {
	topics := map[string][]string{}
	subscriptions := map[string]*Queue{}

	maxNumAttempts := cfg.MaxNumAttempts
	if maxNumAttempts == 0 {
		maxNumAttempts = defaultNumAttempts
	}

	for _, ts := range cfg.TopicsSubscriptions {
		topics[ts.Topic] = append(topics[ts.Topic], ts.Subscription)
		if _, ok := subscriptions[ts.Subscription]; ok {
			log.Fatalf("invalid config, duplicate subscriber: %s\n", ts.Subscription)
		}

		subscriptions[ts.Subscription] = NewQueue(maxNumAttempts)
	}

	return &PubSub{
		subscriptions: subscriptions,
		topics:        topics,

		maxNumAttempts: maxNumAttempts,
		verbose:        cfg.Verbose,
	}
}

func NewServer(cfg *Config) *grpc.Server {
	svc := New(cfg)
	s := grpc.NewServer()
	pb.RegisterSubscriberServer(s, svc)
	pb.RegisterPublisherServer(s, svc)

	return s
}
