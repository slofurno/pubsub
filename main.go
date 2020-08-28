package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"strings"

	"github.com/slofurno/pubsub/emulator"
)

func parseArgs(args []string) (*emulator.Config, string) {
	address := "127.0.0.1:4004"
	cfg := &emulator.Config{}

	for i := 0; i < len(args); i += 2 {
		switch args[i] {
		case "-sub", "--sub":
			parts := strings.Split(args[i+1], ":")
			if len(parts) != 2 {
				log.Fatalf("expected format topic:subscriber, got: %s\n", args[i+1])
			}
			cfg.TopicsSubscriptions = append(cfg.TopicsSubscriptions, emulator.TopicSubscription{
				Topic:        parts[0],
				Subscription: parts[1],
			})
		case "-address", "--address":
			address = args[i+1]
		default:
			log.Fatalf("unknown arg: %s\n", args[i])
		}
	}

	return cfg, address
}

func main() {
	args := os.Args[1:]

	cfg, address := parseArgs(args)
	svc := emulator.NewServer(cfg)
	fmt.Printf("listening on: %s\n%s\n", address, svc)

	lis, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	if err := svc.Serve(lis); err != nil {
		fmt.Printf("failed to serve: %v", err)
	}
}
