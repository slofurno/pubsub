#### Partial implementation of google pubsub publisher/subscriber server

Yes, there is already a [google cloud pubsub emulator](https://cloud.google.com/pubsub/docs/emulator),
but this doesn't require a jdk and is easier to configure.

Does not implement:
- topic/subscription creation, these must be configured at runtime
- deadletter topics

#### Usage

```sh
go build

./pubsub --address 127.0.0.1:4004 --sub my-topic:my-subscription --sub my-topic-2:my-subscription-2
```

or in docker

```sh
docker run --rm -p4004:4004 slofurno/pubsub --address :4004 --sub my-topic:my-subscription --sub my-topic-2:my-subscription-2

```

fan out

```
--sub topic:subscription1 -- sub topic:subscription2
```

set env for the pubsub sdk

```sh
export PUBSUB_EMULATOR_HOST=127.0.0.1:4004
```

use proto clients as normal

```go
import (
  "fmt"
  "context"

  "cloud.google.com/go/pubsub"
)


func main() {
  ctx := context.Background()

  ps, _ := pubsub.NewClient(ctx, "fake-project-name")
  topic := ps.Topic("my-topic")

  res := topic.Publish(ctx, &pubsub.Message{
    Data: []byte("hi"),
  })

  sub := ps.Subscription("my-subscription")
  sub.Receive(context.Background(), func(ctx context.Context, msg *pubsub.Message) {
    fmt.Printf("recv: %s\n", string(msg.Data))
    msg.Ack()
  })
}
```

