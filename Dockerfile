FROM scratch

COPY pubsub /pubsub

ENTRYPOINT ["/pubsub"]
