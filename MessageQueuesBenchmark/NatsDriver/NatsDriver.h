#pragma once
#include <string>
#include <stdexcept>

extern "C" {
#include <nats/nats.h>
}

enum class CommMode {
    PUSH,
    PULL
};


class NatsDriver {
private:
    natsConnection* nc = nullptr;
    jsCtx* js = nullptr;
    natsSubscription* sub = nullptr;
    CommMode activeMode = CommMode::PUSH;
    std::string currentSubject = "benchmark.topic";
	jsOptions jsOpts;

    void cleanupSubscription();

public:
    NatsDriver() = default;
    ~NatsDriver();

    // Prevent accidental slicing or memory copying of underlying network socket descriptors
    NatsDriver(const NatsDriver&) = delete;
    NatsDriver& operator=(const NatsDriver&) = delete;

    void connect(const std::string& url);
    void send(const std::string& payload);
    int receive(std::string& outPayload, CommMode mode, int batchSize);
    void ensureConsumer(const std::string& streamName, const std::string& consumerName, CommMode mode);
};