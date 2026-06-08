#pragma once
#include <string>
#include <vector>

extern "C" {
	#include <nats/nats.h>
}

class NatsDriver {
private:
	natsConnection* nc = nullptr;
	natsSubscription* sub = nullptr;
	jsCtx* js = nullptr;

public:
	void connect(const std::string& url);
	void send(const std::string& payload);
	void receive(std::string& outPayload, std::string mode);
};

