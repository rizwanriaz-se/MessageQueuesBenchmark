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

public:
	void connect(const std::string& connectionString);
	void send(const std::vector<uint8_t>& payload);
	void receive(std::vector<uint8_t> outPayload);
};

