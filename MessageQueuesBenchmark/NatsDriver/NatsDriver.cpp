// NatsDriver.cpp : Defines the functions for the static library.
//

#include "pch.h"
#include "framework.h"
#include "NatsDriver.h"
#include <stdexcept>

void NatsDriver::connect(const std::string& url)
{
	natsStatus status = natsConnection_ConnectTo(&nc, url.c_str());
	if (status != NATS_OK) {
		throw std::runtime_error("NATS Connection Failed!");
	}

	status = natsConnection_JetStream(&js, nc, NULL);
	if (status != NATS_OK) {
		natsConnection_Destroy(nc);
		throw std::runtime_error("NATS JetStream Context Initialization Failed!");
	}
}

void NatsDriver::send(const std::string& payload)
{
	natsMsg* msg = NULL;

	// Create the message container targeted at a specific subject topic
	natsStatus status = natsMsg_Create(&msg, "benchmark.topic", NULL, payload.data(), (int)payload.size());
	if (status != NATS_OK) {
		throw std::runtime_error("NATS Message Allocation Failed!");
	}

	status = js_PublishMsg(NULL, js, msg, NULL, NULL);
	if (status != NATS_OK) {
		natsMsg_Destroy(msg); // Prevent memory leak on failure path
		throw std::runtime_error("NATS Publish Failed!");
	}

	// Always free the message buffer frame after a synchronous publish
	natsMsg_Destroy(msg);
}

void NatsDriver::receive(std::string& outPayload, std::string mode)
{
    natsStatus status;
    natsMsg* msg = NULL;
	natsMsgList* msgList = nullptr; // For batch fetching in pull mode

    // FIRST-TIME INITIALIZATION: If the sub doesn't exist yet, bind it once.
    if (sub == NULL) {
        // Fix: Pass your active 'js' class context instead of a local NULL pointer.
        // We bind to "benchmark.topic" to listen to the producer's data stream.
        status = js_SubscribeSync(&sub, js, "benchmark.topic", NULL, NULL, NULL);
        if (status != NATS_OK) {
            throw std::runtime_error("NATS Subscribe Failed!");
        }
    }

    // FIX: Distinguish between configurations using the unified Enum path
    if (mode == "PULL") {
        // Pull mode: Explicitly fetch a message block
        status = natsSubscription_Fetch(msgList, sub, 1, 1000, NULL); // Fetch 1 message with a 1000ms timeout
    }
    else {
        // Push mode: Pull the next streaming frame already cached in local memory
        status = natsSubscription_NextMsg(&msg, sub, 1000);
    }

    if (status == NATS_OK && msg != NULL) {
        // Extract raw data pointers and assign them directly into your outPayload string
        const char* data = natsMsg_GetData(msg);
        int length = natsMsg_GetDataLength(msg);
        outPayload.assign(data, length);

        // Acknowledge receipt to clear the transactional log
        natsMsg_Ack(msg, NULL);

        // Clean up message memory context
        natsMsg_Destroy(msg);
    }
    else {
        throw std::runtime_error("NATS Receive Timeout or Interruption!");
    }
}