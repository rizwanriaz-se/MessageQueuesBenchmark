#include "pch.h"
#include "framework.h"
#include "NatsDriver.h"
#include <iostream>

NatsDriver::~NatsDriver() {
    cleanupSubscription();
    if (nc) {
        natsConnection_Close(nc);
        natsConnection_Destroy(nc);
        nc = nullptr;
    }
}

// Add this helper in NatsDriver.cpp
void NatsDriver::ensureConsumer(const std::string& streamName,
    const std::string& consumerName,
    CommMode mode) {
    jsConsumerConfig cfg;
    jsConsumerConfig_Init(&cfg);
    cfg.Durable = consumerName.c_str();
    cfg.AckPolicy = js_AckExplicit;
    cfg.DeliverPolicy = js_DeliverAll;
    //cfg.FilterSubject = currentSubject.c_str();


    if (mode == CommMode::PUSH) {
        // Push consumer needs a deliver subject
        std::string deliverSubject = streamName + ".deliver." + consumerName;
        cfg.DeliverSubject = deliverSubject.c_str();
    }
    // Pull consumer: DeliverSubject stays NULL — that's what makes it pull

    jsConsumerInfo* info = NULL;
    jsErrCode       jerr = (jsErrCode)0;
    natsStatus s = js_AddConsumer(&info, js, streamName.c_str(), &cfg, &jsOpts, &jerr);
    if (s != NATS_OK) {
        const char* lastErr = nats_GetLastError(NULL);
        throw std::runtime_error(
            "ensureConsumer failed: " + std::string(natsStatus_GetText(s)) +
            " | JetStream code: " + std::to_string(jerr) +
            " | Detail: " + (lastErr ? lastErr : "none")
        );
    }
    jsConsumerInfo_Destroy(info);
}

void NatsDriver::cleanupSubscription() {
    if (sub) {
        natsSubscription_Unsubscribe(sub);
        natsSubscription_Destroy(sub);
        sub = nullptr;
    }
}

void NatsDriver::connect(const std::string& url) {
    natsStatus status = natsConnection_ConnectTo(&nc, url.c_str());
	jsOptions_Init(&jsOpts);
    if (status != NATS_OK) {
        throw std::runtime_error("Failed to connect to NATS Server core socket layer.");
    }

    status = natsConnection_JetStream(&js, nc, &jsOpts);
    if (status != NATS_OK) {
        natsConnection_Destroy(nc);
        nc = nullptr;
        throw std::runtime_error("Failed to initialize JetStream log processing engine context.");
    }
}

void NatsDriver::send(const std::string& payload) {
    if (!js) throw std::runtime_error("Driver uninitialized. Call connect() before issuing operations.");

    natsMsg* msg = NULL;
    natsStatus status = natsMsg_Create(&msg, currentSubject.c_str(), NULL, payload.data(), static_cast<int>(payload.size()));
    if (status != NATS_OK) {
        throw std::runtime_error("In-memory allocation of outbound message container failed.");
    }

    //jsAck ack;
    status = js_PublishMsg(NULL, js, msg, NULL, NULL);
    if (status != NATS_OK) {
        natsMsg_Destroy(msg);
        throw std::runtime_error("JetStream sync ledger drop failed. Status code: " + std::to_string(status));
    }

    natsMsg_Destroy(msg);
}

int NatsDriver::receive(std::string& outPayload, CommMode mode, int batchSize) {
    if (!js) throw std::runtime_error("Driver uninitialized.");

    natsStatus status;
    natsMsg* msg = NULL;
    natsMsgList msgList = { 0 };
	jsSubOptions subOpts;
	jsSubOptions_Init(&subOpts);
	subOpts.Stream = "Benchmark";
	

    // Dynamically manage infrastructure configuration switches when the matrix shifts axis styles
    if (sub == NULL || activeMode != mode) {
        cleanupSubscription();
        activeMode = mode;

        jsSubOptions subOpts;
        jsSubOptions_Init(&subOpts);
        subOpts.Stream = "Benchmark";

        if (mode == CommMode::PULL) {
            subOpts.Consumer = "benchmark-pull-consumer";
            status = js_PullSubscribe(&sub, js, currentSubject.c_str(),
                "benchmark-pull-consumer",
                NULL, &subOpts, NULL);
        }
        else {
            subOpts.Consumer = "benchmark-push-consumer";
            status = js_SubscribeSync(&sub, js, currentSubject.c_str(),
                NULL, &subOpts, NULL);
        }

        if (status != NATS_OK) {
			std::cout << "Subscription setup failure. Status code: " << status << std::endl;
            throw std::runtime_error("Failed to bind subscription model style. Code: " + std::to_string(status));
        }
    }

    if (mode == CommMode::PULL) {
        // Safe internal tracking allocation approach instead of zero-initialized pointer parameters
        status = natsSubscription_Fetch(&msgList, sub, batchSize, 2000, NULL);
        //std::cout << msgList.Count << std::endl;
        //std::cout << msgList.Msgs << std::endl;
        if (status == NATS_TIMEOUT || (status == NATS_OK && msgList.Count == 0)) {
            natsMsgList_Destroy(&msgList);
            throw std::runtime_error("TIMEOUT");
        }

        if (status != NATS_OK) {
            natsMsgList_Destroy(&msgList);
            throw std::runtime_error("Fetch failed. Code: " + std::to_string(status));
        }
        for (int i = 0; i < msgList.Count; i++) {
            natsMsg* currentMsg = msgList.Msgs[i];
            // In a production test scenario, you would parse each message here
            outPayload.assign(
                natsMsg_GetData(currentMsg),
                natsMsg_GetDataLength(currentMsg)
            );

            natsMsg_Ack(currentMsg, NULL);
        }
            //natsMsg_Ack(msgList.Msgs[i], NULL);

        //outPayload.assign(
        //    natsMsg_GetData(msgList.Msgs[msgList.Count - 1]),
        //    natsMsg_GetDataLength(msgList.Msgs[msgList.Count - 1])
        //);

        int count = msgList.Count;
        natsMsgList_Destroy(&msgList);
        return count;  // <-- caller adds this to processedCount       
    }
    else {
        status = natsSubscription_NextMsg(&msg, sub, 2000);
        if (status == NATS_OK && msg != NULL) {
            outPayload.assign(natsMsg_GetData(msg), natsMsg_GetDataLength(msg));
            natsMsg_Ack(msg, NULL);
            natsMsg_Destroy(msg);
            return 1;
        }
    }

}