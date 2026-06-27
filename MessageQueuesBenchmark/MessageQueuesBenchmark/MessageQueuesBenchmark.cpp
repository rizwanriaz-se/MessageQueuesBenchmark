#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <atomic>
#include <chrono>
#include <memory>
#include "NatsDriver.h"
#include "spmc_queue.h"

int main()
{
	// High-performance test constants for your paper metrics
	const int TOTAL_TEST_MESSAGES = 1000000;
	const int SPMC_QUEUE_CAPACITY = 4096;
	const int WORKER_THREAD_COUNT = 4;
	const CommMode RUN_MODE = CommMode::PULL;

	try {
		auto driver = std::make_shared<NatsDriver>();
		std::cout << "[INIT] Connecting network wrapper to cluster endpoint..." << std::endl;
		driver->connect("nats://localhost:4222");

		if (RUN_MODE == CommMode::PULL) {
			driver->ensureConsumer("Benchmark", "benchmark-pull-consumer", RUN_MODE);
		}
		else {
			driver->ensureConsumer("Benchmark", "benchmark-push-consumer", RUN_MODE);
		}
		// =================================================================
		// PHASE 1: MASS SEED INGESTION (PRODUCER INTENSITY LOAD)
		// =================================================================
		std::cout << "[PRODUCER] Blasting " << TOTAL_TEST_MESSAGES << " messages into the broker log..." << std::endl;
		auto startPublish = std::chrono::high_resolution_clock::now();

		for (int i = 0; i < TOTAL_TEST_MESSAGES; ++i) {
			std::string payload = "Payload Frame Identification Index String: " + std::to_string(i);
			driver->send(payload);
		}

		auto endPublish = std::chrono::high_resolution_clock::now();
		auto publishDuration = std::chrono::duration_cast<std::chrono::milliseconds>(endPublish - startPublish).count();
		std::cout << "[SUCCESS] Production phase complete in: " << publishDuration << " ms" << std::endl;

		// =================================================================
		// PHASE 2: MUTLI-THREADED STAGING AND PIPELINE EVALUATION
		// =================================================================
		std::atomic<bool> isRunning{ true };
		std::atomic<int> processedCount{ 0 };
		std::vector<std::thread> workerPool;

		auto startConsume = std::chrono::high_resolution_clock::now();

		if (RUN_MODE == CommMode::PUSH) {
			// =============================================================
			// PUSH PATH: Single Network Thread -> SPMC Queue -> Workers
			// =============================================================
			std::cout << "[MODE] Running PUSH Mode (Memory-Buffered Pipeline)" << std::endl;
			SpmcQueue processingQueue(SPMC_QUEUE_CAPACITY);

			//auto processingQueue = std::make_shared<SpmcQueue>(SPMC_QUEUE_CAPACITY);

			// 1. Spawning the Single isolated Network Ingestion Thread
			std::thread networkIngestionThread([driver, &processingQueue, &isRunning]() {
				while (isRunning.load(std::memory_order_relaxed)) {
					try {
						std::string transitBuffer;
						driver->receive(transitBuffer, RUN_MODE, 100);

						// Keep trying to push to the local queue until a slot opens up
						while (!processingQueue.enqueue(transitBuffer)) {
							_mm_pause(); // Low-latency execution hint to hardware processor
						}
					}
					catch (const std::runtime_error& e) {
						if (std::string(e.what()) == "TIMEOUT") {
							// The broker log has been completely emptied
							break;
						}
						std::cerr << "[NETWORK ERROR] " << e.what() << std::endl;
						break;
					}
				}
				});

			for (int i = 0; i < WORKER_THREAD_COUNT; ++i) {
				workerPool.emplace_back(([&processingQueue, &isRunning, &processedCount, TOTAL_TEST_MESSAGES]() {
					std::string workPayload;
					while (isRunning.load(std::memory_order_relaxed)) {
						if (processingQueue.dequeue(workPayload)) {

							// -------------------------------------------------
							// METRIC MONITORING LOCATION HERE
							// -------------------------------------------------
							// In your final code, extract timestamps from workPayload 
							// and compute p95/p99 latency calculations here.

							int currentProgress = ++processedCount;
							if (currentProgress >= TOTAL_TEST_MESSAGES) {
								isRunning.store(false, std::memory_order_release);
							}
						}
						else {
							// Queue is temporarily empty, wait for network ingestion to catch up
							if (!isRunning.load(std::memory_order_relaxed)) break;
							_mm_pause();
						}
					}
					}));
			}

			// Wait for the pipeline infrastructure to drain completely
			if (networkIngestionThread.joinable()) {
				networkIngestionThread.join();
			}

		}
		else {
			// =============================================================
			// PULL PATH: Workers Hit the Network Directly in Parallel
			// =============================================================
			std::cout << "[MODE] Running PULL Mode (Distributed Batch Contention)" << std::endl;
			//driver->ensureConsumer("Benchmark", "shared-benchmark-consumer-group");
			//auto processingQueue = std::make_shared<SpmcQueue>(SPMC_QUEUE_CAPACITY);
			for (int i = 0; i < WORKER_THREAD_COUNT; ++i) {
				workerPool.emplace_back(([&isRunning, &processedCount, TOTAL_TEST_MESSAGES]() {
					try {
						NatsDriver threadLocalDriver;
						threadLocalDriver.connect("nats://localhost:4222");

						std::string workPayload;
						while (isRunning.load(std::memory_order_relaxed)) {
							try {

								int consumed = threadLocalDriver.receive(workPayload, RUN_MODE, 200);

								// -------------------------------------------------s
								// METRIC MONITORING LOCATION HERE
								// -------------------------------------------------
								// In your final code, extract timestamps from workPayload 
								// and compute p95/p99 latency calculations here.

								int currentProgress = processedCount.fetch_add(consumed, std::memory_order_relaxed) + consumed;
								if (currentProgress >= TOTAL_TEST_MESSAGES) {
									isRunning.store(false, std::memory_order_release);
								}
							}
							catch (const std::runtime_error& e) {
								if (std::string(e.what()) == "TIMEOUT") {
									// FIX: If the message goal is reached, exit gracefully
									if (processedCount.load() >= TOTAL_TEST_MESSAGES) {
										isRunning.store(false, std::memory_order_release);
										break;
									}
									// OTHERWISE: Keep spinning! The queue is just warming up.
									continue;
								}
								else {
									// Genuine network/driver crash path
									std::cerr << "[DRIVER TRACE] Error: " << e.what() << std::endl;
									break;
								}
							}
						}
					}

					catch (const std::exception& e) {
						std::cerr << "[WORKER ERROR] " << e.what() << std::endl;
					}
					}));
			}
		}
		std::cout << "[ORCHESTRATOR] Initializing background SPMC isolation memory layers..." << std::endl;



	for (auto& thread : workerPool) {
		if (thread.joinable()) thread.join();
	}

	auto endConsume = std::chrono::high_resolution_clock::now();
	auto consumeDuration = std::chrono::duration_cast<std::chrono::milliseconds>(endConsume - startConsume).count();

	std::cout << "\n========================================================" << std::endl;
	std::cout << "               FINAL BENCHMARK SUMMARY                 " << std::endl;
	std::cout << "========================================================" << std::endl;
	std::cout << " Configured Mode           : " << (RUN_MODE == CommMode::PUSH ? "PUSH" : "PULL") << std::endl;
	std::cout << " Configured Threads        : " << WORKER_THREAD_COUNT << " workers." << std::endl;
	std::cout << " Total Core Evaluated Load : " << processedCount.load() << " messages." << std::endl;
	std::cout << " Multi-Core Consumption Time : " << consumeDuration << " ms" << std::endl;
	std::cout << " System Performance Yield  : " << (processedCount.load() / (consumeDuration / 1000.0)) << " msg/sec" << std::endl;
	std::cout << "========================================================" << std::endl;

}
catch (const std::exception& e) {
	std::cerr << "[FATAL CRASH] Execution loop halted: " << e.what() << std::endl;
	return 1;
}

return 0;
}