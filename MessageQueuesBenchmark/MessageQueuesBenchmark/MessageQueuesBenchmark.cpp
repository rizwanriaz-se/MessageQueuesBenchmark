// MessageQueuesBenchmark.cpp : This file contains the 'main' function. Program execution begins and ends there.
//

#include <iostream>
#include "NatsDriver.h"
#include "spmc_queue.h"

int main()
{
	NatsDriver driver;
	driver.connect("nats://localhost:4222");

	std::string payload = "Hello, NATS!";
	for (int i = 0; i < 100; ++i) {
		payload = "Hello, NATS!";
		payload += payload +" " + std::to_string(i);
		driver.send(payload);
	}


	std::string receivedPayload;

	for (int i = 0; i < 5; ++i) {
		driver.receive(receivedPayload, "PUSH");
	}


	SpmcQueue queue(10);
	queue.enqueue(receivedPayload);

	// worker threads will dequeue spmc queue
	for (int i = 0; i < 5; ++i) {
		std::string item;
		while (true) {
			if (queue.dequeue(item)) {
				// Process the item
				std::cout << "Processed Item: " << item << std::endl;
			}
		}
	}

	//std::cout << "Received Payload: " << receivedPayload << std::endl;

	return 0;
}

// Run program: Ctrl + F5 or Debug > Start Without Debugging menu
// Debug program: F5 or Debug > Start Debugging menu

// Tips for Getting Started: 
//   1. Use the Solution Explorer window to add/manage files
//   2. Use the Team Explorer window to connect to source control
//   3. Use the Output window to see build output and other messages
//   4. Use the Error List window to view errors
//   5. Go to Project > Add New Item to create new code files, or Project > Add Existing Item to add existing code files to the project
//   6. In the future, to open this project again, go to File > Open > Project and select the .sln file
