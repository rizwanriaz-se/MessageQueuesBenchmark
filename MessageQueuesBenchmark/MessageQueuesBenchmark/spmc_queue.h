#pragma once
#include <vector>
#include <atomic>
#include <string>

class SpmcQueue {
private:
    std::vector<std::string> buffer;
    size_t capacity;

    // These are our atomic "pins". They are tracked directly by CPU hardware.
    std::atomic<size_t> head{ 0 }; // Points to the next item consumers can read
    std::atomic<size_t> tail{ 0 }; // Points to the next slot the producer can write into

public:
    SpmcQueue(size_t size) : capacity(size) {
        buffer.resize(size);
    }

    bool enqueue(const std::string& item);
    bool dequeue(std::string& outItem);
};