#include "spmc_queue.h"

bool SpmcQueue::enqueue(const std::string& item) {
    // 1. Grab current head and tail locations
    size_t currentTail = tail.load(std::memory_order_relaxed);
    size_t currentHead = head.load(std::memory_order_acquire);

    // 2. Check if the ring buffer is full
    if ((currentTail + 1) % capacity == currentHead) {
        return false; // Queue is full, return immediately without blocking!
    }

    // 3. Drop the data into the raw array slot safely
    buffer[currentTail] = item;

    // 4. Move the tail forward by 1, letting the CPU know new data is ready
    tail.store((currentTail + 1) % capacity, std::memory_order_release);
    return true;
}

bool SpmcQueue::dequeue(std::string& outItem) {
    size_t currentHead = head.load(std::memory_order_relaxed);

    while (true) {
        size_t currentTail = tail.load(std::memory_order_acquire);

        // 1. Check if the queue is completely empty
        if (currentHead == currentTail) {
            return false; // No data to read
        }

        // 2. Speculatively grab the data from the slot
        std::string item = buffer[currentHead];

        // 3. THE HARDWARE FIGHT: 
        // This line asks the CPU: "Is 'head' still equal to 'currentHead'? 
        // If YES, change 'head' to 'currentHead + 1' atomically so no one else can steal it!"
        size_t nextHead = (currentHead + 1) % capacity;
        if (head.compare_exchange_weak(currentHead, nextHead,
            std::memory_order_release,
            std::memory_order_relaxed)) {
            // SUCCESS! This specific consumer thread won the race.
            outItem = std::move(item);
            return true;
        }

        // FAILURE! Another consumer thread updated 'head' first. 
        // The loop instantly loops back, updates 'currentHead' to the new reality, and tries again.
    }
}