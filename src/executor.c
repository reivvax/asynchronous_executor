#include "executor.h"

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>

#include "debug.h"
#include "utils.h"
#include "err.h"
#include "future.h"
#include "mio.h"
#include "waker.h"
#include "queue.h"

/**
 * @brief Structure to represent the current-thread executor.
 */
struct Executor {
    Queue futures_queue;
    int futures_queue_size;
    int queue_capacity;
    bool running;
    Mio* mio;
};

// TODO: delete this once not needed.
#define UNIMPLEMENTED (exit(42))

Executor* executor_create(size_t max_queue_size) {
    Executor* res = (Executor*) malloc(sizeof(Executor));
    ASSERT_MALLOC_SUCCEEDED(res);
    queue_init(&res->futures_queue, max_queue_size);

    res->futures_queue_size = 0;
    res->queue_capacity = max_queue_size;
    res->running = false;
    res->mio = mio_create(res);

    return res;
}

void waker_wake(Waker* waker) {
    Executor* exec = (Executor*) waker->executor;
    queue_push(&exec->futures_queue, waker->future);
}

void executor_spawn(Executor* executor, Future* fut) {
    queue_push(&executor->futures_queue, fut); // something more perhaps?
}

void executor_run(Executor* executor) {
    UNIMPLEMENTED;

    executor->running = true;
    while (executor->running) {
        // ...
    }
}

void executor_destroy(Executor* executor) {
    queue_destroy(&executor->futures_queue);
    mio_destroy(executor->mio);
    free(executor);
}
