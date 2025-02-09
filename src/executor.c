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
    fut->is_active = true;
    queue_push(&executor->futures_queue, fut);
}

void executor_run(Executor* executor) {
    Queue* q = &executor->futures_queue;
    while (!empty(q)) {
        while (!empty(q)) {
            Future* current_fut = (Future*) queue_pop(q);
            Waker waker = (Waker) {.executor = executor, .future = current_fut};
            FutureState state = current_fut->progress(current_fut, executor->mio, waker);
            switch (state) {
                case FUTURE_COMPLETED:
                    current_fut->is_active = false;
                    break;
                case FUTURE_FAILURE:
                    current_fut->is_active = false;
                    break;
                case FUTURE_PENDING:
                    break;
            }
        }
        mio_poll(executor->mio);
    }
}

void executor_destroy(Executor* executor) {
    queue_destroy(&executor->futures_queue);
    mio_destroy(executor->mio);
    free(executor);
}
