#include "executor.h"

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>

#include "debug.h"
#include "err.h"
#include "future.h"
#include "mio.h"
#include "waker.h"

#define ASSERT_MALLOC_SUCCEEDED(expr)                                          \
    do {                                                                       \
        if ((expr) == NULL) {                                                  \
            fprintf(stderr,                                                    \
            "Memory allocation failed in %s:%d: In function %s():\n\t",        \
            __FILE__, __LINE__, __func__);                                     \
            exit(EXIT_FAILURE);                                                \
        }                                                                      \
    } while (0)

struct Node {
    void* data;
    struct Node* next;
};

typedef struct Node Node;

typedef struct Queue {
    Node* begin;
    Node* end;
    size_t size;
    size_t capacity;
} Queue;

static void queue_init(Queue* q, size_t capacity) {
    q->begin = q->end = NULL;
    q->size = 0;
    q->capacity = capacity;
}

static bool empty(Queue* q) {
    return q->size == 0;
}

// Returns the node that contains pushed data
static Node* queue_push(Queue* q, void* data) {
    if (q->size == q->capacity)
        return NULL;

    Node* res = (Node*) malloc(sizeof(Node));
    if (!res)
        return res;

    res->data = data;
    res->next = NULL;

    if (q->end == NULL) {
        q->begin = q->end = res;
    } else {
        q->end->next = res;
        q->end = res;
    }
    q->size++;
    return res;
}

// Returns node->data, frees the 'begin' node
static void* queue_pop(Queue* q) {
    if (empty(q))
        return NULL;

    void* res = q->begin->data;
    Node* old_begin = q->begin;
    q->begin = old_begin->next;
    if (q->begin == NULL)
        q->end = NULL;

    free(old_begin);

    q->size--;
    return res;
}

static void queue_destroy(Queue* q) {
    while (!empty(q))
        queue_pop(q);
}

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
    if (exec && waker->future)
        queue_push(&exec->futures_queue, waker->future);
}

void executor_spawn(Executor* executor, Future* fut) {
    fut->is_active = true;
    queue_push(&executor->futures_queue, fut);
}

void executor_run(Executor* executor) {
    Queue* q = &executor->futures_queue;
    while (!empty(q)) {
        size_t q_size = q->size;
        for (size_t i = 0; i < q_size; i++) {
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
