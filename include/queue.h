#ifndef QUEUE_H
#define QUEUE_H

#include <stddef.h>
#include <stdlib.h>

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

static void queue_destroy(Queue* q) {
    while (!empty(q))
        queue_pop(q);
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

    q->end->next = res;
    q->end = res;
    q->size++;
    return res;
}

// Returns node->data, frees the 'begin' node
static void* queue_pop(Queue* q) {
    if (empty(q))
        return NULL;

    void* res = q->begin->data;
    Node* old_begin = q->begin;
    q->begin = q->begin->next;
    free(old_begin);

    q->size--;
    return res;
}

#endif