#include "mio.h"

#include <stdint.h>
#include <stdlib.h>
#include <sys/epoll.h>
#include <unistd.h>

#include "utils.h"
#include "err.h"
#include "debug.h"
#include "executor.h"
#include "waker.h"
#include "errno.h"

// Maximum number of events to handle per epoll_wait call.
#define MAX_EVENTS 64

struct Mio {
    int registered_events_count;
    int epoll_fd;
    struct epoll_event events[MAX_EVENTS];
};

Mio* mio_create(Executor* executor) {
    int epoll_fd = epoll_create1(0);
    if (epoll_fd == -1)
        return NULL;

    Mio* res = (Mio*) calloc(1, sizeof(Mio));
    if (!res) {
        close(epoll_fd);
        return NULL;
    }

    res->epoll_fd = epoll_fd;
    return res;
}

void mio_destroy(Mio* mio) {
    close(mio->epoll_fd);
    free(mio);
}

int mio_register(Mio* mio, int fd, uint32_t events, Waker waker) {
    debug("Registering (in Mio = %p) fd = %d\n", mio, fd);

    Waker* waker_copy = (Waker*) malloc(sizeof(Waker));
    if (!waker_copy)
        return -1;

    *waker_copy = waker;

    struct epoll_event ev;
    ev.events = events;
    ev.data.ptr = (void*) waker_copy;
    int res = epoll_ctl(mio->epoll_fd, EPOLL_CTL_ADD, fd, &ev);
    
    if (res == 0)
        mio->registered_events_count++;
    else if (res == -1 && errno == EEXIST)
        res = epoll_ctl(mio->epoll_fd, EPOLL_CTL_MOD, fd, &ev);

    return res;
}

int mio_unregister(Mio* mio, int fd) {
    debug("Unregistering (from Mio = %p) fd = %d\n", mio, fd);

    int res = epoll_ctl(mio->epoll_fd, EPOLL_CTL_DEL, fd, NULL);
    if (res == 0)
        mio->registered_events_count--;
    if (res == -1 && errno == ENOENT)
        res = 0;

    return res;
}

void mio_poll(Mio* mio) {
    debug("Mio (%p) polling\n", mio);

    if (mio->registered_events_count > 0) {
        int event_count = epoll_wait(mio->epoll_fd, mio->events, MAX_EVENTS, -1);
        ASSERT_SYS_OK(event_count);
        for (int i = 0; i < event_count; i++) {
            Waker* waker = (Waker*) mio->events[i].data.ptr;
            waker_wake(waker);
            free(waker);
        }
    }
}
