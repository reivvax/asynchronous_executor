#include "future_combinators.h"
#include <stdlib.h>

#include "future.h"
#include "waker.h"

// TODO: delete this once not needed.
#define UNIMPLEMENTED (exit(42))

// ========================= ThenFuture =========================

FutureState future_then_progress(Future* base, Mio* mio, Waker waker) {
    ThenFuture* fut = (ThenFuture*) base;

}

ThenFuture future_then(Future* fut1, Future* fut2)
{
    return (ThenFuture) {
        .base = future_create(future_then_progress),
        .fut1 = fut1,
        .fut2 = fut2,
        .fut1_completed = false
    };
}

// ========================= JoinFuture =========================

FutureState future_join_progress(Future* base, Mio* mio, Waker waker) {
    JoinFuture* fut = (JoinFuture*) base;

}

JoinFuture future_join(Future* fut1, Future* fut2)
{
    return (JoinFuture) {
        .base = future_create(future_join_progress),
        .fut1 = fut1,
        .fut2 = fut2,
        .fut1_completed = false,
        .fut2_completed = false,
        .result = {0}
    };
}

// ========================= SelectFuture =========================

FutureState future_select_progress(Future* base, Mio* mio, Waker waker) {
    SelectFuture* fut = (SelectFuture*) base;

}


SelectFuture future_select(Future* fut1, Future* fut2)
{
    UNIMPLEMENTED;
    return (SelectFuture) {
        .base = future_create(future_select_progress),
        .fut1 = fut1,
        .fut2 = fut2,
        .which_completed = SELECT_COMPLETED_NONE
    };
}
