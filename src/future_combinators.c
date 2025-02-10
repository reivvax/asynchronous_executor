#include "future_combinators.h"
#include <stdlib.h>

#include "future.h"
#include "waker.h"

// TODO: delete this once not needed.
#define UNIMPLEMENTED (exit(42))

// ========================= ThenFuture =========================

FutureState future_then_progress(Future* base, Mio* mio, Waker waker) {
    ThenFuture* fut = (ThenFuture*) base;

    if (!fut->fut1_completed) {
        Future* fut1 = fut->fut1;
        FutureState state = fut1->progress(fut1, mio, waker);
        switch (state) {
            case FUTURE_COMPLETED:
                fut->fut1_completed = true;
                fut->fut2->arg = fut1->ok; // set the argument of another future
                waker_wake(&waker);
                return FUTURE_PENDING;
            case FUTURE_FAILURE:
                fut->base.errcode = THEN_FUTURE_ERR_FUT1_FAILED;
                return FUTURE_FAILURE;
            case FUTURE_PENDING:
                return FUTURE_PENDING;
        }
    } else {
        Future* fut2 = fut->fut2;
        FutureState state = fut2->progress(fut2, mio, waker);
        switch (state) {
            case FUTURE_COMPLETED:
                fut->base.ok = fut2->ok;
                break;
            case FUTURE_FAILURE:
                fut->base.errcode = THEN_FUTURE_ERR_FUT2_FAILED;
                break;
            case FUTURE_PENDING:
                break;
        }

        return state;
    }
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
    UNIMPLEMENTED;
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
    UNIMPLEMENTED;
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
