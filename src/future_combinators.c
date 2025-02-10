#include "future_combinators.h"
#include <stdlib.h>

#include "future.h"
#include "waker.h"

// ========================= ThenFuture =========================

FutureState future_then_progress(Future* base, Mio* mio, Waker waker) {
    ThenFuture* fut = (ThenFuture*) base;
    FutureState state;
    if (!fut->fut1_completed) {
        Future* fut1 = fut->fut1;
        state = fut1->progress(fut1, mio, waker);
        switch (state) {
            case FUTURE_COMPLETED:
                fut->fut1_completed = true;
                fut->fut2->arg = fut1->ok; // set the argument of another future
                waker_wake(&waker);
                return FUTURE_PENDING;
            case FUTURE_FAILURE:
                fut->base.errcode = THEN_FUTURE_ERR_FUT1_FAILED;
                break;
            case FUTURE_PENDING:
                break;
            default:
                debug("SOMETHINGS WRONG IN THENFUTURE = %p\n", fut);
        }
    } else {
        Future* fut2 = fut->fut2;
        state = fut2->progress(fut2, mio, waker);
        switch (state) {
            case FUTURE_COMPLETED:
                fut->base.ok = fut2->ok;
                break;
            case FUTURE_FAILURE:
                fut->base.errcode = THEN_FUTURE_ERR_FUT2_FAILED;
                break;
            case FUTURE_PENDING:
                break;
            default:
                debug("SOMETHINGS WRONG IN THENFUTURE = %p\n", fut);
        }
    }

    return state;
}

ThenFuture future_then(Future* fut1, Future* fut2) {
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

    FutureState state1 = FUTURE_COMPLETED;
    FutureState state2 = FUTURE_COMPLETED;

    Future* fut1 = fut->fut1;
    Future* fut2 = fut->fut2;

    if (!fut->fut1_completed 
        || ((fut->base.errcode != JOIN_FUTURE_ERR_FUT1_FAILED) && (fut->base.errcode != JOIN_FUTURE_ERR_BOTH_FUTS_FAILED))
    ) {
        state1 = fut1->progress(fut1, mio, waker);
        debug("Calling fut1->progress() in JoinFuture = %p, got state %d\n", fut, state1);
        switch (state1) {
            case FUTURE_COMPLETED:
                debug("fut1->progress() in JoinFuture = %p returned COMPLETED, fut1->ok = %p\n", fut1, fut1->ok);
                fut->fut1_completed = true;
                fut->result.fut1.ok = fut1->ok;
                break;
            case FUTURE_FAILURE:
                fut->base.errcode = 
                    (fut->base.errcode == JOIN_FUTURE_ERR_FUT2_FAILED ? JOIN_FUTURE_ERR_BOTH_FUTS_FAILED : JOIN_FUTURE_ERR_FUT1_FAILED);
                fut->result.fut1.errcode = fut1->errcode;
                break;
            case FUTURE_PENDING:
                break;
            default:
                debug("SOMETHINGS WRONG IN JOINFUTURE = %p\n", fut);
        }
    }

    if (!fut->fut2_completed
        || ((fut->base.errcode != JOIN_FUTURE_ERR_FUT2_FAILED) && (fut->base.errcode != JOIN_FUTURE_ERR_BOTH_FUTS_FAILED))
    ) {
        // allow enqueuing Future* base twice
        state2 = fut2->progress(fut2, mio, waker);
        debug("Calling fut2->progress() in JoinFuture = %p, got state %d\n", fut, state2);
        switch (state2) {
            case FUTURE_COMPLETED:        
                debug("fut2->progress() in JoinFuture = %p returned COMPLETED, fut2->ok = %p\n", fut2, fut2->ok);
                fut->fut2_completed = true;
                fut->result.fut2.ok = fut2->ok;
                break;
            case FUTURE_FAILURE:
                fut->base.errcode = 
                    (fut->base.errcode == JOIN_FUTURE_ERR_FUT1_FAILED ? JOIN_FUTURE_ERR_BOTH_FUTS_FAILED : JOIN_FUTURE_ERR_FUT2_FAILED);
                fut->result.fut2.errcode = fut2->errcode;
                break;
            case FUTURE_PENDING:
                break;
            default:
                debug("SOMETHINGS WRONG IN JOINFUTURE = %p\n", fut);
        }
    }

    if ((state1 == FUTURE_PENDING) || (state2 == FUTURE_PENDING))
        return FUTURE_PENDING;

    if ((fut->base.errcode == JOIN_FUTURE_ERR_FUT1_FAILED) 
        || (fut->base.errcode = JOIN_FUTURE_ERR_FUT2_FAILED) 
        || (fut->base.errcode == JOIN_FUTURE_ERR_BOTH_FUTS_FAILED)
    )
        return FUTURE_FAILURE;
    else
        return FUTURE_SUCCESS;
}

JoinFuture future_join(Future* fut1, Future* fut2) {
    return (JoinFuture) {
        .base = future_create(future_join_progress),
        .fut1 = fut1,
        .fut2 = fut2,
        .fut1_completed = false,
        .fut2_completed = false,
        .result = {
            .fut1 = {.ok = NULL, .errcode = FUTURE_SUCCESS},
            .fut2 = {.ok = NULL, .errcode = FUTURE_SUCCESS}
        }
    };
}

// ========================= SelectFuture =========================

FutureState future_select_progress(Future* base, Mio* mio, Waker waker) {
    SelectFuture* fut = (SelectFuture*) base;

    if (fut->which_completed == SELECT_COMPLETED_FUT1 || fut->which_completed == SELECT_COMPLETED_FUT2)
        return FUTURE_COMPLETED;

    Future* fut1 = fut->fut1;
    Future* fut2 = fut->fut2;
    
    FutureState state1 = fut1->progress(fut1, mio, waker);
    switch (state1) {
        case FUTURE_COMPLETED:
            fut->which_completed = SELECT_COMPLETED_FUT1;
            return FUTURE_COMPLETED;
        case FUTURE_FAILURE:
            if (fut->which_completed == SELECT_FAILED_FUT2) {
                fut->which_completed = SELECT_FAILED_BOTH;
                return FUTURE_FAILURE;
            } else {
                fut->which_completed = SELECT_FAILED_FUT1;
            }
            break;
        case FUTURE_PENDING:
            break;
    }

    // allow enqueuing Future* base twice
    FutureState state2 = fut2->progress(fut2, mio, waker);
    switch (state2) {
        case FUTURE_COMPLETED:
            fut->which_completed = SELECT_COMPLETED_FUT2;
            return FUTURE_COMPLETED;
        case FUTURE_FAILURE:
            if (fut->which_completed == SELECT_FAILED_FUT1) {
                fut->which_completed = SELECT_FAILED_BOTH;
                return FUTURE_FAILURE;
            } else {
                fut->which_completed = SELECT_FAILED_FUT2;
            }
            break;
        case FUTURE_PENDING:
            break;
    }

    return FUTURE_PENDING;
}


SelectFuture future_select(Future* fut1, Future* fut2) {
    return (SelectFuture) {
        .base = future_create(future_select_progress),
        .fut1 = fut1,
        .fut2 = fut2,
        .which_completed = SELECT_COMPLETED_NONE
    };
}
