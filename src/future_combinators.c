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

// ====================== JoinWrapperFuture =====================

typedef struct JoinWrapperFuture {
    Future base;
    JoinFuture* parent;
    bool is_first_child;
} JoinWrapperFuture;

FutureState future_join_wrapper_progress(Future* base, Mio* mio, Waker waker) {
    JoinWrapperFuture* fut = (JoinWrapperFuture*) fut;
    FutureState state = base->progress(base, mio, waker);
    switch (state) {
        case FUTURE_COMPLETED:
            bool* my_completion = fut->is_first_child ? &fut->parent->fut1_completed : &fut->parent->fut2_completed;
            bool* sibling_completion = fut->is_first_child ? &fut->parent->fut2_completed : &fut->parent->fut1_completed;
            int check_sibling_failed_value = fut->is_first_child ? JOIN_FUTURE_ERR_FUT2_FAILED : JOIN_FUTURE_ERR_FUT1_FAILED;
            void** future_ok_address = fut->is_first_child ? &fut->parent->result.fut1.ok : &fut->parent->result.fut2.ok;
            
            *my_completion = true;
            *future_ok_address = fut->base.ok;
 
            if (*sibling_completion || (fut->parent->base.errcode == check_sibling_failed_value)) { // wake up parent if we are the second one finished
                Waker paren_waker = (Waker) {.executor = waker.executor, .future = (Future*) fut->parent};
                waker_wake(&paren_waker);
            }
            break;
        case FUTURE_FAILURE:
            bool* sibling_completion = fut->is_first_child ? &fut->parent->fut2_completed : &fut->parent->fut1_completed;
            int* future_errcode_address = fut->is_first_child ? &fut->parent->result.fut1.errcode : &fut->parent->result.fut2.errcode;
            int fail_values[2] = {JOIN_FUTURE_ERR_FUT1_FAILED, JOIN_FUTURE_ERR_FUT2_FAILED};
            int check_sibling_failed_value = fail_values[fut->is_first_child];
            
            *future_errcode_address = base->errcode;
            fut->parent->base.errcode = (fut->parent->base.errcode == check_sibling_failed_value ? JOIN_FUTURE_ERR_BOTH_FUTS_FAILED : fail_values[1 - fut->is_first_child]);

            if (*sibling_completion || (fut->parent->base.errcode == JOIN_FUTURE_ERR_BOTH_FUTS_FAILED)) { // wake up parent if we are the second one finished
                Waker paren_waker = (Waker) {.executor = waker.executor, .future = (Future*) fut->parent};
                waker_wake(&paren_waker);
            }
            break;
        case FUTURE_PENDING:
            break;
    }
    return state;
}

JoinWrapperFuture join_wrapper_future(Future* parent, bool is_first_child) {
    return (JoinWrapperFuture) {
        .base = future_create(future_join_wrapper_progress),
        .parent = parent,
        .is_first_child = is_first_child
    };
}

// ========================= JoinFuture =========================

FutureState future_join_progress(Future* base, Mio* mio, Waker waker) {
    JoinFuture* fut = (JoinFuture*) base;

    FutureState state1 = FUTURE_COMPLETED;
    FutureState state2 = FUTURE_COMPLETED;

    Future* fut1 = fut->fut1;
    Future* fut2 = fut->fut2;

    if (!fut->fut1_completed && ((fut->base.errcode == JOIN_FUTURE_ERR_FUT2_FAILED) || (fut->base.errcode == FUTURE_SUCCESS))) {
        state1 = fut1->progress(fut1, mio, waker);
        switch (state1) {
            case FUTURE_COMPLETED:
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
        }
    }

    if (!fut->fut2_completed && ((fut->base.errcode == JOIN_FUTURE_ERR_FUT1_FAILED) || (fut->base.errcode == FUTURE_SUCCESS))) {
        // allow enqueuing Future* base twice
        state2 = fut2->progress(fut2, mio, waker);
        switch (state2) {
            case FUTURE_COMPLETED:        
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
        }
    }

    if ((state1 == FUTURE_PENDING) || (state2 == FUTURE_PENDING))
        return FUTURE_PENDING;

    if ((fut->base.errcode == JOIN_FUTURE_ERR_FUT1_FAILED) 
        || (fut->base.errcode == JOIN_FUTURE_ERR_FUT2_FAILED) 
        || (fut->base.errcode == JOIN_FUTURE_ERR_BOTH_FUTS_FAILED)
    ) {
        return FUTURE_FAILURE;
    } else {
        fut->base.errcode = FUTURE_SUCCESS;
        return FUTURE_SUCCESS;
    }
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
    debug("ENTERING SELECT FUTURE\n");
    if ((fut->which_completed == SELECT_COMPLETED_FUT1) || (fut->which_completed == SELECT_COMPLETED_FUT2)) {
        fut->base.errcode = FUTURE_SUCCESS;
        debug("RETURNING EARLY...\n");
        return FUTURE_COMPLETED;
    }

    Future* fut1 = fut->fut1;
    Future* fut2 = fut->fut2;
    
    FutureState state1 = fut1->progress(fut1, mio, waker);
    debug("FUT1 returned state = %d\n", state1);
    switch (state1) {
        case FUTURE_COMPLETED:
            fut->which_completed = SELECT_COMPLETED_FUT1;
            fut->base.errcode = FUTURE_SUCCESS;
            return FUTURE_COMPLETED;
        case FUTURE_FAILURE:
            fut->base.errcode = fut1->errcode;
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

    // don't allow enqueuing Future* base twice
    FutureState state2 = fut2->progress(fut2, mio, (Waker) {.executor = NULL, .future = fut2});
    debug("FUT2 returned state = %d\n", state2);
    switch (state2) {
        case FUTURE_COMPLETED:
            fut->which_completed = SELECT_COMPLETED_FUT2;
            fut->base.errcode = FUTURE_SUCCESS;
            return FUTURE_COMPLETED;
        case FUTURE_FAILURE:
            fut->base.errcode = fut2->errcode;
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
