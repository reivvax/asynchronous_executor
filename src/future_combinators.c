#include "future_combinators.h"
#include <stdlib.h>

#include "future.h"
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
    Future* subtask;
    bool is_first_child;
} JoinWrapperFuture;

FutureState future_join_wrapper_progress(Future* base, Mio* mio, Waker waker) {
    debug("ENTERING WRAPPER PROGRESS\n");
    JoinWrapperFuture* fut = (JoinWrapperFuture*) fut;
    JoinFuture* parent = (JoinFuture*) waker.future;
    Future* subtask = fut->subtask;
    debug("before call\n");
    FutureState state = subtask->progress(subtask, mio, (Waker) {.executor = waker.executor, .future = base});
    debug("after call\n");
    FutureState* my_state = (fut->is_first_child ? &parent->fut1_completed : &parent->fut2_completed);
    FutureState* sibling_state = fut->is_first_child ? &parent->fut2_completed : &parent->fut1_completed;
    int check_sibling_failed_value;

    switch (state) {
        case FUTURE_COMPLETED:
            check_sibling_failed_value = fut->is_first_child ? JOIN_FUTURE_ERR_FUT2_FAILED : JOIN_FUTURE_ERR_FUT1_FAILED;
            // void** future_ok_address = fut->is_first_child ? &parent->result.fut1.ok : &parent->result.fut2.ok;

            *my_state = FUTURE_COMPLETED;
            if (fut->is_first_child)
                parent->result.fut1.ok = fut->base.ok;
            else
                parent->result.fut2.ok = fut->base.ok;
            // *future_ok_address = fut->base.ok;

            if (*sibling_state == FUTURE_COMPLETED || (parent->base.errcode == check_sibling_failed_value)) // wake up parent if we are the second one finished
                waker_wake(&waker);

            break;
        case FUTURE_FAILURE:
            int* future_errcode_address = fut->is_first_child ? &parent->result.fut1.errcode : &parent->result.fut2.errcode;
            int fail_values[2] = {JOIN_FUTURE_ERR_FUT1_FAILED, JOIN_FUTURE_ERR_FUT2_FAILED};
            check_sibling_failed_value = fail_values[fut->is_first_child];

            *my_state = FUTURE_COMPLETED;
            *future_errcode_address = base->errcode;
            parent->base.errcode = (parent->base.errcode == check_sibling_failed_value ? JOIN_FUTURE_ERR_BOTH_FUTS_FAILED : fail_values[1 - fut->is_first_child]);

            if (*sibling_state == FUTURE_COMPLETED || (parent->base.errcode == JOIN_FUTURE_ERR_BOTH_FUTS_FAILED)) // wake up parent if we are the second one finished
                waker_wake(&waker);

            break;
        case FUTURE_PENDING:
            break;
    }
    return state;
}

JoinWrapperFuture* join_wrapper_future(Future* subtask, bool is_first_child) {
    JoinWrapperFuture* res = (JoinWrapperFuture*) malloc(sizeof(JoinWrapperFuture));
    ASSERT_MALLOC_SUCCEEDED(res);

    *res = (JoinWrapperFuture) {
        .base = future_create(future_join_wrapper_progress),
        .subtask = subtask,
        .is_first_child = is_first_child
    };
    return res;
}

// ========================= JoinFuture =========================

FutureState future_join_progress(Future* base, Mio* mio, Waker waker) {
    debug("ENTERING JOIN PROGRESS\n");
    JoinFuture* fut = (JoinFuture*) base;

    if ((fut->base.errcode == JOIN_FUTURE_ERR_FUT1_FAILED) 
        || (fut->base.errcode == JOIN_FUTURE_ERR_FUT2_FAILED) 
        || (fut->base.errcode == JOIN_FUTURE_ERR_BOTH_FUTS_FAILED)
    ) {
        return FUTURE_FAILURE;
    } else if (fut->fut1_completed != FUTURE_PENDING && fut->fut2_completed != FUTURE_PENDING) {
        fut->base.errcode = FUTURE_SUCCESS;
        return FUTURE_SUCCESS;
    }

    Future* fut1 = fut->fut1;
    Future* fut2 = fut->fut2;

    fut1->progress(fut1, mio, waker);
    fut2->progress(fut2, mio, waker);

    return FUTURE_PENDING;
}

JoinFuture future_join(Future* fut1, Future* fut2) {
    return (JoinFuture) {
        .base = future_create(future_join_progress),
        .fut1 = (Future*) join_wrapper_future(fut1, true),
        .fut2 = (Future*) join_wrapper_future(fut2, false),
        .fut1_completed = FUTURE_PENDING,
        .fut2_completed = FUTURE_PENDING,
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
