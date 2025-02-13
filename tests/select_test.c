// Required for `unistd.h` include to contain `pipe2`.
#define _GNU_SOURCE

#include <assert.h>
#include <fcntl.h>
#include <stdint.h> // For uint64_t
#include <stdio.h> // For printf
#include <stdlib.h> // For exit
#include <string.h> // For memcmp
#include <sys/timerfd.h> // For timerfd
#include <unistd.h> // For pipe, read, write

#include "err.h"
#include "executor.h"
#include "future.h"
#include "future_combinators.h"
#include "future_examples.h"
#include "mio.h"
#include "utils.h"


/** A function that capitalizes a c-string in place and returns it. */
void* capitalize(void* arg)
{
    char* buffer = arg;
    debug("capitalize received string at: %p\n", buffer);
    size_t const len = strlen(buffer);
    debug("capitalize received string of length %d\n", len);
    for (int i = 0; i < len; ++i) {
        if ('a' <= buffer[i] && buffer[i] <= 'z') {
            buffer[i] = buffer[i] - 'a' + 'A';
        }
    }
    return buffer;
}

void test_select_1() {
    Executor* executor = executor_create(42);

    // Create a pipe that will be filled with `message` 3 bytes at a time,
    // then wait 2 seconds before closing the write end.
    const char* message = "aaabbbcccd\n";
    int read_fd = create_example_read_pipe_end(message, 3, 0, 2);

    // Make a future that reads the message from the pipe, then capitalizes it, the prints it.
    // This should succeed, because we read exactly all the bytes of message, including '\0'.
    uint8_t buffer[strlen(message) + 1];
    PipeReadFuture f1 = pipe_read_future_create(read_fd, buffer, sizeof(buffer));
    Future* good_future = (Future*)&f1;

    // Make another future for another pipe, as before, but with a larger buffer.
    // This should progress concurrently with good_future, but eventually fail
    // (only after the write-end of the pipe is closed), because there aren't enough bytes to read.
    read_fd = create_example_read_pipe_end(message, 3, 0, 2);
    uint8_t large_buffer[1000];
    PipeReadFuture f1_bad = pipe_read_future_create(read_fd, large_buffer, sizeof(large_buffer));
    Future* bad_future = (Future*)&f1_bad;

    SelectFuture select = future_select(good_future, bad_future);

    executor_spawn(executor, (Future*)&select);

    // Run the executor
    executor_run(executor);

    // Check everything is as expected.
    assert(good_future->errcode == FUTURE_SUCCESS);
    assert(good_future->ok == buffer);
    assert(memcmp(buffer, message, sizeof(buffer)) == 0);

    assert(select.which_completed == SELECT_COMPLETED_FUT1);

    // Destroy the executor
    executor_destroy(executor);

    debug("test_select_1 passed\n");
}

void test_select_2() {
    Executor* executor = executor_create(42);

    // Create a pipe that will be filled with `message` 3 bytes at a time,
    // then wait 2 seconds before closing the write end.
    const char* message = "aaabbbcccd\n";
    int read_fd = create_example_read_pipe_end(message, 3, 0, 2);

    // Make a future that reads the message from the pipe, then capitalizes it, the prints it.
    // This should succeed, because we read exactly all the bytes of message, including '\0'.
    uint8_t buffer[strlen(message) + 1];
    PipeReadFuture f1 = pipe_read_future_create(read_fd, buffer, sizeof(buffer));
    Future* good_future = (Future*)&f1;

    // Make another future for another pipe, as before, but with a larger buffer.
    // This should progress concurrently with good_future, but eventually fail
    // (only after the write-end of the pipe is closed), because there aren't enough bytes to read.
    read_fd = create_example_read_pipe_end(message, 3, 0, 2);
    uint8_t large_buffer[1000];
    PipeReadFuture f1_bad = pipe_read_future_create(read_fd, large_buffer, sizeof(large_buffer));
    Future* bad_future = (Future*)&f1_bad;

    SelectFuture select = future_select(bad_future, good_future);

    executor_spawn(executor, (Future*)&select);

    // Run the executor
    executor_run(executor);

    // Check everything is as expected.
    assert(good_future->errcode == FUTURE_SUCCESS);
    assert(good_future->ok == buffer);
    assert(memcmp(buffer, message, sizeof(buffer)) == 0);

    assert(select.which_completed == SELECT_COMPLETED_FUT2);

    // Destroy the executor
    executor_destroy(executor);

    debug("test_select_2 passed\n");
}

void test_select_both_fail() {
    Executor* executor = executor_create(42);

    // Create a pipe that will be filled with `message` 3 bytes at a time,
    // then wait 2 seconds before closing the write end.
    const char* message = "aaabbbcccd\n";
    int read_fd = create_example_read_pipe_end(message, 3, 0, 2);

    // Make a future that reads the message from the pipe, then capitalizes it, the prints it.
    // This should succeed, because we read exactly all the bytes of message, including '\0'.
    uint8_t buffer[100];
    PipeReadFuture f1 = pipe_read_future_create(read_fd, buffer, sizeof(buffer));
    Future* bad_future_1 = (Future*)&f1;

    // Make another future for another pipe, as before, but with a larger buffer.
    // This should progress concurrently with good_future, but eventually fail
    // (only after the write-end of the pipe is closed), because there aren't enough bytes to read.
    read_fd = create_example_read_pipe_end(message, 3, 0, 2);
    uint8_t large_buffer[1000];
    PipeReadFuture f2 = pipe_read_future_create(read_fd, large_buffer, sizeof(large_buffer));
    Future* bad_future_2 = (Future*)&f2;

    SelectFuture select = future_select(bad_future_1, bad_future_2);

    executor_spawn(executor, (Future*)&select);

    // Run the executor
    executor_run(executor);

    // Check everything is as expected.
    assert(bad_future_1->errcode == PIPE_FUTURE_ERR_EOF);
    assert(memcmp(buffer, message, strlen(message) + 1) == 0);

    assert(bad_future_2->errcode == PIPE_FUTURE_ERR_EOF);
    assert(memcmp(large_buffer, message, strlen(message) + 1) == 0);

    assert(select.which_completed == SELECT_FAILED_BOTH);

    // Destroy the executor
    executor_destroy(executor);

    debug("test_select_both_fail passed\n");
}

int main() {
    test_select_1();
    test_select_2();
    test_select_both_fail();
    debug("ALL TESTS PASSED\n");
    return 0;
}