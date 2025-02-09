#ifndef UTILS_H
#define UTILS_H

#include <stdio.h>

#define ASSERT_MALLOC_SUCCEEDED(expr)                                          \
    do {                                                                       \
        if ((expr) == NULL) {                                                  \
            fprintf(stderr,                                                    \
            "Memory allocation failed in %s:%d: In function %s():\n\t",        \
            __FILE__, __LINE__, __func__);                                     \
            exit(EXIT_FAILURE);                                                \
        }                                                                      \
    } while (0)

#endif