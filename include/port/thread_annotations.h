// include/port/thread_annotations.h
#ifndef PRISM_PORT_THREAD_ANNOTATIONS_H_
#define PRISM_PORT_THREAD_ANNOTATIONS_H_

#if defined(__clang__)
#define GUARDED_BY(x) __attribute__((guarded_by(x)))
#define EXCLUSIVE_LOCKS_REQUIRED(...) __attribute__((exclusive_locks_required(__VA_ARGS__)))
#define LOCKS_EXCLUDED(...) __attribute__((locks_excluded(__VA_ARGS__)))
#else
#define GUARDED_BY(x)
#define EXCLUSIVE_LOCKS_REQUIRED(...)
#define LOCKS_EXCLUDED(...)
#endif

#endif
