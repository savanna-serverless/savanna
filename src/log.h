#ifndef LOG_H
#define LOG_H

#include <iostream>
#include <ostream>
#include <sstream>
#include <stdio.h>
#include <string.h>

#define TRACE 6
#define DEBUG 5
#define INFO 4
#define WARNING 3
#define ERROR 2
#define FATAL 1
#define MSG 0

#define SEVERITY_THRESHOLD FATAL

extern std::ostream null_stream;

class NullBuffer : public std::streambuf {
public:
  int overflow(int c);
};

class LogStream : public std::stringstream {
public:
  ~LogStream() {
    operator<<(std::endl);
    std::cout << str();
  }
};

// ===== log macros =====
#define __FILENAME__                                                           \
  (strrchr(__FILE__, '/') ? strrchr(__FILE__, '/') + 1 : __FILE__)
#define PREFIX __FILENAME__ << ":" << __LINE__ << " (" << __FUNCTION__ << ") - "

#define LOG_X LogStream() << PREFIX

#if SEVERITY_THRESHOLD >= TRACE
#define LOG_TRACE LOG_X
#else
#define LOG_TRACE null_stream
#endif

#if SEVERITY_THRESHOLD >= DEBUG
#define LOG_DEBUG LOG_X
#else
#define LOG_DEBUG null_stream
#endif

#if SEVERITY_THRESHOLD >= INFO
#define LOG_INFO LOG_X
#else
#define LOG_INFO null_stream
#endif

#if SEVERITY_THRESHOLD >= WARNING
#define LOG_WARNING LOG_X
#else
#define LOG_WARNING null_stream
#endif

#if SEVERITY_THRESHOLD >= ERROR
#define LOG_ERROR LOG_X
#else
#define LOG_ERROR null_stream
#endif

#if SEVERITY_THRESHOLD >= FATAL
#define LOG_FATAL LOG_X
#else
#define LOG_FATAL null_stream
#endif

#if SEVERITY_THRESHOLD >= MSG
#define LOG_MSG LOG_X
#else
#define LOG_MSG null_stream
#endif

#define DIE(M, ...)                                                            \
  do {                                                                         \
    printf("DIE %s:%d (%s) -- ", __FILENAME__, __LINE__, __FUNCTION__);        \
    printf(M, ##__VA_ARGS__);                                                  \
    printf("\n");                                                              \
    exit(1);                                                                   \
  } while (0);

#endif
