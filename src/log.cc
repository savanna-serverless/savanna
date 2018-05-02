#include "log.h"

int NullBuffer::overflow(int c) { return c; }

NullBuffer null_buffer;
std::ostream null_stream(&null_buffer);
