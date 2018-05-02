#include "cacheserver.h"
#include "log.h"
#include <csignal>

void signal_callback_handler(int signum) {
  LOG_ERROR << "Caught signal SIGPIPE " << signum;
}

int main(int argc, char **argv) {
  signal(SIGPIPE, signal_callback_handler);
  CacheServer c(argv[1]);
  c.run();
  return 0;
}
