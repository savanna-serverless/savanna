#include "master.h"
#include "log.h"
#include "masterworker.h"

#include <arpa/inet.h>
#include <cerrno>
#include <csignal>
#include <ctime>
#include <fcntl.h>
#include <iostream>
#include <netinet/tcp.h>
#include <pthread.h>
#include <sys/socket.h>

void signal_callback_handler(int signum) {
  LOG_ERROR << "Caught signal SIGPIPE " << signum;
}

Master::Master(unsigned short port) : port(port), workers(), socket_fd(-1) {
#if USE_EPOLL == 1
  num_core = sysconf(_SC_NPROCESSORS_ONLN);
  LOG_MSG << "Detected " << num_core << " cores";
  for (int i = 0; i < num_core * 2; i++) {
    epoll_master_workers.push_back(new EpollMasterWorker());
  }
#endif
}

int Master::make_socket_non_blocking(int sfd) {
  int flags, s;
  flags = fcntl(sfd, F_GETFL, 0);
  if (flags == -1) {
    perror("fcntl");
    return -1;
  }

  flags |= O_NONBLOCK;
  s = fcntl(sfd, F_SETFL, flags);
  if (s == -1) {
    perror("fcntl");
    return -1;
  }
  return 0;
}

Master::~Master() {
  for (auto i = workers.begin(); i != workers.end(); ++i)
    delete *i;
}

bool Master::init() {
  if ((socket_fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0)
    DIE("error: cannot create socket");

  int yes = 1;
  if (setsockopt(socket_fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int)))
    DIE("error: unable to set socket option");
  if (setsockopt(socket_fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(int)))
    DIE("error: unable to set socket option");

  sockaddr_in address;
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = INADDR_ANY;
  address.sin_port = htons(port);
  if (bind(socket_fd, (sockaddr *)(&address), sizeof(sockaddr_in)) < 0)
    DIE("error: cannot bind socket to port ");

  if (listen(socket_fd, port) < 0)
    DIE("error: cannot listen on port ");

  LOG_MSG << "listening on port " << port;

  return true;
}

void Master::cleanup() {
  if (socket_fd >= 0) {
    close(socket_fd);
    socket_fd = -1;
    sleep(2);
  }
}

void Master::run() {

  if (!init()) {
    cleanup();
    return;
  }

  sigset_t signal_mask;
  sigemptyset(&signal_mask);
  sigaddset(&signal_mask, SIGPIPE);
  if (pthread_sigmask(SIG_BLOCK, &signal_mask, NULL))
    LOG_ERROR << "error setting sigmask";

  for (;;) {
    int worker_socket = accept(socket_fd, nullptr, nullptr);
    if (worker_socket < 0) {
      if (errno == EINTR) {
        LOG_ERROR << "stopping server, waiting for threads to terminate";
        break;
      } else {
        LOG_ERROR << "error: unable to accept client " << strerror(errno);
      }
    } else {
      LOG_DEBUG << "new client";
      MasterWorker *worker = new MasterWorker(*this, worker_socket);
#if USE_EPOLL == 1
      make_socket_non_blocking(worker_socket);
      int r1 = rand() % epoll_master_workers.size();
      int r2 = rand() % epoll_master_workers.size();
      EpollMasterWorker *selected =
          epoll_master_workers[r1]->get_count() <
                  epoll_master_workers[r2]->get_count()
              ? epoll_master_workers[r1]
              : epoll_master_workers[r2];
      selected->add(worker_socket, worker);
#else
      workers.push_back(worker);
      pthread_t thread;
      if (pthread_create(&thread, 0, &MasterWorker::pthread_helper, worker)) {
        LOG_ERROR << "error: unable to create thread";
        continue;
      }
      if (pthread_detach(thread)) {
        LOG_ERROR << "error: unable to detach thread";
        continue;
      }
#endif
    }
  }
  cleanup();
}

int main() {
  signal(SIGPIPE, SIG_IGN);
  Master m(1988);
  m.run();
}
