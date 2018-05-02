#include "epollobjserver.h"
#include "log.h"
#include <boost/algorithm/string.hpp>
#include <csignal>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>

#define NUM_THREADS 32

EpollObjServer::EpollObjServer(int port) : port(port) { start_obj_server(); }

void EpollObjServer::start_obj_server() {
  for (int i = 0; i < NUM_THREADS; i++) {
    epoll_workers.push_back(new EpollWorker());
  }

  if ((obj_server_sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0)
    DIE("Socket failure");

  sigset_t signal_mask;
  sigemptyset(&signal_mask);
  sigaddset(&signal_mask, SIGPIPE);
  if (pthread_sigmask(SIG_BLOCK, &signal_mask, NULL))
    LOG_ERROR << "error setting sigmask";

  int yes = 1;
  if (setsockopt(obj_server_sock, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int)))
    DIE("setsockopt failure");
  if (setsockopt(obj_server_sock, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(int)))
    LOG_ERROR << "error: unable to set socket option";

  sockaddr_in address;
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = INADDR_ANY;
  address.sin_port = htons(port);
  if (bind(obj_server_sock, (sockaddr *)(&address), sizeof(sockaddr_in)) < 0)
    DIE("bind failure");

  if (listen(obj_server_sock, port) < 0)
    DIE("failed to listen port");

  LOG_INFO << "Object server listening on port " << port;
  pthread_t thread;
  int rc = pthread_create(&thread, NULL, &EpollObjServer::run_helper, this);
  if (rc)
    DIE("Unable to create thread to run object server");
}

void *EpollObjServer::run_helper(void *s) {
  ((EpollObjServer *)s)->run();
  return NULL;
}

void EpollObjServer::stop_obj_server() {
  for (auto &w : epoll_workers)
    delete w;
  if (obj_server_sock >= 0) {
    close(obj_server_sock);
    obj_server_sock = -1;
  }
}

int EpollObjServer::make_socket_non_blocking(int sfd) {
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

void EpollObjServer::run() {
  LOG_INFO << "Running object server";
  for (;;) {
    int worker_socket = accept(obj_server_sock, nullptr, nullptr);
    if (worker_socket < 0) {
      if (errno == EINTR) {
        LOG_ERROR << "stopping server, waiting for threads to terminate";
        break;
      } else
        LOG_ERROR << "error: unable to accept client" << std::endl;
    } else {
      LOG_ERROR << "new client";
      make_socket_non_blocking(worker_socket);
      ObjWorker *worker = new ObjWorker(worker_socket);
      int r1 = rand() % NUM_THREADS;
      int r2 = rand() % NUM_THREADS;
      EpollWorker *selected =
          epoll_workers[r1]->get_count() < epoll_workers[r2]->get_count()
              ? epoll_workers[r1]
              : epoll_workers[r2];
      selected->add(worker_socket, worker);
    }
  }
}
