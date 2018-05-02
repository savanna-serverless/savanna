#include "objserver.h"
#include "log.h"
#include <boost/algorithm/string.hpp>
#include <csignal>
#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>

ObjServer::ObjServer(int port) : port(port) { start_obj_server(); }

void ObjServer::start_obj_server() {
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
  int rc =
      pthread_create(&thread, NULL, &ObjServer::run_obj_worker_helper, this);
  if (rc)
    DIE("Unable to create thread to run object server");
}

void *ObjServer::run_obj_worker_helper(void *s) {
  ((ObjServer *)s)->run_obj_worker();
  return NULL;
}

void ObjServer::stop_obj_server() {
  for (auto &w : obj_workers)
    delete w;
  if (obj_server_sock >= 0) {
    close(obj_server_sock);
    obj_server_sock = -1;
  }
}

void ObjServer::run_obj_worker() {
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
      ObjWorker *worker = new ObjWorker(worker_socket);
      obj_workers.push_back(worker);

      pthread_t thread;
      if (pthread_create(&thread, 0, &ObjWorker::pthread_helper,
                         (void *)worker)) {
        LOG_ERROR << "error: unable to create thread";
        continue;
      }
      if (pthread_detach(thread)) {
        LOG_ERROR << "error: unable to detach thread";
        continue;
      }
    }
  }
}
