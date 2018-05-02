#include "epollworker.h"
#include "log.h"
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#define MAXEVENTS 64

EpollWorker::EpollWorker() : count(0) {
  epoll_fd = epoll_create1(0);
  if (epoll_fd == -1)
    DIE("Can't create epoll fd");
  events = (epoll_event *)calloc(MAXEVENTS, sizeof(epoll_event));
  pthread_t thread;
  int ret = pthread_create(&thread, NULL, &EpollWorker::pthread_helper, this);
  if (ret)
    DIE("Can't create thread");
}

EpollWorker::~EpollWorker() { free(events); }

void EpollWorker::add(int fd, ObjWorker *worker) {
  struct epoll_event event;
  event.data.fd = fd;
  event.events = EPOLLIN | EPOLLET | EPOLLRDHUP;
  obj_workers[fd] = worker;
  int ret = epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &event);
  if (ret == -1)
    LOG_ERROR << "failed to add fd to epoll, fd:" << fd;
  count++;
  LOG_DEBUG << "fd " << fd << " added to epollworker";
}

void EpollWorker::remove(int fd) {
  delete obj_workers[fd];
  obj_workers.erase(fd);
  count--;
  LOG_DEBUG << "fd " << fd << " is removed from epollworker";
}

void EpollWorker::run() {
  int n;
  while (true) {
    n = epoll_wait(epoll_fd, events, MAXEVENTS, -1);
    for (int i = 0; i < n; i++) {
      if (events[i].events & EPOLLRDHUP) {
        remove(events[i].data.fd);
        close(events[i].data.fd);
      } else if ((events[i].events & EPOLLERR) ||
                 (events[i].events & EPOLLHUP) ||
                 (!(events[i].events & EPOLLIN))) {
        LOG_ERROR << "epoll error";
        remove(events[i].data.fd);
        close(events[i].data.fd);
        continue;
      } else {
        try {
          obj_workers[events[i].data.fd]->handle_msg();
        } catch (exception &e) {
          LOG_ERROR << "Caught exception, removing";
          remove(events[i].data.fd);
          close(events[i].data.fd);
        }
      }
    }
  }
}

void *EpollWorker::pthread_helper(void *worker) {
  static_cast<EpollWorker *>(worker)->run();
  return nullptr;
}
