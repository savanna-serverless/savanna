#include "objworker.h"
#include "log.h"
#include "objserver.h"
#include <arpa/inet.h>
#include <boost/algorithm/string.hpp>
#include <ctime>
#include <exception>
#include <fcntl.h>
#include <fcntl.h>
#include <fstream>
#include <iomanip>
#include <iosfwd>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <string>
#include <sys/file.h>
#include <sys/sendfile.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <vector>

#define BUFSIZE 1024 * 1500
#define STORAGE "/dev/shm/cache/"
#define min(a, b) (a < b ? a : b)
using namespace std;

ObjWorker::ObjWorker(int socket) : socket(socket) {
  fcntl(socket, F_SETFL, fcntl(socket, F_GETFL, 0) | O_NONBLOCK);
  int yes = 1;
  if (setsockopt(socket, SOL_TCP /*IPPROTO_TCP*/, TCP_NODELAY, &yes,
                 sizeof(int)))
    LOG_ERROR << "error: unable to set socket option";
  int tcp_send_buf = BUFSIZE * 10;
  if (setsockopt(socket, SOL_SOCKET, SO_SNDBUF, &tcp_send_buf,
                 sizeof(tcp_send_buf)) < 0)
    LOG_ERROR << "Error setsockopt";
  remote_ip = get_remote_ip(socket);
}

void ObjWorker::exit() {
  close(socket);
  LOG_INFO << "client lost";
  pthread_exit(nullptr);
}

string ObjWorker::get_remote_ip(int socket) {
  socklen_t len;
  struct sockaddr_storage addr;
  char ipstr[INET6_ADDRSTRLEN];
  int port;

  len = sizeof addr;
  getpeername(socket, (struct sockaddr *)&addr, &len);

  if (addr.ss_family == AF_INET) {
    struct sockaddr_in *s = (struct sockaddr_in *)&addr;
    port = ntohs(s->sin_port);
    inet_ntop(AF_INET, &s->sin_addr, ipstr, sizeof ipstr);
  } else {
    struct sockaddr_in6 *s = (struct sockaddr_in6 *)&addr;
    port = ntohs(s->sin6_port);
    inet_ntop(AF_INET6, &s->sin6_addr, ipstr, sizeof ipstr);
  }
  return ipstr;
}

int ObjWorker::reply(int socket, const char *msg, int size, int retry,
                     bool silent) {
  if (!silent)
    LOG_DEBUG << "Replying(" << remote_ip << "): " << msg;
  int ret, i;
  int sent = 0;
  // int cork = 0;
  // setsockopt(socket, SOL_TCP, TCP_CORK, &cork, sizeof(cork));
  int remaining = size;
  bool keep_retry = retry < 0;
  for (i = 0; keep_retry || i < retry; i++) {
    ret = write(socket, msg + sent, remaining);
    if (ret > 0) {
      // LOG_DEBUG << "write " << ret << " bytes";
      sent += ret;
      remaining -= ret;
      if (remaining == 0)
        break;
    } else {
      if (errno == EAGAIN)
        continue;
      else if (errno == ECONNRESET || errno == EPIPE) {
        LOG_ERROR << "Can't write socket," << std::strerror(errno);
        throw exception();
      } else
        LOG_DEBUG << "Can't write socket, ret = " << ret
                  << " error = " << std::strerror(errno);
    }
  }
  if (retry > 0 && i >= retry)
    LOG_ERROR << "Retried sending for " << i << " times";
  // cork = 1;
  // setsockopt(socket, SOL_TCP, TCP_CORK, &cork, sizeof(cork));
  return sent;
}

void ObjWorker::handle_get(vector<string> parts) {
  string key_fn(STORAGE + parts[1]);
  char real_fn_char[PATH_MAX];
  char *real_fn_p = realpath(key_fn.c_str(), real_fn_char);
  string fn(real_fn_p);

  struct stat fileStat;
  if (stat(fn.c_str(), &fileStat) != 0) {
    LOG_ERROR << "Failed to open file " << fn << ", err " << strerror(errno);
    string response = "get_fail|" + parts[1] + ";";
    reply(socket, response.c_str(), response.size());
  } else {
    LOG_DEBUG << "Found file " << fn;
    string response("get_success|" + fn.substr(strlen(STORAGE)) + "|" +
                    to_string(fileStat.st_size) + ";");
    reply(socket, response.c_str(), response.size());
#if USESENDFILE
    int shm_file = open(fn.c_str(), O_RDONLY);
    if (shm_file < 0)
      LOG_ERROR << "failed to open file, errno " << strerror(errno);
    int sent;
    uint64_t total_sent = 0;
    while (total_sent < fileStat.st_size) {
      sent = sendfile(socket, shm_file, NULL, fileStat.st_size - total_sent);
      if (sent >= 0) {
        total_sent += sent;
        LOG_TRACE << "sent " << sent << " total_sent " << total_sent;
      } else if (errno == EINTR || errno == EAGAIN) {
        // LOG_ERROR << "send error, sent = " << sent << " errno " <<
        // strerror(errno);
        continue;
      } else {
        LOG_ERROR << "Sent = " << sent << " errno = " << strerror(errno);
        break;
      }
    }
    if (total_sent != fileStat.st_size) {
      LOG_ERROR << "sendfile fail, total_sent = " << total_sent << " fsize "
                << fileStat.st_size;
      DIE("send fail");
    }
    LOG_DEBUG << "Sent " << total_sent << " bytes to client";
    close(shm_file);
#else
    // char* shm = mmap(0, fileStat.st_size, PROT_READ, MAP_SHARED, shm_fd, 0);
    // int fd = open(fn.c_str(), O_RDONLY | O_DIRECT);
    // LOG_DEBUG << "file opened" << fd << strerror(errno);
    FILE *shm_file = fopen(fn.c_str(), "rb");
    int shm_fd = fileno(shm_file);
    // assert(shm_fd == fd);
    // int shm_file = open(fn.c_str(), O_RDONLY);
    LOG_DEBUG << "Locking file " << fn;
    // flock(shm_fd, LOCK_SH);
    struct flock fl;
    fl.l_type = F_RDLCK;    /* F_RDLCK, F_WRLCK, F_UNLCK    */
    fl.l_whence = SEEK_SET; /* SEEK_SET, SEEK_CUR, SEEK_END */
    fl.l_start = 0;         /* Offset from l_whence         */
    fl.l_len = 0;           /* length, 0 = to EOF           */
    fl.l_pid = getpid();    /* our PID                      */
    fcntl(shm_fd, F_SETLKW, &fl);

    char readBuf[BUFSIZE];
    uint64_t total_read = 0;
    size_t actual_read;

    /*
    fseek(shm_file, 0L, SEEK_END);
    int ftell_size = ftell(shm_file);
    rewind(shm_file);
    LOG_DEBUG << "statsize: " << fileStat.st_size << " ftell size: " <<
    ftell_size;

    struct stat fileStat2;
    if(stat(fn.c_str() ,&fileStat2) != 0) {
      LOG_DEBUG << "failed to stat";
    }
    LOG_DEBUG << "statsize after lock: " << fileStat2.st_size;
    */
    while (true) {
      // while(total_read < fileStat.st_size) {
      // actual_read = shm_file.read(readBuf, BUFSIZE).gcount();
      // actual_read = fread(shm_file, readBuf, BUFSIZE);
      actual_read = fread(readBuf, 1, BUFSIZE, shm_file);
      if (actual_read == 0) {
        if (ferror(shm_file) != 0)
          LOG_DEBUG << "we have an error";
        break;
      }
      total_read += actual_read;
      // LOG_DEBUG << fn << " sending " << actual_read << " bytes";
      int actual_sent = reply(socket, readBuf, actual_read, -1, true);
      if (actual_sent != actual_read)
        LOG_ERROR << "Error: Actual sent = " << actual_sent
                  << " Actual read = " << actual_read;
      LOG_TRACE << "Read " << actual_read << " Sent " << actual_sent
                << " Total Read " << total_read;
      if (total_read == fileStat.st_size)
        break; // needed with st_size is a multiple of BUFSIZE
    }
    if (total_read != fileStat.st_size)
      LOG_ERROR << "total_read != fileStat.st_size " << total_read << " "
                << fileStat.st_size;
    fl.l_type = F_UNLCK;
    fcntl(shm_fd, F_SETLK, &fl);
    fclose(shm_file);
    LOG_DEBUG << "Finished sending " << fn << " totalbytes: " << total_read;
#endif
  }
}

void ObjWorker::handle_put(vector<string> parts, char *remaining_start,
                           int remaining_size) {
  string fn(STORAGE + parts[1]);
  char *end;
  uint64_t fsize = strtoull(parts[2].c_str(), &end, 10);
  int real_remaining = min(remaining_size, fsize);

  ofstream shm_file(fn, ios::binary);
  if (!shm_file.is_open()) {
    LOG_ERROR << "Failed to open file " << fn << ", err " << strerror(errno);
    return;
  } else {
    LOG_DEBUG << "Successfully open file " << fn
              << " Remaining bytes: " << remaining_size;
    if (remaining_size > 0) {
      shm_file.write(remaining_start, real_remaining);
    }
    char bodybuf[BUFSIZE];
    uint64_t rsize = real_remaining;
    int n;
    while (rsize < fsize) {
      n = read(socket, bodybuf, min(BUFSIZE, fsize - rsize));
      if (n <= 0) {
        LOG_ERROR << "Error reading, n = " << n << " errno " << strerror(errno);
        if (errno == 11)
          continue;
        else
          break;
      }
      shm_file.write(bodybuf, n);
      rsize += n;
      LOG_TRACE << "Received " << rsize << " bytes";
    }
    if (rsize != fsize) {
      LOG_ERROR << "header size: " << fsize << " received size: " << rsize;
    }
    shm_file.close();
    LOG_DEBUG << "Done receiving key " << fn << " size " << rsize;
  }
  string response("put_success|" + parts[1] + ";");
  reply(socket, response.c_str(), response.size());
}

void ObjWorker::handle_msg() {
  char buffer[1024];
  int read_size;
  while ((read_size = read(socket, buffer, sizeof(buffer))) > 0) {
    for (int i = 0; i < read_size; ++i) {
      if (buffer[i] == ';') {
        buffer[i] = '\0';
        string msg(buffer);
        LOG_DEBUG << "Received msg (" << remote_ip << ")" << msg
                  << " read_size:" << read_size;
        vector<string> parts;
        boost::split(parts, msg, boost::is_any_of("|"));
        if (parts[0] == "get") {
          handle_get(parts);
        } else if (parts[0] == "put") {
          handle_put(parts, buffer + i + 1, read_size - (i + 1));
        }
        break;
      }
    }
  }
}

void ObjWorker::run() {
  try {
    while (true) {
      handle_msg();
    }
  } catch (exception &e) {
    LOG_ERROR << "Caught exception";
    return;
  }
}

void *ObjWorker::pthread_helper(void *worker) {
  ((ObjWorker *)worker)->run();
  return (void *)NULL;
}
