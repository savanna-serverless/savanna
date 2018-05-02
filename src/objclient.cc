#include "objclient.h"
#include "log.h"
#include <arpa/inet.h>
#include <boost/algorithm/string.hpp>
#include <fcntl.h>
#include <fstream>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/sendfile.h>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>

#define HDRBUF 512
#define BODYBUF 1024 * 128
#define USESENDFILE 0
#define min(a, b) (a < b ? a : b)

ObjClient::ObjClient() {
  if (pipe(pipefd) < 0)
    LOG_ERROR << "pipe failed";
}

bool ObjClient::fetch(string node, string key) {
  int conn = get_or_create_sock(node);
  string request = "get|" + key + ";";
  LOG_DEBUG << "Sending msg: " << request;
  if (write(conn, request.c_str(), request.size()) < 0) {
    LOG_ERROR << "error sending message";
    return false;
  }

  string line;
  char buf[HDRBUF];
  int n = 0;
  int c = 0;
  int hdr_len = -1;
  int read_end = -1;
  while (n = read(conn, buf + c, HDRBUF - c)) {
    char *pos = find(buf + c, buf + c + n, ';');
    if (pos != buf + c + n) {
      hdr_len = pos - buf;
      *pos = '\0';
      read_end = c + n - 1;
      break;
    } else {
      c += n;
    }
  }
  if (hdr_len < 0) {
    LOG_ERROR << "Malformat header";
    return false;
  }
  string hdr_str(buf);
  LOG_DEBUG << "Received msg header: " << hdr_str;
  vector<string> parts;
  split(parts, hdr_str, boost::is_any_of("|"));
  if (parts[0] != "get_success")
    return false;
  if (parts[1] != key) {
    LOG_ERROR << "Requesting " << key << " returning " << parts[1];
    return false;
  }
  char *end;
  uint64_t fsize = strtoull(parts[2].c_str(), &end, 10);
  LOG_DEBUG << "file size is " << fsize;
  string fn("/dev/shm/" + parts[1]);
  int remaining_bytes = read_end - hdr_len;
  LOG_DEBUG << "file " << fn << " remaining_bytes " << remaining_bytes;
#if USESENDFILE
  int shm_file = open(fn.c_str(), O_WRONLY | O_CREAT);
  if (shm_file < 0) {
    LOG_ERROR << "failed to open file " << shm_file << " , errno "
              << strerror(errno);
    return false;
  }
  LOG_DEBUG << "Opened file " << fn << " for writing";
  if (remaining_bytes > 0) {
    if (write(shm_file, buf + hdr_len + 1, remaining_bytes) != remaining_bytes)
      LOG_ERROR << "Write bytes != Remaining bytes";
  }
  uint64_t total_recv = remaining_bytes;
  int recv;
  /*while(total_recv < fsize) {
    recv = sendfile(shm_file, conn, NULL, fsize - total_recv);
    if (recv >= 0)
      total_recv += recv;
    else if (errno == EINTR || errno == EAGAIN)
      continue;
    else
      break;
  }*/
  loff_t offset = 0;
  while (total_recv < fsize) {
    if ((recv =
             splice(conn, NULL, pipefd[1], NULL, min(fsize - total_recv, 16384),
                    SPLICE_F_MORE | SPLICE_F_MOVE)) <= 0) {
      if (errno == EINTR || errno == EAGAIN) {
        continue;
      }
      LOG_ERROR << "Splice error";
    } else {
      LOG_TRACE << "recv " << recv << " total_recv " << total_recv;
    }

    int bytes_in_pipe = recv;
    int bytes;
    while (bytes_in_pipe > 0) {
      if ((bytes = splice(pipefd[0], NULL, shm_file, &offset, bytes_in_pipe,
                          SPLICE_F_MORE | SPLICE_F_MOVE)) <= 0) {
        if (errno == EINTR || errno == EAGAIN)
          continue;
        LOG_ERROR << "Splice error";
      }
      bytes_in_pipe -= bytes;
      LOG_TRACE << "Write bytes " << bytes << " Remaining " << bytes_in_pipe;
    }
    total_recv += recv;
  }
  if (total_recv != fsize)
    LOG_ERROR << "sendfile fail, total_recv = " << total_recv << " fsize "
              << fsize << " errno " << strerror(errno);
  close(shm_file);
  LOG_DEBUG << "Received " << total_recv << " bytes to from socket";
  if (total_recv < 0) {
    LOG_ERROR << "sendfile fail, ret = " << total_recv
              << " errno = " << strerror(errno);
    return false;
  }
  return true;
#else
  ofstream shm_file(fn, ios::binary);
  if (!shm_file.is_open()) {
    LOG_ERROR << "Failed to open file " << fn << ", err " << strerror(errno);
    return false;
  } else {
    LOG_DEBUG << "Successfully open file " << fn
              << " Remaining bytes: " << remaining_bytes;
    if (remaining_bytes > 0) {
      shm_file.write(buf + hdr_len + 1, remaining_bytes);
    }
    char bodybuf[BODYBUF];
    uint64_t rsize = remaining_bytes;
    while (rsize < fsize) {
      n = read(conn, bodybuf, min(BODYBUF, fsize - rsize));
      if (n <= 0) {
        LOG_ERROR << "Error reading, n = " << n << " errno " << strerror(errno);
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
    return true;
  }
#endif
}

int ObjClient::get_or_create_sock(string node) {
  if (obj_client_socks.find(node) == obj_client_socks.end()) {
    LOG_INFO << "Connect to obj server " << node;
    vector<string> ip_port;
    boost::split(ip_port, node, boost::is_any_of(":"));
    if (ip_port.size() != 2)
      LOG_ERROR << "Malformat server " << node;
    string ip = ip_port[0];
    int port = atoi(ip_port[1].c_str());
    struct sockaddr_in serv_addr;
    struct hostent *server;
    int obj_client_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (obj_client_sock < 0)
      LOG_ERROR << "Error opening socket";
    int yes = 1;
    if (setsockopt(obj_client_sock, IPPROTO_TCP, TCP_NODELAY, &yes,
                   sizeof(int)))
      LOG_ERROR << "error: unable to set socket option";
    server = gethostbyname(ip.c_str());
    if (server == NULL)
      LOG_ERROR << "Can't find host";
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    memcpy(&serv_addr.sin_addr.s_addr, server->h_addr, server->h_length);
    serv_addr.sin_port = htons(port);
    if (connect(obj_client_sock, (struct sockaddr *)&serv_addr,
                sizeof(serv_addr)) < 0)
      LOG_ERROR << "Failed to connect to socket";
    int recv_buf = BODYBUF * 10;
    if (setsockopt(obj_client_sock, SOL_SOCKET, SO_SNDBUF, &recv_buf,
                   sizeof(recv_buf)) < 0)
      LOG_ERROR << "Error setsockopt";
    obj_client_socks[node] = obj_client_sock;
  }
  return obj_client_socks[node];
}

ObjClient::~ObjClient() {
  for (auto &kvp : obj_client_socks)
    close(kvp.second);
}
