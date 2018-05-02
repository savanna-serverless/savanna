#include "threadpool.h"
#include "log.h"
#include <algorithm>
#include <iterator>

ThreadPool::ThreadPool(std::size_t threadCount)
    : threadsWaiting(0), terminate(false), paused(false) {
  LOG_INFO << "Creating threads pool with " << threadCount << " threads";
  threads.reserve(threadCount);
  std::generate_n(std::back_inserter(threads), threadCount, [this]() {
    return std::thread{threadTask, this};
  });
}

ThreadPool::~ThreadPool() {
  clear();
  terminate = true;
  jobsAvailable.notify_all();
  for (auto &t : threads) {
    if (t.joinable())
      t.join();
  }
}

std::size_t ThreadPool::threadCount() const { return threads.size(); }

std::size_t ThreadPool::waitingJobs() const {
  std::lock_guard<std::mutex> jobLock(jobsMutex);
  return jobs.size();
}

ThreadPool::Ids ThreadPool::ids() const {
  Ids ret(threads.size());
  std::transform(threads.begin(), threads.end(), ret.begin(),
                 [](const std::thread &t) { return t.get_id(); });
  return ret;
}

void ThreadPool::clear() {
  std::lock_guard<std::mutex> lock{jobsMutex};
  while (!jobs.empty())
    jobs.pop();
}

void ThreadPool::pause(bool state) {
  paused = state;
  if (!paused)
    jobsAvailable.notify_all();
}

void ThreadPool::wait() {
  while (threadsWaiting != threads.size())
    ;
}

void ThreadPool::threadTask(ThreadPool *pool) {
  while (true) {
    if (pool->terminate)
      break;
    std::unique_lock<std::mutex> jobsLock{pool->jobsMutex};

    if (pool->jobs.empty() || pool->paused) {
      ++pool->threadsWaiting;
      pool->jobsAvailable.wait(jobsLock, [&]() {
        return pool->terminate || !(pool->jobs.empty() || pool->paused);
      });
      --pool->threadsWaiting;
    }

    if (pool->terminate)
      break;

    auto job = std::move(pool->jobs.front());
    pool->jobs.pop();

    jobsLock.unlock();

    job();
  }
}
