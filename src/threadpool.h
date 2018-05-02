#ifndef THREADPOOL_H
#define THREADPOOL_H

#include <atomic>
#include <condition_variable>
#include <functional>
#include <future>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

class ThreadPool {
public:
  using Ids = std::vector<std::thread::id>;
  ThreadPool(std::size_t threadCount = 4);
  ThreadPool(const ThreadPool &) = delete;
  ThreadPool &operator=(const ThreadPool &) = delete;
  ThreadPool(ThreadPool &&) = default;
  ThreadPool &operator=(ThreadPool &&) = default;
  ~ThreadPool();

  template <typename Func, typename... Args>
  auto add(Func &&func, Args &&... args)
      -> std::future<typename std::result_of<Func(Args...)>::type>;
  std::size_t threadCount() const;
  std::size_t waitingJobs() const;
  Ids ids() const;
  void clear();
  void pause(bool state);
  void wait();

private:
  using Job = std::function<void()>;
  static void threadTask(ThreadPool *pool);
  std::queue<Job> jobs;
  mutable std::mutex jobsMutex;
  std::condition_variable jobsAvailable;
  std::vector<std::thread> threads;
  std::atomic<std::size_t> threadsWaiting;
  std::atomic<bool> terminate;
  std::atomic<bool> paused;
};

template <typename Func, typename... Args>
auto ThreadPool::add(Func &&func, Args &&... args)
    -> std::future<typename std::result_of<Func(Args...)>::type> {
  using PackedTask =
      std::packaged_task<typename std::result_of<Func(Args...)>::type()>;
  auto task = std::make_shared<PackedTask>(
      std::bind(std::forward<Func>(func), std::forward<Args>(args)...));
  auto ret = task->get_future();
  {
    std::lock_guard<std::mutex> lock{jobsMutex};
    jobs.emplace([task]() { (*task)(); });
  }
  jobsAvailable.notify_one();
  return ret;
}

#endif
