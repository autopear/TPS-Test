#ifndef TIMER_HPP
#define TIMER_HPP

#ifdef _WIN32
#include <windows.h>
#else
#include <chrono>
#endif
#include <time.h>

namespace tps {

class Timer {
 public:
  // Start or restart the timer
  virtual void start() = 0;

  // Stop the timer
  virtual void stop() = 0;

  // Elapsed time in nanoseconds
  virtual long long elapsed_ns() const = 0;

  // Elapsed time in microseconds
  virtual long long elapsed_us() const = 0;

  // Elapsed time in milliseconds
  virtual long long elapsed_ms() const = 0;

  // Elapsed time in seconds
  virtual long long elapsed_s() const = 0;
};

// High resolution timer that provides precision in nanoseconds
class HighResTimer : public Timer {
 public:
  explicit HighResTimer() {
#ifdef _WIN32
    LARGE_INTEGER freq;
    QueryPerformanceCounter(&freq);
    freq_ = freq.QuadPart;
#endif
  }
  ~HighResTimer() {}

  void start() {
#ifdef _WIN32
    QueryPerformanceCounter(&start_);
#else
    start_ = std::chrono::steady_clock::now();
#endif
  }

  void stop() {
#ifdef _WIN32
    QueryPerformanceCounter(&end_);
#else
    end_ = std::chrono::steady_clock::now();
#endif
  }

  long long elapsed_ns() const {
#ifdef _WIN32
    return (end_.QuadPart - start_.QuadPart) * 1000000000 / freq_;
#else
    return std::chrono::duration_cast<std::chrono::nanoseconds>(end_ - start_)
        .count();
#endif
  }

  long long elapsed_us() const {
#ifdef _WIN32
    return (end_.QuadPart - start_.QuadPart) * 1000000 / freq_;
#else
    return std::chrono::duration_cast<std::chrono::microseconds>(end_ - start_)
        .count();
#endif
  }

  long long elapsed_ms() const {
#ifdef _WIN32
    return (end_.QuadPart - start_.QuadPart) * 1000 / freq_;
#else
    return std::chrono::duration_cast<std::chrono::milliseconds>(end_ - start_)
        .count();
#endif
  }

  long long elapsed_s() const {
#ifdef _WIN32
    return (end_.QuadPart - start_.QuadPart) / freq_;
#else
    return std::chrono::duration_cast<std::chrono::seconds>(end_ - start_)
        .count();
#endif
  }

 private:
#ifdef _WIN32
  long long freq_;
  LARGE_INTEGER start_;
  LARGE_INTEGER end_;
#else
  std::chrono::time_point<std::chrono::steady_clock> start_;
  std::chrono::time_point<std::chrono::steady_clock> end_;
#endif
};

// Low resolution timer that provides precision in microseconds
class LowResTimer : public Timer {
 public:
  explicit LowResTimer() {}
  ~LowResTimer() {}

  void start() { start_ = clock(); }

  void stop() { end_ = clock(); }

  long long elapsed_ns() const {
    return static_cast<long long>(1000000000.0 * (end_ - start_) /
                                  CLOCKS_PER_SEC);
  }

  long long elapsed_us() const {
    return static_cast<long long>(1000000.0 * (end_ - start_) / CLOCKS_PER_SEC);
  }

  long long elapsed_ms() const {
    return static_cast<long long>(1000.0 * (end_ - start_) / CLOCKS_PER_SEC);
  }

  long long elapsed_s() const {
    return static_cast<long long>(1.0 * (end_ - start_) / CLOCKS_PER_SEC);
  }

 private:
  clock_t start_;
  clock_t end_;
};

}  // namespace files

#endif  // TIMER_HPP