#include "file_lookup.hpp"

#include <fcntl.h>

#include <cmath>
#include <random>
#include <thread>

#include "timer.hpp"

namespace tps {

FileLookup::FileLookup(const std::string dir_path, size_t record_size,
                       long long max_time, bool buffered, int num_threads)
    : FileRead(dir_path, record_size, max_time, buffered, num_threads) {}

void FileLookup::start_read() {
  if (num_threads_ < 2) {
    do_read();
    return;
  }

  std::vector<std::thread> threads;
  threads.reserve(num_threads_);

  for (int i = 0; i < num_threads_; i++)
    threads.emplace_back(&FileLookup::do_read, this);

  for (int i = 0; i < num_threads_; i++) threads[i].join();
}

void FileLookup::do_read() {
  size_t local_ops = 0;
  size_t local_bytes = 0;

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<size_t> file_dist(0, files_.size() - 1);
  std::uniform_real_distribution<double> pos_dist(0.0, 1.0);

  size_t blk_size = get_block_size();
  bool single_file = files_.size() == 1;
  size_t buf_size = buffered_ ? record_size_ : align_buf(record_size_, blk_size);
  char *buf = new char[buf_size];

  int flags = O_RDONLY;
  if (!buffered_) flags |= O_DIRECT;

  HighResTimer timer;
  timer.start();
  while (true) {
    size_t ridx = single_file ? 0 : file_dist(gen);
    std::string picked_file = dir_ + "/" + files_[ridx];
    size_t picked_size = file_sizes_[ridx];
    size_t rpos = std::min(picked_size - std::min(record_size_, picked_size),
                           get_round(pos_dist(gen), 0, picked_size - 1));
    if (!buffered_) rpos = align_floor(rpos, blk_size);

    int fd;
    if ((fd = open(picked_file.c_str(), flags)) == -1)
      throw IOException("Filed to open " + files_[ridx] + ", error " +
                        std::to_string(errno));
    if (rpos > 0 && lseek(fd, rpos, SEEK_SET) == -1)
      throw IOException("Filed to seek " + files_[ridx] + ", error " +
                        std::to_string(errno));
    if (read(fd, buf, buf_size) == -1)
      throw IOException("Filed to read " + files_[ridx] + ", error " +
                        std::to_string(errno));
    close(fd);

    local_ops++;
    local_bytes += record_size_;

    timer.stop();
    if (timer.elapsed_ns() >= max_time_) break;
  }
  delete[] buf;

  update_stats(timer.elapsed_ns(), local_ops, local_bytes);
}

void FileLookup::update_stats(long long time, size_t ops, size_t bytes) {
  const std::lock_guard<std::mutex> lock(mtx_);
  if (time > total_time_) total_time_ = time;
  total_ops_ += ops;
  total_bytes_ += bytes;
}

}  // namespace tps