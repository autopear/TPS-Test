#include "file_scan.hpp"

#include <fcntl.h>

#include <algorithm>
#include <cmath>
#include <random>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "timer.hpp"

namespace tps {

FileScan::FileScan(const std::string dir_path, size_t record_size,
                   long long max_time, bool buffered, int num_threads,
                   const Bounds &ex_bounds, const Bounds &in_bounds,
                   bool sequential_files, bool sequential_scan,
                   bool full_middle)
    : FileRead(dir_path, record_size, max_time, buffered, num_threads),
      seq_file_(sequential_files),
      seq_scan_(sequential_scan),
      full_middle_(full_middle),
      full_scan_(false),
      total_files_(0) {
  if (files_.size() == 1) {
    min_files_ = 1;
    max_files_ = 1;
    seq_file_ = true;
    seq_scan_ = true;
  } else {
    if (ex_bounds.is_ratio) {
      if (ex_bounds.min_ratio < 0.0 || ex_bounds.max_ratio < 0.0) {
        throw IOException(
            "Invalid min_file_ratio " + std::to_string(ex_bounds.min_ratio) +
            " or max_file_ratio " + std::to_string(ex_bounds.max_ratio));
      } else if (ex_bounds.min_ratio == 0.0 && ex_bounds.max_ratio == 0.0) {
        min_files_ = files_.size();
        max_files_ = min_files_;
      } else {
        min_files_ = std::min(files_.size(),
                              get_ceil(ex_bounds.min_ratio, 0, files_.size()));
        max_files_ = std::min(files_.size(),
                              get_ceil(ex_bounds.max_ratio, 0, files_.size()));
      }
    } else {
      min_files_ = ex_bounds.min_size;
      max_files_ = ex_bounds.max_size;
    }
    if (min_files_ == 0 && max_files_ > 0) {
      min_files_ = 1;
    } else if (min_files_ == 0 && max_files_ == 0 ||
               min_files_ >= files_.size()) {
      min_files_ = files_.size();
      max_files_ = min_files_;
    } else if (max_files_ > files_.size()) {
      max_files_ = files_.size();
    }
  }
  size_bounds_ = in_bounds;
  if (size_bounds_.is_ratio) {
    if (size_bounds_.min_ratio < 0.0)
      throw IOException("Invalid min_ratio " +
                        std::to_string(size_bounds_.min_ratio));
    if (size_bounds_.max_ratio > 1.0) size_bounds_.max_ratio = 1.0;
    if (size_bounds_.min_ratio == 0.0 && size_bounds_.max_ratio == 0.0) {
      size_bounds_.min_ratio = 1.0;
      size_bounds_.max_ratio = 1.0;
    }
    if (size_bounds_.min_ratio == 1.0 && size_bounds_.max_ratio == 1.0) {
      full_scan_ = true;
    }
  } else {
    if (size_bounds_.min_size == 0 && size_bounds_.max_size == 0) {
      full_scan_ = true;
    }
  }
}

void FileScan::start_read() {
  if (num_threads_ < 2) {
    do_read();
    return;
  }

  std::vector<std::thread> threads;
  threads.reserve(num_threads_);

  for (int i = 0; i < num_threads_; i++)
    threads.emplace_back(&FileScan::do_read, this);

  for (int i = 0; i < num_threads_; i++) threads[i].join();
}

void FileScan::rand_read_info(size_t *pos, size_t *read_size, size_t file_size,
                              double pos_ratio, double size_ratio,
                              size_t align_size) {
  if (size_ratio == 1.0) {
    *pos = 0;
    *read_size = file_size;
    return;
  }

  size_t s =
      align_floor(get_ceil(size_ratio, align_size, file_size), align_size);
  size_t p =
      align_floor(get_round(pos_ratio, 0, file_size - s - 1), align_size);
  *pos = p;
  *read_size = s;
}

void FileScan::do_read() {
  size_t local_ops = 0;
  size_t local_bytes = 0;
  size_t local_files = 0;

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<size_t> file_dist(min_files_, max_files_);
  std::uniform_real_distribution<double> ratio_dist(size_bounds_.min_size,
                                                    size_bounds_.max_ratio);
  std::uniform_int_distribution<size_t> size_dist(size_bounds_.min_size,
                                                  size_bounds_.max_size);
  std::uniform_real_distribution<double> pos_dist(0.0, 1.0);

  size_t blk_size = get_block_size();
  size_t buf_size =
      buffered_ ? record_size_ : align_buf(record_size_, blk_size);
  char *buf = new char[buf_size];
  size_t align_size = buffered_ ? record_size_ : blk_size;

  int flags = O_RDONLY;
  if (!buffered_) flags |= O_DIRECT;

  bool running = true;

  HighResTimer timer;
  timer.start();
  if (files_.size() == 1) {
    std::string picked_file = dir_ + "/" + files_[0];
    size_t fsize = file_sizes_[0];

    while (running) {
      int fd;
      if ((fd = open(picked_file.c_str(), flags)) == -1) {
        throw IOException("Failed to open " + picked_file + ", error " +
                          std::to_string(errno));
      }
      local_files++;
      timer.stop();
      if (timer.elapsed_ns() >= max_time_) {
        running = false;
        break;
      }

      size_t pos;
      size_t len;
      if (full_scan_) {
        rand_read_info(&pos, &len, fsize, 0.0, 1.0, align_size);
      } else if (size_bounds_.is_ratio) {
        size_t size_ratio = size_bounds_.min_ratio == size_bounds_.max_ratio
                                ? size_bounds_.min_ratio
                                : ratio_dist(gen);
        size_t pos_ratio = pos_dist(gen);
        rand_read_info(&pos, &len, fsize, pos_ratio, size_ratio, align_size);
      } else {
        len = size_bounds_.min_size == size_bounds_.max_size
                  ? size_bounds_.min_size
                  : size_dist(gen);
        if (len >= fsize) {
          len = fsize;
          pos = 0;
        } else {
          pos = align_floor(get_round(pos_dist(gen), 0, fsize - len - 1),
                            align_size);
        }
      }

      if (running) {
        if (pos > 0 && lseek(fd, pos, SEEK_SET) == -1) {
          throw IOException("Failed to seek " + picked_file + ", error " +
                            std::to_string(errno));
        }
        timer.stop();
        if (timer.elapsed_ns() >= max_time_) running = false;
      }

      if (running) {
        size_t records_read =
            static_cast<size_t>(floor(1.0 * len / record_size_));
        while (running && len > 0) {
          size_t bytes_read = read(fd, buf, buf_size);
          if (bytes_read == IO_ERROR) {
            throw IOException("Failed to read " + picked_file + ", error " +
                              std::to_string(errno));
          }
          len -= bytes_read;
          local_bytes += bytes_read;

          timer.stop();
          if (timer.elapsed_ns() >= max_time_) running = false;
        }
      }
      close(fd);

      local_ops++;
      if (running) {
        timer.stop();
        if (timer.elapsed_ns() >= max_time_) break;
      }
    }
  } else {
    while (running) {
      size_t rand_num_files =
          min_files_ == max_files_ ? min_files_ : file_dist(gen);

      std::vector<size_t> rand_indexes;
      std::unordered_set<size_t> uniques;
      rand_indexes.reserve(rand_num_files);

      while (uniques.size() < rand_num_files) {
        size_t ridx = get_round(pos_dist(gen), 0, files_.size() - 1);
        if (uniques.insert(ridx).second) rand_indexes.push_back(ridx);
      }

      if (seq_file_) std::sort(rand_indexes.begin(), rand_indexes.end());
      std::unordered_map<size_t, size_t> positions;
      std::unordered_map<size_t, size_t> lengths;

      size_t pos;
      size_t len;

      if (full_middle_) {
        size_t fidx = rand_indexes[0];
        if (size_bounds_.is_ratio) {
          size_t size_ratio = size_bounds_.min_ratio == size_bounds_.max_ratio
                                  ? size_bounds_.min_ratio
                                  : ratio_dist(gen);
          rand_read_info(&pos, &len, file_sizes_[fidx], 0.0, size_ratio,
                         align_size);
        } else {
          len = size_bounds_.min_size == size_bounds_.max_size
                    ? size_bounds_.min_size
                    : size_dist(gen);
          size_t fsize = file_sizes_[fidx];
          if (len >= fsize) {
            len = fsize;
            pos = 0;
          } else {
            pos = align_floor(get_round(pos_dist(gen), 0, fsize - len - 1),
                              align_size);
          }
        }
        positions.insert({fidx, file_sizes_[fidx] - len});
        lengths.insert({fidx, len});

        if (rand_num_files > 1) {
          for (size_t i = 1; i < rand_num_files - 1; i++) {
            fidx = rand_indexes[i];
            positions.insert({fidx, 0});
            lengths.insert({fidx, file_sizes_[fidx]});
          }

          fidx = rand_indexes[rand_num_files - 1];
          if (size_bounds_.is_ratio) {
            size_t size_ratio = size_bounds_.min_ratio == size_bounds_.max_ratio
                                    ? size_bounds_.min_ratio
                                    : ratio_dist(gen);
            rand_read_info(&pos, &len, file_sizes_[fidx], 0.0, size_ratio,
                           align_size);
          } else {
            len = std::min(file_sizes_[fidx],
                           size_bounds_.min_size == size_bounds_.max_size
                               ? size_bounds_.min_size
                               : size_dist(gen));
          }
          positions.insert({fidx, 0});
          lengths.insert({fidx, len});
        }
      } else {
        for (size_t i = 0; i < rand_num_files; i++) {
          size_t fidx = rand_indexes[i];

          if (size_bounds_.is_ratio) {
            size_t size_ratio = size_bounds_.min_ratio == size_bounds_.max_ratio
                                    ? size_bounds_.min_ratio
                                    : ratio_dist(gen);
            rand_read_info(&pos, &len, file_sizes_[fidx], pos_dist(gen),
                           size_ratio, align_size);
          } else {
            len = size_bounds_.min_size == size_bounds_.max_size
                      ? size_bounds_.min_size
                      : size_dist(gen);
            size_t fsize = file_sizes_[fidx];
            if (len >= fsize) {
              len = fsize;
              pos = 0;
            } else {
              pos = align_floor(get_round(pos_dist(gen), 0, fsize - len - 1),
                                align_size);
            }
          }

          positions.insert({fidx, pos});
          lengths.insert({fidx, len});
        }
      }

      if (seq_scan_) {
        for (size_t fidx : rand_indexes) {
          std::string picked_file = dir_ + "/" + files_[fidx];
          pos = positions[fidx];
          len = lengths[fidx];

          int fd;
          if ((fd = open(picked_file.c_str(), flags)) == -1) {
            throw IOException("Failed to open " + picked_file + ", error " +
                              std::to_string(errno));
          }
          local_files++;
          timer.stop();
          if (timer.elapsed_ns() >= max_time_) running = false;

          if (running) {
            if (pos > 0 && lseek(fd, pos, SEEK_SET) == -1) {
              throw IOException("Failed to seek " + picked_file + ", error " +
                                std::to_string(errno));
            }
            timer.stop();
            if (timer.elapsed_ns() >= max_time_) running = false;
          }

          while (running && len > 0) {
            size_t bytes_read = read(fd, buf, buf_size);
            if (bytes_read == 0) break;
            if (bytes_read == IO_ERROR) {
              throw IOException("Failed to read " + picked_file + ", error " +
                                std::to_string(errno));
            }
            len -= bytes_read;
            local_bytes += bytes_read;
          }
          close(fd);

          if (!running) break;

          timer.stop();
          if (timer.elapsed_ns() >= max_time_) {
            running = false;
            break;
          }
        }
      } else {
        std::unordered_map<size_t, int> fds;
        for (size_t fidx : rand_indexes) {
          std::string picked_file = dir_ + "/" + files_[fidx];
          int fd;
          if ((fd = open(picked_file.c_str(), flags)) == -1) {
            throw IOException("Failed to open " + picked_file + ", error " +
                              std::to_string(errno));
          }
          local_files++;
          timer.stop();
          if (timer.elapsed_ns() >= max_time_) running = false;

          if (running) {
            pos = positions[fidx];
            if (pos > 0 && lseek(fd, pos, SEEK_SET) == -1) {
              std::cout << "fidx = " << fidx << picked_file << std::endl;
              throw IOException("Failed to seek " + picked_file + ", error " +
                                std::to_string(errno));
            }
            fds.insert({fidx, fd});
          }

          if (!running) break;

          timer.stop();
          if (timer.elapsed_ns() >= max_time_) {
            running = false;
            break;
          }
        }

        while (running && !lengths.empty()) {
          size_t r = get_round(pos_dist(gen), 0, lengths.size() - 1);
          auto rand_it = std::next(std::begin(lengths), r);
          size_t rand_fidx = rand_it->first;
          size_t rand_len = rand_it->second;
          size_t num_reads =
              static_cast<size_t>(floor(1.0 * rand_len / buf_size));
          size_t rand_reads = get_round(pos_dist(gen), 1, num_reads);

          int fd = fds[rand_fidx];

          for (size_t i = 0; running && i < rand_reads; i++) {
            size_t bytes_read = read(fd, buf, buf_size);
            if (bytes_read == 0) break;
            if (bytes_read == IO_ERROR) {
              std::string picked_file = dir_ + "/" + files_[rand_fidx];
              throw IOException("Failed to read " + picked_file + ", error " +
                                std::to_string(errno));
            }
            local_bytes += bytes_read;

            timer.stop();
            if (timer.elapsed_ns() >= max_time_) running = false;
          }

          if (num_reads == rand_reads) lengths.erase(rand_fidx);
        }

        for (auto it : fds) close(it.second);
      }

      local_ops++;
      if (running) {
        timer.stop();
        if (timer.elapsed_ns() >= max_time_) break;
      }
    }
  }

  delete[] buf;

  update_stats(timer.elapsed_ns(), local_ops, local_bytes,
               static_cast<size_t>(floor(1.0 * local_bytes / record_size_)),
               local_files);
}

void FileScan::update_stats(long long time, size_t ops, size_t bytes,
                            size_t records, size_t files) {
  const std::lock_guard<std::mutex> lock(mtx_);
  if (time > total_time_) total_time_ = time;
  total_ops_ += ops;
  total_bytes_ += bytes;
  total_records_ += records;
  total_files_ += files;
}

void FileScan::print_arguments() {
  print_argument("page-size", get_page_size());
  print_argument("block-size", get_block_size());
  print_argument("dir", dir_);
  print_argument("record-size", record_size_);
  print_argument("max-time", std::to_string(max_time_));
  print_argument("buffered", buffered_);
  print_argument("threads", std::to_string(num_threads_));
  print_argument("file-ratio", min_files_, max_files_);
  if (size_bounds_.is_ratio)
    print_argument("size-ratio", size_bounds_.min_ratio,
                   size_bounds_.max_ratio);
  else
    print_argument("size-ratio", size_bounds_.min_size, size_bounds_.max_size);
  print_argument("seq-file", seq_file_);
  print_argument("seq-scan", seq_scan_);
  print_argument("full-middle", full_middle_);
}

}  // namespace tps