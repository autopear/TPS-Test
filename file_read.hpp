#ifndef FILE_READ_HPP
#define FILE_READ_HPP

#include <algorithm>
#include <cmath>
#include <string>
#include <vector>

#include "helper.hpp"
#include "io_exception.hpp"

namespace tps {

class FileRead {
 public:
  FileRead(const std::string dir_path, size_t record_size, long long max_time,
           bool buffered, int num_threads)
      : dir_(dir_path),
        record_size_(record_size),
        max_time_(max_time),
        buffered_(buffered),
        num_threads_(num_threads),
        total_ops_(0),
        total_records_(0),
        total_time_(0),
        total_bytes_(0) {
    bool is_dir;
    bool exists = file_exists(dir_, &is_dir);
    if (!exists) throw IOException("Directory not exists: " + dir_);
    if (!is_dir) throw IOException("Not a directory: " + dir_);

    for (std::string f : list_dir(dir_)) {
      if (has_suffix(f, ".bin", false)) {
        files_.push_back(f);
        size_t fsize = get_file_size(dir_ + "/" + f);
        if (fsize % record_size_ != 0)
          throw IOException("Invalid file: " + f +
                            ", file size: " + std::to_string(fsize) +
                            ", record size: " + std::to_string(record_size_));
        file_sizes_.push_back(fsize);
      }
    }
    if (files_.empty()) throw IOException("Empty directory: " + dir_);
    std::sort(files_.begin(), files_.end());
  }

  virtual void start_read() = 0;

  size_t total_ops() const { return total_ops_; }
  size_t total_records() const { return total_records_; }
  long long total_time() const { return total_time_; }
  size_t total_bytes() const { return total_bytes_; }

  static size_t align_buf(size_t record_size, size_t blk_size) {
    size_t r;
    for (r = blk_size; r < record_size; r += blk_size) {
    }
    return r;
  }

  static size_t get_round(double ratio, size_t min, size_t max) {
    return static_cast<size_t>(round(ratio * (max - min))) + min;
  }

  static size_t get_floor(double ratio, size_t min, size_t max) {
    return static_cast<size_t>(floor(ratio * (max - min))) + min;
  }

  static size_t get_ceil(double ratio, size_t min, size_t max) {
    return static_cast<size_t>(ceil(ratio * (max - min))) + min;
  }

  static size_t align_round(size_t a, size_t b) {
    return static_cast<size_t>(round(1.0 * a / b)) * b;
  }

  static size_t align_floor(size_t a, size_t b) {
    return static_cast<size_t>(floor(1.0 * a / b)) * b;
  }

  static size_t align_ceil(size_t a, size_t b) {
    return static_cast<size_t>(ceil(1.0 * a / b)) * b;
  }

  static constexpr size_t IO_ERROR = (size_t)-1;

  void print_argument(const std::string &key, const std::string &value) {
    std::cout << "# " << key << " = " << value << std::endl;
  }

  void print_argument(const std::string &key, size_t value) {
    std::cout << "# " << key << " = " << value << std::endl;
  }

  void print_argument(const std::string &key, bool value) {
    std::cout << "# " << key << " = " << (value ? "true" : "false")
              << std::endl;
  }

  void print_argument(const std::string &key, size_t v1, size_t v2) {
    std::cout << "# " << key << " = [" << v1 << ", " << v2 << "]" << std::endl;
  }

  void print_argument(const std::string &key, double v1, double v2) {
    std::cout << "# " << key << " = [" << v1 << ", " << v2 << "]" << std::endl;
  }

  virtual void print_arguments() = 0;

 protected:
  std::string dir_;
  size_t record_size_;
  long long max_time_;
  bool buffered_;
  int num_threads_;
  std::mutex mtx_;
  std::vector<std::string> files_;
  std::vector<size_t> file_sizes_;
  size_t total_ops_;
  size_t total_records_;
  long long total_time_;
  size_t total_bytes_;
};

}  // namespace tps

#endif  // FILE_READ_HPP