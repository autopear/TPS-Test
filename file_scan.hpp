#ifndef FILE_SCAN_HPP
#define FILE_SCAN_HPP

#include <limits>
#include <mutex>
#include <string>
#include <utility>

#include "file_read.hpp"
#include "helper.hpp"
#include "io_exception.hpp"

namespace tps {

typedef struct Bounds {
  double min_ratio;
  double max_ratio;
  size_t min_size;
  size_t max_size;
  bool is_ratio;

  void set_ratios(double min, double max) {
    min_ratio = std::min(min, max);
    max_ratio = std::max(min, max);
    min_size = std::numeric_limits<size_t>::min();
    max_size = std::numeric_limits<size_t>::max();
    is_ratio = true;
  }

  void set_sizes(size_t min, size_t max) {
    min_size = std::min(min, max);
    max_size = std::max(min, max);
    min_ratio = -std::numeric_limits<double>::infinity();
    max_ratio = std::numeric_limits<double>::infinity();
    is_ratio = false;
  }

  Bounds() {
    min_ratio = -std::numeric_limits<double>::infinity();
    max_ratio = std::numeric_limits<double>::infinity();
    min_size = std::numeric_limits<size_t>::min();
    max_size = std::numeric_limits<size_t>::max();
    is_ratio = false;
  }

  Bounds(double min, double max) { set_ratios(min, max); }

  Bounds(size_t min, size_t max) { set_sizes(min, max); }

} Bounds;

class FileScan : public FileRead {
 public:
  FileScan(const std::string dir_path, size_t record_size, long long max_time,
           bool buffered, int num_threads, const Bounds &ex_bounds,
           const Bounds &in_bounds, bool sequential_files, bool sequential_scan,
           bool full_middle);

  void start_read();

  size_t total_files() const { return total_files_; }

  void print_arguments();

 private:
  size_t min_files_;
  size_t max_files_;
  Bounds size_bounds_;
  bool seq_file_;
  bool seq_scan_;
  bool full_middle_;
  bool full_scan_;

  size_t total_files_;

  static void rand_read_info(size_t *pos, size_t *read_size, size_t file_size,
                             double pos_ratio, double size_ratio,
                             size_t align_size);

  void do_read();
  void update_stats(long long time, size_t ops, size_t bytes, size_t records,
                    size_t files);
};

}  // namespace tps

#endif  // FILE_SCAN_HPP