#ifndef FILE_SCAN_HPP
#define FILE_SCAN_HPP

#include <mutex>
#include <string>
#include <utility>

#include "file_read.hpp"
#include "helper.hpp"
#include "io_exception.hpp"

namespace tps {

class FileScan : public FileRead {
 public:
  FileScan(const std::string dir_path, size_t record_size, long long max_time,
           bool buffered, int num_threads,
           const std::pair<size_t, size_t> &ex_bounds,
           const std::pair<double, double> &in_bounds,
           bool sequential_files, bool sequential_scan);
  FileScan(const std::string dir_path, size_t record_size, long long max_time,
           bool buffered, int num_threads,
           const std::pair<double, double> &ex_bounds,
           const std::pair<double, double> &in_bounds,
           bool sequential_files, bool sequential_scan);


  void start_read();

  size_t total_records() const { return total_records_; }
  size_t total_files() const { return total_files_; }

 private:
  size_t min_files_;
  size_t max_files_;
  double min_ratio_;
  double max_ratio_;
  bool full_middle_;
  bool seq_files_;
  bool seq_scan_;
  bool full_scan_;

  size_t total_records_;
  size_t total_files_;

  static void rand_read_info(size_t *pos, size_t *read_size, size_t file_size,
                             double pos_ratio, double size_ratio, size_t align_size);

  void do_read();
  void update_stats(long long time, size_t ops, size_t bytes, size_t records,
                    size_t files);
};

}  // namespace tps

#endif  // FILE_SCAN_HPP