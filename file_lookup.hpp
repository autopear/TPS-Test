#ifndef FILE_LOOKUP_HPP
#define FILE_LOOKUP_HPP

#include <mutex>
#include <string>

#include "file_read.hpp"
#include "helper.hpp"
#include "io_exception.hpp"

namespace tps {

class FileLookup : public FileRead {
 public:
  FileLookup(const std::string dir_path, size_t record_size, long long max_time,
             bool buffered, int num_threads);

  void start_read();

  void print_arguments();

 private:
  void do_read();
  void update_stats(long long time, size_t ops, size_t bytes);
};

}  // namespace tps

#endif  // FILE_LOOKUP_HPP