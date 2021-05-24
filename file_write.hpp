#ifndef FILE_WRITE_HPP
#define FILE_WRITE_HPP

#include <string>
#include <unordered_map>

namespace tps {

class FileWrite {
 public:
  FileWrite(const std::string dir_path, size_t total_size, size_t file_size,
            bool sequential);

  std::unordered_map<std::string, long long> write();

  void print_arguments();

 private:
  std::string dir_;
  size_t total_;
  size_t size_;
  bool seq_;
};

}  // namespace tps

#endif  // FILE_WRITE_HPP