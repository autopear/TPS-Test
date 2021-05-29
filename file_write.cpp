
#include "file_write.hpp"

#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <fstream>
#include <iostream>
#include <random>
#include <vector>

#include "helper.hpp"
#include "io_exception.hpp"
#include "timer.hpp"

namespace tps {

FileWrite::FileWrite(const std::string dir_path, size_t total_size,
                     size_t file_size, bool sequential)
    : dir_(dir_path),
      total_(total_size),
      size_(file_size > total_size || file_size == 0 ? total_size : file_size),
      seq_(sequential) {
  bool is_dir;
  bool exists = file_exists(dir_, &is_dir);
  if (exists) {
    if (!is_dir) throw IOException(dir_ + " is not a directory.");
    for (std::string f : list_dir(dir_)) {
      if (has_suffix(f, ".bin", false))
        throw IOException(dir_path + " is not empty.");
    }
  } else if (mkdir(dir_.c_str(), 0755) != 0) {
    throw IOException("Failed to mkdir " + dir_);
  }
}

std::unordered_map<std::string, long long> FileWrite::write() {
  std::vector<std::string> files;
  if (size_ == total_) {
    files.push_back("0.bin");
  } else {
    size_t num_files =
        static_cast<size_t>(ceil(static_cast<double>(total_) / size_));
    size_t name_len = std::to_string(num_files - 1).size();

    files.reserve(num_files);
    for (size_t i = 0; i < num_files; ++i) {
      std::string name = std::to_string(i);
      while (name.size() < name_len) name = "0" + name;
      files.push_back(name + ".bin");
    }

    if (!seq_) {
      unsigned seed =
          std::chrono::system_clock::now().time_since_epoch().count();
      std::shuffle(files.begin(), files.end(),
                   std::default_random_engine(seed));
    }
  }

  size_t buf_size = 128 * 1024 * 1024;  // 128 MB
  if (buf_size > size_) buf_size = size_;
  char *buf = new char[buf_size];

  std::fstream rs;
  rs.open("/dev/urandom", std::ios::in | std::ios::binary);

  std::unordered_map<std::string, long long> ret;
  ret.reserve(files.size());
  HighResTimer timer;
  for (std::string name : files) {
    timer.start();
    std::fstream fs;
    fs.open(dir_ + "/" + name, std::ios::out | std::ios::binary);

    size_t remains = size_;
    while (remains > 0) {
      size_t r = std::min(buf_size, remains);
      rs.read(buf, r);
      fs.write(buf, r);
      remains -= r;
    }

    fs.flush();
    fs.close();

    timer.stop();

    ret.insert({name, timer.elapsed_ns()});
  }
  delete[] buf;
  return ret;
}

void FileWrite::print_arguments() {
  std::cout << "# dir = " << dir_ << std::endl;
  std::cout << "# total-size = " << total_ << std::endl;
  std::cout << "# file-size = " << size_ << std::endl;
  std::cout << "# sequential = " << (seq_ ? "true" : "false") << std::endl;
}

}  // namespace tps
