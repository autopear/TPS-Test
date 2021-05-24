#include <algorithm>
#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>

#include "file_write.hpp"
#include "helper.hpp"

int main(int argc, char *argv[]) {
  if (argc != 5) {
    std::cout << "Usage: " << argv[0] << " [key=value]..." << std::endl;
    std::cout << "       Keys:" << std::endl;
    std::cout << "         -        dir: Path to the output directory."
              << std::endl;
    std::cout << "         - total-size: Total data size to write."
              << std::endl;
    std::cout << "                       e.g. 12, 34b, 2kB, 3MB, 4GB"
              << std::endl;
    std::cout << "         -  file-size: Maximum file size." << std::endl;
    std::cout << "                       e.g. 12, 34b, 2kB, 3MB, 4GB"
              << std::endl;
    std::cout << "                       if 0 or file-size >= total-size, "
                 "write to a single file."
              << std::endl;
    std::cout << "         - sequential: Write files sequentially."
              << std::endl;
    std::cout
        << "                       {true, t, yes, y, 1, false, f, no, n, 0}"
        << std::endl;
    return 0;
  }

  std::string dir_path;
  size_t total_size;
  size_t file_size;
  bool sequential;

  for (int i = 1; i < argc; i++) {
    std::pair<std::string, std::string> arg = tps::parse_arg(argv[i]);
    if (arg.first.compare("dir") == 0)
      dir_path = arg.second;
    else if (arg.first.compare("total-size") == 0)
      total_size = tps::size_in_bytes(arg.second);
    else if (arg.first.compare("file-size") == 0)
      file_size = tps::size_in_bytes(arg.second);
    else if (arg.first.compare("sequential") == 0) {
      std::string value = tps::to_upper(arg.second);
      if (value.compare("TRUE") == 0 || value.compare("T") == 0 ||
          value.compare("YES") == 0 || value.compare("Y") == 0 ||
          value.compare("1") == 0)
        sequential = true;
      else if (value.compare("FALSE") == 0 || value.compare("F") == 0 ||
               value.compare("NO") == 0 || value.compare("N") == 0 ||
               value.compare("0") == 0)
        sequential = false;
      else {
        std::cerr << "Value of 'sequential' is invalid. Valid values are "
                     "{true, t, yes, y, 1, false, f, no, n, 0}."
                  << std::endl;
        return -1;
      }
    } else {
      std::cerr << "Invalid key '" << arg.first
                << "'. Valid keys are "
                   "{dir, total-size, file-size, sequential}."
                << std::endl;
      return -1;
    }
  }

  tps::FileWrite fw(dir_path, total_size, file_size, sequential);
  std::unordered_map<std::string, long long> results = fw.write();
  std::vector<std::string> files;
  files.reserve(results.size());
  for (auto it = results.begin(); it != results.end(); ++it)
    files.push_back(it->first);
  std::sort(files.begin(), files.end());

  size_t total_written = 0;
  size_t total_time = 0;
  std::cout.imbue(std::locale(""));
  for (std::string f : files) {
    size_t fsize = tps::get_file_size(dir_path + "/" + f);
    std::cout << "file=" << f << ", size=" << fsize << ", time=" << results[f]
              << std::endl;
    total_written += fsize;
    total_time += results[f];
  }
  std::cout << std::endl;
  std::cout << "total time: " << total_time << " ns" << std::endl;
  std::cout << "total size: " << total_written << " bytes" << std::endl;
  std::cout << "throughput: " << tps::to_bytes_per_sec(total_written, total_time)
            << " bytes/sec" << std::endl;
  return 0;
}