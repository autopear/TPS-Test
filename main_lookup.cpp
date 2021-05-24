#include <iostream>
#include <string>

#include "file_lookup.hpp"
#include "helper.hpp"

int main(int argc, char *argv[]) {
  if (argc != 6) {
    std::cout << "Usage: " << argv[0] << " [key=value]..." << std::endl;
    std::cout << "       Keys:" << std::endl;
    std::cout << "            -      dir: Path to the output directory."
              << std::endl;
    std::cout << "         - record-size: Record size." << std::endl;
    std::cout << "                        e.g. 12, 34b, 2kB, 3MB, 4GB"
              << std::endl;
    std::cout << "         -    max-time: Max running time." << std::endl;
    std::cout << "                        e.g. 300, 10{h, min, s, ms, us, ns}"
              << std::endl;
    std::cout << "         -    buffered: Buffered read." << std::endl;
    std::cout
        << "                        {true, t, yes, y, 1, false, f, no, n, 0}"
        << std::endl;
    std::cout << "         -     threads: Number of threads." << std::endl;
    return 0;
  }

  std::string dir_path;
  size_t record_size = 0;
  long long max_time = 0;
  bool buffered = true;
  int num_threads = 1;

  for (int i = 1; i < argc; i++) {
    std::pair<std::string, std::string> arg = tps::parse_arg(argv[i]);
    if (arg.first.compare("dir") == 0)
      dir_path = arg.second;
    else if (arg.first.compare("record-size") == 0)
      record_size = tps::size_in_bytes(arg.second);
    else if (arg.first.compare("max-time") == 0)
      max_time = std::max(0LL, tps::time_in_ns(arg.second));
    else if (arg.first.compare("buffered") == 0) {
      std::string value = tps::to_upper(arg.second);
      if (value.compare("TRUE") == 0 || value.compare("T") == 0 ||
          value.compare("YES") == 0 || value.compare("Y") == 0 ||
          value.compare("1") == 0)
        buffered = true;
      else if (value.compare("FALSE") == 0 || value.compare("F") == 0 ||
               value.compare("NO") == 0 || value.compare("N") == 0 ||
               value.compare("0") == 0)
        buffered = false;
      else {
        std::cerr << "Value of 'buffered' is invalid. Valid values are "
                     "{true, t, yes, y, 1, false, f, no, n, 0}."
                  << std::endl;
        return -1;
      }
    } else if (arg.first.compare("threads") == 0)
      num_threads = std::max(1, std::stoi(arg.second));
    else {
      std::cerr << "Invalid key '" << arg.first
                << "'. Valid keys are "
                   "{dir, record-size, max-time, buffered, threads}."
                << std::endl;
      return -1;
    }
  }

  tps::FileLookup fl(dir_path, record_size, max_time, buffered, num_threads);
  fl.start_read();
  std::cout.imbue(std::locale("en_US.UTF-8"));
  std::cout << "operations: " << fl.total_ops() << std::endl;
  std::cout << "total time: " << fl.total_time() << " ns" << std::endl;
  std::cout << "total size: " << fl.total_bytes() << " bytes" << std::endl;
  std::cout << "total records: " << fl.total_records() << std::endl;
  std::cout << "throughput: "
            << tps::to_bytes_per_sec(fl.total_bytes(), fl.total_time())
            << " bytes/sec, "
            << tps::to_bytes_per_sec(fl.total_records(), fl.total_time())
            << " records/sec" << std::endl;
  return 0;
}