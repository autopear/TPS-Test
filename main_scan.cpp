#include <iostream>
#include <string>
#include <utility>

#include "file_scan.hpp"
#include "helper.hpp"

int main(int argc, char *argv[]) {
  if (argc != 11) {
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
    std::cout << "         -  file-ratio: \"m,n\". Random number of files "
                 "between m and n. "
              << std::endl;
    std::cout << "                        m and n can be float (ratio) or int "
                 "(numbers)"
              << std::endl;
    std::cout << "         -  size-ratio: \"m,n\". Scan size ratio of each "
                 "file between m and n. "
              << std::endl;
    std::cout << "                        m and n must be float" << std::endl;
    std::cout << "         -    seq-file: If true, order files to scan "
                 "sequentially by their names."
              << std::endl;
    std::cout
        << "                        {true, t, yes, y, 1, false, f, no, n, 0}"
        << std::endl;
    std::cout << "         -    seq-scan: If true, scan each file sequentially."
              << std::endl;
    std::cout
        << "                        {true, t, yes, y, 1, false, f, no, n, 0}"
        << std::endl;
    std::cout << "         - full-middle: If true, full scan middle files."
              << std::endl;
    std::cout
        << "                        {true, t, yes, y, 1, false, f, no, n, 0}"
        << std::endl;
    return 0;
  }

  std::string dir_path;
  size_t record_size = 0;
  long long max_time = 0;
  bool buffered = true;
  int num_threads = 1;
  std::pair<size_t, size_t> ex_bouds_i;
  std::pair<double, double> ex_bouds_f;
  bool int_bound = true;
  std::pair<double, double> in_bouds;
  bool seq_file = true;
  bool seq_scan = true;
  bool full_middle = false;

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
    else if (arg.first.compare("file-ratio") == 0) {
      size_t cidx = arg.second.find_first_of(",");
      std::string min = arg.second.substr(0, cidx);
      std::string max =
          arg.second.substr(cidx + 1, arg.second.size() - cidx - 1);
      if (min.find_first_of(".") == std::string::npos &&
          max.find_first_of(".") == std::string::npos) {
        int_bound = true;
        ex_bouds_i.first = tps::to_size_t(min);
        ex_bouds_i.second = tps::to_size_t(max);
      } else {
        int_bound = false;
        ex_bouds_f.first = std::stod(min);
        ex_bouds_f.second = std::stod(max);
      }
    } else if (arg.first.compare("size-ratio") == 0) {
      size_t cidx = arg.second.find_first_of(",");
      std::string min = arg.second.substr(0, cidx);
      std::string max =
          arg.second.substr(cidx + 1, arg.second.size() - cidx - 1);
      in_bouds.first = std::stod(min);
      in_bouds.second = std::stod(max);
    } else if (arg.first.compare("seq-file") == 0) {
      std::string value = tps::to_upper(arg.second);
      if (value.compare("TRUE") == 0 || value.compare("T") == 0 ||
          value.compare("YES") == 0 || value.compare("Y") == 0 ||
          value.compare("1") == 0)
        seq_file = true;
      else if (value.compare("FALSE") == 0 || value.compare("F") == 0 ||
               value.compare("NO") == 0 || value.compare("N") == 0 ||
               value.compare("0") == 0)
        seq_file = false;
      else {
        std::cerr << "Value of 'seq-file' is invalid. Valid values are "
                     "{true, t, yes, y, 1, false, f, no, n, 0}."
                  << std::endl;
        return -1;
      }
    } else if (arg.first.compare("seq-scan") == 0) {
      std::string value = tps::to_upper(arg.second);
      if (value.compare("TRUE") == 0 || value.compare("T") == 0 ||
          value.compare("YES") == 0 || value.compare("Y") == 0 ||
          value.compare("1") == 0)
        seq_scan = true;
      else if (value.compare("FALSE") == 0 || value.compare("F") == 0 ||
               value.compare("NO") == 0 || value.compare("N") == 0 ||
               value.compare("0") == 0)
        seq_scan = false;
      else {
        std::cerr << "Value of 'seq-scan' is invalid. Valid values are "
                     "{true, t, yes, y, 1, false, f, no, n, 0}."
                  << std::endl;
        return -1;
      }
    } else if (arg.first.compare("full-middle") == 0) {
      std::string value = tps::to_upper(arg.second);
      if (value.compare("TRUE") == 0 || value.compare("T") == 0 ||
          value.compare("YES") == 0 || value.compare("Y") == 0 ||
          value.compare("1") == 0)
        full_middle = true;
      else if (value.compare("FALSE") == 0 || value.compare("F") == 0 ||
               value.compare("NO") == 0 || value.compare("N") == 0 ||
               value.compare("0") == 0)
        full_middle = false;
      else {
        std::cerr << "Value of 'full-middle' is invalid. Valid values are "
                     "{true, t, yes, y, 1, false, f, no, n, 0}."
                  << std::endl;
        return -1;
      }
    } else {
      std::cerr << "Invalid key '" << arg.first
                << "'. Valid keys are "
                   "{dir, record-size, max-time, buffered, threads, "
                   "file-ratio, size-ratio, seq-file, seq-scan, full-middle}."
                << std::endl;
      return -1;
    }
  }

  if (int_bound) {
    tps::FileScan fs(dir_path, record_size, max_time, buffered, num_threads,
                     ex_bouds_i, in_bouds, seq_file, seq_scan, full_middle);
    fs.print_arguments();
    fs.start_read();
    std::cout.imbue(std::locale("en_US.UTF-8"));
    std::cout << "operations: " << fs.total_ops() << std::endl;
    std::cout << "total time: " << fs.total_time() << " ns" << std::endl;
    std::cout << "total size: " << fs.total_bytes() << " bytes" << std::endl;
    std::cout << "total records: " << fs.total_records() << std::endl;
    std::cout << "total files: " << fs.total_files() << std::endl;
    std::cout << "throughput: "
              << tps::to_bytes_per_sec(fs.total_bytes(), fs.total_time())
              << " bytes/sec, "
              << tps::to_bytes_per_sec(fs.total_records(), fs.total_time())
              << " records/sec" << std::endl;
  } else {
    tps::FileScan fs(dir_path, record_size, max_time, buffered, num_threads,
                     ex_bouds_f, in_bouds, seq_file, seq_scan, full_middle);
    fs.print_arguments();
    fs.start_read();
    std::cout.imbue(std::locale("en_US.UTF-8"));
    std::cout << "operations: " << fs.total_ops() << std::endl;
    std::cout << "total time: " << fs.total_time() << " ns" << std::endl;
    std::cout << "total size: " << fs.total_bytes() << " bytes" << std::endl;
    std::cout << "total records: " << fs.total_records() << std::endl;
    std::cout << "total files: " << fs.total_files() << std::endl;
    std::cout << "throughput: "
              << tps::to_bytes_per_sec(fs.total_bytes(), fs.total_time())
              << " bytes/sec, "
              << tps::to_bytes_per_sec(fs.total_records(), fs.total_time())
              << " records/sec" << std::endl;
  }

  return 0;
}