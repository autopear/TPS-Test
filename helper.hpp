#ifndef HELPER_HPP
#define HELPER_HPP

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <cmath>
#include <iostream>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace tps {

static std::string to_upper(const std::string &str) {
  std::string ret;
  ret.resize(str.size());
  for (size_t i = 0; i < str.size(); i++) ret[i] = std::toupper(str[i]);
  return ret;
}

static std::string to_lower(const std::string &str) {
  std::string ret;
  ret.resize(str.size());
  for (size_t i = 0; i < str.size(); i++) ret[i] = std::tolower(str[i]);
  return ret;
}

static bool has_suffix(const std::string &str, const std::string &suf,
                       bool case_sensitive = true) {
  size_t l1 = str.size();
  size_t l2 = suf.size();
  if (l1 < l2) return false;
  if (case_sensitive) {
    return str.compare(l1 - l2, l2, suf) == 0;
  } else {
    for (size_t i = 0; i < l2; i++) {
      if (std::toupper(str[l1 - l2 + i]) != std::toupper(suf[i])) return false;
    }
    return true;
  }
}

static std::pair<std::string, std::string> parse_arg(const std::string &value) {
  std::pair<std::string, std::string> ret;
  size_t i = value.find_first_of("=");
  if (i == 0 || i == std::string::npos) return ret;
  ret.first = value.substr(0, i);
  ret.second = value.substr(i + 1, value.size() - i);
  return ret;
}

static size_t to_size_t(const std::string &str) {
  std::stringstream ss(str);
  size_t n;
  ss >> n;
  return n;
}

static size_t size_in_bytes(const std::string &size) {
  std::string tmp = to_upper(size);
  size_t l = tmp.size();
  if (tmp.at(l - 1) == 'B') tmp = tmp.substr(0, l-- - 1);
  size_t scalar;
  char u = tmp.at(l - 1);
  if (u == 'K') {
    scalar = 1024;
    tmp = tmp.substr(0, l-- - 1);
  } else if (u == 'M') {
    scalar = 1024 * 1024;
    tmp = tmp.substr(0, l-- - 1);
  } else if (u == 'G') {
    scalar = 1024 * 1024 * 1024;
    tmp = tmp.substr(0, l-- - 1);
  } else {
    scalar = 1;
  }
  return to_size_t(tmp) * scalar;
}

static long long time_in_ns(const std::string &time) {
  size_t l = time.size();
  if (has_suffix(time, "h", false))
    return std::stoll(time.substr(0, l - 1)) * 60 * 60 * 1000 * 1000 * 1000;
  else if (has_suffix(time, "min", false))
    return std::stoll(time.substr(0, l - 3)) * 60 * 1000 * 1000 * 1000;
  else if (has_suffix(time, "s", false))
    return std::stoll(time.substr(0, l - 1)) * 1000 * 1000 * 1000;
  else if (has_suffix(time, "ms", false))
    return std::stoll(time.substr(0, l - 2)) * 1000 * 1000;
  else if (has_suffix(time, "us", false))
    return std::stoll(time.substr(0, l - 2)) * 1000;
  else if (has_suffix(time, "ns", false))
    return std::stoll(time.substr(0, l - 2));
  else
    return std::stoll(time);
}

static bool file_exists(const std::string &path, bool *is_dir) {
  struct stat st;
  if (stat(path.c_str(), &st) != 0) return false;
  if (is_dir != nullptr) *is_dir = (st.st_mode & S_IFDIR);
  return true;
}

static std::vector<std::string> list_dir(const std::string &dir_path) {
  DIR *dir;
  std::vector<std::string> files;
  struct dirent *ent;
  if ((dir = opendir(dir_path.c_str())) != NULL) {
    /* print all the files and directories within directory */
    while ((ent = readdir(dir)) != NULL) {
      std::string fname(ent->d_name);
      if (fname.compare(".") != 0 && fname.compare("..") != 0)
        files.emplace_back(ent->d_name);
    }
    closedir(dir);
  }
  return files;
}

static size_t get_block_size() {
  struct stat st_f;
  stat("/", &st_f);
  return static_cast<size_t>(st_f.st_blksize);
}

static size_t get_page_size() {
  return static_cast<size_t>(sysconf(_SC_PAGE_SIZE));
}

static size_t get_total_memory() {
  return static_cast<size_t>(sysconf(_SC_PHYS_PAGES) * sysconf(_SC_PAGE_SIZE));
}

static size_t get_file_size(const std::string &path) {
  struct stat st;
  if (stat(path.c_str(), &st) != 0 || !(st.st_mode & S_IFREG))
    return (size_t)-1;
  return static_cast<size_t>(st.st_size);
}

static size_t get_file_blocks(const std::string &path) {
  struct stat st;
  if (stat(path.c_str(), &st) != 0 || !(st.st_mode & S_IFREG))
    return (size_t)-1;
  return static_cast<size_t>(st.st_blocks);
}

static size_t to_bytes_per_sec(size_t size, long long time) {
  double time_in_s = static_cast<double>(time) / (1000 * 1000 * 1000);
  return static_cast<size_t>(round(static_cast<double>(size) / time_in_s));
}

}  // namespace tps

#endif  // HELPER_HPP