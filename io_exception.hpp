#ifndef IO_EXCEPTION_HPP
#define IO_EXCEPTION_HPP

#include <exception>

namespace tps {

class IOException : public std::exception {
 public:
  IOException(const std::string& msg) : std::exception(), msg_(msg) {}
  virtual const char* what() const throw() { return msg_.c_str(); }

 private:
  std::string msg_;
};

}  // namespace tps

#endif  // IO_EXCEPTION_HPP