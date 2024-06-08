#ifndef OPERATING_SYSTEMS_COURSE_WORK_COMMON_EXTRA_UTILITY_H
#define OPERATING_SYSTEMS_COURSE_WORK_COMMON_EXTRA_UTILITY_H

#include <string>
#include <flyweight_string_pool.h>
#include <memory>

namespace extra_utility
{

    template<
        typename T>
    std::string make_string(T const &value);
    
    template<> std::string make_string(int const &value);
    template<> std::string make_string(long const &value);
    template<> std::string make_string(long long const &value);
    template<> std::string make_string(unsigned const &value);
    template<> std::string make_string(unsigned long const &value);
    template<> std::string make_string(unsigned long long const &value);
    template<> std::string make_string(float const &value);
    template<> std::string make_string(double const &value);
    template<> std::string make_string(long double const &value);
    template<> std::string make_string(std::string const &value);
    template<> std::string make_string(char * const &value);
    template<> std::string make_string(std::shared_ptr<flyweight_string> const &value);
    
    std::string make_path(
	    std::initializer_list<std::string> list);
    
    std::string make_path(
	    std::initializer_list<char const *> list);
    
}

#endif //OPERATING_SYSTEMS_COURSE_WORK_COMMON_EXTRA_UTILITY_H