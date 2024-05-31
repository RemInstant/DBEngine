#include <type_traits>

#include <file_cannot_be_opened.h>

#include "../include/extra_utility.h"

template<
    typename T>
std::string extra_utility::make_string(T const &value)
{
    return "unimplemented parse";
}

template<> std::string extra_utility::make_string(int const &value)                 { return std::to_string(value); }
template<> std::string extra_utility::make_string(long const &value)                { return std::to_string(value); }
template<> std::string extra_utility::make_string(long long const &value)           { return std::to_string(value); }
template<> std::string extra_utility::make_string(unsigned const &value)            { return std::to_string(value); }
template<> std::string extra_utility::make_string(unsigned long const &value)       { return std::to_string(value); }
template<> std::string extra_utility::make_string(unsigned long long const &value)  { return std::to_string(value); }
template<> std::string extra_utility::make_string(float const &value)               { return std::to_string(value); }
template<> std::string extra_utility::make_string(double const &value)              { return std::to_string(value); }
template<> std::string extra_utility::make_string(long double const &value)         { return std::to_string(value); }
template<> std::string extra_utility::make_string(std::string const &value)         { return value; }
