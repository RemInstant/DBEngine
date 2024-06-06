#ifndef OPERATING_SYSTEMS_COURSE_WORK_COMMON_NOT_IMPLEMENTED_H
#define OPERATING_SYSTEMS_COURSE_WORK_COMMON_NOT_IMPLEMENTED_H

#include <stdexcept>

class not_implemented final:
    public std::logic_error
{

public:

    explicit not_implemented(
        std::string const &method_name,
        std::string const &message);

};

#endif //OPERATING_SYSTEMS_COURSE_WORK_COMMON_NOT_IMPLEMENTED_H