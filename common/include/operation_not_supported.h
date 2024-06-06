#ifndef OPERATING_SYSTEMS_COURSE_WORK_COMMON_OPERATION_NOT_SUPPORTED_H
#define OPERATING_SYSTEMS_COURSE_WORK_COMMON_OPERATION_NOT_SUPPORTED_H

#include <stdexcept>

class operation_not_supported final:
    public std::logic_error
{

public:

    explicit operation_not_supported();

};

#endif //OPERATING_SYSTEMS_COURSE_WORK_COMMON_OPERATION_NOT_SUPPORTED_H