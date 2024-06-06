#ifndef OPERATING_SYSTEMS_COURSE_WORK_COMMON_FILE_CANNOT_BE_OPENED_H
#define OPERATING_SYSTEMS_COURSE_WORK_COMMON_FILE_CANNOT_BE_OPENED_H

#include <stdexcept>

class file_cannot_be_opened final:
    public std::runtime_error
{

public:

    explicit file_cannot_be_opened(
        std::string const &file_path);

};

#endif //OPERATING_SYSTEMS_COURSE_WORK_COMMON_FILE_CANNOT_BE_OPENED_H