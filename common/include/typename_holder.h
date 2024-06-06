#ifndef OPERATING_SYSTEMS_COURSE_WORK_COMMON_TYPENAME_HOLDER_H
#define OPERATING_SYSTEMS_COURSE_WORK_COMMON_TYPENAME_HOLDER_H

#include <string>

class typename_holder
{

public:

    virtual ~typename_holder() noexcept = default;

protected:

    virtual inline std::string get_typename() const noexcept = 0;

};

#endif //OPERATING_SYSTEMS_COURSE_WORK_COMMON_TYPENAME_HOLDER_H