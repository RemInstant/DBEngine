#ifndef OPERATING_SYSTEMS_COURSE_WORK_ALLOCATOR_WITH_FIT_MODE_H
#define OPERATING_SYSTEMS_COURSE_WORK_ALLOCATOR_WITH_FIT_MODE_H

#include "allocator.h"

class allocator_with_fit_mode:
    public allocator
{

public:
    
    enum class fit_mode
    {
        first_fit,
        the_best_fit,
        the_worst_fit
    };

public:
    
    inline virtual void set_fit_mode(
        fit_mode mode) = 0;
    
};

#endif //OPERATING_SYSTEMS_COURSE_WORK_ALLOCATOR_WITH_FIT_MODE_H