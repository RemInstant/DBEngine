#ifndef OPERATING_SYSTEMS_COURSE_WORK_LOGGER_H
#define OPERATING_SYSTEMS_COURSE_WORK_LOGGER_H

#include <iostream>

class logger
{

public:

    enum class severity
    {
        trace,
        debug,
        information,
        warning,
        error,
        critical
    };

public:
    
    virtual ~logger() noexcept = default;

public:

    virtual logger const *log(
        std::string const &message,
        logger::severity severity) const noexcept = 0;

public:

    logger const *trace(
        std::string const &message) const noexcept;

    logger const *debug(
        std::string const &message) const noexcept;

    logger const *information(
        std::string const &message) const noexcept;

    logger const *warning(
        std::string const &message) const noexcept;

    logger const *error(
        std::string const &message) const noexcept;

    logger const *critical(
        std::string const &message) const noexcept;

protected:

    static std::string severity_to_string(
        logger::severity severity);
    
    static logger::severity string_to_severity(
        std::string str);

    static std::string global_datetime_to_string() noexcept;
    
    static std::string current_datetime_to_string() noexcept;

};

#endif //OPERATING_SYSTEMS_COURSE_WORK_LOGGER_H