#ifndef OPERATING_SYSTEMS_COURSE_WORK_DATABASE_MANAGEMENT_SYSTEM_COMMON_TYPES_TDATA
#define OPERATING_SYSTEMS_COURSE_WORK_DATABASE_MANAGEMENT_SYSTEM_COMMON_TYPES_TDATA

#include <allocator.h>

using tkey = std::string;

class tkey_comparer final
{

public:

    int operator()(
        tkey const &lhs,
        tkey const &rhs) const;

};

class tvalue final
{

public:

	uint64_t hashed_password;
	std::string name;

public:
	
	tvalue(
		uint64_t hashed_password,
		std::string const &name);

public:

	// ~tvalue();
	
	// tvalue(
	// 	tvalue const &other);
	
	// tvalue operator=(
	// 	tvalue const &other);
	
	// tvalue(
	// 	tvalue &&other) noexcept;
	
	// tvalue operator=(
	// 	tvalue &&other) noexcept;

};

class tdata
{

public:

	virtual ~tdata() = default;

};

class ram_tdata final
	: public tdata
{

public:

	tvalue value;

public:

	ram_tdata(
		tvalue const &value);

	ram_tdata(
		tvalue &&value);

};

class file_tdata final
	: public tdata
{

private:

	long file_pos;

public:

	void serialize(
		std::string const &path,
		tkey const &key,
		tvalue const &value);
	
	tvalue deserialize(
		std::string const &path) const;

};

#endif //OPERATING_SYSTEMS_COURSE_WORK_DATABASE_MANAGEMENT_SYSTEM_COMMON_TYPES_TDATA