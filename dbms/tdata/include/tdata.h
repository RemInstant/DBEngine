#include <allocator.h>

using tkey = char *;

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
	char *name;

public:

	tvalue(
		uint64_t hashed_password,
		char *name);

public:

	~tvalue();
	
	tvalue(
		tvalue const &other);
	
	tvalue operator=(
		tvalue const &other);
	
	tvalue(
		tvalue &&other) noexcept;
	
	tvalue operator=(
		tvalue &&other) noexcept;

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
		tvalue const &data);
	
	tvalue deserialize(
		std::string const &path) const;

};