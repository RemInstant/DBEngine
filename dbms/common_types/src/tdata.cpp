#include <cstring>

#include "../include/tdata.h"

int tkey_comparer::operator()(
	tkey const &lhs,
	tkey const &rhs) const
{
	if (lhs != rhs)
	{
		return lhs < rhs ? -1 : 1;
	}
	return 0;
}

tvalue::tvalue(
	uint64_t hashed_password,
	std::string const &name):
		hashed_password(hashed_password),
		name(name)
{
	
}


// tvalue::~tvalue()
// {
// 	delete[] name;
// }

// tvalue::tvalue(
// 	tvalue const &other):
// 		hashed_password(other.hashed_password),
// 		name(new char[strlen(other.name) + 1])
// {
// 	strcpy(name, other.name);
// }

// tvalue tvalue::operator=(
// 	tvalue const &other)
// {
// 	if (this != &other)
// 	{
// 		delete[] name;
// 		name = (new char[strlen(other.name) + 1]);
// 		strcpy(name, other.name);
// 	}
	
// 	return *this;
// }

// tvalue::tvalue(
// 	tvalue &&other) noexcept:
// 		hashed_password(other.hashed_password),
// 		name(other.name)
// {
// 	other.name = nullptr;
// }

// tvalue tvalue::operator=(
// 	tvalue &&other) noexcept
// {
// 	if (this != &other)
// 	{
// 		hashed_password = other.hashed_password;
// 		name = other.name;
		
// 		other.name = nullptr;
// 	}
	
// 	return *this;
// }

ram_tdata::ram_tdata(
	tvalue const &value):
		value(value)
{ }

ram_tdata::ram_tdata(
	tvalue &&value):
		value(std::move(value))
{ }

void file_tdata::serialize(
	std::string const &path,
	tkey const &key,
	tvalue const &data)
{
	// TODO
}

tvalue file_tdata::deserialize(
	std::string const &path) const
{
	// TODO
}
