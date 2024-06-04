#include <cstring>
#include <iostream>
#include <fstream>

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

tvalue::tvalue():
		hashed_password(0),
		name("")
{ }

tvalue::tvalue(
	uint64_t hashed_password,
	std::string const &name):
		hashed_password(hashed_password),
		name(name)
{ }


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


file_tdata::file_tdata(
	long file_pos):
		_file_pos(file_pos)
{ }

void file_tdata::serialize(
	std::string const &path,
	tkey const &key,
	tvalue const &value,
	bool update_flag)
{
	std::ofstream data_stream(path, std::ios::app | std::ios::binary);
	
    if (!data_stream.is_open())
    {
        throw std::runtime_error("File error!");
    }
	
	// TODO UPDATE WITH BIGGER DATA
	
	if (_file_pos == -1 || update_flag)
	{
		_file_pos = data_stream.tellp();
	}
	else
	{
		data_stream.seekp(_file_pos, std::ios::beg);
	}
	
	size_t login_len = key.size();
	size_t name_len = value.name.size();
	
    data_stream.write(reinterpret_cast<char const *>(&login_len), sizeof(size_t));
    data_stream.write(key.c_str(), sizeof(char) * login_len);
    data_stream.write(reinterpret_cast<char const *>(&value.hashed_password), sizeof(int64_t));
    data_stream.write(reinterpret_cast<char const *>(&name_len), sizeof(size_t));
    data_stream.write(value.name.c_str(), sizeof(char) * name_len);
	data_stream.flush();
}

tvalue file_tdata::deserialize(
	std::string const &path) const
{
	std::ifstream file(path, std::ios::binary);
    if (!file.is_open() || _file_pos == -1)
    {
        throw std::runtime_error("File error!");
    }

    file.seekg(_file_pos, std::ios::beg);
	
	tvalue value;
	size_t login_len, name_len;
	
	file.read(reinterpret_cast<char *>(&login_len), sizeof(size_t));
	file.seekg(login_len, std::ios::cur);
    file.read(reinterpret_cast<char *>(&value.hashed_password), sizeof(size_t));
	file.read(reinterpret_cast<char *>(&name_len), sizeof(size_t));
	
	value.name.resize(name_len);
	for (auto &ch : value.name)
	{
		file.read(&ch, sizeof(char));
	}
	
    return value;
}
