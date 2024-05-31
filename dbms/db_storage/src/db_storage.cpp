

#include "../include/db_storage.h"


#pragma region collection implementation

db_storage::collection::collection(
	search_tree_variant variant,
	size_t t_for_b_trees):
		_variant(variant)
{
	switch (variant)
	{
	case search_tree_variant::b:
		//break;
	case search_tree_variant::b_plus:
		//break;
	case search_tree_variant::b_star:
		//break;
	case search_tree_variant::b_star_plus:
		//break;
	default:
		try
		{
			_data = new b_tree<tkey, tvalue>(t_for_b_trees, tkey_comparer());
		}
		catch (std::bad_alloc const &)
		{
			// TODO LOG
			throw;
		}
		break;
	}
}

db_storage::collection::~collection()
{
	clear();
}

db_storage::collection::collection(
	collection const &other)
{
	copy_from(other);
}

db_storage::collection &db_storage::collection::operator=(
	collection const &other)
{
	if (this != &other)
	{
		clear();
		copy_from(other);
	}
	
	return *this;
}

db_storage::collection::collection(
	collection &&other) noexcept
{
	move_from(std::move(other));
}

db_storage::collection &db_storage::collection::operator=(
	db_storage::collection &&other) noexcept
{
	if (this != &other)
	{
		clear();
		move_from(std::move(other));
	}

	return *this;
}

void db_storage::collection::insert(
	tkey const &key,
	tvalue const &value)
{
	_data->insert(key, value);
}

void db_storage::collection::insert(
	tkey const &key,
	tvalue &&value)
{
	_data->insert(key, std::move(value));
}

void db_storage::collection::update(
	tkey const &key,
	tvalue const &value)
{
	_data->update(key, value);
}

void db_storage::collection::update(
	tkey const &key,
	tvalue &&value)
{
	_data->update(key, std::move(value));
}

void db_storage::collection::dispose(
	tkey const &key)
{
	_data->dispose(key);
};

tvalue &db_storage::collection::obtain(
	tkey const &key)
{
	return _data->obtain(key);
};

std::vector<typename associative_container<tkey, tvalue>::key_value_pair>
db_storage::collection::obtain_between(
	tkey const &lower_bound,
	tkey const &upper_bound,
	bool lower_bound_inclusive,
	bool upper_bound_inclusive)
{
	return _data->obtain_between(lower_bound, upper_bound, lower_bound_inclusive, upper_bound_inclusive);
};


void db_storage::collection::clear()
{
	delete _data;
	_data = nullptr;
};

void db_storage::collection::copy_from(
	collection const &other)
{
	switch (_variant = other._variant)
	{
	case search_tree_variant::b:
		//break;
	case search_tree_variant::b_plus:
		//break;
	case search_tree_variant::b_star:
		//break;
	case search_tree_variant::b_star_plus:
		//break;
	default:
		try
		{
			_data = new b_tree<tkey, tvalue>(
				*dynamic_cast<b_tree<tkey, tvalue> *>(other._data));
		}
		catch (std::bad_alloc const &)
		{
			// TODO LOG
			throw;
		}
		break;
	}
};

void db_storage::collection::move_from(
	collection &&other)
{
	switch (_variant = other._variant)
	{
	case search_tree_variant::b:
		//break;
	case search_tree_variant::b_plus:
		//break;
	case search_tree_variant::b_star:
		//break;
	case search_tree_variant::b_star_plus:
		//break;
	default:
		try
		{
			_data = new b_tree<tkey, tvalue>(
				std::move(*dynamic_cast<b_tree<tkey, tvalue> *>(other._data)));
		}
		catch (std::bad_alloc const &)
		{
			// TODO LOG
			throw;
		}
		break;
	}
	
	other._data = nullptr;
};

#pragma endregion collection implementation

#pragma region scheme implementation

db_storage::schema::schema(
	search_tree_variant variant,
	size_t t_for_b_trees)
{
	switch (variant)
	{
	case search_tree_variant::b:
		//break;
	case search_tree_variant::b_plus:
		//break;
	case search_tree_variant::b_star:
		//break;
	case search_tree_variant::b_star_plus:
		//break;
	default:
		try
		{
			_collections = new b_tree<std::string, collection>(t_for_b_trees);
		}
		catch (std::bad_alloc const &)
		{
			// TODO LOG
			throw;
		}
		break;
	}
}

db_storage::schema::~schema()
{
	clear();
}

db_storage::schema::schema(
	db_storage::schema const &other)
{
	copy_from(other);
}

db_storage::schema &db_storage::schema::operator=(
	db_storage::schema const &other)
{
	if (this != &other)
	{
		clear();
		copy_from(other);
	}
	
	return *this;
}

db_storage::schema::schema(
	db_storage::schema &&other) noexcept
{
	move_from(std::move(other));
}

db_storage::schema &db_storage::schema::operator=(
	db_storage::schema &&other) noexcept
{
	if (this != &other)
	{
		clear();
		move_from(std::move(other));
	}
	
	return *this;
}

void db_storage::schema::add(
	std::string const &collection_name,
	search_tree_variant variant,
	size_t t_for_b_trees)
{
	_collections->insert(collection_name, collection(variant, t_for_b_trees));
}

void db_storage::schema::dispose(
	std::string const &collection_name)
{
	_collections->dispose(collection_name);
}

db_storage::collection &db_storage::schema::obtain(
	std::string const &collection_name)
{
	return _collections->obtain(collection_name);
}

void db_storage::schema::clear()
{
	delete _collections;
	_collections = nullptr;
}

void db_storage::schema::copy_from(
	schema const &other)
{
	switch (_variant = other._variant)
	{
	case search_tree_variant::b:
		//break;
	case search_tree_variant::b_plus:
		//break;
	case search_tree_variant::b_star:
		//break;
	case search_tree_variant::b_star_plus:
		//break;
	default:
		try
		{
			_collections = new b_tree<std::string, collection>(
				*dynamic_cast<b_tree<std::string, collection> *>(other._collections));
		}
		catch (std::bad_alloc const &)
		{
			// TODO LOG
			throw;
		}
		break;
	}
}

void db_storage::schema::move_from(
	schema &&other)
{
	switch (_variant = other._variant)
	{
	case search_tree_variant::b:
		//break;
	case search_tree_variant::b_plus:
		//break;
	case search_tree_variant::b_star:
		//break;
	case search_tree_variant::b_star_plus:
		//break;
	default:
		try
		{
			_collections = new b_tree<std::string, collection>(
				std::move(*dynamic_cast<b_tree<std::string, collection> *>(other._collections)));
		}
		catch (std::bad_alloc const &)
		{
			// TODO LOG
			throw;
		}
		break;
	}
	
	other._collections = nullptr;
}	

#pragma endregion scheme implementation

#pragma region pool implementation

db_storage::pool::pool(
	search_tree_variant variant,
	size_t t_for_b_trees)
{
	switch (variant)
	{
	case search_tree_variant::b:
		//break;
	case search_tree_variant::b_plus:
		//break;
	case search_tree_variant::b_star:
		//break;
	case search_tree_variant::b_star_plus:
		//break;
	default:
		try
		{
			_schemas = new b_tree<std::string, schema>(t_for_b_trees);
		}
		catch (std::bad_alloc const &)
		{
			// TODO LOG
			throw;
		}
		break;
	}
}

db_storage::pool::~pool()
{
	clear();
}

db_storage::pool::pool(
	pool const &other)
{
	copy_from(other);
}

db_storage::pool &db_storage::pool::operator=(
	db_storage::pool const &other)
{
	if (this != &other)
	{
		clear();
		copy_from(other);
	}
	
	return *this;
}

db_storage::pool::pool(
	db_storage::pool &&other) noexcept
{
	move_from(std::move(other));
}

db_storage::pool &db_storage::pool::operator=(
	db_storage::pool &&other) noexcept
{
	if (this != &other)
	{
		clear();
		move_from(std::move(other));
	}
	
	return *this;
}

void db_storage::pool::add(
	std::string const &schema_name,
	search_tree_variant variant,
	size_t t_for_b_trees)
{
	_schemas->insert(schema_name, schema(variant, t_for_b_trees));
}

void db_storage::pool::dispose(
	std::string const &schema_name)
{
	_schemas->dispose(schema_name);
}

db_storage::schema &db_storage::pool::obtain(
	std::string const &schema_name)
{
	return _schemas->obtain(schema_name);
}

void db_storage::pool::clear()
{
	delete _schemas;
	_schemas = nullptr;
}

void db_storage::pool::copy_from(
	db_storage::pool const &other)
{
	switch (_variant = other._variant)
	{
	case search_tree_variant::b:
		//break;
	case search_tree_variant::b_plus:
		//break;
	case search_tree_variant::b_star:
		//break;
	case search_tree_variant::b_star_plus:
		//break;
	default:
		try
		{
			_schemas = new b_tree<std::string, schema>(
				*dynamic_cast<b_tree<std::string, schema> *>(other._schemas));
		}
		catch (std::bad_alloc const &)
		{
			// TODO LOG
			throw;
		}
		break;
	}
}

void db_storage::pool::move_from(
	db_storage::pool &&other)
{
	switch (_variant = other._variant)
	{
	case search_tree_variant::b:
		//break;
	case search_tree_variant::b_plus:
		//break;
	case search_tree_variant::b_star:
		//break;
	case search_tree_variant::b_star_plus:
		//break;
	default:
		try
		{
			_schemas = new b_tree<std::string, schema>(
				std::move(*dynamic_cast<b_tree<std::string, schema> *>(other._schemas)));
		}
		catch (std::bad_alloc const &)
		{
			// TODO LOG
			throw;
		}
		break;
	}
	
	other._schemas = nullptr;
}

#pragma endregion pool implementation



#pragma region db storage instance getter and constructor implementation

db_storage *db_storage::get_instance()
{
	static auto *instance = new db_storage();
	return instance;
}

db_storage::db_storage():
	_pools(8),
	_mode(mode::uninitialized)
{ }

#pragma endregion db storage instance getter and constructor implementation

#pragma region db storage public operations implementation

db_storage *db_storage::set_mode(
	db_storage::mode mode)
{
	throw_if_initialized_at_setup()
		.throw_if_uninitialized_at_setup(mode);
	
	_mode = mode;
	
	return this;
}

db_storage *db_storage::add_pool(
	std::string const &pool_name,
	db_storage::search_tree_variant variant,
	size_t t_for_b_trees)
{
	throw_if_uninutialized_at_perform()
		.add(pool_name, variant, t_for_b_trees);
	
	return this;
}

db_storage *db_storage::dispose_pool(
	std::string const &pool_name)
{
	throw_if_uninutialized_at_perform()
		.dispose(pool_name);
	
	return this;
}

db_storage *db_storage::add_schema(
	std::string const &pool_name,
	std::string const &schema_name,
	db_storage::search_tree_variant variant,
	size_t t_for_b_trees)
{
	throw_if_uninutialized_at_perform()
		.throw_if_invalid_path(pool_name)
		.throw_if_invalid_file_name(schema_name)
		.throw_if_path_is_too_long(pool_name, schema_name)
		.obtain(pool_name)
		.add(schema_name, variant, t_for_b_trees);
	
	return this;
}

db_storage *db_storage::dispose_schema(
	std::string const &pool_name,
	std::string const &schema_name)
{
	throw_if_uninutialized_at_perform()
		.obtain(pool_name)
		.dispose(schema_name);
	
	return this;
}

db_storage *db_storage::add_collection(
	std::string const &pool_name,
	std::string const &schema_name,
	std::string const &collection_name,
	db_storage::search_tree_variant variant,
	size_t t_for_b_trees)
{
	throw_if_uninutialized_at_perform()
		.throw_if_invalid_path(pool_name, schema_name)
		.throw_if_invalid_file_name(collection_name)
		.throw_if_path_is_too_long(pool_name, schema_name, collection_name)
		.obtain(pool_name)
		.obtain(schema_name)
		.add(collection_name, variant, t_for_b_trees);
	
	return this;
}

db_storage *db_storage::dispose_collection(
	std::string const &pool_name,
	std::string const &schema_name,
	std::string const &collection_name)
{
	throw_if_uninutialized_at_perform()
		.obtain(pool_name)
		.obtain(schema_name)
		.dispose(collection_name);
	
	return this;
}

db_storage *db_storage::add(
	std::string const &pool_name,
	std::string const &schema_name,
	std::string const &collection_name,
	tkey const &key,
	tvalue const &value)
{
	throw_if_uninutialized_at_perform()
		.throw_if_invalid_path(pool_name, schema_name, collection_name)
		.obtain(pool_name)
		.obtain(schema_name)
		.obtain(collection_name)
		.insert(key, value);
	
	return this;
}

db_storage *db_storage::add(
	std::string const &pool_name,
	std::string const &schema_name,
	std::string const &collection_name,
	tkey const &key,
	tvalue &&value)
{
	throw_if_uninutialized_at_perform()
		.throw_if_invalid_path(pool_name, schema_name, collection_name)
		.obtain(pool_name)
		.obtain(schema_name)
		.obtain(collection_name)
		.insert(key, std::move(value));
	
	return this;
}

db_storage *db_storage::update(
	std::string const &pool_name,
	std::string const &schema_name,
	std::string const &collection_name,
	tkey const &key,
	tvalue const &value)
{
	throw_if_uninutialized_at_perform()
		.throw_if_invalid_path(pool_name, schema_name, collection_name)
		.obtain(pool_name)
		.obtain(schema_name)
		.obtain(collection_name)
		.update(key, value);
	
	return this;
}

db_storage *db_storage::update(
	std::string const &pool_name,
	std::string const &schema_name,
	std::string const &collection_name,
	tkey const &key,
	tvalue &&value)
{
	throw_if_uninutialized_at_perform()
		.throw_if_invalid_path(pool_name, schema_name, collection_name)
		.obtain(pool_name)
		.obtain(schema_name)
		.obtain(collection_name)
		.update(key, std::move(value));
	
	return this;
}

db_storage *db_storage::dispose(
	std::string const &pool_name,
	std::string const &schema_name,
	std::string const &collection_name,
	tkey const &key)
{
	throw_if_uninutialized_at_perform()
		.throw_if_invalid_path(pool_name, schema_name, collection_name)
		.obtain(pool_name)
		.obtain(schema_name)
		.obtain(collection_name)
		.dispose(key);
	
	return this;
}

tvalue db_storage::obtain(
	std::string const &pool_name,
	std::string const &schema_name,
	std::string const &collection_name,
	tkey const &key)
{
	return throw_if_uninutialized_at_perform()
			.throw_if_invalid_path(pool_name, schema_name, collection_name)
			.obtain(pool_name)
			.obtain(schema_name)
			.obtain(collection_name)
			.obtain(key);
}

std::vector<typename associative_container<tkey, tvalue>::key_value_pair>
db_storage::obtain_between(
	std::string const &pool_name,
	std::string const &schema_name,
	std::string const &collection_name,
	tkey const &lower_bound,
	tkey const &upper_bound,
	bool lower_bound_inclusive,
	bool upper_bound_inclusive)
{
	return throw_if_uninutialized_at_perform()
			.throw_if_invalid_path(pool_name, schema_name, collection_name)
			.obtain(pool_name)
			.obtain(schema_name)
			.obtain(collection_name)
			.obtain_between(lower_bound, upper_bound, lower_bound_inclusive,upper_bound_inclusive);
}

#pragma endregion db storage public operations implementation

#pragma region db storage utility data operations implementation

void db_storage::add(
	std::string const &pool_name,
	search_tree_variant variant,
	size_t t_for_b_trees)
{
	_pools.insert(pool_name, pool(variant, t_for_b_trees));
}

void db_storage::dispose(
	std::string const &pool_name)
{
	_pools.dispose(pool_name);
}

db_storage::pool &db_storage::obtain(
	std::string const &pool_name)
{
	return _pools.obtain(pool_name);
}

#pragma endregion db storage utility data operations implementation

#pragma region db storage validators implementation

db_storage &db_storage::throw_if_uninitialized(
	db_storage::mode mode,
	std::string const &exception_message)
{
	if (mode != mode::uninitialized)
	{
		return *this;
	}
	
	throw std::logic_error(exception_message);
}

db_storage &db_storage::throw_if_initialized_at_setup()
{
	if (_mode == mode::uninitialized)
	{
		return *this;
	}
	
	throw std::logic_error("attempt to change previously set up mode");
}

db_storage &db_storage::throw_if_uninitialized_at_setup(
	db_storage::mode mode)
{
	return throw_if_uninitialized(mode, "invalid mode");
}

db_storage &db_storage::throw_if_uninutialized_at_perform()
{
	return throw_if_uninitialized(_mode, "attempt to perform an operation while mode not initialized");
}

db_storage &db_storage::throw_if_invalid_path(
	std::string const &pool_name,
	std::string const &schema_name,
	std::string const &collection_name)
{
	if (_mode == mode::file_system)
	{
		// TODO check path existance
	}
	
	return *this;
}

db_storage &db_storage::throw_if_invalid_file_name(
	std::string const &subpath)
{
	if (_mode == mode::file_system)
        {
            // TODO: validate file name
        }

        return *this;
}

db_storage &db_storage::throw_if_path_is_too_long(
		std::string const &pool_name,
		std::string const &schema_name,
		std::string const &collection_name)
{
	if (_mode == mode::file_system)
	{
		// TODO: validate file path length
	}

	return *this;
}

#pragma endregion db storage validators implementation
