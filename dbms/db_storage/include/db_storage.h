#include <search_tree.h>
#include <b_tree.h>

using tkey = int;
using tvalue = int;

// class tdata final
// {

// public:

// 	tkey key;
// 	tvalue value;

// };

class tkey_comparer final
{

public:

    int operator()(
        tkey const &key1,
        tkey const &key2) const
    {
        return rand() % 3 - 1;
    }

};


class db_storage final
{

public:

	enum class mode
	{
		uninitialized,
		in_memory_cache,
		file_system
	};
	
	enum class search_tree_variant
	{
		b,
		b_plus,
		b_star,
		b_star_plus
	};

private:

	class collection final
	{
	
	private:
	
		search_tree<tkey, tvalue> *_data;
		search_tree_variant _variant;
	
	public:
	
		explicit collection(
			search_tree_variant variant,
			size_t t_for_b_trees = 8);
		
	public:
	
		~collection();
		
		collection(
			collection const &other);
		
		collection &operator=(
			collection const &other);
		
		collection(
			collection &&other) noexcept;
		
		collection &operator=(
			collection &&other) noexcept;
	
	public:
	
		void insert(
			tkey const &key,
			tvalue const &value);
		
		void insert(
			tkey const &key,
			tvalue &&value);
		
		void update(
			tkey const &key,
			tvalue const &value);
		
		void update(
			tkey const &key,
			tvalue &&value);
		
		void dispose(
			tkey const &key);
		
		tvalue &obtain(
			tkey const &key);
		
		std::vector<typename associative_container<tkey, tvalue>::key_value_pair> obtain_between(
			tkey const &lower_bound,
			tkey const &upper_bound,
			bool lower_bound_inclusive,
			bool upper_bound_inclusive);
	
	private:
	
		void clear();
		
		void copy_from(
			collection const &other);
		
		void move_from(
			collection &&other);
	
	};

	class schema final
	{
	
	private:
	
		search_tree<std::string, collection> *_collections;
		search_tree_variant _variant;
	
	public:
	
		explicit schema(
			search_tree_variant variant,
			size_t t_for_b_trees = 8);
		
	public:
	
		~schema();
		
		schema(
			schema const &other);
		
		schema &operator=(
			schema const &other);
		
		schema(
			schema &&other) noexcept;
		
		schema &operator=(
			schema &&other) noexcept;
	
	public:
	
		void add(
			std::string const &collection_name,
			search_tree_variant variant,
			size_t t_for_b_trees = 8);
		
		void dispose(
			std::string const &collection_name);
		
		collection &obtain(
			std::string const &collection_name);
	
	private:
	
		void clear();
		
		void copy_from(
			schema const &other);
		
		void move_from(
			schema &&other);	
	
	};

	class pool final
	{
	
	private:
	
		search_tree<std::string, schema> *_schemas;
		search_tree_variant _variant;
	
	public:
	
		explicit pool(
			search_tree_variant variant,
			size_t t_for_b_trees = 8);
		
	public:
	
		~pool();
		
		pool(
			pool const &other);
		
		pool &operator=(
			pool const &other);
		
		pool(
			pool &&other) noexcept;
		
		pool &operator=(
			pool &&other) noexcept;
	
	public:
	
		void add(
			std::string const &schema_name,
			search_tree_variant variant,
			size_t t_for_b_trees = 8);
		
		void dispose(
			std::string const &schema_name);
		
		schema &obtain(
			std::string const &schema_name);
	
	private:
	
		void clear();
		
		void copy_from(
			pool const &other);
		
		void move_from(
			pool &&other);	
	
	};

private:

	b_tree<std::string, pool> _pools;
	mode _mode;

public:

	static db_storage *get_instance();

public:

	db_storage(
		db_storage const &) = delete;
	
	db_storage(
		db_storage &&) = delete;

public:

	db_storage *set_mode(
		mode mode);

	db_storage *add_pool(
		std::string const &pool_name,
		search_tree_variant variant,
		size_t t_for_b_trees = 8);
	
	db_storage *dispose_pool(
		std::string const &pool_name);
	
	db_storage *add_schema(
		std::string const &pool_name,
		std::string const &schema_name,
		search_tree_variant variant,
		size_t t_for_b_trees = 8);
	
	db_storage *dispose_schema(
		std::string const &pool_name,
		std::string const &schema_name);
	
	db_storage *add_collection(
		std::string const &pool_name,
		std::string const &schema_name,
		std::string const &collection_name,
		search_tree_variant variant,
		size_t t_for_b_trees = 8);
	
	db_storage *dispose_collection(
		std::string const &pool_name,
		std::string const &schema_name,
		std::string const &collection_name);
	
	db_storage *add(
		std::string const &pool_name,
		std::string const &schema_name,
		std::string const &collection_name,
		tkey const &key,
		tvalue const &value);
	
	db_storage *add(
		std::string const &pool_name,
		std::string const &schema_name,
		std::string const &collection_name,
		tkey const &key,
		tvalue &&value);
	
	db_storage *update(
		std::string const &pool_name,
		std::string const &schema_name,
		std::string const &collection_name,
		tkey const &key,
		tvalue &&value);
	
	db_storage *update(
		std::string const &pool_name,
		std::string const &schema_name,
		std::string const &collection_name,
		tkey const &key,
		tvalue const &value);
	
	db_storage *dispose(
		std::string const &pool_name,
		std::string const &schema_name,
		std::string const &collection_name,
		tkey const &key);
	
	tvalue obtain(
		std::string const &pool_name,
		std::string const &schema_name,
		std::string const &collection_name,
		tkey const &key);
	
	std::vector<typename associative_container<tkey, tvalue>::key_value_pair> obtain_between(
		std::string const &pool_name,
		std::string const &schema_name,
		std::string const &collection_name,
        tkey const &lower_bound,
        tkey const &upper_bound,
        bool lower_bound_inclusive,
        bool upper_bound_inclusive);

private:

	db_storage();

private:

	void add(
		std::string const &pool_name,
		search_tree_variant variant,
		size_t t_for_b_trees = 8);
	
	void dispose(
		std::string const &pool_name);
	
	pool &obtain(
		std::string const &pool_name);

private:

	db_storage &throw_if_uninitialized(
		mode mode,
		std::string const &exception_message);
	
	db_storage &throw_if_initialized_at_setup();
	
	db_storage &throw_if_uninitialized_at_setup(
		mode mode);
	
	db_storage &throw_if_uninutialized_at_perform();
	
	db_storage &throw_if_invalid_path(
		std::string const &pool_name,
		std::string const &schema_name = "",
		std::string const &collection_name = "");
	
	db_storage &throw_if_invalid_file_name(
		std::string const &file_name);
	
	db_storage &throw_if_path_is_too_long(
		std::string const &pool_name,
		std::string const &schema_name = "",
		std::string const &collection_name = "");

};