#ifndef OPERATING_SYSTEMS_COURSE_WORK_DATABASE_MANAGEMENT_SYSTEM_COMMON_TYPES_IPC_DATA
#define OPERATING_SYSTEMS_COURSE_WORK_DATABASE_MANAGEMENT_SYSTEM_COMMON_TYPES_IPC_DATA

#include <unistd.h>
#include <sys/types.h>

namespace db_ipc
{
	
	enum class search_tree_variant
	{
		B,
		B_PLUS,
		B_STAR,
		B_STAR_PLUS,
	};
	
	enum class allocator_variant
	{
		GLOBAL_HEAP,
		SORTED_LIST,
		BUDDY_SYSTEM,
		BOUNDARY_TAGS,
		RED_BLACK_TREE
	};
	
	enum class command
	{
		// manage commands
		
		ADD_STORAGE_SERVER,
		DISPOSE_STORAGE_SERVER,
		
		SET_IN_MEMORY_CACHE_MODE,
		SET_FILE_SYSTEM_MODE,
		END_SETUP,
		
		SHUTDOWN,
		
		GET_RECORDS_CNT,
		
		// data commands
		
		ADD_POOL,
		ADD_SCHEMA,
		ADD_COLLECTION,
		DISPOSE_POOL,
		DISPOSE_SCHEMA,
		DISPOSE_COLLECTION,
		
		ADD,
		UPDATE,
		DISPOSE,
		OBTAIN,
		OBTAIN_BETWEEN,
	};
	
	enum class command_status
	{
		CLIENT,
		
		OK,
		OBTAIN_BETWEEN_END,
		
		BAD_ALLOC,
		
		INVALID_STRUCT_NAME,
		TOO_BIG_STRUCT_NAME,
		INVALID_PATH,
		
		ATTEMPT_TO_CHANGE_SETUP,
		
		POOL_DOES_NOT_EXIST,
		SCHEMA_DOES_NOT_EXIST,
		COLLECTION_DOES_NOT_EXIST,
		
		ATTEMPT_TO_ADD_POOL_DUBLICATE,
		ATTEMPT_TO_ADD_SCHEMA_DUBLICATE,
		ATTEMPT_TO_ADD_COLLECTION_DUBLICATE,
		
		ATTEMPT_TO_INSERT_EXISTENT_KEY,
		ATTEMPT_TO_UPDATE_NONEXISTENT_KEY,
		ATTEMPT_TO_DISPOSE_NONEXISTENT_KEY,
		ATTTEMT_TO_OBTAIN_NONEXISTENT_KEY,
		
		FAILED_TO_ADD_STRUCT,
		FAILED_TO_PERFORM_DATA_COMMAND,
		FAILED_TO_INSERT_KEY,
		FAILED_TO_UPDATE_KEY,
		FAILED_TO_DISPOSE_KEY,
		FAILED_TO_OBTAIN_KEY,
	};
	
	int constexpr STORAGE_MSG_KEY_SIZE = 64;
	int constexpr STORAGE_MSG_NAME_SIZE = 64;
	int constexpr STORAGE_MSG_STRUCTS_NAME_SIZE = 255;

	struct strg_msg_t
	{
		// metadata
		
		long mtype;
		pid_t pid;
		command cmd;
		command_status status;
		size_t extra_value;
		
		// path
		
		char pool_name[STORAGE_MSG_STRUCTS_NAME_SIZE];
		char schema_name[STORAGE_MSG_STRUCTS_NAME_SIZE];
		char collection_name[STORAGE_MSG_STRUCTS_NAME_SIZE];
		
		// data
		
		search_tree_variant tree_variant;
		allocator_variant alloc_variant;
		size_t t_for_b_trees;
		
		char login[STORAGE_MSG_KEY_SIZE];
		char right_boundary_login[STORAGE_MSG_KEY_SIZE];
		int64_t hashed_password;
		char name[STORAGE_MSG_NAME_SIZE];
	};
	
	

	int constexpr STORAGE_SERVER_MQ_KEY = 100;
	int constexpr STORAGE_SERVER_MSG_SIZE = sizeof(strg_msg_t) - sizeof(long);
	
	int constexpr STORAGE_SERVER_MAX_COMMON_PRIOR = 30;
	int constexpr STORAGE_SERVER_MAX_COMMAND_PRIOR = 50;
	
	int constexpr STORAGE_SERVER_STORAGE_ADDITION_PRIOR = 31;
	
}

#endif //OPERATING_SYSTEMS_COURSE_WORK_DATABASE_MANAGEMENT_SYSTEM_COMMON_TYPES_IPC_DATA