#include <iostream>
#include <iomanip>
#include <fstream>
#include <string>
#include <cstring>
#include <map>
#include <thread>
#include <sys/msg.h>
#include <sys/wait.h>

#include <file_cannot_be_opened.h>
#include <tdata.h>
#include <db_storage.h>
#include <ipc_data.h>
#include <server_logger.h>

int run_flag = 1;
int mq_descriptor = -1;

#include "sys/stat.h"

int main(int argc, char** argv)
{
	if (argc != 3)
	{
		return 1;
	}
		
	pid_t pid = getpid();
	
	while (getpid() <= db_ipc::MANAGER_SERVER_MAX_COMMAND_PRIOR)
	{
		switch (pid = fork())
		{
			case -1:
				return 2;
			case 0:
				break;
			default:
				waitpid(pid, NULL, 0);
				return 0;
		}
	}
	
	pid = getpid();
	size_t id;
	
	if (argc > 0)
	{
		id = std::stoi(argv[0]);
	}
	
	db_ipc::strg_msg_t msg;
	db_storage *db = db_storage::get_instance();
	bool is_setup = false;
	
	logger *logger = nullptr;
	std::string log_base = "[STRG " + std::to_string(id) + ":" + std::to_string(getpid()) + "]";
	
	try
	{
		logger = server_logger_builder()
			.transform_with_configuration(argv[1], argv[2])
			->build();
	}
	catch (...)
	{
		return 3;
	}
	
    mq_descriptor = msgget(db_ipc::MANAGER_SERVER_MQ_KEY, 0666);
    if (mq_descriptor == -1)
    {
		logger->error(log_base + "[-----] Failed to open message queue");
        return 4;
    }
	
	logger->information(log_base + "[-----] Server started");
	
    while (run_flag)
    {
        ssize_t rcv_cnt = msgrcv(mq_descriptor, &msg, db_ipc::MANAGER_SERVER_MSG_SIZE, pid, MSG_NOERROR);
        if (rcv_cnt == -1)
        {
			logger->error(log_base + "[-----] An error occurred while receiving the message. Shutdown");
            break;
        }
		
		
		std::string pid_str = std::to_string(msg.pid);
		while (pid_str.size() < 5) pid_str = "0" + pid_str;
		
		std::string log_start = log_base + "[" + pid_str + "] ";
		
		msg.mtype = db_ipc::MANAGER_SERVER_STORAGE_ANSWER_PRIOR;
		msg.status = db_ipc::command_status::OK;
        
		switch (msg.cmd)
		{
			case db_ipc::command::SET_IN_MEMORY_CACHE_MODE:
			{
				if (is_setup)
				{
					msg.status = db_ipc::command_status::ATTEMPT_TO_CHANGE_SETUP;
					logger->error(log_start + "Attempt to change setup");
				}
				else
				{
					db->setup(msg.extra_value, db_storage::mode::in_memory_cache);
					logger->information(log_start + "Server is setup with 'in memory cache' mode");
				}
				msg.mtype = db_ipc::MANAGER_SERVER_STORAGE_ADDITION_PRIOR;
				msgsnd(mq_descriptor, &msg, db_ipc::MANAGER_SERVER_MSG_SIZE, 0);
				break;
			}
			case db_ipc::command::SET_FILE_SYSTEM_MODE:
			{
				if (is_setup)
				{
					msg.status = db_ipc::command_status::ATTEMPT_TO_CHANGE_SETUP;
					logger->error(log_start + "Attempt to change setup");
				}
				else
				{
					try
					{
						db->setup(msg.extra_value, db_storage::mode::file_system)
							->load_db("pools");
					}
					catch (db_storage::setup_failure const &)
					{
						logger->information(log_start + "Failed to setup with 'file system' mode");
						msg.status = db_ipc::command_status::FAILED_TO_SETUP_STORAGE_SERVER;
					}
					catch (db_storage::invalid_path_exception const &)
					{
						logger->information(log_start + "Failed to setup with 'file system' mode due to invalid path");
						msg.status = db_ipc::command_status::FAILED_TO_SETUP_STORAGE_SERVER;
					}
					catch (std::ios::failure const &)
					{
						logger->information(log_start + "Failed to setup with 'file system' mode dut to fyle system error");
						msg.status = db_ipc::command_status::FAILED_TO_SETUP_STORAGE_SERVER;
					}
				}
				
				if (msg.status == db_ipc::command_status::OK)
				{
					logger->information(log_start + "Server is setup with 'file_system' mode");
				}
				
				msg.mtype = db_ipc::MANAGER_SERVER_STORAGE_ADDITION_PRIOR;
				msgsnd(mq_descriptor, &msg, db_ipc::MANAGER_SERVER_MSG_SIZE, 0);
				break;
			}
			case db_ipc::command::END_SETUP:
			{
				is_setup = true;
			}
			case db_ipc::command::SHUTDOWN:
			{
				run_flag = 0;
				logger->information(log_start + "Server shutdowns");
				break;
			}
			case db_ipc::command::TERMINATE:
			{
				run_flag = 0;
				db->clear();
				logger->information(log_start + "Server shutdowns");
				break;
			}
			case db_ipc::command::GET_RECORDS_CNT:
			{
				msg.mtype = db_ipc::MANAGER_SERVER_STORAGE_GETTING_RECORDS_CNT_PRIOR;
				try
				{
					msg.extra_value = db->get_collection_records_cnt(msg.pool_name, msg.schema_name, msg.collection_name);
				}
				catch(db_storage::invalid_path_exception const&)
				{
					msg.status = db_ipc::command_status::INVALID_PATH;
				}
				
				msgsnd(mq_descriptor, &msg, db_ipc::MANAGER_SERVER_MSG_SIZE, 0);
				break;
			}
			case db_ipc::command::ADD_POOL:
			{
				try
				{
					db->add_pool(msg.pool_name, static_cast<db_storage::search_tree_variant>(msg.tree_variant), msg.t_for_b_trees);
				}
				catch (db_storage::setup_failure const &)
				{
                    logger->error(log_start + "Failed to add pool");
					msg.status = db_ipc::command_status::FAILED_TO_ADD_STRUCT;
				}
				catch (db_storage::invalid_path_exception const &)
				{
					logger->error(log_start + "Failed to add pool due to invalid path");
					msg.status = db_ipc::command_status::INVALID_PATH;
				}
				catch (db_storage::invalid_struct_name_exception const &)
				{
                    logger->error(log_start + "Failed to add pool due to invalid name");
					msg.status = db_ipc::command_status::INVALID_STRUCT_NAME;
				}
				catch (db_storage::too_big_struct_name_exception const &)
				{
                    logger->error(log_start + "Failed to add pool due to too big name");
					msg.status = db_ipc::command_status::TOO_BIG_STRUCT_NAME;
				}
				catch (db_storage::insertion_of_existent_struct_attempt_exception const &)
				{
                    logger->error(log_start + "Attempt to add pool dublicate");
					msg.status = db_ipc::command_status::ATTEMPT_TO_ADD_POOL_DUBLICATE;
				}
				catch (db_storage::insertion_of_struct_failure const &)
				{
                    logger->error(log_start + "Failed to add pool");
					msg.status = db_ipc::command_status::FAILED_TO_ADD_STRUCT;
				}
				catch (std::bad_alloc const &)
				{
                    logger->error(log_start + "Failed to add pool due to bad alloc");
					msg.status = db_ipc::command_status::BAD_ALLOC;
				}
				
				if (msg.status == db_ipc::command_status::OK)
				{
                    logger->information(log_start + "Added pool '" + msg.pool_name + "'");
				}
				
				msg.mtype = db_ipc::MANAGER_SERVER_STRUCT_ADDITION_PRIOR;
				msgsnd(mq_descriptor, &msg, db_ipc::MANAGER_SERVER_MSG_SIZE, 0);
				break;
			}
			case db_ipc::command::ADD_SCHEMA:
			{
				try
				{
					db->add_schema(msg.pool_name, msg.schema_name,
							static_cast<db_storage::search_tree_variant>(msg.tree_variant), msg.t_for_b_trees);
				}
				catch (db_storage::setup_failure const &)
				{
                    logger->error(log_start + "Failed to add schema");
					msg.status = db_ipc::command_status::FAILED_TO_ADD_STRUCT;
				}
				catch (db_storage::invalid_path_exception const &)
				{
					logger->error(log_start + "Failed to add schema due to invalid path");
					msg.status = db_ipc::command_status::INVALID_PATH;
				}
				catch (db_storage::invalid_struct_name_exception const &)
				{
                    logger->error(log_start + "Failed to add schema due to invalid name");
					msg.status = db_ipc::command_status::INVALID_STRUCT_NAME;
				}
				catch (db_storage::too_big_struct_name_exception const &)
				{
                    logger->error(log_start + "Failed to add schema due to too big name");
					msg.status = db_ipc::command_status::TOO_BIG_STRUCT_NAME;
				}
				catch (db_storage::insertion_of_struct_failure const &)
				{
                    logger->error(log_start + "Failed to add schema");
					msg.status = db_ipc::command_status::FAILED_TO_ADD_STRUCT;
				}
				catch (db_storage::insertion_of_existent_struct_attempt_exception const &)
				{
                    logger->error(log_start + "Attempt to add schema dublicate");
					msg.status = db_ipc::command_status::ATTEMPT_TO_ADD_POOL_DUBLICATE;
				}
				catch (std::bad_alloc const &)
				{
                    logger->error(log_start + "Failed to add schema due to bad alloc");
					msg.status = db_ipc::command_status::BAD_ALLOC;
				}
				
				if (msg.status == db_ipc::command_status::OK)
				{
                    logger->information(log_start + "Added schema '" + msg.pool_name + '/' + msg.schema_name + "'");
				}
				
				msg.mtype = db_ipc::MANAGER_SERVER_STRUCT_ADDITION_PRIOR;
				msgsnd(mq_descriptor, &msg, db_ipc::MANAGER_SERVER_MSG_SIZE, 0);
				break;
			}
			case db_ipc::command::ADD_COLLECTION:
			{
				try
				{
					db->add_collection(msg.pool_name, msg.schema_name, msg.collection_name,
							static_cast<db_storage::search_tree_variant>(msg.tree_variant),
							static_cast<db_storage::allocator_variant>(msg.alloc_variant),
							static_cast<allocator_with_fit_mode::fit_mode>(msg.alloc_fit_mode),
							msg.t_for_b_trees);
				}
				catch (db_storage::setup_failure const &)
				{
                    logger->error(log_start + "Failed to add collection");
					msg.status = db_ipc::command_status::FAILED_TO_ADD_STRUCT;
				}
				catch (db_storage::invalid_path_exception const &)
				{
					logger->error(log_start + "Failed to add collection due to invalid path");
					msg.status = db_ipc::command_status::INVALID_PATH;
				}
				catch (db_storage::invalid_struct_name_exception const &)
				{
                    logger->error(log_start + "Failed to add collection due to invalid name");
					msg.status = db_ipc::command_status::INVALID_STRUCT_NAME;
				}
				catch (db_storage::too_big_struct_name_exception const &)
				{
                    logger->error(log_start + "Failed to add collection due to too big name");
					msg.status = db_ipc::command_status::TOO_BIG_STRUCT_NAME;
				}
				catch (db_storage::insertion_of_struct_failure const &)
				{
                    logger->error(log_start + "Failed to add collection");
					msg.status = db_ipc::command_status::FAILED_TO_ADD_STRUCT;
				}
				catch (db_storage::insertion_of_existent_struct_attempt_exception const &)
				{
                    logger->error(log_start + "Attempt to add collection dublicate");
					msg.status = db_ipc::command_status::ATTEMPT_TO_ADD_POOL_DUBLICATE;
				}
				catch (std::bad_alloc const &)
				{
                    logger->error(log_start + "Failed to add collection due to bad alloc");
					msg.status = db_ipc::command_status::BAD_ALLOC;
				}
				
				if (msg.status == db_ipc::command_status::OK)
				{
                    logger->information(log_start + "Added collection '" + msg.pool_name + '/' +
							msg.schema_name + '/' + msg.collection_name + "'");
				}
				
				msg.mtype = db_ipc::MANAGER_SERVER_STRUCT_ADDITION_PRIOR;
				msgsnd(mq_descriptor, &msg, db_ipc::MANAGER_SERVER_MSG_SIZE, 0);
				break;
			}
			case db_ipc::command::DISPOSE_POOL:
			{
				try
				{
					db->dispose_pool(msg.pool_name);
				}
				catch (db_storage::setup_failure const &)
				{
                    logger->error(log_start + "Failed to dispose pool");
					msg.status = db_ipc::command_status::FAILED_TO_DISPOSE_STRUCT;
				}
				catch (db_storage::invalid_path_exception const &)
				{
                    logger->error(log_start + "Failed to dispose pool due to invalid path");
					msg.status = db_ipc::command_status::INVALID_PATH;
				}
				catch (db_storage::disposal_of_nonexistent_struct_attempt_exception)
				{
                    logger->error(log_start + "Attempt to dispose non-existent pool");
					msg.status = db_ipc::command_status::POOL_DOES_NOT_EXIST;
				}
				
				if (msg.status == db_ipc::command_status::OK)
				{
                    logger->information(log_start + "Disposed collection '" + msg.pool_name + "'");
				}
				
				msg.schema_name[0] = '\0';
				msg.collection_name[0] = '\0';
				
				msg.mtype = db_ipc::MANAGER_SERVER_STRUCT_DISPOSAL_PRIOR;
				msgsnd(mq_descriptor, &msg, db_ipc::MANAGER_SERVER_MSG_SIZE, 0);
				break;
			}
			case db_ipc::command::DISPOSE_SCHEMA:
			{
				try
				{
					db->dispose_schema(msg.pool_name, msg.schema_name);
				}
				catch (db_storage::setup_failure const &)
				{
                    logger->error(log_start + "Failed to dispose schema");
					msg.status = db_ipc::command_status::FAILED_TO_DISPOSE_STRUCT;
				}
				catch (db_storage::invalid_path_exception const &)
				{
                    logger->error(log_start + "Failed to dispose schema due to invalid path");
					msg.status = db_ipc::command_status::INVALID_PATH;
				}
				catch (db_storage::disposal_of_nonexistent_struct_attempt_exception)
				{
                    logger->error(log_start + "Attempt to dispose non-existent schema");
					msg.status = db_ipc::command_status::POOL_DOES_NOT_EXIST;
				}
				
				if (msg.status == db_ipc::command_status::OK)
				{
                    logger->information(log_start + "Disposed schema '" + msg.pool_name + '/' + msg.schema_name + "'");
				}
				
				msg.schema_name[0] = '\0';
				
				msg.mtype = db_ipc::MANAGER_SERVER_STRUCT_DISPOSAL_PRIOR;
				msgsnd(mq_descriptor, &msg, db_ipc::MANAGER_SERVER_MSG_SIZE, 0);
				break;
			}
			case db_ipc::command::DISPOSE_COLLECTION:
			{
				try
				{
					db->dispose_collection(msg.pool_name, msg.schema_name, msg.collection_name);
				}
				catch (db_storage::setup_failure const &)
				{
                    logger->error(log_start + "Failed to dispose collection");
					msg.status = db_ipc::command_status::FAILED_TO_DISPOSE_STRUCT;
				}
				catch (db_storage::invalid_path_exception const &)
				{
                    logger->error(log_start + "Failed to dispose collection due to invalid path");
					msg.status = db_ipc::command_status::INVALID_PATH;
				}
				catch (db_storage::disposal_of_nonexistent_struct_attempt_exception)
				{
                    logger->error(log_start + "Attempt to dispose non-existent collection");
					msg.status = db_ipc::command_status::POOL_DOES_NOT_EXIST;
				}
				
				if (msg.status == db_ipc::command_status::OK)
				{
                    logger->information(log_start + "Disposed collection '" + msg.pool_name + '/' +
							msg.schema_name + '/' + msg.collection_name + "'");
				}
				
				msg.mtype = db_ipc::MANAGER_SERVER_STRUCT_DISPOSAL_PRIOR;
				msgsnd(mq_descriptor, &msg, db_ipc::MANAGER_SERVER_MSG_SIZE, 0);
				break;
			}
			case db_ipc::command::ADD:
			{
				try
				{
					db->add(msg.pool_name, msg.schema_name, msg.collection_name, msg.login, tvalue(msg.karma, msg.name));
				}
				catch (db_storage::setup_failure const &)
				{
                    logger->error(log_start + "Failed to insert key '" + msg.login + "'");
					msg.status = db_ipc::command_status::FAILED_TO_INSERT_KEY;
				}
				catch (db_storage::invalid_struct_name_exception const &)
				{
                    logger->error(log_start + "Failed to insert key '" + msg.login + "' due to invalid struct name");
					msg.status = db_ipc::command_status::INVALID_STRUCT_NAME;
				}
				catch (db_storage::invalid_path_exception const &)
				{
                    logger->error(log_start + "Failed to insert key '" + msg.login + "' due to invalid path");
					msg.status = db_ipc::command_status::INVALID_PATH;
				}
				catch (db_storage::insertion_of_existent_key_attempt_exception const &)
				{
                    logger->error(log_start + "Attempt to insert existent key '" + msg.login + "'");
					msg.status = db_ipc::command_status::ATTEMPT_TO_INSERT_EXISTENT_KEY;
				}
				catch (std::bad_alloc const &)
				{
                    logger->error(log_start + "Failed to insert key '" + msg.login + "' due to bad alloc");
					msg.status = db_ipc::command_status::BAD_ALLOC;
				}
				catch (std::ios::failure const &)
				{
                    logger->error(log_start + "Failed to insert key '" + msg.login + "' due to filesystem failure");
					msg.status = db_ipc::command_status::FAILED_TO_INSERT_KEY;
				}
				
				if (msg.status == db_ipc::command_status::OK)
				{
                    logger->information(log_start + "Added key '" + msg.login + "' in collection '" +
							msg.pool_name + '/' + msg.schema_name + '/' + msg.collection_name + "'");
				}
				
				msgsnd(mq_descriptor, &msg, db_ipc::MANAGER_SERVER_MSG_SIZE, 0);
				break;
			}
			case db_ipc::command::UPDATE:
			{
				try
				{
					db->update(msg.pool_name, msg.schema_name, msg.collection_name, msg.login, tvalue(msg.karma, msg.name));
				}
				catch (db_storage::setup_failure const &)
				{
                    logger->error(log_start + "Failed to update key '" + msg.login + "'");
					msg.status = db_ipc::command_status::FAILED_TO_UPDATE_KEY;
				}
				catch (db_storage::invalid_struct_name_exception const &)
				{
                    logger->error(log_start + "Failed to update key '" + msg.login + "' due to invalid struct name");
					msg.status = db_ipc::command_status::INVALID_STRUCT_NAME;
				}
				catch (db_storage::invalid_path_exception const &)
				{
                    logger->error(log_start + "Failed to update key '" + msg.login + "' due to invalid path");
					msg.status = db_ipc::command_status::INVALID_PATH;
				}
				catch (db_storage::updating_of_nonexistent_key_attempt_exception const &)
				{
                    logger->error(log_start + "Attempt to update non-existent key '" + msg.login + "'");
					msg.status = db_ipc::command_status::ATTEMPT_TO_UPDATE_NONEXISTENT_KEY;
				}
				catch (std::bad_alloc const &)
				{
                    logger->error(log_start + "Failed to update key '" + msg.login + "' due to bad alloc");
					msg.status = db_ipc::command_status::BAD_ALLOC;
				}
				catch (std::ios::failure const &)
				{
                    logger->error(log_start + "Failed to update key '" + msg.login + "' due to filesystem failure");
					msg.status = db_ipc::command_status::FAILED_TO_UPDATE_KEY;
				}
				
				if (msg.status == db_ipc::command_status::OK)
				{
                    logger->information(log_start + "Updated key '" + msg.login + "' in collection '" +
							msg.pool_name + '/' + msg.schema_name + '/' + msg.collection_name + "'");
				}
				
				msgsnd(mq_descriptor, &msg, db_ipc::MANAGER_SERVER_MSG_SIZE, 0);
				break;
			}
			case db_ipc::command::DISPOSE:
			{
				try
				{
					tvalue value = db->dispose(msg.pool_name, msg.schema_name, msg.collection_name, msg.login);
					msg.karma = value.karma;
					strcpy(msg.name, value.name.c_str());
				}
				catch (db_storage::setup_failure const &)
				{
                    logger->error(log_start + "Failed to dispose key '" + msg.login + "'");
					msg.status = db_ipc::command_status::FAILED_TO_DISPOSE_KEY;
				}
				catch (db_storage::invalid_struct_name_exception const &)
				{
                    logger->error(log_start + "Failed to dispose key '" + msg.login + "' due to invalid struct name");
					msg.status = db_ipc::command_status::INVALID_STRUCT_NAME;
				}
				catch (db_storage::invalid_path_exception const &)
				{
                    logger->error(log_start + "Failed to dispose key '" + msg.login + "' due to invalid path");
					msg.status = db_ipc::command_status::INVALID_PATH;
				}
				catch (db_storage::disposal_of_nonexistent_key_attempt_exception const &)
				{
                    logger->error(log_start + "Attempt to dispose non-existent key '" + msg.login + "'");
					msg.status = db_ipc::command_status::ATTEMPT_TO_DISPOSE_NONEXISTENT_KEY;
				}
				catch (std::ios::failure const &)
				{
                    logger->error(log_start + "Failed to dispose key '" + msg.login + "' due to filesystem failure");
					msg.status = db_ipc::command_status::FAILED_TO_DISPOSE_KEY;
				}
				
				if (msg.status == db_ipc::command_status::OK)
				{
                    logger->information(log_start + "Disposed key '" + msg.login + "' in collection '" +
							msg.pool_name + '/' + msg.schema_name + '/' + msg.collection_name + "'");
				}
				
				msgsnd(mq_descriptor, &msg, db_ipc::MANAGER_SERVER_MSG_SIZE, 0);
				break;
			}
			case db_ipc::command::OBTAIN:
			{
				try
				{
					tvalue value = db->obtain(msg.pool_name, msg.schema_name, msg.collection_name, msg.login);
					msg.karma = value.karma;
					strcpy(msg.name, value.name.c_str());
				}
				catch (db_storage::setup_failure const &)
				{
                    logger->error(log_start + "Failed to obtain key '" + msg.login + "'");
					msg.status = db_ipc::command_status::FAILED_TO_OBTAIN_KEY;
				}
				catch (db_storage::invalid_struct_name_exception const &)
				{
                    logger->error(log_start + "Failed to obtain key '" + msg.login + "' due to invalid struct name");
					msg.status = db_ipc::command_status::INVALID_STRUCT_NAME;
				}
				catch (db_storage::invalid_path_exception const &)
				{
                    logger->error(log_start + "Failed to obtain key '" + msg.login + "' due to invalid path");
					msg.status = db_ipc::command_status::INVALID_PATH;
				}
				catch (db_storage::obtaining_of_nonexistent_key_attempt_exception const &)
				{
                    logger->error(log_start + "Attempt to obtain non-existent key '" + msg.login + "'");
					msg.status = db_ipc::command_status::ATTTEMT_TO_OBTAIN_NONEXISTENT_KEY;
				}
				catch (std::ios::failure const &)
				{
                    logger->error(log_start + "Failed to obtain key '" + msg.login + "' due to filesystem failure");
					msg.status = db_ipc::command_status::FAILED_TO_OBTAIN_KEY;
				}
				
				if (msg.status == db_ipc::command_status::OK)
				{
                    logger->information(log_start + "Obtained key '" + msg.login + "' in collection '" +
							msg.pool_name + '/' + msg.schema_name + '/' + msg.collection_name + "'");
				}
				
				msgsnd(mq_descriptor, &msg, db_ipc::MANAGER_SERVER_MSG_SIZE, 0);
				break;
			}
			case db_ipc::command::OBTAIN_BETWEEN:
			{
				std::string keys = std::string("('") + msg.login + "','" + msg.right_boundary_login + "')";
				
				std::vector<std::pair<tkey, tvalue>> range;
				try
				{
					range = db->obtain_between(msg.pool_name, msg.schema_name, msg.collection_name, msg.login, msg.right_boundary_login, true, true);
				}
				catch (db_storage::setup_failure const &)
				{
                    logger->error(log_start + "Failed to obtain between keys " + keys);
					msg.status = db_ipc::command_status::FAILED_TO_OBTAIN_KEY;
				}
				catch (db_storage::invalid_struct_name_exception const &)
				{
                    logger->error(log_start + "Failed to obtain between keys " + keys + " due to invalid struct name");
					msg.status = db_ipc::command_status::INVALID_STRUCT_NAME;
				}
				catch (db_storage::invalid_path_exception const &)
				{
                    logger->error(log_start + "Failed to obtain between keys " + keys + " due to invalid path");
					msg.status = db_ipc::command_status::INVALID_PATH;
				}
				catch (std::ios::failure const &)
				{
                    logger->error(log_start + "Failed to obtain between keys " + keys + " due to filesystem failure");
					msg.status = db_ipc::command_status::FAILED_TO_OBTAIN_KEY;
				}
				
				if (msg.status == db_ipc::command_status::OK)
				{
                    logger->information(log_start + "Obtained between keys " + keys + " in collection '" +
							msg.pool_name + '/' + msg.schema_name + '/' + msg.collection_name + "'");
				}
				
				if (range.empty())
				{
					msg.status = db_ipc::command_status::ATTTEMT_TO_OBTAIN_NONEXISTENT_KEY;
					msgsnd(mq_descriptor, &msg, db_ipc::MANAGER_SERVER_MSG_SIZE, 0);
				}
				for (int i = 0; i < range.size(); ++i)
                {
                    if (i == range.size() - 1)
                    {
						usleep(200);
						//msg.mtype = 9; // TODO CHECK
                        msg.status = db_ipc::command_status::OBTAIN_BETWEEN_END;
                    }
                    strcpy(msg.login, range[i].first.c_str());
                    msg.karma = range[i].second.karma;
                    strcpy(msg.name, range[i].second.name.c_str());
					
                    msgsnd(mq_descriptor, &msg, db_ipc::MANAGER_SERVER_MSG_SIZE, 0);
                }
				
				break;
			}
			case db_ipc::command::OBTAIN_MIN:
            {
                try
                {
					std::pair<tkey, tvalue> kvp = db->obtain_min(msg.pool_name, msg.schema_name, msg.collection_name);
					strcpy(msg.login, kvp.first.c_str());
					msg.karma = kvp.second.karma;
					strcpy(msg.name, kvp.second.name.c_str());
                }
				catch (db_storage::setup_failure const &)
				{
                    logger->error(log_start + "Failed to obtain min key");
					msg.status = db_ipc::command_status::FAILED_TO_OBTAIN_KEY;
				}
				catch (db_storage::invalid_struct_name_exception const &)
				{
                    logger->error(log_start + "Failed to obtain min key due to invalid struct name");
					msg.status = db_ipc::command_status::INVALID_STRUCT_NAME;
				}
				catch (db_storage::invalid_path_exception const &)
				{
                    logger->error(log_start + "Failed to obtain min key due to invalid path");
					msg.status = db_ipc::command_status::INVALID_PATH;
				}
				catch (db_storage::obtaining_of_nonexistent_key_attempt_exception const &)
				{
                    logger->error(log_start + "Attempt to obtain non-existent min key");
					msg.status = db_ipc::command_status::ATTTEMT_TO_OBTAIN_NONEXISTENT_KEY;
				}
				catch (std::ios::failure const &)
				{
                    logger->error(log_start + "Failed to obtain min key due to filesystem failure");
					msg.status = db_ipc::command_status::FAILED_TO_OBTAIN_KEY;
				}
				
				if (msg.status == db_ipc::command_status::OK)
				{
                    logger->information(log_start + "Obtained min key '" + msg.login + "' in collection '" +
							msg.pool_name + '/' + msg.schema_name + '/' + msg.collection_name + "'");
				}
				
                msgsnd(mq_descriptor, &msg, db_ipc::MANAGER_SERVER_MSG_SIZE, 0);
                break;
            }
			case db_ipc::command::OBTAIN_MAX:
            {
                try
                {
					std::pair<tkey, tvalue> kvp = db->obtain_max(msg.pool_name, msg.schema_name, msg.collection_name);
					strcpy(msg.login, kvp.first.c_str());
					msg.karma = kvp.second.karma;
					strcpy(msg.name, kvp.second.name.c_str());
                }
				catch (db_storage::setup_failure const &)
				{
                    logger->error(log_start + "Failed to obtain max key");
					msg.status = db_ipc::command_status::FAILED_TO_OBTAIN_KEY;
				}
				catch (db_storage::invalid_struct_name_exception const &)
				{
                    logger->error(log_start + "Failed to obtain max key due to invalid struct name");
					msg.status = db_ipc::command_status::INVALID_STRUCT_NAME;
				}
				catch (db_storage::invalid_path_exception const &)
				{
                    logger->error(log_start + "Failed to obtain max key due to invalid path");
					msg.status = db_ipc::command_status::INVALID_PATH;
				}
				catch (db_storage::obtaining_of_nonexistent_key_attempt_exception const &)
				{
                    logger->error(log_start + "Attempt to obtain non-existent max key");
					msg.status = db_ipc::command_status::ATTTEMT_TO_OBTAIN_NONEXISTENT_KEY;
				}
				catch (std::ios::failure const &)
				{
                    logger->error(log_start + "Failed to obtain max key due to filesystem failure");
					msg.status = db_ipc::command_status::FAILED_TO_OBTAIN_KEY;
				}
				
				if (msg.status == db_ipc::command_status::OK)
				{
                    logger->information(log_start + "Obtained max key '" + msg.login + "' in collection '" +
							msg.pool_name + '/' + msg.schema_name + '/' + msg.collection_name + "'");
				}
				
                msgsnd(mq_descriptor, &msg, db_ipc::MANAGER_SERVER_MSG_SIZE, 0);
                break;
            }
			case db_ipc::command::OBTAIN_NEXT:
            {
				std::string prev_key = msg.login;
				
                try
                {
					std::pair<tkey, tvalue> kvp = db->obtain_next(msg.pool_name, msg.schema_name, msg.collection_name, msg.login);
					strcpy(msg.login, kvp.first.c_str());
					msg.karma = kvp.second.karma;
					strcpy(msg.name, kvp.second.name.c_str());
                }
				catch (db_storage::setup_failure const &)
				{
                    logger->error(log_start + "Failed to obtain next of key '" + msg.login + "'");
					msg.status = db_ipc::command_status::FAILED_TO_OBTAIN_KEY;
				}
				catch (db_storage::invalid_struct_name_exception const &)
				{
                    logger->error(log_start + "Failed to obtain next of key '" + msg.login + "' due to invalid struct name");
					msg.status = db_ipc::command_status::INVALID_STRUCT_NAME;
				}
				catch (db_storage::invalid_path_exception const &)
				{
                    logger->error(log_start + "Failed to obtain next of key '" + msg.login + "' due to invalid path");
					msg.status = db_ipc::command_status::INVALID_PATH;
				}
				catch (db_storage::obtaining_of_nonexistent_key_attempt_exception const &)
				{
                    logger->error(log_start + "Attempt to obtain next of non-existent key '" + msg.login + "'");
					msg.status = db_ipc::command_status::ATTTEMT_TO_OBTAIN_NONEXISTENT_KEY;
				}
				catch (std::ios::failure const &)
				{
                    logger->error(log_start + "Failed to obtain next of key '" + msg.login + "' due to filesystem failure");
					msg.status = db_ipc::command_status::FAILED_TO_OBTAIN_KEY;
				}
				
				if (msg.status == db_ipc::command_status::OK)
				{
                    logger->information(log_start + "Obtained key '" + msg.login + "' next of key '" + prev_key + "' in collection '" +
							msg.pool_name + '/' + msg.schema_name + '/' + msg.collection_name + "'");
				}
				
                msgsnd(mq_descriptor, &msg, db_ipc::MANAGER_SERVER_MSG_SIZE, 0);
                break;
            }
			default:
				break;
		}
    }
    
	db->consolidate();
	delete logger;
}
