#include <iostream>
#include <fstream>
#include <string>
#include <cstring>
#include <map>
#include <thread>
#include <sys/msg.h>
#include <sys/wait.h>

#include <tdata.h>
#include <db_storage.h>
#include <ipc_data.h>

int run_flag = 1;
int mq_descriptor = -1;

void run_terminal_reader();

#include "sys/stat.h"

int main()
{
	pid_t pid = getpid();
	
	while (getpid() <= db_ipc::STORAGE_SERVER_MAX_COMMAND_PRIOR)
	{
		switch (pid = fork())
		{
			case -1:
				std::cout << "An error occurred while starting client process" << std::endl;
				return 1;
			case 0:
				break;
			default:
				waitpid(pid, NULL, 0);
				return 0;
		}
	}
	
	pid = getpid();
	
	db_ipc::strg_msg_t msg;
	db_storage *db = db_storage::get_instance();
	bool is_setup = false;
	
    mq_descriptor = msgget(db_ipc::STORAGE_SERVER_MQ_KEY, 0666);
    if (mq_descriptor == -1)
    {
        std::cout << "Cannot create the queue. Shut down." << std::endl;
        return 1;
    }
    
	// db->setup(1, db_storage::mode::in_memory_cache);
	// db->setup(1, db_storage::mode::file_system);
	// db->load_db("pools");
	
	// db->add_pool("p", db_storage::search_tree_variant::b, 4);
	// db->add_schema("p", "s", db_storage::search_tree_variant::b, 4);
	// db->add_collection("p", "s", "c", db_storage::search_tree_variant::b, db_storage::allocator_variant::boundary_tags, 4);
	
	// db->add("p", "s", "c", "1", tvalue(1, "1"));
	// db->add("p", "s", "c", "2", tvalue(2, "2"));
	// db->add("p", "s", "c", "3", tvalue(3, "3"));
	// db->add("p", "s", "c", "4", tvalue(4, "4"));
	// db->add("p", "s", "c", "5", tvalue(5, "5"));
	
	// db->dispose("p", "s", "c", "2");
	// db->dispose("p", "s", "c", "4");
	
    //std::thread cmd_thread(run_terminal_reader);
	
    while (run_flag)
    {
        ssize_t rcv_cnt = msgrcv(mq_descriptor, &msg, db_ipc::STORAGE_SERVER_MSG_SIZE, pid, MSG_NOERROR);
        if (rcv_cnt == -1)
        {
            std::cout << "An error occured while receiving the message" << std::endl;
            break;
        }
		
		std::cout << "read from " << msg.pid << std::endl;
		
		msg.mtype = 9;
		msg.status = db_ipc::command_status::OK;
        
		switch (msg.cmd)
		{
			case db_ipc::command::SET_IN_MEMORY_CACHE_MODE:
			{
				if (is_setup)
				{
					msg.status = db_ipc::command_status::ATTEMPT_TO_CHANGE_SETUP;
				}
				else
				{
					db->setup(msg.extra_value, db_storage::mode::in_memory_cache);
				}
				msg.mtype = db_ipc::STORAGE_SERVER_STORAGE_ADDITION_PRIOR;
				msgsnd(mq_descriptor, &msg, db_ipc::STORAGE_SERVER_MSG_SIZE, msg.pid);
				break;
			}
			case db_ipc::command::SET_FILE_SYSTEM_MODE:
			{
				if (is_setup)
				{
					msg.status = db_ipc::command_status::ATTEMPT_TO_CHANGE_SETUP;
				}
				else
				{
					db->setup(msg.extra_value, db_storage::mode::file_system);
				}
				msgsnd(mq_descriptor, &msg, db_ipc::STORAGE_SERVER_MSG_SIZE, msg.pid);
				break;
			}
			case db_ipc::command::END_SETUP:
			{
				is_setup = true;
			}
			case db_ipc::command::SHUTDOWN:
			{
				run_flag = 0;
				break;
			}
			case db_ipc::command::GET_RECORDS_CNT:
			{
				msg.extra_value = db->get_records_cnt();
				msgsnd(mq_descriptor, &msg, db_ipc::STORAGE_SERVER_MSG_SIZE, msg.pid);
				break;
			}
			case db_ipc::command::ADD_POOL:
			{
				try
				{
					db->add_pool(msg.pool_name, static_cast<db_storage::search_tree_variant>(msg.tree_variant), msg.t_for_b_trees);
				}
				catch (db_storage::invalid_struct_name_exception const &)
				{
					msg.status = db_ipc::command_status::INVALID_STRUCT_NAME;
				}
				catch (db_storage::too_big_struct_name_exception const &)
				{
					msg.status = db_ipc::command_status::TOO_BIG_STRUCT_NAME;
				}
				catch (db_storage::insertion_of_existent_struct_attempt_exception const &)
				{
					msg.status = db_ipc::command_status::ATTEMPT_TO_ADD_POOL_DUBLICATE;
				}
				catch (std::bad_alloc const &)
				{
					msg.status = db_ipc::command_status::BAD_ALLOC;
				}
				
				msgsnd(mq_descriptor, &msg, db_ipc::STORAGE_SERVER_MSG_SIZE, msg.pid);
				break;
			}
			case db_ipc::command::ADD_SCHEMA:
			{
				try
				{
					db->add_schema(msg.pool_name, msg.schema_name,
							static_cast<db_storage::search_tree_variant>(msg.tree_variant), msg.t_for_b_trees);
				}
				catch (db_storage::invalid_struct_name_exception const &)
				{
					msg.status = db_ipc::command_status::INVALID_STRUCT_NAME;
				}
				catch (db_storage::too_big_struct_name_exception const &)
				{
					msg.status = db_ipc::command_status::TOO_BIG_STRUCT_NAME;
				}
				catch (db_storage::invalid_path_exception const &)
				{
					msg.status = db_ipc::command_status::INVALID_PATH;
				}
				catch (db_storage::insertion_of_existent_struct_attempt_exception const &)
				{
					msg.status = db_ipc::command_status::ATTEMPT_TO_ADD_SCHEMA_DUBLICATE;
				}
				catch (std::bad_alloc const &)
				{
					msg.status = db_ipc::command_status::BAD_ALLOC;
				}
				
				msgsnd(mq_descriptor, &msg, db_ipc::STORAGE_SERVER_MSG_SIZE, msg.pid);
				break;
			}
			case db_ipc::command::ADD_COLLECTION:
			{
				try
				{
					db->add_collection(msg.pool_name, msg.schema_name, msg.collection_name,
							static_cast<db_storage::search_tree_variant>(msg.tree_variant),
							static_cast<db_storage::allocator_variant>(msg.alloc_variant),
							msg.t_for_b_trees);
				}
				catch (db_storage::invalid_struct_name_exception const &)
				{
					msg.status = db_ipc::command_status::INVALID_STRUCT_NAME;
				}
				catch (db_storage::too_big_struct_name_exception const &)
				{
					msg.status = db_ipc::command_status::TOO_BIG_STRUCT_NAME;
				}
				catch (db_storage::invalid_path_exception const &)
				{
					msg.status = db_ipc::command_status::INVALID_PATH;
				}
				catch (db_storage::insertion_of_existent_struct_attempt_exception const &)
				{
					msg.status = db_ipc::command_status::ATTEMPT_TO_ADD_COLLECTION_DUBLICATE;
				}
				catch (std::bad_alloc const &)
				{
					msg.status = db_ipc::command_status::BAD_ALLOC;
				}
				
				msgsnd(mq_descriptor, &msg, db_ipc::STORAGE_SERVER_MSG_SIZE, msg.pid);
				break;
			}
			case db_ipc::command::DISPOSE_POOL:
			{
				try
				{
					db->dispose_pool(msg.pool_name);
				}
				catch (db_storage::invalid_path_exception const &)
				{
					msg.status = db_ipc::command_status::INVALID_PATH;
				}
				
				msgsnd(mq_descriptor, &msg, db_ipc::STORAGE_SERVER_MSG_SIZE, msg.pid);
				break;
			}
			case db_ipc::command::DISPOSE_SCHEMA:
			{
				try
				{
					db->dispose_schema(msg.pool_name, msg.schema_name);
				}
				catch (db_storage::invalid_path_exception const &)
				{
					msg.status = db_ipc::command_status::INVALID_PATH;
				}
				
				msgsnd(mq_descriptor, &msg, db_ipc::STORAGE_SERVER_MSG_SIZE, msg.pid);
				break;
			}
			case db_ipc::command::DISPOSE_COLLECTION:
			{
				try
				{
					db->dispose_collection(msg.pool_name, msg.schema_name, msg.collection_name);
				}
				catch (db_storage::invalid_path_exception const &)
				{
					msg.status = db_ipc::command_status::INVALID_PATH;
				}
				
				msgsnd(mq_descriptor, &msg, db_ipc::STORAGE_SERVER_MSG_SIZE, msg.pid);
				break;
			}
			case db_ipc::command::ADD:
			{
				try
				{
					db->add(msg.pool_name, msg.schema_name, msg.collection_name, msg.login, tvalue(msg.hashed_password, msg.name));
				}
				catch (db_storage::invalid_struct_name_exception const &)
				{
					msg.status = db_ipc::command_status::INVALID_STRUCT_NAME;
				}
				catch (db_storage::invalid_path_exception const &)
				{
					msg.status = db_ipc::command_status::INVALID_PATH;
				}
				catch (db_storage::insertion_of_existent_key_attempt_exception const &)
				{
					msg.status = db_ipc::command_status::ATTEMPT_TO_INSERT_EXISTENT_KEY;
				}
				catch (std::bad_alloc const &)
				{
					msg.status = db_ipc::command_status::BAD_ALLOC;
				}
				catch (std::ios::failure const &)
				{
					msg.status = db_ipc::command_status::FAILED_TO_INSERT_KEY;
				}
				
				msgsnd(mq_descriptor, &msg, db_ipc::STORAGE_SERVER_MSG_SIZE, msg.pid);
				break;
			}
			case db_ipc::command::UPDATE:
			{
				try
				{
					db->update(msg.pool_name, msg.schema_name, msg.collection_name, msg.login, tvalue(msg.hashed_password, msg.name));
				}
				catch (db_storage::invalid_struct_name_exception const &)
				{
					msg.status = db_ipc::command_status::INVALID_STRUCT_NAME;
				}
				catch (db_storage::invalid_path_exception const &)
				{
					msg.status = db_ipc::command_status::INVALID_PATH;
				}
				catch (db_storage::updating_of_nonexistent_key_attempt_exception const &)
				{
					msg.status = db_ipc::command_status::ATTEMPT_TO_UPDATE_NONEXISTENT_KEY;
				}
				catch (std::bad_alloc const &)
				{
					msg.status = db_ipc::command_status::BAD_ALLOC;
				}
				catch (std::ios::failure const &)
				{
					msg.status = db_ipc::command_status::FAILED_TO_UPDATE_KEY;
				}
				
				msgsnd(mq_descriptor, &msg, db_ipc::STORAGE_SERVER_MSG_SIZE, msg.pid);
				break;
			}
			case db_ipc::command::DISPOSE:
			{
				try
				{
					tvalue value = db->dispose(msg.pool_name, msg.schema_name, msg.collection_name, msg.login);
					msg.hashed_password = value.hashed_password;
					strcpy(msg.name, value.name.c_str());
				}
				catch (db_storage::invalid_struct_name_exception const &)
				{
					msg.status = db_ipc::command_status::INVALID_STRUCT_NAME;
				}
				catch (db_storage::invalid_path_exception const &)
				{
					msg.status = db_ipc::command_status::INVALID_PATH;
				}
				catch (db_storage::disposal_of_nonexistent_key_attempt_exception const &)
				{
					msg.status = db_ipc::command_status::ATTEMPT_TO_DISPOSE_NONEXISTENT_KEY;
				}
				catch (std::ios::failure const &)
				{
					// TODO
				}
				
				msgsnd(mq_descriptor, &msg, db_ipc::STORAGE_SERVER_MSG_SIZE, msg.pid);
				break;
			}
			case db_ipc::command::OBTAIN:
			{
				try
				{
					tvalue value = db->obtain(msg.pool_name, msg.schema_name, msg.collection_name, msg.login);
					msg.hashed_password = value.hashed_password;
					strcpy(msg.name, value.name.c_str());
				}
				catch (db_storage::invalid_struct_name_exception const &)
				{
					msg.status = db_ipc::command_status::INVALID_STRUCT_NAME;
				}
				catch (db_storage::invalid_path_exception const &)
				{
					msg.status = db_ipc::command_status::INVALID_PATH;
				}
				catch (db_storage::obtaining_of_nonexistent_key_attempt_exception const &)
				{
					msg.status = db_ipc::command_status::ATTTEMT_TO_OBTAIN_NONEXISTENT_KEY;
				}
				catch (std::ios::failure const &)
				{
					msg.status = db_ipc::command_status::FAILED_TO_OBTAIN_KEY;
				}
				
				msgsnd(mq_descriptor, &msg, db_ipc::STORAGE_SERVER_MSG_SIZE, msg.pid);
				break;
			}
			case db_ipc::command::OBTAIN_BETWEEN:
			{
				std::vector<std::pair<tkey, tvalue>> range;
				try
				{
					range = db->obtain_between(msg.pool_name, msg.schema_name, msg.collection_name, msg.login, msg.right_boundary_login, true, true);
				}
				catch (db_storage::invalid_struct_name_exception const &)
				{
					msg.status = db_ipc::command_status::INVALID_STRUCT_NAME;
				}
				catch (db_storage::invalid_path_exception const &)
				{
					msg.status = db_ipc::command_status::INVALID_PATH;
				}
				catch (db_storage::obtaining_of_nonexistent_key_attempt_exception const &)
				{
					msg.status = db_ipc::command_status::ATTTEMT_TO_OBTAIN_NONEXISTENT_KEY;
				}
				catch (std::ios::failure const &)
				{
					msg.status = db_ipc::command_status::FAILED_TO_OBTAIN_KEY;
				}
				
				// TODO
				msg.hashed_password = range[0].second.hashed_password;
				strcpy(msg.name, range[0].second.name.c_str());
				msgsnd(mq_descriptor, &msg, db_ipc::STORAGE_SERVER_MSG_SIZE, msg.pid);
				break;
			}
			
			default:
				break;
		}
    }
    
	db->consolidate();
    
    std::cout << "Storage server shutdowned" << std::endl;
    
    //cmd_thread.detach();
}



void run_terminal_reader()
{
    db_ipc::strg_msg_t msg;
    std::string cmd;
    
    while (std::cin >> cmd)
    {
        if (cmd == "shutdown")
        {
			msg.mtype = 1;
            msg.cmd = db_ipc::command::SHUTDOWN;
			
            msgsnd(mq_descriptor, &msg, db_ipc::STORAGE_SERVER_MSG_SIZE, 0);
            run_flag = 0;
            
            break;
        }
    }
}