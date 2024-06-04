#include <iostream>
#include <fstream>
#include <string>
#include <cstring>
#include <map>
#include <thread>
#include <unistd.h>
#include <sys/msg.h>
#include <sys/wait.h>

#include <ipc_data.h>

int run_flag = 1;
int mq_descriptor = -1;

void terminal_reader();

pid_t run_storage_server();

int main()
{
	db_ipc::strg_msg_t msg;
	std::map<int, pid_t> strg_servers;
	
    mq_descriptor = msgget(db_ipc::STORAGE_SERVER_MQ_KEY, IPC_CREAT | 0666);
    if (mq_descriptor == -1)
    {
        std::cout << "Cannot create the queue. Shut down." << std::endl;
        return 1;
    }
    
	// db->set_mode(db_storage::mode::in_memory_cache);
	
    // std::thread cmd_thread(terminal_reader);
    
    // while (run_flag)
    // {
    //     ssize_t rcv_cnt = msgrcv(mq_descriptor, &msg, db_ipc::STORAGE_SERVER_MSG_SIZE, -db_ipc::STORAGE_SERVER_MAX_COMMAND_PRIOR, MSG_NOERROR);
    //     if (rcv_cnt == -1)
    //     {
    //         std::cout << "An error occured while receiving the message" << std::endl;
    //         break;
    //     }
		
	// 	std::cout << "read from " << msg.pid << std::endl;
		
	// 	msg.status = db_ipc::command_status::OK;
	// 	msg.mtype = msg.pid;
        
	// 	switch (msg.cmd)
	// 	{
	// 		case db_ipc::command::SET_IN_MEMORY_CACHE_MODE:
	// 		{
	// 			// if (is_setup)
	// 			// {
	// 			// 	msg.status = db_ipc::command_status::ATTEMPT_TO_CHANGE_SETUP;
	// 			// }
	// 			// else
	// 			// {
	// 			// 	db->set_mode(db_storage::mode::in_memory_cache);
	// 			// }
	// 			msgsnd(mq_descriptor, &msg, db_ipc::STORAGE_SERVER_MSG_SIZE, msg.pid);
	// 			break;
	// 		}
	// 		case db_ipc::command::SET_FILE_SYSTEM_MODE:
	// 		{
	// 			// if (is_setup)
	// 			// {
	// 			// 	msg.status = db_ipc::command_status::ATTEMPT_TO_CHANGE_SETUP;
	// 			// }
	// 			// else
	// 			// {
	// 			// 	db->set_mode(db_storage::mode::file_system);
	// 			// }
	// 			msgsnd(mq_descriptor, &msg, db_ipc::STORAGE_SERVER_MSG_SIZE, msg.pid);
	// 			break;
	// 		}
	// 		case db_ipc::command::END_SETUP:
	// 		{
	// 			is_setup = true;
	// 		}
	// 		case db_ipc::command::SHUTDOWN:
	// 		{
	// 			run_flag = 0;
	// 			break;
	// 		}
	// 		case db_ipc::command::GET_RECORDS_CNT:
	// 		{
	// 			msg.records_cnt = db->get_records_cnt();
	// 			msgsnd(mq_descriptor, &msg, db_ipc::STORAGE_SERVER_MSG_SIZE, msg.pid);
	// 			break;
	// 		}
	// 		case db_ipc::command::ADD_POOL:
	// 		{
	// 			try
	// 			{
	// 				db->add_pool(msg.pool_name, static_cast<db_storage::search_tree_variant>(msg.tree_variant), msg.t_for_b_trees);
	// 			}
	// 			catch (db_storage::invalid_struct_name_exception const &)
	// 			{
	// 				msg.status = db_ipc::command_status::INVALID_STRUCT_NAME;
	// 			}
	// 			catch (db_storage::too_big_struct_name_exception const &)
	// 			{
	// 				msg.status = db_ipc::command_status::TOO_BIG_STRUCT_NAME;
	// 			}
	// 			catch (db_storage::insertion_of_existent_struct_attempt_exception const &)
	// 			{
	// 				msg.status = db_ipc::command_status::ATTEMPT_TO_ADD_POOL_DUBLICATE;
	// 			}
	// 			catch (std::bad_alloc const &)
	// 			{
	// 				msg.status = db_ipc::command_status::BAD_ALLOC;
	// 			}
				
	// 			msgsnd(mq_descriptor, &msg, db_ipc::STORAGE_SERVER_MSG_SIZE, msg.pid);
	// 			break;
	// 		}
	// 		case db_ipc::command::ADD_SCHEMA:
	// 		{
	// 			try
	// 			{
	// 				db->add_schema(msg.pool_name, msg.schema_name,
	// 						static_cast<db_storage::search_tree_variant>(msg.tree_variant), msg.t_for_b_trees);
	// 			}
	// 			catch (db_storage::invalid_struct_name_exception const &)
	// 			{
	// 				msg.status = db_ipc::command_status::INVALID_STRUCT_NAME;
	// 			}
	// 			catch (db_storage::too_big_struct_name_exception const &)
	// 			{
	// 				msg.status = db_ipc::command_status::TOO_BIG_STRUCT_NAME;
	// 			}
	// 			catch (db_storage::invalid_path_exception const &)
	// 			{
	// 				msg.status = db_ipc::command_status::INVALID_PATH;
	// 			}
	// 			catch (db_storage::insertion_of_existent_struct_attempt_exception const &)
	// 			{
	// 				msg.status = db_ipc::command_status::ATTEMPT_TO_ADD_SCHEMA_DUBLICATE;
	// 			}
	// 			catch (std::bad_alloc const &)
	// 			{
	// 				msg.status = db_ipc::command_status::BAD_ALLOC;
	// 			}
				
	// 			msgsnd(mq_descriptor, &msg, db_ipc::STORAGE_SERVER_MSG_SIZE, msg.pid);
	// 			break;
	// 		}
	// 		case db_ipc::command::ADD_COLLECTION:
	// 		{
	// 			try
	// 			{
	// 				db->add_collection(msg.pool_name, msg.schema_name, msg.collection_name,
	// 						static_cast<db_storage::search_tree_variant>(msg.tree_variant),
	// 						static_cast<db_storage::allocator_variant>(msg.alloc_variant),
	// 						msg.t_for_b_trees);
	// 			}
	// 			catch (db_storage::invalid_struct_name_exception const &)
	// 			{
	// 				msg.status = db_ipc::command_status::INVALID_STRUCT_NAME;
	// 			}
	// 			catch (db_storage::too_big_struct_name_exception const &)
	// 			{
	// 				msg.status = db_ipc::command_status::TOO_BIG_STRUCT_NAME;
	// 			}
	// 			catch (db_storage::invalid_path_exception const &)
	// 			{
	// 				msg.status = db_ipc::command_status::INVALID_PATH;
	// 			}
	// 			catch (db_storage::insertion_of_existent_struct_attempt_exception const &)
	// 			{
	// 				msg.status = db_ipc::command_status::ATTEMPT_TO_ADD_COLLECTION_DUBLICATE;
	// 			}
	// 			catch (std::bad_alloc const &)
	// 			{
	// 				msg.status = db_ipc::command_status::BAD_ALLOC;
	// 			}
				
	// 			msgsnd(mq_descriptor, &msg, db_ipc::STORAGE_SERVER_MSG_SIZE, msg.pid);
	// 			break;
	// 		}
	// 		case db_ipc::command::DISPOSE_POOL:
	// 			break;
	// 		case db_ipc::command::DISPOSE_SCHEMA:
	// 			break;
	// 		case db_ipc::command::DISPOSE_COLLECTION:
	// 			break;
	// 		case db_ipc::command::ADD:
	// 		{
	// 			try
	// 			{
	// 				db->add(msg.pool_name, msg.schema_name, msg.collection_name, msg.login, tvalue(msg.hashed_password, msg.name));
	// 			}
	// 			catch (db_storage::invalid_struct_name_exception const &)
	// 			{
	// 				msg.status = db_ipc::command_status::INVALID_STRUCT_NAME;
	// 			}
	// 			catch (db_storage::invalid_path_exception const &)
	// 			{
	// 				msg.status = db_ipc::command_status::INVALID_PATH;
	// 			}
	// 			catch (db_storage::insertion_of_existent_key_attempt_exception const &)
	// 			{
	// 				msg.status = db_ipc::command_status::ATTEMPT_TO_INSERT_EXISTENT_KEY;
	// 			}
	// 			catch (std::bad_alloc const &)
	// 			{
	// 				msg.status = db_ipc::command_status::BAD_ALLOC;
	// 			}
				
	// 			msgsnd(mq_descriptor, &msg, db_ipc::STORAGE_SERVER_MSG_SIZE, msg.pid);
	// 			break;
	// 		}
	// 		case db_ipc::command::UPDATE:
	// 			break;
	// 		case db_ipc::command::DISPOSE:
	// 			break;
	// 		case db_ipc::command::OBTAIN:
	// 		{	
	// 			tvalue value;
				
	// 			try
	// 			{
	// 				value = db->obtain(msg.pool_name, msg.schema_name, msg.collection_name, msg.login);
	// 			}
	// 			catch (db_storage::invalid_struct_name_exception const &)
	// 			{
	// 				msg.status = db_ipc::command_status::INVALID_STRUCT_NAME;
	// 			}
	// 			catch (db_storage::invalid_path_exception const &)
	// 			{
	// 				msg.status = db_ipc::command_status::INVALID_PATH;
	// 			}
	// 			catch (db_storage::obtaining_of_nonexistent_key_attempt_exception const &)
	// 			{
	// 				msg.status = db_ipc::command_status::ATTTEMT_TO_OBTAIN_NONEXISTENT_KEY;
	// 			}
				
	// 			msg.hashed_password = value.hashed_password;
	// 			strcpy(msg.name, value.name.c_str());
				
	// 			msgsnd(mq_descriptor, &msg, db_ipc::STORAGE_SERVER_MSG_SIZE, msg.pid);
	// 			break;
	// 		}
	// 		case db_ipc::command::OBTAIN_BETWEEN:
	// 			break;
			
	// 		default:
	// 			break;
	// 	}
    // }
    

    msgctl(mq_descriptor, IPC_RMID, nullptr);
    
    std::cout << "Server shutdowned" << std::endl;
    
    //cmd_thread.detach();
}

pid_t run_storage_server(
	size_t strg_server_id)
{
	pid_t pid = fork();
	
	if (pid == 0)
	{
		int code = execl("../db_storage/server/db_storage_server", std::to_string(strg_server_id).c_str(), NULL);
		
		if (code == -1)
		{
			exit(1);
		}
	}
	
	if (pid == -1)
	{
		std::cout << "fork error" << std::endl;
		// todo throw;
		return;
	}
	
	
	
	
}



void terminal_reader()
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




// int main()
// {
// 	tkey key1 = "aboba62";
// 	tvalue value1(0, "Aboba abobich");
	
// 	tkey key2 = "aboba12";
// 	tvalue value2(0, "Aboba boo");
	
// 	tkey key3 = "aboba82";
// 	tvalue value3(0, "Aboba bee");
	
	
// 	auto vec = db_storage::get_instance()
// 				->set_mode(db_storage::mode::in_memory_cache)
// 				->add_pool("pool", db_storage::search_tree_variant::b)
// 				->add_schema("pool", "schema", db_storage::search_tree_variant::b)
// 				->add_collection("pool", "schema", "collection", db_storage::search_tree_variant::b, db_storage::allocator_variant::boundary_tags)
// 				->add("pool", "schema", "collection", key1, value1)
// 				->add("pool", "schema", "collection", key2, value2)
// 				->add("pool", "schema", "collection", key3, value3)
// 				->obtain_between("pool", "schema", "collection", "", "zzzzzzzzzzzzzzzzzz", 1, 1);
	
// 	for (auto v : vec)
// 	{
// 		std::cout << v.key << ' ' << v.value.hashed_password << ' ' << v.value.name << std::endl;
// 	}
	
// 	return 0;
// }