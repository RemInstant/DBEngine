#include <iostream>
#include <string.h>
#include <unistd.h>
#include <sys/msg.h>
#include <wait.h>

#include <ipc_data.h>

// server_logger::server_logger(std::map<std::string, std::set<severity>> const &log_dest):
//     _configuration(log_dest)
// {
//     #ifdef __linux__
//     _mq_descriptor = msgget(LINUX_MSG_QUEUE_KEY, 0666);
	
// 	if (_mq_descriptor == -1)
// 	{
//         throw std::runtime_error("Cannot connect to the server");
// 	}
//     #endif
    
//     #ifdef _WIN32
//     _hFile = CreateFile(WIN32_MAILSLOT_NAME, GENERIC_WRITE, FILE_SHARE_READ,
//             (LPSECURITY_ATTRIBUTES) NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, (HANDLE) NULL);
    
//     if (_hFile == INVALID_HANDLE_VALUE)
//     {
//         throw std::runtime_error("Cannot connect to the server");
//     }
//     #endif
// }

// logger const *server_logger::log(
//     const std::string &text,
//     logger::severity severity) const noexcept
// {
//     for (auto record : _configuration)
//     {
//         const std::string &file_path = record.first;
//         const auto &severities = record.second;
        
//         if (severities.count(severity))
//         {
//             mutex.lock();
            
//             msg_t msg;
//             msg.mtype = LOG_PRIOR;
//             msg.pid = getpid();
//             msg.packet_cnt = std::ceil(1.0 * text.size() / MAX_MSG_TEXT_SIZE);
//             msg.severity = static_cast<int>(severity);
            
//             strcpy(msg.file_path, file_path.c_str());
            
//             for (size_t i = 0; i < msg.packet_cnt; ++i)
//             {
//                 msg.packet_id = i + 1;
//                 strcpy(msg.mtext, text.substr(i * MAX_MSG_TEXT_SIZE, MAX_MSG_TEXT_SIZE).c_str());
                
//                 #ifdef __linux__
// 	            msgsnd(_mq_descriptor, &msg, sizeof(msg_t), 0);
//                 #endif
                
//                 #ifdef _WIN32
//                 WriteFile(_hFile, &msg, sizeof(msg_t), (LPDWORD) NULL, (LPOVERLAPPED) NULL);
//                 #endif
//             }
            
//             mutex.unlock();
//         }
//     }
    
//     return this;
// }




int main()
{
	pid_t pid = getpid();
	
	while (getpid() <= db_ipc::STORAGE_SERVER_MAX_COMMAND_PRIOR)
	{
		std::cout << "!";
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
	
	int mq_descriptor = msgget(db_ipc::STORAGE_SERVER_MQ_KEY, 0666);
	
	if (mq_descriptor == -1)
	{
		std::cout << "Cannot connect to the server" << std::endl;
		return 2;
	}
	
	db_ipc::strg_msg_t msg;
	
	// long prior;
	// pid_t pid;
	// command cmd;
	// command_status status;
	// size_t records_cnt;
	
	// // path
	
	// char pool_name[STORAGE_MSG_STRUCTS_NAME_SIZE];
	// char schema_name[STORAGE_MSG_STRUCTS_NAME_SIZE];
	// char collection_name[STORAGE_MSG_STRUCTS_NAME_SIZE];
	
	// // data
	
	// char tree_variant;
	// char allocator_variant;
	// size_t t_for_b_trees;
	
	// char login[STORAGE_MSG_KEY_SIZE];
	// int64_t hashed_password;
	// char name[STORAGE_MSG_NAME_SIZE];
	
	msg.mtype = 10;
	msg.pid = pid;
	msg.status = db_ipc::command_status::OK;
	msg.tree_variant = 0; // b
	msg.t_for_b_trees = 8;
	msg.allocator_variant = 3; //bndr
	
	
	int code;
	msg.cmd = db_ipc::command::SET_IN_MEMORY_CACHE_MODE;
	
	// code = msgsnd(mq_descriptor, &msg, db_ipc::STORAGE_SERVER_MSG_SIZE, 0);
	// if (code == -1)
	// {
	// 	std::cout << "ww" << std::endl;
	// 	return 2;
	// }
	
	// code = msgrcv(mq_descriptor, &msg, db_ipc::STORAGE_SERVER_MSG_SIZE, pid, 0);
	// if (code == -1)
	// {
	// 	std::cout << "ww" << std::endl;
	// 	return 2;
	// }
	// if (msg.status != db_ipc::command_status::OK)
	// {
	// 	std::cout << "another ww" << std::endl;
	// 	return 2;
	// }
	
	
	strcpy(msg.pool_name, "pool");
	strcpy(msg.schema_name, "schema");
	strcpy(msg.collection_name, "collection");
	
	strcpy(msg.login, "aboba52");
	msg.hashed_password = 124;
	strcpy(msg.name, "Aboba Abobich");
	
	msg.cmd = db_ipc::command::ADD_POOL;
	
	code = msgsnd(mq_descriptor, &msg, db_ipc::STORAGE_SERVER_MSG_SIZE, 0);
	if (code == -1)
	{
		std::cout << "ww" << std::endl;
		return 2;
	}
	
	msg.status = db_ipc::command_status::OK;
	code = msgrcv(mq_descriptor, &msg, db_ipc::STORAGE_SERVER_MSG_SIZE, pid, 0);
	if (code == -1)
	{
		std::cout << "ww" << std::endl;
		return 2;
	}
	if (msg.status != db_ipc::command_status::OK)
	{
		std::cout << "another 1ww:" << static_cast<int>(msg.status) << std::endl;
	}
	
	msg.cmd = db_ipc::command::ADD_SCHEMA;
	
	msg.status = db_ipc::command_status::OK;
	msg.mtype = 10;
	code = msgsnd(mq_descriptor, &msg, db_ipc::STORAGE_SERVER_MSG_SIZE, 0);
	if (code == -1)
	{
		std::cout << "ww" << std::endl;
		return 2;
	}
	
	msg.status = db_ipc::command_status::OK;
	code = msgrcv(mq_descriptor, &msg, db_ipc::STORAGE_SERVER_MSG_SIZE, pid, 0);
	if (code == -1)
	{
		std::cout << "ww" << std::endl;
		return 2;
	}
	if (msg.status != db_ipc::command_status::OK)
	{
		std::cout << "another 2ww:" << static_cast<int>(msg.status) << std::endl;
	}
	
	msg.cmd = db_ipc::command::ADD_COLLECTION;
	
	msg.mtype = 10;
	code = msgsnd(mq_descriptor, &msg, db_ipc::STORAGE_SERVER_MSG_SIZE, 0);
	if (code == -1)
	{
		std::cout << "ww" << std::endl;
		return 2;
	}
	
	msg.status = db_ipc::command_status::OK;
	code = msgrcv(mq_descriptor, &msg, db_ipc::STORAGE_SERVER_MSG_SIZE, pid, 0);
	if (code == -1)
	{
		std::cout << "ww" << std::endl;
		return 2;
	}
	if (msg.status != db_ipc::command_status::OK)
	{
		std::cout << "another 3ww:" << static_cast<int>(msg.status) << std::endl;
	}
	
	msg.cmd = db_ipc::command::ADD;
	
	msg.mtype = 10;
	code = msgsnd(mq_descriptor, &msg, db_ipc::STORAGE_SERVER_MSG_SIZE, 0);
	if (code == -1)
	{
		std::cout << "ww" << std::endl;
		return 2;
	}
	
	msg.status = db_ipc::command_status::OK;
	code = msgrcv(mq_descriptor, &msg, db_ipc::STORAGE_SERVER_MSG_SIZE, pid, 0);
	if (code == -1)
	{
		std::cout << "ww" << std::endl;
		return 2;
	}
	if (msg.status != db_ipc::command_status::OK)
	{
		std::cout << "another 4ww:" << static_cast<int>(msg.status) << std::endl;
	}
	
	msg.cmd = db_ipc::command::OBTAIN;
	
	msg.mtype = 10;
	code = msgsnd(mq_descriptor, &msg, db_ipc::STORAGE_SERVER_MSG_SIZE, 0);
	if (code == -1)
	{
		std::cout << "ww" << std::endl;
		return 2;
	}
	
	msg.login[0] = '\0';
	
	msg.status = db_ipc::command_status::OK;
	code = msgrcv(mq_descriptor, &msg, db_ipc::STORAGE_SERVER_MSG_SIZE, pid, 0);
	if (code == -1)
	{
		std::cout << "ww" << std::endl;
		return 2;
	}
	if (msg.status != db_ipc::command_status::OK)
	{
		std::cout << "another 5ww:" << static_cast<int>(msg.status) << std::endl;
	}
	
	std::cout << msg.login << std::endl;
	
	
}