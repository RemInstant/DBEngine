#include <iostream>
#include <fstream>
#include <string>
#include <cstring>
#include <vector>
#include <map>
#include <thread>
#include <mutex>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/msg.h>
#include <sys/wait.h>

#include <extra_utility.h>
#include <ipc_data.h>
#include <b_tree.h>

int run_flag = 1;
int mq_descriptor = -1;
std::mutex mtx;

void run_terminal_reader(
    std::map<int, pid_t> *strg_servers);


pid_t run_storage_server(
    size_t strg_server_id);

void redistribute_keys();


void handle_add_server_command(
    bool is_filesystem,
    std::map<int, pid_t> &strg_servers,
    std::vector<std::string> &separators,
    db_ipc::strg_msg_t &msg);

void handle_add_struct_command(
    bool is_filesystem,
    std::map<int, pid_t> const &strg_servers,
    std::vector<std::string> const &separators,
    db_ipc::strg_msg_t &msg,
    std::initializer_list<const char*> structs);

void handle_struct_management_command(
    std::map<int, pid_t> const &strg_servers,
    std::vector<std::string> const &separators,
    db_ipc::strg_msg_t &msg);

void handle_data_command(
    std::map<int, pid_t> const &strg_servers,
    std::vector<std::string> const &separators,
    db_ipc::strg_msg_t &msg);

int main()
{
    msgctl(msgget(db_ipc::STORAGE_SERVER_MQ_KEY, 0666), IPC_RMID, nullptr);
    
    bool is_filesystem = true;
    // std::set<std::pair<std::string, std::set<std::pair<std::string, std::set<std::string>>>>> structure;
    
    db_ipc::strg_msg_t msg;
    std::map<int, pid_t> strg_servers;
    std::vector<std::string> separators;
    
    std::thread cmd_thread(run_terminal_reader, &strg_servers);
    
    mq_descriptor = msgget(db_ipc::STORAGE_SERVER_MQ_KEY, IPC_CREAT | 0666);
    // if (mq_descriptor == -1)
    // {
    //     std::cout << "Cannot create the queue. Shut down." << std::endl;
    //     return 1;
    // }
    
    ssize_t rcv = -1;
    do
    {
        rcv = msgrcv(mq_descriptor, &msg, db_ipc::STORAGE_SERVER_MSG_SIZE, 0, IPC_NOWAIT);
    } while (rcv != -1);
    
    msg.mtype = 1;
    msg.cmd = db_ipc::command::ADD_STORAGE_SERVER;
    msg.extra_value = 1;
    msgsnd(mq_descriptor, &msg, db_ipc::STORAGE_SERVER_MSG_SIZE, MSG_NOERROR);
    
    while (run_flag)
    {
        ssize_t rcv_cnt = msgrcv(mq_descriptor, &msg, db_ipc::STORAGE_SERVER_MSG_SIZE, -db_ipc::STORAGE_SERVER_MAX_COMMON_PRIOR, MSG_NOERROR);
        
        // mtx.lock();
        // mtx.unlock();
        
        if (rcv_cnt == -1)
        {
            std::cout << "An error occured while receiving the message" << std::endl;
            break;
        }
        
        std::cout << "MNGR (" << getpid() << ") read from " << msg.pid << std::endl;
        
        msg.mtype = msg.pid;
        
        switch (msg.cmd)
        {
            case db_ipc::command::ADD_STORAGE_SERVER:
            {
                //std::lock_guard<std::mutex> guard(mtx);
                handle_add_server_command(is_filesystem, strg_servers, separators, msg);
                break;
            }
            case db_ipc::command::ADD_POOL:
            {
                handle_add_struct_command(is_filesystem, strg_servers, separators, msg, {msg.pool_name});
                break;
            }
            case db_ipc::command::ADD_SCHEMA:
            {
                handle_add_struct_command(is_filesystem, strg_servers, separators, msg, {msg.pool_name, msg.schema_name});
                break;
            }
            case db_ipc::command::ADD_COLLECTION:
            {
                handle_add_struct_command(is_filesystem, strg_servers, separators, msg, {msg.pool_name, msg.schema_name, msg.collection_name});
                break;
            }
            case db_ipc::command::DISPOSE_POOL:
            case db_ipc::command::DISPOSE_SCHEMA:
            case db_ipc::command::DISPOSE_COLLECTION:
            {
                handle_struct_management_command(strg_servers, separators, msg);
                break;
            }
            case db_ipc::command::ADD:
            case db_ipc::command::UPDATE:
            case db_ipc::command::DISPOSE:
            case db_ipc::command::OBTAIN:
            case db_ipc::command::OBTAIN_BETWEEN:
            {
                handle_data_command(strg_servers, separators, msg);
                break;
            }
            
            case db_ipc::command::SET_IN_MEMORY_CACHE_MODE:
            {
                // if (is_setup)
                // {
                // 	msg.status = db_ipc::command_status::ATTEMPT_TO_CHANGE_SETUP;
                // }
                // else
                // {
                // 	db->set_mode(db_storage::mode::in_memory_cache);
                // }
                msgsnd(mq_descriptor, &msg, db_ipc::STORAGE_SERVER_MSG_SIZE, msg.pid);
                break;
            }
            default:
                break;
        }
    }
    
    msgctl(mq_descriptor, IPC_RMID, nullptr);
    
    std::cout << "Server shutdowned" << std::endl;
    
    cmd_thread.detach();
}



void run_terminal_reader(
    std::map<int, pid_t> *strg_servers)
{
    db_ipc::strg_msg_t msg;
    std::string cmd;
    
    while (std::cin >> cmd)
    {
        if (cmd == "shutdown")
        {
            run_flag = 0;
            msg.cmd = db_ipc::command::SHUTDOWN;
            
            for (auto iter = strg_servers->begin(); iter != strg_servers->end(); ++iter)
            {
                msg.mtype = iter->second;
                msgsnd(mq_descriptor, &msg, db_ipc::STORAGE_SERVER_MSG_SIZE, 0);
            }
            
            while(wait(NULL) > 0);
            
            msg.mtype = 1;
            msgsnd(mq_descriptor, &msg, db_ipc::STORAGE_SERVER_MSG_SIZE, 0);
            
            break;
        }
    }
}


void handle_add_server_command(
    bool is_filesystem,
    std::map<int, pid_t> &strg_servers,
    std::vector<std::string> &separators,
    db_ipc::strg_msg_t &msg)
{
    size_t id = strg_servers.size() + 1;
    
    pid_t pid = run_storage_server(id);
    
    if (pid == -1)
    {
        std::cout << "Failed to add storage server" << std::endl;
        return;
    }
    
    msg.mtype = pid;
    msg.cmd = is_filesystem ? db_ipc::command::SET_FILE_SYSTEM_MODE : db_ipc::command::SET_IN_MEMORY_CACHE_MODE;
    msg.extra_value = id;
    
    msgsnd(mq_descriptor, &msg, db_ipc::STORAGE_SERVER_MSG_SIZE, MSG_NOERROR);
    
    ssize_t rcv_cnt = -1;
    size_t counter = 0;
    
    do
    {
        sleep(counter);
        rcv_cnt = msgrcv(mq_descriptor, &msg, db_ipc::STORAGE_SERVER_MSG_SIZE, db_ipc::STORAGE_SERVER_STORAGE_ADDITION_PRIOR, MSG_NOERROR || IPC_NOWAIT);
    } while (++counter < 5 && rcv_cnt == -1);
    
    if (rcv_cnt == -1 || msg.status == db_ipc::command_status::FAILED_TO_SETUP_STORAGE_SERVER)
    {
        kill(pid, SIGKILL);
        std::cout << "Failed to add storage server" << std::endl;
        return;
    }
    
    
    
    
    if (id > 1)
    {
        // TODO
    }
    
    strg_servers[id] = pid;
}


pid_t run_storage_server(
    size_t strg_server_id)
{
    pid_t pid = fork();
    
    if (pid == 0)
    {
        int code = execl("../db_storage/server/os_cw_dbms_db_strg_srvr", std::to_string(strg_server_id).c_str(), NULL);
        
        if (code == -1)
        {
            exit(1);
        }
    }
    
    if (pid == -1)
    {
        std::cout << "fork error" << std::endl;
        // todo throw;
        return -1;
    }
    
    sleep(1);
    
    if (waitpid(pid, NULL, WNOHANG) == 0)
    {
        return pid;
    }
    
    return -1;
}


void redistribute_keys(
    //bool is_filesystem,
    std::map<int, pid_t> const &strg_servers,
    std::vector<std::string> &separators)
{
    db_ipc::strg_msg_t msg;
    
    for (size_t i = 0; i < strg_servers.size(); ++i)
    {
        
    }
    
    
    msgsnd(mq_descriptor, &msg, db_ipc::STORAGE_SERVER_MSG_SIZE, MSG_NOERROR);
}

void handle_add_struct_command(
    bool is_filesystem,
    std::map<int, pid_t> const &strg_servers,
    std::vector<std::string> const &separators,
    db_ipc::strg_msg_t &msg,
    std::initializer_list<const char*> structs)
{
    if (msg.status == db_ipc::command_status::CLIENT)
    {
        if (strg_servers.empty())
        {
            msg.mtype = msg.pid;
            msg.status = db_ipc::command_status::FAILED_TO_ADD_STRUCT;
            msgsnd(mq_descriptor, &msg, db_ipc::STORAGE_SERVER_MSG_SIZE, MSG_NOERROR);
        }
        else if (separators.empty())
        {
            msg.mtype = strg_servers.at(1);
            msgsnd(mq_descriptor, &msg, db_ipc::STORAGE_SERVER_MSG_SIZE, MSG_NOERROR);
        }
        else
        {
            // TODO;
        }
    }
    else
    {
        const char *cpath = nullptr;
        // if (is_filesystem && msg.status == db_ipc::command_status::OK && 
        //         access(cpath = extra_utility::make_path(structs).c_str(), F_OK) == -1)
        // {
        //     mkdir(cpath, 0777);
            
        //     std::string path(cpath);
            
        //     if (structs.size() == 3)
        //     {
                
        //         for (size_t i = 0; i < strg_servers.size(); ++i)
        //         {
        //             std::string strg_path = extra_utility::make_path( {path, std::to_string(i + 1)} );
                    
        //             int descriptor = open(strg_path.c_str(), O_CREAT);
        //             close(descriptor);
        //         }
        //     }
        // }
        
        msg.mtype = msg.pid;
        msgsnd(mq_descriptor, &msg, db_ipc::STORAGE_SERVER_MSG_SIZE, MSG_NOERROR);
    }
}

void handle_struct_management_command(
    std::map<int, pid_t> const &strg_servers,
    std::vector<std::string> const &separators,
    db_ipc::strg_msg_t &msg)
{
    if (msg.status == db_ipc::command_status::CLIENT)
    {
        if (strg_servers.empty())
        {
            msg.mtype = msg.pid;
            msg.status = db_ipc::command_status::STRUCT_DOES_NOT_EXIST;
            msgsnd(mq_descriptor, &msg, db_ipc::STORAGE_SERVER_MSG_SIZE, MSG_NOERROR);
        }
        else if (separators.empty())
        {
            msg.mtype = strg_servers.at(1);
            msgsnd(mq_descriptor, &msg, db_ipc::STORAGE_SERVER_MSG_SIZE, MSG_NOERROR);
        }
        else
        {
            // TODO;
        }
    }
    else
    {
        const char *cpath = nullptr;
        msg.mtype = msg.pid;
        msgsnd(mq_descriptor, &msg, db_ipc::STORAGE_SERVER_MSG_SIZE, MSG_NOERROR);
    }
}

void handle_data_command(
    std::map<int, pid_t> const &strg_servers,
    std::vector<std::string> const &separators,
    db_ipc::strg_msg_t &msg)
{
    if (msg.status == db_ipc::command_status::CLIENT)
    {
        if (strg_servers.empty())
        {
            msg.mtype = msg.pid;
            msg.status = db_ipc::command_status::FAILED_TO_PERFORM_DATA_COMMAND;
            msgsnd(mq_descriptor, &msg, db_ipc::STORAGE_SERVER_MSG_SIZE, MSG_NOERROR);
        }
        else if (separators.empty())
        {
            msg.mtype = strg_servers.at(1);
            msgsnd(mq_descriptor, &msg, db_ipc::STORAGE_SERVER_MSG_SIZE, MSG_NOERROR);
        }
        else
        {
            // TODO;
        }
    }
    else
    {
        msg.mtype = msg.pid;
        msgsnd(mq_descriptor, &msg, db_ipc::STORAGE_SERVER_MSG_SIZE, MSG_NOERROR);
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