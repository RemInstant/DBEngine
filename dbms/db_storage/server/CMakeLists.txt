cmake_minimum_required(VERSION 3.21)
project(os_cw_dbms_db_strg_srvr)

add_executable(
        os_cw_dbms_db_strg_srvr
        db_storage_server.cpp)
target_include_directories(
        os_cw_dbms_db_strg_srvr
        PUBLIC
        ./include)
target_link_libraries(
        os_cw_dbms_db_strg_srvr
        PUBLIC
        os_cw_dbms_db_strg)
target_link_libraries(
        os_cw_dbms_db_strg_srvr
        PUBLIC
        os_cw_dbms_tdata)
set_target_properties(
        os_cw_dbms_db_strg_srvr PROPERTIES
        LANGUAGES CXX
        CXX_STANDARD 17
        CXX_STANDARD_REQUIRED ON
        CXX_EXTENSIONS OFF
        VERSION 1.0
        DESCRIPTION "database storage server")