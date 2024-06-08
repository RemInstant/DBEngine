cmake_minimum_required(VERSION 3.21)
project(os_cw_str_pool)

add_library(
        os_cw_str_pool
        src/os_cw_str_pool.cpp)
target_include_directories(
        os_cw_dbms_db_strg
        PUBLIC
        ./include)
set_target_properties(
        os_cw_dbms_db_strg PROPERTIES
        LANGUAGES CXX
        CXX_STANDARD 17
        CXX_STANDARD_REQUIRED ON
        CXX_EXTENSIONS OFF
        VERSION 1.0
        DESCRIPTION "database storage library")