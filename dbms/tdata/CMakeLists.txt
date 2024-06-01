cmake_minimum_required(VERSION 3.21)
project(os_cw_dbms_tdata)

add_library(
        os_cw_dbms_tdata
        src/tdata.cpp)
target_include_directories(
        os_cw_dbms_tdata
        PUBLIC
        ./include)
target_link_libraries(
        os_cw_dbms_tdata
        PUBLIC
        os_cw_cmmn)
target_link_libraries(
        os_cw_dbms_tdata
        PUBLIC
        os_cw_allctr_allctr)
set_target_properties(
        os_cw_dbms_tdata PROPERTIES
        LANGUAGES CXX
        CXX_STANDARD 17
        CXX_STANDARD_REQUIRED ON
        CXX_EXTENSIONS OFF
        VERSION 1.0
        DESCRIPTION "tdata library")