## g++ main.cpp -lcassandra -o build/connect && ./build/connect

cmake_minimum_required(VERSION 3.16)
project(ScyllaDB_Samples)

set(CMAKE_CXX_STANDARD 17)

include_directories(/usr/local/lib/)
include_directories(/usr/local/include/)

add_executable(ScyllaDB_Samples main.cpp)
