cmake_minimum_required(VERSION 3.14)
project(alfred)

include(FetchContent)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

SET(CMAKE_CXX_FLAGS "-g")

# -lboost_log
set(Boost_USE_STATIC_LIBS OFF)
set(Boost_USE_MULTITHREADED ON)
set(Boost_USE_STATIC_RUNTIME OFF)
set(THREADS_PREFER_PTHREAD_FLAG ON)

# -DBOOST_LOG_DYN_LINK
# definitions
add_definitions(-DBOOST_ALL_NO_LIB)
add_definitions(-DBOOST_LOG_DYN_LINK=1)

# -lboost_log -lboost_thread -lboost_log_setup
find_package(Boost 1.74.0 COMPONENTS thread log log_setup REQUIRED)
find_package(PkgConfig)

# Check libraries of loki: pkg-config --libs loki-cpp
# Result: -L/usr/local/lib -lpthread -lprotobuf -lsnappy -lfmt -lcurl -lloki-cpp
#pkg_check_modules(LOKI-CPP REQUIRED IMPORTED_TARGET loki-cpp)
pkg_check_modules(PROTOBUF REQUIRED IMPORTED_TARGET protobuf)
pkg_check_modules(SNAPPY REQUIRED IMPORTED_TARGET snappy)
pkg_check_modules(CURLPP REQUIRED IMPORTED_TARGET curlpp)
pkg_check_modules(LIBCURL REQUIRED IMPORTED_TARGET libcurl)

#find_package(Boost 1.74.0 COMPONENTS log_setup REQUIRED)
# git clone https://github.com/redis/hiredis.git
# cd hiredis; mkdir -p build; cd build; cmake ..; make; sudo make install
# find_package(hiredis)
# find_package(redis++)

# lcurlp
#FetchContent_Declare(curlpp
#        GIT_REPOSITORY https://github.com/jpbarrette/curlpp.git
#        GIT_TAG master
#        )

# -lfmt
#FetchContent_Declare(fmt
#  GIT_REPOSITORY https://github.com/fmtlib/fmt.git
#  GIT_TAG master
#)

#loki-cpp, dependencies: fmt, curl protobuf + snappy (optional)
#FetchContent_Declare(loki-cpp
#        GIT_REPOSITORY https://github.com/mircodezorzi/loki-cpp.git
#        GIT_TAG master
#        )
# -lhiredis
#FetchContent_Declare(hiredis
#        GIT_REPOSITORY https://github.com/redis/hiredis.git
#        GIT_TAG master
#        )
# -lhiredis
#FetchContent_Declare(redis++
#        GIT_REPOSITORY https://github.com/sewenew/redis-plus-plus
#        GIT_TAG master
#        )

      #FetchContent_Declare(re2
      #        GIT_REPOSITORY https://github.com/google/re2.git
      #        GIT_TAG main
      #        )
      #FetchContent_Declare(c-ares
      #        GIT_REPOSITORY https://github.com/c-ares/c-ares
      #        GIT_TAG main
      #        )
# -lSimpleAmqpClient
#FetchContent_Declare(SimpleAmqpClient
#        GIT_REPOSITORY https://github.com/alanxz/SimpleAmqpClient
#        GIT_TAG master
#        )
#
#FetchContent_MakeAvailable(fmt hiredis redis++ SimpleAmqpClient curlpp)

#set(Parquet_DIR /usr/lib/x86_64-linux-gnu/cmake/arrow)
find_package(Parquet REQUIRED)

# -lpthread
find_package(Threads REQUIRED)
# -larrow
find_package(Arrow CONFIG REQUIRED)
# -lparquet
find_package(Parquet CONFIG REQUIRED)
# sudo apt-get install uuid-dev
# -laws-cpp-sdk-core
find_package(aws-cpp-sdk-core CONFIG REQUIRED)
# -laws-cpp-sdk-s3
find_package(aws-cpp-sdk-s3 CONFIG REQUIRED)
find_package(Protobuf MODULE REQUIRED)


file(GLOB_RECURSE SRC_FILES src/alfred.cpp )#/home/gtoscano/django/api4opt4/optimization/misc/src/misc2.cpp)
list(REMOVE_ITEM SRC_FILES ${CMAKE_BINARY_DIR}/CMakeFiles/3.18.4/CompilerIdCXX/CMakeCXXCompilerId.cpp)
add_executable(alfred ${SRC_FILES})
target_include_directories(alfred PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include )#/home/gtoscano/django/api4opt4/optimization/misc/include)
target_link_libraries(alfred PUBLIC Boost::log Boost::log_setup hiredis redis++ fmt arrow_shared parquet aws-cpp-sdk-core aws-cpp-sdk-s3 SimpleAmqpClient Threads::Threads PkgConfig::LIBCURL)
#PkgConfig::PROTOBUF PkgConfig::SNAPPY)

install(TARGETS alfred RUNTIME DESTINATION /usr/local/bin)