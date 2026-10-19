cmake_minimum_required(VERSION 3.20)

project(raft LANGUAGES CXX)

include(CTest)

add_library(raft src/raft.cpp)
target_compile_features(raft PUBLIC cxx_std_17)
target_include_directories(raft PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/src")

if(BUILD_TESTING)
    find_package(PkgConfig QUIET)
    if(PkgConfig_FOUND)
        pkg_check_modules(GTest QUIET IMPORTED_TARGET gtest_main)
    endif()

    add_executable(raft_test tests/raft_test.cpp)
    if(TARGET PkgConfig::GTest)
        target_link_libraries(raft_test PRIVATE raft PkgConfig::GTest)
    else()
        find_package(GTest CONFIG REQUIRED)
        target_link_libraries(raft_test PRIVATE raft GTest::gtest_main)
    endif()

    include(GoogleTest)
    gtest_discover_tests(raft_test)
endif()
