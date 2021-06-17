cmake_minimum_required(VERSION 3.15)

project(bcos-dispatcher-test)

hunter_add_package(Microsoft.GSL)
hunter_add_package(tbb)
hunter_add_package(Boost COMPONENTS all)

find_package(Microsoft.GSL CONFIG REQUIRED)
find_package(Boost CONFIG REQUIRED unit_test_framework thread)
find_package(tbb CONFIG REQUIRED)

add_executable(testDispatcher testDispatcher.cpp main.cpp)
target_compile_options(testDispatcher PRIVATE -Wno-error -Wno-unused-parameter -Wno-variadic-macros -Wno-return-type -Wno-pedantic)
# add_dependencies(dispatcher bcostars bcos-framework bcos-front)
target_link_libraries(testDispatcher dispatcher libcodecd.a libprotocold.a Microsoft.GSL::GSL Boost::unit_test_framework tbb::tbb Boost::thread)