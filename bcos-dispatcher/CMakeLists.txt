cmake_minimum_required(VERSION 3.15)

project(bcos-dispatcher)

hunter_add_package(Microsoft.GSL)
hunter_add_package(tbb)
hunter_add_package(Boost COMPONENTS all)

find_package(Microsoft.GSL CONFIG REQUIRED)
find_package(Boost CONFIG REQUIRED log thread chrono system filesystem serialization)
find_package(tbb CONFIG REQUIRED)

add_library(dispatcher DispatcherImpl.cpp)
# target_compile_options(dispatcher PRIVATE -Wno-error -Wno-unused-parameter -Wno-variadic-macros -Wno-return-type -Wno-pedantic)
# add_dependencies(dispatcher bcostars bcos-framework bcos-front)
target_link_libraries(dispatcher Microsoft.GSL::GSL tbb::tbb)