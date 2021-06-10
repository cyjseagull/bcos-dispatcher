cmake_minimum_required(VERSION 3.15)

set (URL_BASE "github.com.cnpmjs.org")
set (DEPENDENCIES_DIR ${CMAKE_CURRENT_BINARY_DIR})

include(FetchContent)
FetchContent_Declare(bcos-cmake-scripts
    GIT_REPOSITORY https://${URL_BASE}/FISCO-BCOS/bcos-cmake-scripts.git
    GIT_TAG dev
    SOURCE_DIR ${DEPENDENCIES_DIR}/bcos-cmake-scripts
)
FetchContent_MakeAvailable(bcos-cmake-scripts)
list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_BINARY_DIR}/bcos-cmake-scripts)

include(ExternalProject)
ExternalProject_Add(bcos-framework
    GIT_REPOSITORY https://${URL_BASE}/FISCO-BCOS/bcos-framework.git
    GIT_TAG dev
    SOURCE_DIR ${DEPENDENCIES_DIR}/bcos-framework
    CMAKE_ARGS -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} -DURL_BASE=${URL_BASE} -DCMAKE_INSTALL_PREFIX=${DEPENDENCIES_DIR}/bcos-framework-install
)
include_directories(${DEPENDENCIES_DIR}/bcos-framework-install/include)
link_directories(${DEPENDENCIES_DIR}/bcos-framework-install/lib)