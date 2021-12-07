cmake_minimum_required(VERSION 3.15)

set (URL_BASE "github.com")
set (DEPENDENCIES_DIR ${CMAKE_CURRENT_BINARY_DIR})

include(FetchContent)
set(BCOS_CMAKE_SCRIPTS_DIR ${CMAKE_CURRENT_BINARY_DIR}/bcos-cmake-scripts)
FetchContent_Declare(bcos-cmake-scripts
    GIT_REPOSITORY https://${URL_BASE}/FISCO-BCOS/bcos-cmake-scripts.git
    GIT_TAG 9b2ac5b979cbc97f640b2285feb2c74a18cfb935
    SOURCE_DIR ${BCOS_CMAKE_SCRIPTS_DIR}
)
FetchContent_MakeAvailable(bcos-cmake-scripts)
list(APPEND CMAKE_MODULE_PATH ${BCOS_CMAKE_SCRIPTS_DIR})