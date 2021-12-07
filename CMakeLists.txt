cmake_minimum_required(VERSION 3.15)

# import dependencies
include (Dependencies.cmake)
include(HunterGate)
HunterGate(
    URL "https://${URL_BASE}/FISCO-BCOS/hunter/archive/203039fdd0826330facd9c3841082dbf35d0c808.tar.gz"
    SHA1 "26ca1840eb26da410913469f2f278569c8d383ae"
    FILEPATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake/config.cmake"
)
project(bcos-scheduler VERSION "3.0.0")
set(VERSION_SUFFIX "-rc1")

# basic settings
include(Options)
configure_project()
include(CompilerSettings)
# copy .clang-format into the project path
include(CopyClangFormat)

include_directories(${CMAKE_CURRENT_SOURCE_DIR})

# install dependencies
include(InstallBcosFrameworkDependencies)

set(BCOS_DISPATCHER_TARGET "scheduler")
add_subdirectory(bcos-scheduler)

# install bcos-dispatcher target
include(InstallConfig)
install(
    TARGETS ${BCOS_DISPATCHER_TARGET}
    EXPORT "${TARGETS_EXPORT_NAME}"
    LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
    INCLUDES DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
)

set(DESTINATION_INCLUDE_DIR "${CMAKE_INSTALL_INCLUDEDIR}")
install(
    DIRECTORY "bcos-dispatcher"
    DESTINATION "${DESTINATION_INCLUDE_DIR}"
    FILES_MATCHING PATTERN "*.h"
)

install(
    DIRECTORY "bcos-scheduler"
    DESTINATION "${DESTINATION_INCLUDE_DIR}"
    FILES_MATCHING PATTERN "*.h"
)

if(TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

if (COVERAGE)
    include(Coverage)
    config_coverage("coverage" "'/usr*' '${CMAKE_CURRENT_SOURCE_DIR}/bcos-cmake-scripts*' '${CMAKE_CURRENT_SOURCE_DIR}/test/bcos-test*'")
endif ()
