cmake_minimum_required(VERSION 3.15)

# import dependencies
include (Dependencies.cmake)
include(HunterGate)
HunterGate(
    URL "https://${URL_BASE}/FISCO-BCOS/hunter/archive/7268240de829ba351c98d574db9dd7dfddb1ad2e.tar.gz"
    SHA1 "29a725d3fac8b23f255530b7e40ffb47b34973f9"
    FILEPATH "${CMAKE_SOURCE_DIR}/hunterConfig.cmake"
)
project(Bcos-Dispatcher VERSION "3.0.0")
set(VERSION_SUFFIX "")

# basic settings
include(Options)
configure_project()
include(CompilerSettings)

include_directories(${CMAKE_CURRENT_SOURCE_DIR})

# install dependencies
include(InstallBcosFrameworkDependencies)

set(BCOS_DISPATCHER_TARGET "dispatcher")
add_subdirectory(bcos-dispatcher)

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

if(TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()