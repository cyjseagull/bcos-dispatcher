cmake_minimum_required(VERSION 3.15)

# import dependencies
include (Dependencies.cmake)

include(HunterGate)
HunterGate(
    URL "https://${URL_BASE}/FISCO-BCOS/hunter/archive/7268240de829ba351c98d574db9dd7dfddb1ad2e.tar.gz"
    SHA1 "29a725d3fac8b23f255530b7e40ffb47b34973f9"
    FILEPATH "${CMAKE_SOURCE_DIR}/hunterConfig.cmake"
)

project(Bcos-Dispatcher)

include(CompilerSettings)

add_subdirectory(bcos-dispatcher)

if(TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()