file(GLOB_RECURSE SOURCES "*.cpp" "*.h" "*.sol")

# cmake settings
include(SearchTestCases)
set(TEST_BINARY_NAME bcos-dispatcher-test)
config_test_cases("" "${SOURCES}" bin/${TEST_BINARY_NAME} "")

add_executable(${TEST_BINARY_NAME} ${SOURCES})
target_include_directories(${TEST_BINARY_NAME} PRIVATE . ${CMAKE_SOURCE_DIR})

hunter_add_package(wedpr-crypto)
find_package(wedpr-crypto CONFIG QUIET REQUIRED)
find_package(Boost CONFIG QUIET REQUIRED unit_test_framework)

target_link_libraries(${TEST_BINARY_NAME} ${BCOS_DISPATCHER_TARGET} bcos-framework::codec bcos-framework::protocol bcos-framework::protocol-pb wedpr-crypto::crypto Boost::unit_test_framework)