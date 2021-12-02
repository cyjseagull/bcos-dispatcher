file(GLOB SRC_LIST "*.cpp")
file(GLOB HEADERS "*.h")

add_library(scheduler SchedulerImpl.cpp ExecutorManager.cpp BlockExecutive.cpp GraphKeyLocks.cpp)
target_link_libraries(scheduler bcos-framework::utilities)