file(GLOB SRC_LIST "*.cpp")
file(GLOB HEADERS "*.h")

add_library(scheduler SchedulerImpl.cpp ExecutorManager.cpp BlockExecutive.cpp KeyLocks.cpp)
target_link_libraries(scheduler bcos-framework::utilities)