cmake_minimum_required (VERSION 2.6)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=gnu++0x")



project (cacheserver)
add_executable(cacheserver main.cc cacheserver.cc log.cc threadpool.cc objworker.cc objserver.cc objclient.cc epollobjserver epollworker)

target_link_libraries(cacheserver pthread)
target_link_libraries(cacheserver boost_thread)
target_link_libraries(cacheserver boost_system)
target_link_libraries(cacheserver rt)




project (master)
add_executable(master master.cc masterworker.cc log.cc masterregistry.cc readerwriterlock.cc epollmasterworker.cc)

target_link_libraries(master pthread)
target_link_libraries(master boost_thread)
target_link_libraries(master boost_system)
target_link_libraries(master tbb)



project (objserver)
add_executable(objserver objservermain.cc objworker.cc objserver.cc log.cc)

target_link_libraries(objserver pthread)
target_link_libraries(objserver boost_thread)
target_link_libraries(objserver boost_system)
target_link_libraries(objserver rt)

