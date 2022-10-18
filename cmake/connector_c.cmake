INCLUDE(FindGit)

IF(NOT EXISTS ${CMAKE_SOURCE_DIR}/libobclient/CMakeLists.txt AND GIT_EXECUTABLE)
  EXECUTE_PROCESS(COMMAND "${GIT_EXECUTABLE}" submodule init
                  WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}")
  EXECUTE_PROCESS(COMMAND "${GIT_EXECUTABLE}" submodule update
                  WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}")
ENDIF()
IF(NOT EXISTS ${CMAKE_SOURCE_DIR}/libobclient/libmariadb/CMakeLists.txt)
  MESSAGE(FATAL_ERROR "No MariaDB Connector/C! Run
    git submodule init
    git submodule update
Then restart the build.
")
ENDIF()

SET(OPT CONC_)

IF (CMAKE_BUILD_TYPE STREQUAL "Debug")
  SET(CONC_WITH_RTC ON)
ENDIF()

SET(CONC_WITH_SIGNCODE ${SIGNCODE})
SET(SIGN_OPTIONS ${SIGNTOOL_PARAMETERS})

IF(TARGET zlib)
  GET_PROPERTY(ZLIB_LIBRARY_LOCATION TARGET zlib PROPERTY LOCATION)
ELSE()
  SET(ZLIB_LIBRARY_LOCATION ${ZLIB_LIBRARY})
ENDIF()

SET(CONC_WITH_CURL OFF)
SET(CONC_WITH_MYSQLCOMPAT ON)

IF (INSTALL_LAYOUT STREQUAL "RPM")
  SET(CONC_INSTALL_LAYOUT "RPM")
ELSE()
  SET(CONC_INSTALL_LAYOUT "DEFAULT")
ENDIF()

SET(PLUGIN_INSTALL_DIR ${INSTALL_PLUGINDIR})
SET(MARIADB_UNIX_ADDR ${MYSQL_UNIX_ADDR})

MESSAGE("== Configuring MariaDB Connector/C")
ADD_DEFINITIONS(-DWIN32_LEAN_AND_MEAN)
ADD_SUBDIRECTORY(libobclient)