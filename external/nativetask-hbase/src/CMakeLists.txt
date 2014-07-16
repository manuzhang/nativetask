#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

cmake_minimum_required(VERSION 2.6 FATAL_ERROR)

# Default to release builds
set(CMAKE_BUILD_TYPE, Release)


include(JNIFlags.cmake NO_POLICY_SCOPE)
find_package(JNI REQUIRED)

# Compile a library with both shared and static variants
function(add_dual_library LIBNAME)
    add_library(${LIBNAME} SHARED ${ARGN})
    add_library(${LIBNAME}_static STATIC ${ARGN})
    set_target_properties(${LIBNAME}_static PROPERTIES OUTPUT_NAME ${LIBNAME})
endfunction(add_dual_library)

# Link both a static and a dynamic target against some libraries
function(target_link_dual_libraries LIBNAME)
    target_link_libraries(${LIBNAME} ${ARGN})
    target_link_libraries(${LIBNAME}_static ${ARGN})
endfunction(target_link_dual_libraries)

function(output_directory TGT DIR)
    SET_TARGET_PROPERTIES(${TGT} PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/${DIR}")
    SET_TARGET_PROPERTIES(${TGT} PROPERTIES
        ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/${DIR}")
    SET_TARGET_PROPERTIES(${TGT} PROPERTIES
        LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/${DIR}")
endfunction(output_directory TGT DIR)

function(dual_output_directory TGT DIR)
    output_directory(${TGT} "${DIR}")
    output_directory(${TGT}_static "${DIR}")
endfunction(dual_output_directory TGT DIR)

#
# This macro alters the behavior of find_package and find_library.
# It does this by setting the CMAKE_FIND_LIBRARY_SUFFIXES global variable. 
# You should save that variable before calling this function and restore it
# after you have accomplished your goal.
#
# The behavior is altered in two ways:
# 1. We always find shared libraries, never static;
# 2. We find shared libraries with the given version number.
#
# On Windows this function is a no-op.  Windows does not encode
# version number information information into library path names.
#
macro(set_find_shared_library_version LVERS)
    IF(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
        # Mac OS uses .dylib
        SET(CMAKE_FIND_LIBRARY_SUFFIXES ".${LVERS}.dylib")
    ELSEIF(${CMAKE_SYSTEM_NAME} MATCHES "FreeBSD")
        # FreeBSD has always .so installed.
        SET(CMAKE_FIND_LIBRARY_SUFFIXES ".so")
    ELSEIF(${CMAKE_SYSTEM_NAME} MATCHES "Windows")
        # Windows doesn't support finding shared libraries by version.
    ELSE()
        # Most UNIX variants use .so
        SET(CMAKE_FIND_LIBRARY_SUFFIXES ".so.${LVERS}")
    ENDIF()
endmacro(set_find_shared_library_version LVERS)


SET(STORED_CMAKE_FIND_LIBRARY_SUFFIXES CMAKE_FIND_LIBRARY_SUFFIXES)
set_find_shared_library_version("1")
SET(CMAKE_FIND_LIBRARY_SUFFIXES STORED_CMAKE_FIND_LIBRARY_SUFFIXES)

# primitive configs
set(PRFLAGS "-DSIMPLE_MEMCPY")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${PRFLAGS} -Wall")
set(CMAKE_LD_FLAGS "${CMAKE_LD_FLAGS} -no-undefined -version-info 0:1:0")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_C_FLAGS} -g -O2 -DNDEBUG -fPIC")
set(D main/native/)

SET(STORED_CMAKE_FIND_LIBRARY_SUFFIXES CMAKE_FIND_LIBRARY_SUFFIXES)
set_find_shared_library_version("1")
SET(CMAKE_FIND_LIBRARY_SUFFIXES STORED_CMAKE_FIND_LIBRARY_SUFFIXES)

INCLUDE(CheckFunctionExists)
INCLUDE(CheckCSourceCompiles)
#INCLUDE(CheckLibraryExists)
INCLUDE(CheckIncludeFiles)
#CHECK_FUNCTION_EXISTS(sync_file_range HAVE_SYNC_FILE_RANGE)
#CHECK_FUNCTION_EXISTS(posix_fadvise HAVE_POSIX_FADVISE)
#CHECK_LIBRARY_EXISTS(dl dlopen "" NEED_LINK_DL)
CHECK_INCLUDE_FILES(fcntl.h HAVE_FCNTL_H)
CHECK_INCLUDE_FILES(malloc.h HAVE_MALLOC_H)
CHECK_INCLUDE_FILES(mach/mach.h HAVE_MACH_MACH_H)
CHECK_INCLUDE_FILES(memory.h HAVE_MEMORY_H)
CHECK_INCLUDE_FILES(stddef.h HAVE_STDDEF_H)
CHECK_INCLUDE_FILES(stdint.h HAVE_STDINT_H)
CHECK_INCLUDE_FILES(stdlib.h HAVE_STDLIB_H)
CHECK_INCLUDE_FILES(string.h HAVE_STRING_H)
CHECK_INCLUDE_FILES(unistd.h HAVE_UNITSTD_H)
CHECK_FUNCTION_EXISTS(clock_gettime HAVE_CLOCK_GETTIME)
CHECK_FUNCTION_EXISTS(localtime_r HAVE_LOCALTIME_R)
CHECK_FUNCTION_EXISTS(memset HAVE_MEMSET)
CHECK_FUNCTION_EXISTS(strchr HAVE_STRCHR)
CHECK_FUNCTION_EXISTS(strtoul HAVE_STRTOUL)


set(NATIVETASK_DIR 
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../core/src/main/native)
include_directories(
		${D}
    ${D}/src
    ${CMAKE_CURRENT_SOURCE_DIR}
		${NATIVETASK_DIR}/src
		${NATIVETASK_DIR}/src/lib
		${NATIVETASK_DIR}/src/util
    ${JNI_INCLUDE_DIRS}
)

SET(CMAKE_BUILD_WITH_INSTALL_RPATH TRUE)

add_dual_library(nativetaskhbase
    ${D}/src/HBasePlatform.cc
)
target_link_libraries(nativetaskhbase
	  nativetask
	  dl
		rt
		pthread
		z
    ${JAVA_JVM_LIBRARY}
)

#IF (${CMAKE_SYSTEM_NAME} MATCHES "Linux")
#    #
#    # By embedding '$ORIGIN' into the RPATH of libnativetaskhbase.so,
#    # dlopen will look in the directory containing libnativetaskhbase.so.
#    # However, $ORIGIN is not supported by all operating systems.
#    #
#    SET_TARGET_PROPERTIES(nativetaskhbase
#        PROPERTIES INSTALL_RPATH "\$ORIGIN/")
#ENDIF()

SET(LIBNATIVETASKHBASE_VERSION "1.0.0")
SET_TARGET_PROPERTIES(nativetaskhbase PROPERTIES SOVERSION
	${LIBNATIVETASKHBASE_VERSION})
dual_output_directory(nativetaskhbase target/usr/local/lib)
