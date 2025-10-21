-- Tests build targets for Prism
-- All test targets are defined here and included from root xmake.lua

local common_files = {"$(projectdir)/src/*.cpp", "$(projectdir)/util/*.cpp"}
local common_includedirs = {"$(projectdir)/include", "$(projectdir)/src", "$(projectdir)/util"}

target("coding_test")
    set_kind("binary")
    add_files(common_files)
    add_files("$(projectdir)/tests/coding_test.cpp")
    add_includedirs(common_includedirs)
    add_packages("crc32c", "gtest")
    add_tests("default")

target("db_test")
    set_kind("binary")
    add_files(common_files)
    add_files("$(projectdir)/tests/db_test.cpp")
    add_includedirs(common_includedirs)
    add_packages("crc32c", "gtest")
    add_tests("default")

target("skiplist_test")
    set_kind("binary")
    add_files("$(projectdir)/tests/skiplist_test.cpp", "$(projectdir)/util/arena.cpp")
    add_packages("gtest")
    add_includedirs("$(projectdir)/include", "$(projectdir)/util")
    add_tests("default")

target("arena_test")
    set_kind("binary")
    add_files("$(projectdir)/tests/arena_test.cpp", "$(projectdir)/util/arena.cpp")
    add_packages("gtest")
    add_includedirs("$(projectdir)/include", "$(projectdir)/util")
    add_tests("default")

target("dbformat_test")
    set_kind("binary")
    add_files("$(projectdir)/tests/dbformat_test.cpp")
    add_files("$(projectdir)/include/dbformat.cpp", "$(projectdir)/include/comparator.cpp", "$(projectdir)/util/coding.cpp")
    add_includedirs("$(projectdir)/include", "$(projectdir)/util")
    add_packages("gtest")
    add_tests("default")
