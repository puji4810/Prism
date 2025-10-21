add_rules("mode.debug", "mode.release")
add_rules("plugin.compile_commands.autoupdate", {outputdir = "."})

add_requires("crc32c", "gtest")

set_languages("cxx23")

local common_files = {"src/*.cpp", "util/*.cpp"}
local common_includedirs = {"include", "src", "util"}

-- include tests subdirectory targets
includes("tests/xmake.lua")

target("prism")
    set_kind("binary")
    add_files(common_files)
    add_files("tests/main.cpp")
    add_includedirs(common_includedirs)
    add_packages("crc32c")
    
    if is_mode("debug") then
        set_symbols("debug")
        set_optimize("none")
        add_defines("DEBUG")
    end

target("main")
    set_kind("binary")
    add_files(common_files)
    add_files("tests/main.cpp")
    add_includedirs(common_includedirs)
    add_packages("crc32c")

target("coding_test")
    set_kind("binary")
    add_files(common_files)
    add_files("tests/coding_test.cpp")
    add_includedirs(common_includedirs)
    add_packages("crc32c", "gtest")
    add_tests("default")

target("db_test")
    set_kind("binary")
    add_files(common_files)
    add_files("tests/db_test.cpp")
    add_includedirs(common_includedirs)
    add_packages("crc32c", "gtest")
    add_tests("default")

target("skiplist_test")
    set_kind("binary")
    add_files("tests/skiplist_test.cpp", "util/arena.cpp")
    add_packages("gtest")
    add_includedirs("include", "util")
    add_tests("default")

target("arena_test")
    set_kind("binary")
    add_files("tests/arena_test.cpp", "util/arena.cpp")
    add_packages("gtest")
    add_includedirs("include", "util")
    add_tests("default")