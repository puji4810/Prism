add_rules("mode.debug", "mode.release")
add_rules("plugin.compile_commands.autoupdate", {outputdir = "."})

add_requires("crc32c", "gtest")

set_languages("cxx23")

local common_files = {"src/*.cpp", "util/*.cpp"}
local common_includedirs = {"include", "src", "util"}

if is_mode("debug") then
    add_cxflags("-fsanitize=address", "-fsanitize=leak", "-fno-omit-frame-pointer", {force = true})
    add_ldflags("-fsanitize=address", "-fsanitize=leak", {force = true})
end

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