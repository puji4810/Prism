add_rules("mode.debug", "mode.release")
add_rules("plugin.compile_commands.autoupdate", {outputdir = "."})
includes("@builtin/check")

add_requires("crc32c", "gtest")

set_languages("cxx23")

local common_files = {"src/*.cpp", "util/*.cpp", "include/table/*.cpp"}
local generated_configdir = "$(builddir)/generated"
local common_includedirs = {generated_configdir, "include", "src", "util"}

set_configdir(generated_configdir)
configvar_check_cxxfuncs("HAVE_FDATASYNC", "fdatasync", {includes = "unistd.h"})
if is_plat("macosx") then
    set_configvar("HAVE_FULLFSYNC", 1)
else
    set_configvar("HAVE_FULLFSYNC", 0)
end
if is_plat("linux", "macosx", "bsd") then
    set_configvar("HAVE_O_CLOEXEC", 1)
else
    configvar_check_cxxsnippets("HAVE_O_CLOEXEC", [[
	int value = O_CLOEXEC;
	(void)value;
]], {includes = "fcntl.h"})
end
set_configvar("HAVE_CRC32C", 1)
set_configvar("HAVE_SNAPPY", 0)
set_configvar("HAVE_ZSTD", 0)
add_configfiles("include/port/port_config.h.in", {filename = "port/port_config.h"})

if is_mode("debug") then
    add_cxflags("-fsanitize=address", "-fsanitize=leak", "-fsanitize=undefined", "-fno-omit-frame-pointer", {force = true})
    add_ldflags("-fsanitize=address", "-fsanitize=leak", "-fsanitize=undefined", {force = true})
end

-- include tests subdirectory targets
includes("tests/xmake.lua")

includes("benchmark/xmake.lua")

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
