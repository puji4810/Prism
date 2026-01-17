local common_files = {"$(projectdir)/src/*.cpp", "$(projectdir)/util/*.cpp", "$(projectdir)/include/table/*.cpp"}
local common_includedirs = {"$(projectdir)/include", "$(projectdir)/src", "$(projectdir)/util"}

add_packages("crc32c")

target("kv_bench")
    set_kind("binary")
    add_files(common_files)
    add_files("$(projectdir)/benchmark/kv_bench.cpp")
    add_includedirs(common_includedirs)
    add_packages("crc32c")
