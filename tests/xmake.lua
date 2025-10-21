-- Tests build targets for Prism

target("dbformat_test")
    set_kind("binary")
    -- paths relative to project root
    add_files("$(projectdir)/tests/dbformat_test.cpp")
    -- explicitly add required implementation sources from project
    add_files("$(projectdir)/include/dbformat.cpp", "$(projectdir)/include/comparator.cpp", "$(projectdir)/util/coding.cpp")
    add_includedirs("$(projectdir)/include", "$(projectdir)/util")
    add_packages("gtest")
    add_tests("default")


