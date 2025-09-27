add_rules("mode.debug", "mode.release")
add_rules("plugin.compile_commands.autoupdate", {outputdir = "."})

add_requires("crc32c")

set_languages("cxx23")

target("prism")
    set_kind("binary")
	add_files("src/*.cpp")
    add_includedirs("include", {public = true})
    add_includedirs("src", {public = true})
    add_packages("crc32c")
    

    if is_mode("debug") then
        set_symbols("debug")
        set_optimize("none")
        add_defines("DEBUG")
        add_cflags("-g", "-O0")
    end