kaitai_types = basic_types deletion_time sstable_index sstable_statistics sstable_summary sstable_data
util_types = vint deserialization_helper clustering_blocks columns_bitmask modified_utf8
driver_files = sstable_to_arrow main
all_objs = $(util_types) $(kaitai_types) $(driver_files)

main: out $(foreach obj,$(all_objs),out/$(obj).o)
	g++ -std=c++11 $(foreach file,$(all_objs),out/$(file).o) -l kaitai_struct_cpp_stl_runtime -l arrow -o main

out:
	mkdir -p out

clean:
	rm -f out/* ./main
	
define driver_file_to_out
out/$(1).o: src/$(1).cpp src/$(1).h
	cp src/$(1).cpp src/$(1).h out
	g++ -std=c++11 -c out/$(1).cpp -o out/$(1).o

endef

define ksy_to_cpp
out/$(1).cpp: src/ksy/$(1).ksy
	kaitai-struct-compiler --opaque-types true --outdir out --target cpp_stl --cpp-standard 11 "src/ksy/$(1).ksy"
out/$(1).o: out/$(1).cpp $(foreach type,$(util_types),out/$(type).h)
	g++ -std=c++11 -c out/$(1).cpp -o out/$(1).o

endef

define util_to_out
out/$(1).cpp:
	cp src/util/$(1).cpp out
out/$(1).h:
	cp src/util/$(1).h out
out/$(1).o: out/$(1).cpp out/$(1).h
	g++ -std=c++11 -c out/$(1).cpp -o out/$(1).o

endef

$(eval $(foreach file,$(kaitai_types),$(call ksy_to_cpp,$(file))))

$(eval $(foreach type,$(util_types),$(call util_to_out,$(type))))

$(eval $(foreach file,$(driver_files),$(call driver_file_to_out,$(file))))
