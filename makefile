KAITAI_FILES = deletion_time sstable_index sstable_statistics sstable_summary sstable_data
util_types = columns_bitmask deserialization_helper modified_utf8 vint
all_objs = $(KAITAI_FILES) $(util_types) main

all: $(foreach obj,$(all_objs),out/$(obj).o)
	g++ $(foreach file,$(all_objs),out/$(file).o) -l kaitai_struct_cpp_stl_runtime -o main

out/main.o: src/main.{cpp,h}
	cp src/main.{cpp,h} out
	g++ -c out/main.cpp -o out/main.o

clean:
	rm out/* *.o

define ksy_to_cpp
out/$(1).cpp: src/ksy/$(1).ksy
	/usr/local/bin/kaitai-struct-compiler --opaque-types true --outdir out --target cpp_stl "src/ksy/$(1).ksy"
out/$(1).o: out/$(1).cpp $(foreach type,$(util_types),out/$(type).h)
	g++ -c out/$(1).cpp -o out/$(1).o

endef

define util_to_out
out/$(1).cpp:
	cp src/util/$(1).cpp out
out/$(1).h:
	cp src/util/$(1).h out
out/$(1).o: out/$(1).{cpp,h}
	g++ -c out/$(1).cpp -o out/$(1).o

endef

$(eval $(foreach file,$(KAITAI_FILES),$(call ksy_to_cpp,$(file))))

$(eval $(foreach type,$(util_types),$(call util_to_out,$(type))))
