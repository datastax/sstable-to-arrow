# linker
all: vint.o columns_bitmask.o deletion_time.o modified_utf8.o deserialization_helper.o sstable.o sstable_statistics.o sstable_index.o main.o
	g++ vint.o columns_bitmask.o deletion_time.o modified_utf8.o deserialization_helper.o sstable.o sstable_statistics.o sstable_index.o main.o \
		-l kaitai_struct_cpp_stl_runtime \
		-o main


# === compilation ===

# helper types
vint.o: vint.cpp vint.h
	g++ -c vint.cpp -o vint.o

modified_utf8.o: modified_utf8.cpp modified_utf8.h
	g++ -c modified_utf8.cpp -o modified_utf8.o

columns_bitmask.o: columns_bitmask.cpp columns_bitmask.h
	g++ -c columns_bitmask.cpp -o columns_bitmask.o

deletion_time.o: deletion_time.cpp deletion_time.h
	g++ -c deletion_time.cpp -o deletion_time.o

deserialization_helper.o: deserialization_helper.cpp deserialization_helper.h
	g++ -c deserialization_helper.cpp -o deserialization_helper.o
	
# sstable types
sstable_statistics.o: sstable_statistics.cpp sstable_statistics.h
	g++ -c sstable_statistics.cpp -o sstable_statistics.o

sstable.o: deletion_time.cpp deletion_time.h sstable.cpp sstable.h
	g++ -c sstable.cpp -o sstable.o

sstable_index.o: deletion_time.cpp deletion_time.h sstable_index.cpp sstable_index.h
	g++ -c sstable_index.cpp -o sstable_index.o

main.o: main.cpp main.h
	g++ -c main.cpp -o main.o

# kaitai
sstable_statistics.cpp: sstable_statistics.ksy
	/usr/local/bin/kaitai-struct-compiler sstable_statistics.ksy -t cpp_stl --opaque-types true

sstable.cpp: sstable_data.ksy
	/usr/local/bin/kaitai-struct-compiler sstable_data.ksy -t cpp_stl --opaque-types true

sstable_index.cpp: sstable_index.ksy
	/usr/local/bin/kaitai-struct-compiler sstable_index.ksy -t cpp_stl --opaque-types true

deletion_time.cpp: deletion_time.ksy
	/usr/local/bin/kaitai-struct-compiler deletion_time.ksy -t cpp_stl --opaque-types true


cleanup:
	rm *.o main {deletion_time,sstable_index,sstable,sstable_statistics}.{h,cpp}
