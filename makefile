# linker
all: main.o vint.o sstable_statistics.o columns_bitmask.o sstable.o modified_utf8.o deserialization_helper.o
	g++ sstable_statistics.o sstable.o columns_bitmask.o main.o vint.o modified_utf8.o deserialization_helper.o \
		-l kaitai_struct_cpp_stl_runtime \
		-o main

# compilation
vint.o: vint.cpp vint.h
	g++ -c vint.cpp -o vint.o

modified_utf8.o: modified_utf8.cpp modified_utf8.h
	g++ -c modified_utf8.cpp -o modified_utf8.o

deserialization_helper.o: deserialization_helper.cpp deserialization_helper.h
	g++ -c deserialization_helper.cpp -o deserialization_helper.o
	
sstable_statistics.o: sstable_statistics.cpp sstable_statistics.h
	g++ -c sstable_statistics.cpp -o sstable_statistics.o

sstable.o: sstable.cpp sstable.h
	g++ -c sstable.cpp -o sstable.o

columns_bitmask.o: columns_bitmask.cpp columns_bitmask.h
	g++ -c columns_bitmask.cpp -o columns_bitmask.o

main.o: main.cpp main.h
	g++ -c main.cpp -o main.o

# kaitai
sstable_statistics.cpp: sstable_statistics.ksy
	/usr/local/bin/kaitai-struct-compiler sstable_statistics.ksy -t cpp_stl

sstable.cpp: sstable_data.ksy
	/usr/local/bin/kaitai-struct-compiler sstable_data.ksy -t cpp_stl

cleanup:
	rm *.o
