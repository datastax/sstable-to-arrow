FROM ubuntu:18.04

RUN apt-get update -y

RUN apt-get install -y git curl build-essential cmake

# download required resources
WORKDIR /tmp
RUN curl -LO https://github.com/kaitai-io/kaitai_struct_compiler/releases/download/0.9/kaitai-struct-compiler_0.9_all.deb
RUN git clone https://github.com/kaitai-io/kaitai_struct_cpp_stl_runtime.git
RUN git clone https://github.com/apache/arrow.git
RUN git clone https://github.com/lz4/lz4

# install kaitai-struct-compiler
RUN apt-get install -y ./kaitai-struct-compiler_0.9_all.deb

# install kaitai_struct_cpp_stl_runtime
WORKDIR /tmp/kaitai_struct_cpp_stl_runtime/build
RUN cmake ..
RUN make install

# install arrow
RUN mkdir /tmp/arrow/cpp/release
WORKDIR /tmp/arrow/cpp/release
RUN cmake -DARROW_PARQUET=ON ..
RUN make install

# install lz4
WORKDIR /tmp/lz4
RUN make install

# copy and build project
WORKDIR /home
COPY . .
RUN mkdir /home/build
WORKDIR /home/build
RUN cmake -DCMAKE_BUILD_TYPE=Release ..
RUN make

EXPOSE 9143

ENTRYPOINT [ "./sstable_to_arrow" ]
