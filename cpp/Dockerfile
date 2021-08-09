FROM ubuntu:18.04

# install required packages
RUN apt update -y
RUN apt install -y \
    build-essential \
    cmake \
    curl \
    git \
    libcurl4-openssl-dev \
    libssl-dev \
    ninja-build

# download required resources into /tmp
WORKDIR /tmp
RUN curl -LO https://github.com/kaitai-io/kaitai_struct_compiler/releases/download/0.9/kaitai-struct-compiler_0.9_all.deb
RUN curl -LO https://boostorg.jfrog.io/artifactory/main/release/1.76.0/source/boost_1_76_0.tar.bz2
RUN git clone https://github.com/kaitai-io/kaitai_struct_cpp_stl_runtime.git
RUN git clone https://github.com/apache/arrow.git
RUN git clone https://github.com/lz4/lz4

# install kaitai-struct-compiler
RUN apt install -y ./kaitai-struct-compiler_0.9_all.deb

# install boost
RUN tar --bzip2 -xf ./boost_1_76_0.tar.bz2
WORKDIR /tmp/boost_1_76_0
RUN ./bootstrap.sh --with-libraries=filesystem,log,python,program_options
RUN ./b2 install

# install kaitai_struct_cpp_stl_runtime
WORKDIR /tmp/kaitai_struct_cpp_stl_runtime/build
RUN cmake -GNinja ..
RUN ninja
RUN ninja install

# install arrow
RUN mkdir /tmp/arrow/cpp/release
WORKDIR /tmp/arrow/cpp/release
RUN cmake -GNinja -DARROW_FILESYSTEM=ON -DARROW_PARQUET=ON -DARROW_S3=ON -DARROW_WITH_LZ4=ON  ..
RUN ninja
RUN ninja install

# install lz4
WORKDIR /tmp/lz4
RUN make
RUN make install

# copy and build project
WORKDIR /home
COPY . .
RUN mkdir /home/build
WORKDIR /home/build
RUN cmake -GNinja -DCMAKE_BUILD_TYPE=Release ..
RUN ninja

EXPOSE 9143

ENTRYPOINT [ "./sstable_to_arrow" ]
CMD [ "-h" ]
