FROM ubuntu:20.04

# install required packages
ENV DEBIAN_FRONTEND=noninteractive
RUN apt update && apt install -y \
    build-essential \
    cmake \
    curl \
    git \
    libcurl4-openssl-dev \
    libssl-dev \
    ninja-build \
    python3-dev \
    python3-numpy

# download required resources into /tmp
WORKDIR /tmp
RUN curl -LO https://github.com/kaitai-io/kaitai_struct_compiler/releases/download/0.9/kaitai-struct-compiler_0.9_all.deb && \
    curl -LO https://boostorg.jfrog.io/artifactory/main/release/1.76.0/source/boost_1_76_0.tar.bz2 && \
    git clone https://github.com/kaitai-io/kaitai_struct_cpp_stl_runtime.git && \
    git clone https://github.com/apache/arrow.git && \
    git clone https://github.com/lz4/lz4 && \
    tar --bzip2 -xf ./boost_1_76_0.tar.bz2 && \
    mkdir /tmp/arrow/cpp/release /home/build

# install kaitai-struct-compiler
RUN apt install -y ./kaitai-struct-compiler_0.9_all.deb

# install boost
WORKDIR /tmp/boost_1_76_0
RUN ./bootstrap.sh \
        --with-python="$(which python3)" \
        --with-libraries=filesystem,log,python,iostreams,program_options && \
    ./b2 install

# install kaitai_struct_cpp_stl_runtime
WORKDIR /tmp/kaitai_struct_cpp_stl_runtime/build
RUN cmake -GNinja .. && \
    ninja && ninja install

# install arrow
WORKDIR /tmp/arrow/cpp/release
RUN cmake -GNinja \
        -DARROW_PARQUET=ON \
        -DARROW_S3=ON \
        -DARROW_WITH_LZ4=ON \
        -DARROW_PYTHON=ON \
        .. && \
    ninja && ninja install

# install lz4
WORKDIR /tmp/lz4
RUN make && make install

# copy and build project
WORKDIR /home
COPY . .
WORKDIR /home/build
RUN cmake -GNinja -DCMAKE_BUILD_TYPE=Release .. && ninja

EXPOSE 9143

ENTRYPOINT [ "./sstable_to_arrow" ]
CMD [ "-h" ]
