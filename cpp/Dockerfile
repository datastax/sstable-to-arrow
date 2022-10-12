FROM ubuntu:20.04

# set environment variables
ENV DEBIAN_FRONTEND=noninteractive
ENV LD_LIBRARY_PATH=/usr/local/lib
ENV PYTHONPATH=/usr/local/lib
ENV ARROW_HOME=/usr/local
ENV PYARROW_CMAKE_GENERATOR=Ninja

# install packages
RUN apt-get update -yq && \
    apt-get install -yq --no-install-recommends \
        build-essential \
        cmake \
        curl \
        git \
        libcurl4-openssl-dev \
        libssl-dev \
        ninja-build \
        openjdk-8-jre-headless \
        python3-dev \
        python3-pip && \
    apt-get clean && rm -rf /var/lib/apt/lists*

# download required resources into /tmp
WORKDIR /tmp
RUN curl -LO https://github.com/kaitai-io/kaitai_struct_compiler/releases/download/0.9/kaitai-struct-compiler_0.9_all.deb && \
    curl -LO https://boostorg.jfrog.io/artifactory/main/release/1.76.0/source/boost_1_76_0.tar.bz2 && \
    git clone https://github.com/kaitai-io/kaitai_struct_cpp_stl_runtime.git && \
    git clone -b apache-arrow-8.0.1 https://github.com/apache/arrow.git && \
    git clone https://github.com/lz4/lz4 && \
    tar --bzip2 -xf ./boost_1_76_0.tar.bz2

# install boost
WORKDIR /tmp/boost_1_76_0
RUN ./bootstrap.sh \
        --with-python="$(which python3)" \
        --with-libraries=filesystem,log,python,iostreams,program_options && \
    ./b2 install

# install kaitai-struct-compiler and the kaitai struct C++ stl runtime
WORKDIR /tmp/kaitai_struct_cpp_stl_runtime/build
RUN apt-get install -yq --no-install-recommends /tmp/kaitai-struct-compiler_0.9_all.deb && \
    cmake -GNinja .. && \
    cmake --build . && \
    cmake --install .

# install arrow python requirements
RUN python3 -m pip install --upgrade pip && \
    python3 -m pip install -U setuptools wheel && \
    python3 -m pip install -r /tmp/arrow/python/requirements-build.txt

# install arrow
WORKDIR /tmp/arrow/cpp/release
RUN cmake -GNinja \
        -DARROW_PARQUET=ON \
        -DARROW_S3=ON \
        -DARROW_WITH_LZ4=ON \
        -DARROW_PYTHON=ON \
        .. && \
    cmake --build . && \
    cmake --install .

# install pyarrow
WORKDIR /tmp/arrow/python
RUN rm -rf build && \
    python3 setup.py install

# install lz4
WORKDIR /tmp/lz4
RUN make && make install

# copy and build project
WORKDIR /home
COPY . .
WORKDIR /home/build
RUN cmake -GNinja -DCMAKE_BUILD_TYPE=Release .. && \
    cmake --build . && \
    cmake --install .

EXPOSE 9143

ENTRYPOINT [ "sstable-to-arrow" ]
CMD [ "-h" ]
