#!/usr/bin/env bash
# SPDX-License-Identifier: BSD-3-Clause
# Copyright 2022, Intel Corporation

git clone -b fix_develop_new_sub https://github.com/byrnedj/CacheLib CacheLib

./CacheLib/contrib/prerequisites-centos8.sh

for pkg in zstd googleflags googlelog googletest sparsemap fmt folly fizz wangle fbthrift ;
do
    if [ "$pkg" == "fbthrift" ]
    then
        git clone https://github.com/facebook/mvfst.git
        cd mvfst
        mkdir build
        cd build
        cmake -DCMAKE_INSTALL_PREFIX=/usr -DCMAKE_BUILD_TYPE=RelWithDebInfo ..
        cmake --build . -j --target install
        cd ../../
        rm -rf mvfst
    fi
    sudo ./CacheLib/contrib/build-package.sh -j -I /opt/ "$pkg"
done

rm -rf CacheLib
