#!/bin/sh
./contrib/build-package.sh -B zstd
./contrib/build-package.sh -B googlelog                           
./contrib/build-package.sh -B googleflags                                  
./contrib/build-package.sh -B googletest                             
./contrib/build-package.sh -B fmt
./contrib/build-package.sh -B sparsemap
./contrib/build-package.sh -B folly                                         
./contrib/build-package.sh -B fizz 
./contrib/build-package.sh -B wangle
./contrib/build-package.sh -B fbthrift
./contrib/build-package.sh -B cachelib
