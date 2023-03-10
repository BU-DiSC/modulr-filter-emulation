
CCOMPILE=g++
CPPFLAGS=-std=c++11 -I -msse4.2 -O3
LDFLAGS= -Wno-deprecated
#pf_LDFLAGS= -Wno-deprecated -O3 -fprefetch-loop-arrays
pf_LDFLAGS= -Wno-deprecated


all: lsm_emu

lsm_emu: BF_bit.cpp lsm-emulation.cpp options.cpp db.cpp hash/md5.cpp hash/murmurhash.cc hash/sha-256.c hash/Crc32.cpp hash/city.cc
	$(CCOMPILE) -g lsm-emulation.cpp db.cpp BF_bit.cpp options.cpp PriorityCache.cpp LRUcache.cpp  hash/md5.cpp hash/murmurhash.cc hash/sha-256.c  hash/Crc32.cpp hash/city.cc $(CPPFLAGS) $(LDFLAGS) -o lsm-emu

clean:
	-rm lsm-emu
