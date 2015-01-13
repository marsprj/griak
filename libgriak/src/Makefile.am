AUTOMAKE_OPTION=foreign
lib_LTLIBRARIES=libgriak.la
AM_CXXFLAGS=-D_LINUX

AM_CPPFLAGS=-I../include \
		-I$(RIAK_C_HOME)/include

libgriak_la_LDFLAGS=\
		-L$(RIAK_C_HOME)/lib

libgriak_la_LIBADD=\
		-lriak_c_client-0.5 \
		-luuid

libgriak_la_SOURCES=\
		RiakFS.cpp \
		RiakFile.cpp \
		RiakFileSet.cpp \
		RiakTileStore.cpp \
		StringSet.cpp
