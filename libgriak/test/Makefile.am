AUTOMAKE_OPTION=foreign
bin_PROGRAMS=griaktest
AM_CXXFLAGS=-D_LINUX
AM_CPPFLAGS=\
	-I$(RIAK_C_HOME)/include \
	-I$(top_srcdir)/libgriak/include

griaktest_LDFLAGS=\
		-L$(top_srcdir)/libgriak/src
	
griaktest_LDADD=-lgriak \
		-lcppunit

griaktest_SOURCES=main.cpp RiakFSTest.cpp RiakStoreTest.cpp
