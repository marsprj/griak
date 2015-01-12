#ifndef __RIAK_STORE_TEST__H__
#define __RIAK_STORE_TEST__H__

#include "RiakFS.h"
#include "RiakTileStore.h"
#include "cppunit/extensions/HelperMacros.h" 

class RiakStoreTest : public CppUnit::TestFixture 
{
	CPPUNIT_TEST_SUITE(RiakStoreTest);
	CPPUNIT_TEST(TestGetTile);
	CPPUNIT_TEST(TestPutTile);
	CPPUNIT_TEST_SUITE_END();
public:
	void setUp();
	void tearDown();

private:
	void TestGetTile();
	void TestPutTile();
private:
	radi::RiakFS m_riak;
	radi::RiakTileStore* m_store;
};

#endif //__RIAK_STORE_TEST__H__
