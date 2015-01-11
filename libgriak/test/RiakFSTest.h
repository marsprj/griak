#ifndef __AUGE_SERVICE_TEST__H__
#define __AUGE_SERVICE_TEST__H__

#include "RiakFS.h"
#include "cppunit/extensions/HelperMacros.h" 

class RiakFSTest : public CppUnit::TestFixture 
{
	CPPUNIT_TEST_SUITE(RiakFSTest);
	//CPPUNIT_TEST(TestBuckets);
	//CPPUNIT_TEST(GetRootFolderTest);
	CPPUNIT_TEST(GetRiakFileTest);
	CPPUNIT_TEST_SUITE_END();
public:
	void setUp();
	void tearDown();

private:
	void TestBuckets();
	void GetRootFolderTest();
	void GetRiakFileTest();
private:
	radi::RiakFS m_riak;
};

#endif //__AUGE_SERVICE_TEST__H__
