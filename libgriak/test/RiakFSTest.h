#ifndef __AUGE_SERVICE_TEST__H__
#define __AUGE_SERVICE_TEST__H__

#include "RiakFS.h"
#include "cppunit/extensions/HelperMacros.h" 

class RiakFSTest : public CppUnit::TestFixture 
{
	CPPUNIT_TEST_SUITE(RiakFSTest);
	//CPPUNIT_TEST(TestBuckets);
	//CPPUNIT_TEST(GetRootFolderTest);
	//CPPUNIT_TEST(GetRiakFileTest);
	//CPPUNIT_TEST(ListRootFileTest);
	//CPPUNIT_TEST(ListVectorFileTest);
	CPPUNIT_TEST(CreateFolder);
	CPPUNIT_TEST_SUITE_END();
public:
	void setUp();
	void tearDown();

private:
	void TestBuckets();
	void GetRootFolderTest();
	void GetRiakFileTest();

	void ListRootFileTest();

	void ListVectorFileTest();

	void CreateFolder();
private:
	radi::RiakFS m_riak;
};

#endif //__AUGE_SERVICE_TEST__H__
