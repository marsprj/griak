#ifndef __AUGE_SERVICE_TEST__H__
#define __AUGE_SERVICE_TEST__H__

#include "RiakFS.h"
#include "cppunit/extensions/HelperMacros.h" 

class RiakFSTest : public CppUnit::TestFixture 
{
	CPPUNIT_TEST_SUITE(RiakFSTest);
	CPPUNIT_TEST(GetRootFolderTest);
	CPPUNIT_TEST(GetRiakFileByKeyTest);
	CPPUNIT_TEST(ListRootFileByKeyTest);
	CPPUNIT_TEST(CreateFolder);
	CPPUNIT_TEST(CreateFile);
	CPPUNIT_TEST(CreateFile_2);
	CPPUNIT_TEST(GetFile_By_File);
	CPPUNIT_TEST(GetFile_By_Path);
	CPPUNIT_TEST(Riak_Store_Import);
	CPPUNIT_TEST(RemoveFile);
	CPPUNIT_TEST_SUITE_END();
public:
	void setUp();
	void tearDown();

private:
	void TestBuckets();
	void GetRootFolderTest();
	void GetRiakFileByKeyTest();

	void ListRootFileByKeyTest();

	void CreateFolder();
	void CreateFile();
	void CreateFile_2();

	void GetFile_By_File();
	void GetFile_By_Path();

	void RemoveFile();

	void Riak_Store_Import();
private:
	radi::RiakFS m_riak;
};

#endif //__AUGE_SERVICE_TEST__H__
