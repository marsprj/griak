#include "RiakFSTest.h"
#include "RiakFile.h"
#include "RiakFileSet.h"

CPPUNIT_TEST_SUITE_REGISTRATION(RiakFSTest);

void RiakFSTest::setUp() 
{
	printf("setUp\n");

	m_riak.SetServer("192.168.111.104");
	m_riak.SetPort("8087");
	if(!m_riak.Connect())
	{
		printf("connect fail\n");
	}
}

void RiakFSTest::tearDown()
{
	m_riak.Close();
	printf("tearDown\n");
}

void RiakFSTest::TestBuckets()
{
	printf("----------------------------------\n");
	printf("ListBuckets\n");

	m_riak.GetBuckets();
}

void RiakFSTest::GetRootFolderTest()
{
	printf("----------------------------------\n");
	printf("GetRootFolderTest\n");

	radi::RiakFile* rf = NULL;
	rf = m_riak.GetRoot();
	printf("%s\n", rf->GetName());

	rf->Release();
}

void RiakFSTest::GetRiakFileTest()
{
	printf("----------------------------------\n");
	printf("GetRiakFileTest\n");

	radi::RiakFile* rf = NULL;
	//rf = m_riak.GetRiakFile("rfs","root","vector");
	rf = m_riak.GetRiakFile("rfs","root","test");
	//rf = m_riak.GetRiakFile("rfs","7242ebad-1f74-43ed-b1b0-4ba4632967a3");
	printf("%s\n", rf->GetName());

	rf->Release();
}

void RiakFSTest::ListRootFileTest()
{
	printf("----------------------------------\n");
	printf("ListRootFileTest\n");

	radi::RiakFile* rf = NULL;
	radi::RiakFileSet* files = NULL;
	files = m_riak.ListFiles("root");

	unsigned int count = files->GetCount();
	for(unsigned int i=0; i<count; i++)
	{
		rf = files->GetRiakFile(i);
		printf("%s\n", rf->GetName());	
	}

	files->Release();
}

void RiakFSTest::ListVectorFileTest()
{
	printf("----------------------------------\n");
	printf("ListVectorFileTest\n");

	radi::RiakFile* rf = NULL;
	radi::RiakFileSet* files = NULL;
	files = m_riak.ListFiles("7242ebad-1f74-43ed-b1b0-4ba4632967a3");

	unsigned int count = files->GetCount();
	for(unsigned int i=0; i<count; i++)
	{
		rf = files->GetRiakFile(i);
		printf("%s\n", rf->GetName());	
	}

	files->Release();
}
