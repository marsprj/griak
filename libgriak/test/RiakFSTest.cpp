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

void RiakFSTest::CreateFolder()
{
	printf("----------------------------------\n");
	printf("CreateFolder\n");

	radi::RiakFile* rf = NULL;
	bool is_folder = true;
	const char* storage_type = "VALUE";
	rf = m_riak.CreateFile("root", "mydir",is_folder);
	//rf->Release();
}

void RiakFSTest::CreateFile()
{
	printf("----------------------------------\n");
	printf("CreateFolder\n");

	radi::RiakFile* rf = NULL;
	bool is_folder = true;
	const char* data_type = "VALUE";
	rf = m_riak.CreateFile("21c8d422-8f77-4ca3-8980-ecb2a867ddc5", "tardb2",false, "PGIS");
	CPPUNIT_ASSERT(rf!=NULL);
	rf->Release();
}

void RiakFSTest::CreateFile_2()
{
	printf("----------------------------------\n");
	printf("CreateFile_2\n");

	radi::RiakFile* rf = NULL;
	radi::RiakFile* root = NULL;
	
	root = m_riak.GetRoot();
	CPPUNIT_ASSERT(root!=NULL);

	rf = root->CreateFile("mydir2", true);
	CPPUNIT_ASSERT(rf!=NULL);
	rf->Release();

	root->Release();
}

void RiakFSTest::GetFile_By_File()
{
	printf("----------------------------------\n");
	printf("GetFile_By_File\n");

	radi::RiakFile* rf = NULL;
	radi::RiakFile* root = NULL;
	
	root = m_riak.GetRoot();
	CPPUNIT_ASSERT(root!=NULL);

	rf = root->GetFile("mydir2");	
	CPPUNIT_ASSERT(rf!=NULL);
	rf->Release();

	root->Release();	
}