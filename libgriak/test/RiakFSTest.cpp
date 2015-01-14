#include "RiakFSTest.h"
#include "RiakFile.h"
#include "RiakFileSet.h"
#include "RiakTileStore.h"

CPPUNIT_TEST_SUITE_REGISTRATION(RiakFSTest);

void RiakFSTest::setUp() 
{
	printf("setUp\n");

	m_riak.SetServer("192.168.111.154");
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

void RiakFSTest::GetRootFolderTest()
{
	printf("----------------------------------\n");
	printf("GetRootFolderTest\n");

	radi::RiakFile* rf = NULL;
	rf = m_riak.GetRoot();
	printf("%s\n", rf->GetName());

	rf->Release();
}

void RiakFSTest::GetRiakFileByKeyTest()
{
	printf("----------------------------------\n");
	printf("GetRiakFileByKeyTest\n");

	radi::RiakFile* rf = NULL;
	//rf = m_riak.GetRiakFile("rfs","root","vector");
	rf = m_riak.GetRiakFile("rfs","root","test");
	//rf = m_riak.GetRiakFile("rfs","7242ebad-1f74-43ed-b1b0-4ba4632967a3");
	printf("%s\n", rf->GetName());

	rf->Release();
}

void RiakFSTest::ListRootFileByKeyTest()
{
	printf("----------------------------------\n");
	printf("ListRootFileByKeyTest\n");

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

void RiakFSTest::CreateFolder()
{
	printf("----------------------------------\n");
	printf("CreateFolder\n");

	radi::RiakFile* rf = NULL;
	rf = m_riak.CreateFile("root", "mydir",true);
	CPPUNIT_ASSERT(rf!=NULL);
	rf->Release();
}

void RiakFSTest::CreateFile()
{
	printf("----------------------------------\n");
	printf("CreateFolder\n");

	radi::RiakFile* rf = NULL;
	bool is_folder = true;
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

	rf = root->GetFile("mydir3");
	CPPUNIT_ASSERT(rf!=NULL);

	radi::RiakFile* rf_2 = NULL;
	rf_2 = rf->CreateFile("tardb3", false, "PGIS");
	CPPUNIT_ASSERT(rf_2!=NULL);	
	rf_2->Release();
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

void RiakFSTest::GetFile_By_Path()
{
	printf("----------------------------------\n");
	printf("GetFile_By_Path\n");

	radi::RiakFile* rf = NULL;
	rf = m_riak.GetFile("/mydir3/tardb3");
	CPPUNIT_ASSERT(rf!=NULL);
	printf("[File Name]:%s\n", rf->GetName());
	rf->Release();
}

void RiakFSTest::Riak_Store_Import()
{
	radi::RiakFile* rf = NULL;
	rf = m_riak.GetFile("/mydir3/tardb3");
	CPPUNIT_ASSERT(rf!=NULL);
	printf("[File Name]:%s\n", rf->GetName());

	radi::RiakTileStore* tile_store = NULL;
	tile_store = rf->GetTileStore();
	CPPUNIT_ASSERT(tile_store!=NULL);

	const char* t_path = "/home/renyc/image/tile.png";
	bool ret = tile_store->PutTile("7x78x45",t_path);
	CPPUNIT_ASSERT(ret);	

	rf->Release();
}

void RiakFSTest::RemoveFile()
{
	radi::RiakFile* rf = NULL;
	//rf = m_riak.GetFile("/test/wgs84_vector_2to9_Layers");
	rf = m_riak.GetFile("/mydir3/tardb3");
	CPPUNIT_ASSERT(rf!=NULL);
	printf("[File Name]:%s\n", rf->GetName());

	rf = m_riak.Delete(rf);
	CPPUNIT_ASSERT(rf!=NULL);

	rf->Release();	
}
