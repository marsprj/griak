#include "RiakStoreTest.h"
#include "RiakFile.h"
#include "RiakFileSet.h"

//CPPUNIT_TEST_SUITE_REGISTRATION(RiakStoreTest);

void RiakStoreTest::setUp() 
{
	printf("setUp\n");

	m_riak.SetServer("192.168.111.88");
	m_riak.SetPort("8087");
	if(!m_riak.Connect())
	{
		printf("connect fail\n");
	}
	m_store = new radi::RiakTileStore("wgs84_vector_2to9_Layers","f444cecd-58c9-44ef-b809-cc41457ed4c0",&m_riak);
}

void RiakStoreTest::tearDown()
{
	m_store->Release();
	m_riak.Close();
	printf("tearDown\n");
}

void RiakStoreTest::TestGetTile()
{
	printf("----------------------------------\n");
	printf("TestGetTile\n");

	printf("----------------------------------\n");
	printf("[7x78x46]\n");
	m_store->GetTile("7x78x46");
	printf("[7x78x45]\n");
	m_store->GetTile("7x78x45");
}


void RiakStoreTest::TestPutTile()
{
	printf("----------------------------------\n");
	printf("TestGetTile\n");

	const char* t_path = "/home/renyc/image/tile.png";

	bool ret = m_store->PutTile("7x78x45","/home/renyc/image/tile.png");
	CPPUNIT_ASSERT(ret);
}
