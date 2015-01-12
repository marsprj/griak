#include "RiakTileStore.h"

namespace radi
{
	RiakTileStore::RiakTileStore()
	{

	}

	RiakTileStore::~RiakTileStore()
	{

	}

	void RiakTileStore::Release()
	{
		delete this;
	}

	const char* RiakTileStore::GetName()
	{
		return m_name.c_str();
	}

	const char* RiakTileStore::GetKey()
	{
		return m_key.c_str();
	}

	void RiakTileStore::GetTile(const char* key)
	{
	}
}