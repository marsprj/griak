#ifndef __RIAK_TILE_STORE_H__
#define __RIAK_TILE_STORE_H__

#include <string>

namespace radi
{
	class RiakTileStore
	{
	public:
		RiakTileStore();
		virtual ~RiakTileStore();
	public:
		const char*	GetKey();
		const char*	GetName();
		
		void		GetTile(const char* key);
		void		Release();

	private:
		std::string	m_name;
		std::string	m_key;
	};
}

#endif //__RIAK_TILE_STORE_H__
