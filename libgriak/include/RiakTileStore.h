#ifndef __RIAK_TILE_STORE_H__
#define __RIAK_TILE_STORE_H__

#include <string>
#include <riak.h>

namespace radi
{
	class RiakFS;

	class RiakTileStore
	{
	public:
		RiakTileStore();
		RiakTileStore(const char* name, const char* key, RiakFS* riak_fs);
		virtual ~RiakTileStore();
	public:
		const char*	GetKey();
		const char*	GetName();
		
		void		GetTile(const char* t_key);
		bool		PutTile(const char* t_key, const unsigned char* t_data, size_t size, const char* img_type);
		bool		PutTile(const char* t_key, const char* t_path);
		void		Release();

	private:
		std::string	m_name;
		std::string	m_key;
		riak_binary*	m_rname;
		riak_binary*	m_rkey;

		RiakFS 		*m_riak_fs;
	};
}

#endif //__RIAK_TILE_STORE_H__
