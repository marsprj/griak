#ifndef __GRIAK_FILE_H__
#define __GRIAK_FILE_H__

#include <riak.h>
#include <string>

namespace radi
{
	class RiakFS;
	class RiakTileStore;

	class RiakFile
	{
	public:
		RiakFile();
		virtual ~RiakFile();
	public:
		bool		IsFolder();

		const char*	GetName();
		void		SetName(const char* name);
		const char*	GetKey();

		const char*	GetDataType() const;
		const char*	GetDataStore() const;

		RiakFile*	GetFile(const char* f_name);
		RiakFile*	CreateFile(const char* f_name, bool is_folder, const char* data_type="PGIS");

		RiakTileStore*	GetTileStore();

		void		Release();

	public:
		bool		Create(RiakFS* riak_fs, riak_connection *rcxn, riak_config *rcfg, riak_object* robj);

	private:
		void		SetIsFolder(const char* val);
		void		SetDataType(const char* val, size_t len);
		void		SetDataStore(const char* val, size_t len);

	private:
		bool		m_isFolder;
		std::string	m_name;
		std::string	m_key;
		std::string	m_data_type;
		std::string	m_data_store;

		RiakFS		*m_riak_fs;
		RiakTileStore	*m_tile_store;
		
		riak_connection *m_cxn;
		riak_config 	*m_cfg;


	};
}

#endif //__GRIAK_FILE_H__
