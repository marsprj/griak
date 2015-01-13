#ifndef __GRIAK_FILE_SYSTEM_H__
#define __GRIAK_FILE_SYSTEM_H__

#include <string>
#include <riak.h>

namespace radi
{
	class RiakFile;
	class RiakFileSet;

	class RiakFS
	{
		friend class RiakFile;
		friend class RiakTileStore;
	public:
		RiakFS();
		RiakFS(const char* server, const char* port);
		virtual ~RiakFS();
	public:
		bool		Connect();
		void		Close();

		void		Release();

		void		SetServer(const char* server);
		void		SetPort(const char* port);

		RiakFile*	GetRoot();

		RiakFile*	GetRiakFile(const char* bucket, const char* key);
		RiakFile*	GetRiakFile(riak_binary* bucket, riak_binary* key);
		RiakFile*	GetRiakFile(const char* bucket, const char* parent_key, const char* name);

		RiakFile*	CreateFile(const char* parent_key, const char* f_name, bool is_folder, const char* data_type="PGIS");
		bool		AddLink(riak_object* r_obj, const char* bucket, const char* key, const char* parent);

		RiakFileSet*	ListFiles(const char* dir_key);

		void		Release(riak_get_response* response);
	public:
		void		GetBuckets();


	private:
		riak_connection*	GetConnection();
		riak_get_response*	GetRiakObjects(const char* bucket, const char* key);
		riak_get_response*	GetRiakObjects(riak_binary * bucket, riak_binary * key);
		bool			HasRiakObject(const char* bucket, const char* key);

		bool			CreateRiakFile(const char* f_name, const char* f_key, bool is_folder, const char* data_type);

	private:
		std::string	m_riak_server;
		std::string	m_riak_port;

		riak_connection *m_riak;
		riak_config 	 *m_cfg;
		riak_binary	 *m_rfs_bucket;

	};

}

#endif  //__GRIAK_FILE_SYSTEM_H__
