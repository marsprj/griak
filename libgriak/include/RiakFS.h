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

		RiakFileSet*	ListFiles(const char* dir_key);

	public:
		void	GetBuckets();

	private:
		riak_connection*	GetConnection();
		riak_get_response*	GetRiakObjects(const char* bucket, const char* key);
		riak_get_response*	GetRiakObjects(riak_binary * bucket, riak_binary * key);

		bool			HasRiakObject(const char* bucket, const char* key);

	private:
		std::string	m_riak_server;
		std::string	m_riak_port;

		riak_connection *m_riak;
		riak_config 	 *m_cfg;

	};

}

#endif  //__GRIAK_FILE_SYSTEM_H__
