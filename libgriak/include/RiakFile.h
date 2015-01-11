#ifndef __GRIAK_FILE_H__
#define __GRIAK_FILE_H__

#include <riak.h>
#include <string>

namespace radi
{
	class  RiakFile
	{
	public:
		RiakFile();
		virtual ~RiakFile();
	public:
		bool		IsFolder();

		const char*	GetName();
		void		SetName(const char* name);

		void		Release();

	public:
		bool		Create(riak_connection *rcxn, riak_config *rcfg, riak_object* robj);

	private:
		void		SetIsFolder(const char* val);

	private:
		bool		m_isFolder;
		std::string	m_name;

		riak_connection *m_cxn;
		riak_config 	 *m_cfg;
	};
}

#endif //__GRIAK_FILE_H__
