#include "RiakFile.h"

#include <limits.h>

namespace radi
{
	RiakFile::RiakFile():
	m_isFolder(false),
	m_cxn(NULL),
	m_cfg(NULL)
	{

	}

	RiakFile::~RiakFile()
	{

	}

	void RiakFile::Release()
	{
		delete this;
	}

	bool RiakFile::IsFolder()
	{
		return m_isFolder;
	}

	void RiakFile::SetIsFolder(const char* val)
	{
		m_isFolder = (!strcmp(val,"true"));
	}

	const char* RiakFile::GetName()
	{
		return m_name.c_str();
	}
	
	void RiakFile::SetName(const char* name)
	{
		if(name)
		{
			m_name = name;
		}
	}

	bool RiakFile::Create(riak_connection *rcxn, riak_config *rcfg, riak_object* robj)
	{		
		m_cxn = rcxn;
		m_cfg = rcfg;

		riak_binary* rname = riak_object_get_value(robj);;
		m_name = (const char*)riak_binary_data(rname);

		riak_error err;
		riak_pair  *meta = NULL;
		riak_binary* rkey = NULL;
		riak_binary* rval = NULL;
		

		const char* key = NULL;
		const char* val = NULL;

		riak_uint32_t n_meta = riak_object_get_n_usermeta(robj);
		for(riak_uint32_t i=0; i<n_meta; i++)
		{
			err = riak_object_get_usermeta(robj, &meta, i);
			if(!err)
			{
				rkey = riak_pair_get_key(meta);
				rval = riak_pair_get_value(meta);
				key = (const char*)riak_binary_data(rkey);
				if(!riak_binary_compare_string(rkey, "IS_FOLDER"))
				{
					SetIsFolder((const char*)riak_binary_data(rval));
				}
				else if(!riak_binary_compare_string(rkey, "DATA_STORAGE_TYPE"))
				{

				}
				else if(!riak_binary_compare_string(rkey, "CREATE_TIME"))
				{

				}
			}
		}

		return true;
	}

}