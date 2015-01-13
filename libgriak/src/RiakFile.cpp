#include "RiakFS.h"
#include "RiakFile.h"
#include "RiakTileStore.h"

#include <limits.h>

namespace radi
{
	RiakFile::RiakFile():
	m_isFolder(false),
	m_riak_fs(NULL),
	m_cxn(NULL),
	m_cfg(NULL),
	m_tile_store(NULL)
	{

	}

	RiakFile::~RiakFile()
	{
		if(m_tile_store!=NULL)
		{
			m_tile_store->Release();
			m_tile_store = NULL;
		}
	}

	void RiakFile::Release()
	{
		delete this;
	}

	RiakTileStore* RiakFile::GetTileStore()
	{
		if(m_isFolder)
		{
			return NULL;
		}
		if(m_tile_store==NULL)
		{
			m_tile_store = new RiakTileStore(m_name.c_str(), m_key.c_str(), m_riak_fs);
		}
		return m_tile_store;
	}

	bool RiakFile::IsFolder()
	{
		return m_isFolder;
	}

	void RiakFile::SetIsFolder(const char* val)
	{
		m_isFolder = (val[0] == 't');
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

	const char* RiakFile::GetKey()
	{
		return m_key.c_str();
	}

	void RiakFile::SetDataType(const char* val, size_t len)
	{
		m_data_type.assign(val, len);
	}

	const char* RiakFile::GetDataType() const
	{
		return m_data_type.c_str();
	}

	void RiakFile::SetDataStore(const char* val, size_t len)
	{
		m_data_store.assign(val, len);	
	}

	const char* RiakFile::GetDataStore() const
	{
		return m_data_store.c_str();
	}

	bool RiakFile::Create(RiakFS *riak_fs, riak_connection *rcxn, riak_config *rcfg, riak_object* robj)
	{		
		m_riak_fs = riak_fs;
		m_cxn = rcxn;
		m_cfg = rcfg;

		riak_binary* rkey = riak_object_get_key(robj);
		m_key.assign((const char*)riak_binary_data(rkey),riak_binary_len(rkey));

		riak_binary* rname = riak_object_get_value(robj);
		m_name.assign((const char*)riak_binary_data(rname),riak_binary_len(rname));

		riak_error err;
		riak_pair  *meta = NULL;
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
				else if(!riak_binary_compare_string(rkey, "DATA_TYPE"))
				{
					SetDataType((const char*)riak_binary_data(rval), riak_binary_len(rval));
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

	RiakFile* RiakFile::CreateFile(const char* f_name, bool is_folder, const char* data_type/*="PGIS"*/)
	{
		if(f_name==NULL || data_type==NULL)
		{
			return NULL;
		}

		return m_riak_fs->CreateFile(m_key.c_str(), f_name, is_folder, data_type);
	}

	RiakFile* RiakFile::GetFile(const char* f_name)
	{
		if(f_name==NULL)
		{
			return NULL;
		}

		riak_get_response* response = m_riak_fs->GetRiakObjects("rfs", m_key.c_str());
		if(response==NULL)
		{
			return NULL;
		}
		riak_int32_t count = riak_get_get_n_content(response);
		if(!count)
		{
			riak_get_response_free(m_cfg, &response);
			return NULL;
		}
		riak_object** robjs = riak_get_get_content(response);
		riak_object* robj = robjs[0];

		riak_error err;
		RiakFile  *rf = NULL;
		riak_link *r_link = NULL;
		riak_binary* l_key = NULL;

		riak_binary* r_bucket = riak_binary_new_shallow(m_cfg, strlen("rfs"), (riak_uint8_t*)"rfs");

		riak_int32_t n_link = riak_object_get_n_links(robj);
		for(riak_uint32_t i=0; i<n_link; i++)
		{
			err = riak_object_get_link(robj, &r_link, i);
			if(!err)
			{
				l_key = riak_link_get_key(r_link);
				rf = m_riak_fs->GetRiakFile(r_bucket, l_key);
				if(!strcmp(rf->GetName(), f_name))
				{
					break;
				}
				else
				{
					rf->Release();
					rf = NULL;
				}
			}
		}

		riak_binary_free(m_cfg, &r_bucket);
		riak_get_response_free(m_cfg, &response);

		return rf;
	}
}