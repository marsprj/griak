#include "RiakTileStore.h"
#include "RiakFS.h"
#include <stdio.h>
#include <string.h>

namespace radi
{
	RiakTileStore::RiakTileStore()
	{
		m_riak_fs = NULL;
		m_rname = NULL;
		m_rkey = NULL;
	}

	RiakTileStore::RiakTileStore(const char* name, const char* key, RiakFS* riak_fs)
	{
		if(name!=NULL)
		{
			m_name = name;
			m_rname = riak_binary_new_shallow(riak_fs->m_cfg, m_name.size(), (riak_uint8_t*)m_name.c_str());
		}
		if(key!=NULL)
		{
			m_key = key;
			m_rkey = riak_binary_new_shallow(riak_fs->m_cfg, m_key.size(), (riak_uint8_t*)m_key.c_str());
		}
		m_riak_fs = riak_fs;
	}

	RiakTileStore::~RiakTileStore()
	{
		if(m_rname!=NULL)
		{
			riak_binary_free(m_riak_fs->m_cfg, &m_rname);
			m_rname = NULL;
		}
		if(m_rname!=NULL)
		{
			riak_binary_free(m_riak_fs->m_cfg, &m_rkey);
			m_rkey = NULL;
		}
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

	void RiakTileStore::GetTile(const char* t_key)
	{
		if(t_key==NULL)
		{
			return;
		}

		riak_binary* rt_key = riak_binary_new_shallow(m_riak_fs->m_cfg, strlen(t_key), (riak_uint8_t*)t_key);

		riak_get_response* response = m_riak_fs->GetRiakObjects(m_rkey, rt_key);
		if(response==NULL)
		{
			riak_binary_free(m_riak_fs->m_cfg, &rt_key);
			return;
		}

		// {
		// 	char output[10240];
		// 	riak_print_state print_state;
		// 	riak_print_init(&print_state, output, sizeof(output));

		// 	riak_get_response_print(&print_state, response);
		// 	printf("%s\n", output);
		// }

		riak_int32_t count = riak_get_get_n_content(response);
		if(!count)
		{
			riak_binary_free(m_riak_fs->m_cfg, &rt_key);
			m_riak_fs->Release(response);
			return;
		}
		riak_object** robjs = riak_get_get_content(response);
		riak_object* robj = robjs[0];

		riak_binary* rval = riak_object_get_value(robj);

		riak_size_t size = riak_binary_len(rval);
		riak_uint8_t* data = riak_binary_data(rval);

		FILE* fp = fopen("/home/renyc/tiles/tile.png","wb+");
		fwrite(data, sizeof(riak_uint8_t), size, fp);
		fclose(fp);

		riak_binary_free(m_riak_fs->m_cfg, &rt_key);
		m_riak_fs->Release(response);
	}

	bool RiakTileStore::PutTile(const char* t_key, const unsigned char* t_data, size_t t_size, const char* img_type)
	{
		if(t_key==NULL || t_data==NULL || img_type==NULL)
		{
			return false;
		}

		riak_config *r_cfg = m_riak_fs->m_cfg;

		riak_binary* rt_key = riak_binary_new_shallow(r_cfg, strlen(t_key), (riak_uint8_t*)t_key);

		riak_object* r_obj = riak_object_new(r_cfg);
		if(r_obj==NULL)
		{
			riak_binary_free(r_cfg, &rt_key);
			return false;
		}

		// set bucket
		riak_error err = riak_object_set_bucket(r_cfg, r_obj, m_rkey);
		if(err)
		{
			riak_binary_free(r_cfg, &rt_key);
			return false;
		}

		// set key
		err = riak_object_set_key(r_cfg, r_obj, rt_key);
		if(err)
		{
			riak_binary_free(r_cfg, &rt_key);
			return false;
		}

		// set image data
		riak_binary* r_data = riak_binary_new_shallow(r_cfg, t_size, (riak_uint8_t*)t_data);
		if(r_data==NULL)
		{
			riak_binary_free(r_cfg, &rt_key);
			riak_object_free(r_cfg, &r_obj);			
			return false;
		}
		err = riak_object_set_value_shallow_copy(r_cfg, r_obj, r_data);
		if(err)
		{
			riak_binary_free(r_cfg, &r_data);
			riak_binary_free(r_cfg, &rt_key);
			riak_object_free(r_cfg, &r_obj);
			return false;			
		}

		// set content type
		riak_binary* r_content_type = riak_binary_new_shallow(r_cfg, strlen(img_type), (riak_uint8_t*)img_type);
		if(r_content_type==NULL)
		{
			riak_binary_free(r_cfg, &r_data);
			riak_binary_free(r_cfg, &rt_key);
			riak_object_free(r_cfg, &r_obj);
			return false;
		}
		err = riak_object_set_content_type(r_cfg, r_obj, r_content_type);
		if(err)
		{
			riak_binary_free(r_cfg, &r_content_type);
			riak_binary_free(r_cfg, &r_data);
			riak_binary_free(r_cfg, &rt_key);
			riak_object_free(r_cfg, &r_obj);
			return false;			
		}

		riak_put_response *put_response = NULL;
		riak_put_options *put_options = riak_put_options_new(r_cfg);
		if(put_options==NULL)
		{
			riak_binary_free(r_cfg, &r_data);
			riak_binary_free(r_cfg, &rt_key);
			riak_object_free(r_cfg, &r_obj);
			return false;			
		}
		riak_put_options_set_return_head(put_options, RIAK_TRUE);
		riak_put_options_set_return_body(put_options, RIAK_TRUE);
		err = riak_put(m_riak_fs->m_riak, r_obj, put_options, &put_response);

		if(!err)
		{
			char output[10240];
			riak_print_state print_state;
    			riak_print_init(&print_state, output, sizeof(output));

			riak_put_response_print(&print_state, put_response);
                		printf("%s\n", output);
		}

		riak_put_options_free(r_cfg, &put_options);
		riak_put_response_free(r_cfg, &put_response);
		riak_binary_free(r_cfg, &r_data);		
		riak_binary_free(r_cfg, &rt_key);
		riak_object_free(r_cfg, &r_obj);
		return (!err);
	}

	bool RiakTileStore::PutTile(const char* t_key, const char* t_path)
	{
		if(t_key==NULL||t_path==NULL)
		{
			return false;
		}

		const char* img_type = "image/png";

		FILE*fp= fopen(t_path, "rb");
		if(fp==NULL)
		{
			return false;
		}

		fseek(fp,0,SEEK_SET);
		fseek(fp,0,SEEK_END);
		
		size_t t_size = ftell(fp);
		unsigned char* t_data = (unsigned char*)malloc(t_size);
		memset(t_data, 0, t_size);

		fseek(fp,0,SEEK_SET);
		size_t nread = fread(t_data, sizeof(unsigned char), t_size, fp);
		if(nread < t_size)
		{
			free(t_data);
			fclose(fp);
			return false;
		}

		bool ret = PutTile(t_key, t_data, t_size, img_type);

		free(t_data);
		fclose(fp);
		return ret;
	}
}