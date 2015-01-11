#include "RiakFS.h"
#include "RiakFile.h"
#include "RiakFileSet.h"

#include <limits.h>

#include <messages/riak_get.h>

namespace radi
{
	RiakFS::RiakFS()
	{
		m_riak_port = "8087";
		m_riak = NULL;
		m_cfg  = NULL;
	}

	RiakFS::RiakFS(const char* server, const char* port)
	{
		if(server)
		{
			m_riak_server = server;
		}
		if(port)
		{
			m_riak_port = port;	
		}
		m_riak = NULL;
		m_cfg  = NULL;
	}

	RiakFS::~RiakFS()
	{
		Close();
	}

	void RiakFS::Release()
	{
		delete this;
	}

	bool RiakFS::Connect()
	{
		if(m_riak)
		{
			return true;
		}

		riak_error err;
		err = riak_config_new_default(&m_cfg);
		if(err)
		{
			return false;
		}

		err = riak_connection_new(m_cfg, &m_riak, m_riak_server.c_str(), m_riak_port.c_str(), NULL);
		if(err)
		{
			riak_config_free(&m_cfg);
			m_cfg = NULL;
			return false;
		}

		return true;
	}

	void RiakFS::Close()
	{
		if(m_riak)
		{
			riak_connection_free(&m_riak);
			m_riak = NULL;
		}
		if(m_cfg)
		{
			riak_config_free(&m_cfg);
			m_cfg = NULL;	
		}
	}

	void RiakFS::SetServer(const char* server)
	{
		if(server)
		{
			m_riak_server = server;
		}
	}

	void RiakFS::SetPort(const char* port)
	{
		if(port)
		{
			m_riak_port = port;	
		}
	}

	riak_connection* RiakFS::GetConnection()
	{
		if(m_riak)
		{
			if(!Connect())
			{
				return NULL;
			}
		}

		return m_riak;
	}

	RiakFile* RiakFS::GetRoot()
	{
		riak_connection *cxn = GetConnection();
		if(cxn==NULL)
		{
			return NULL;
		}

		riak_error err;
		riak_binary *bucket_type_bin = NULL;
		riak_get_response *key_response = NULL;
		riak_binary *bucket_bin = riak_binary_copy_from_string(m_cfg, "rfs"); 
		riak_binary *key_bin       = riak_binary_copy_from_string(m_cfg, "root");
		riak_get_options *key_options = riak_get_options_new(m_cfg);

		err = riak_get(cxn, bucket_type_bin, bucket_bin, key_bin, key_options, &key_response);

		riak_get_options_free(m_cfg, &key_options);
		riak_binary_free(m_cfg, &key_bin);
		riak_binary_free(m_cfg, &bucket_bin);

		if(err)
		{
			printf("Error [%s]\n", riak_strerror(err));
			return NULL;
		}

		{	// print key
			char output[10240];
			riak_print_state print_state;
			riak_print_init(&print_state, output, sizeof(output));
			riak_get_response_print(&print_state, key_response);
			printf("%s\n", output);

		}

		riak_int32_t count = riak_get_get_n_content(key_response);
		if(!count)
		{
			return NULL;
		}

		riak_object** robjs = riak_get_get_content(key_response);
		riak_object* robj = robjs[0];


		

		RiakFile* rf = new RiakFile();
		rf->Create(m_riak, m_cfg, robj);


		riak_get_response_free(m_cfg, &key_response);

		return rf;
	}

	RiakFile* RiakFS::GetRiakFile(const char* bucket, const char* key)
	{
		riak_binary *bucket_bin = riak_binary_copy_from_string(m_cfg, bucket); 
		riak_binary *key_bin       = riak_binary_copy_from_string(m_cfg, key);
		
		RiakFile* rf = GetRiakFile(bucket_bin, key_bin);

		riak_binary_free(m_cfg, &key_bin);
		riak_binary_free(m_cfg, &bucket_bin);
		return rf;
	}

	RiakFile* RiakFS::GetRiakFile(riak_binary* bucket, riak_binary* key)
	{
		riak_connection *cxn = GetConnection();
		if(cxn==NULL)
		{
			return NULL;
		}

		riak_error err;
		riak_get_response *robj_response = NULL;
		riak_get_options *key_options = riak_get_options_new(m_cfg);

		err = riak_get(cxn, NULL, bucket, key, key_options, &robj_response);
		riak_get_options_free(m_cfg, &key_options);
		if(err)
		{
			printf("Error [%s]\n", riak_strerror(err));
			return NULL;
		}

		// {	// print key
		// 	char output[10240];
		// 	riak_print_state print_state;
		// 	riak_print_init(&print_state, output, sizeof(output));
		// 	riak_get_response_print(&print_state, robj_response);
		// 	printf("%s\n", output);

		// }

		riak_int32_t count = riak_get_get_n_content(robj_response);
		if(!count)
		{
			riak_get_response_free(m_cfg, &robj_response);
			return NULL;
		}

		riak_object** robjs = riak_get_get_content(robj_response);
		riak_object* robj = robjs[0];

		RiakFile* rf = new RiakFile();
		rf->Create(m_riak, m_cfg, robj);

		riak_get_response_free(m_cfg, &robj_response);

		return rf;
	}

	RiakFile* RiakFS::GetRiakFile(const char* bucket, const char* parent_key, const char* name)
	{
		riak_get_response *p_robj_response = NULL;
		p_robj_response = GetRiakObjects(bucket, parent_key);
		if(p_robj_response==NULL)
		{
			return NULL;
		}

		riak_int32_t count = riak_get_get_n_content(p_robj_response);
		if(!count)
		{
			riak_get_response_free(m_cfg, &p_robj_response);
			return NULL;
		}
		riak_object** robjs = riak_get_get_content(p_robj_response);
		riak_object* robj = robjs[0];

		RiakFile* prFile = NULL;
		riak_binary* rbucket =  NULL;
		riak_binary* rl_key = NULL;
		riak_binary* rl_tag = NULL;
		riak_link* rlink = NULL;
		riak_error err;

		rbucket = riak_binary_copy_from_string(m_cfg, bucket);

		riak_int32_t nlinks = riak_object_get_n_links(robj);
		for(riak_uint32_t i=0; i<nlinks; i++)
		{
			err = riak_object_get_link(robj, &rlink, i);
			if(!err)
			{
				rl_tag = riak_link_get_tag(rlink);
				if(!riak_binary_compare_string(rl_tag, "parent"))
				{
					rl_key = riak_link_get_key(rlink);

					prFile = GetRiakFile(rbucket, rl_key);
					if(prFile != NULL)
					{
						if(!strcmp(name,prFile->GetName()))
						{
							break;
						}
					}
				}
			}
		}

		riak_binary_free(m_cfg, &rbucket);
		riak_get_response_free(m_cfg, &p_robj_response);
		
		return prFile;
	}

	RiakFileSet* RiakFS::ListFiles(const char* dir_key)
	{
		RiakFileSet* files = new RiakFileSet();

		const char* bucket = "rfs";
		riak_get_response *p_robj_response = NULL;
		p_robj_response = GetRiakObjects(bucket, dir_key);
		if(p_robj_response==NULL)
		{
			return NULL;
		}

		riak_int32_t count = riak_get_get_n_content(p_robj_response);
		if(!count)
		{
			riak_get_response_free(m_cfg, &p_robj_response);
			return NULL;
		}
		riak_object** robjs = riak_get_get_content(p_robj_response);
		riak_object* robj = robjs[0];

		RiakFile* prFile = NULL;
		riak_binary* rbucket =  NULL;
		riak_binary* rl_key = NULL;
		riak_binary* rl_tag = NULL;
		riak_link* rlink = NULL;
		riak_error err;

		rbucket = riak_binary_copy_from_string(m_cfg, bucket);

		riak_int32_t nlinks = riak_object_get_n_links(robj);
		for(riak_uint32_t i=0; i<nlinks; i++)
		{
			err = riak_object_get_link(robj, &rlink, i);
			if(!err)
			{
				rl_tag = riak_link_get_tag(rlink);
				if(!riak_binary_compare_string(rl_tag, "parent"))
				{
					rl_key = riak_link_get_key(rlink);

					prFile = GetRiakFile(rbucket, rl_key);
					if(prFile != NULL)
					{
						files->Add(prFile);
					}
				}
			}
		}

		riak_binary_free(m_cfg, &rbucket);
		riak_get_response_free(m_cfg, &p_robj_response);
		
		return files;
	}

	riak_get_response* RiakFS::GetRiakObjects(const char* bucket, const char* key)
	{
		riak_connection *cxn = GetConnection();
		if(cxn==NULL)
		{
			return NULL;
		}

		riak_error err;
		riak_binary *bucket_type_bin = NULL;
		riak_get_response *robj_response = NULL;
		riak_binary *bucket_bin = riak_binary_copy_from_string(m_cfg, bucket); 
		riak_binary *key_bin       = riak_binary_copy_from_string(m_cfg, key);
		riak_get_options *key_options = riak_get_options_new(m_cfg);

		err = riak_get(cxn, bucket_type_bin, bucket_bin, key_bin, key_options, &robj_response);

		riak_get_options_free(m_cfg, &key_options);
		riak_binary_free(m_cfg, &key_bin);
		riak_binary_free(m_cfg, &bucket_bin);

		return robj_response;
	}

	riak_get_response* RiakFS::GetRiakObjects(riak_binary * bucket, riak_binary * key)
	{
		riak_connection *cxn = GetConnection();
		if(cxn==NULL)
		{
			return NULL;
		}

		riak_error err;
		riak_get_response *robj_response = NULL;		
		riak_get_options *key_options = riak_get_options_new(m_cfg);

		err = riak_get(cxn, NULL, bucket, key, key_options, &robj_response);
		riak_get_options_free(m_cfg, &key_options);

		if(err)
		{
			return NULL;
		}

		return robj_response;
	}

	bool RiakFS::HasRiakObject(const char* bucket, const char* key)
	{
		riak_connection *cxn = GetConnection();
		if(cxn==NULL)
		{
			return false;
		}

		riak_error err;
		riak_binary *bucket_type_bin = NULL;
		riak_get_response *robj_response = NULL;
		riak_binary *bucket_bin = riak_binary_copy_from_string(m_cfg, bucket); 
		riak_binary *key_bin       = riak_binary_copy_from_string(m_cfg, key);
		riak_get_options *key_options = riak_get_options_new(m_cfg);

		err = riak_get(cxn, bucket_type_bin, bucket_bin, key_bin, key_options, &robj_response);

		riak_get_options_free(m_cfg, &key_options);
		riak_binary_free(m_cfg, &key_bin);
		riak_binary_free(m_cfg, &bucket_bin);

		if(err)
		{
			printf("Error [%s]\n", riak_strerror(err));
			return false;
		}

		riak_int32_t count = riak_get_get_n_content(robj_response);
		riak_get_response_free(m_cfg, &robj_response);
		return count;
	}








	//////////////////////////////////////////////////////////////////////
	void RiakFS::GetBuckets()
	{
		riak_connection *cxn = GetConnection();
		if(cxn==NULL)
		{
			return;
		}

		riak_error err;
		riak_binary *bucket_type_bin = NULL;
		riak_listbuckets_response *buckets_response = NULL;
		err = riak_listbuckets(cxn, bucket_type_bin, 10 * 1000, &buckets_response);
		if(err)
		{
			printf("[Error]:%s\n", riak_strerror(err));
		}

		char buffer[PATH_MAX];
		riak_uint32_t len=PATH_MAX;
		riak_size_t ret = 0;
		riak_uint32_t num = riak_listbuckets_get_n_buckets(buckets_response);
		riak_binary** buckets = riak_listbuckets_get_buckets(buckets_response);
		printf("---------------------------\n");
		printf("[buckets]:%d\n",num);
		for(riak_uint32_t i=0; i<num; i++)
		{
			memset(buffer,0,len);
			ret = riak_binary_print(buckets[i], buffer, len);
			printf("[%d]:\t%s\n", i, buffer);
		}

		riak_listbuckets_response_free(m_cfg, &buckets_response);
	}

}
