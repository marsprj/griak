#include "RiakFS.h"
#include "RiakFile.h"
#include "RiakFileSet.h"

#include <messages/riak_get.h>

#include <limits.h>
#include <sys/time.h>
#include <uuid/uuid.h>

namespace radi
{
	long get_current_time_millis()
	{
		struct timeval tv;
		gettimeofday(&tv,NULL);
		return tv.tv_sec * 1000 + tv.tv_usec / 1000;
	}

	void griak_uuid_generate(char* uuid, size_t size)
	{
		uuid_t uu;
		uuid_generate( uu );
		uuid_unparse(uu, uuid);
	}

	RiakFS::RiakFS()
	{
		m_riak_port = "8087";
		m_riak = NULL;
		m_cfg  = NULL;
		m_rfs_bucket = NULL;
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
		m_rfs_bucket = NULL;
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
		m_rfs_bucket = riak_binary_new_shallow(m_cfg, strlen("rfs"), (riak_uint8_t*)"rfs");

		return true;
	}

	void RiakFS::Close()
	{
		if(m_rfs_bucket!=NULL)
		{
			riak_binary_free(m_cfg, &m_rfs_bucket);
			m_rfs_bucket = NULL;
		}

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

		// {	// print key
		// 	char output[10240];
		// 	riak_print_state print_state;
		// 	riak_print_init(&print_state, output, sizeof(output));
		// 	riak_get_response_print(&print_state, key_response);
		// 	printf("%s\n", output);

		// }

		riak_int32_t count = riak_get_get_n_content(key_response);
		if(!count)
		{
			return NULL;
		}

		riak_object** robjs = riak_get_get_content(key_response);
		riak_object* robj = robjs[0];


		

		RiakFile* rf = new RiakFile();
		rf->Create(this, m_riak, m_cfg, robj);


		riak_get_response_free(m_cfg, &key_response);

		return rf;
	}

	RiakFile* RiakFS::GetFile(const char* path)
	{
		if(path==NULL)
		{
			return NULL;
		}
		if(path[0] != '/')
		{
			return NULL;
		}

		RiakFile* rr = NULL;
		RiakFile* rf = NULL;

		rr = GetRoot();
		if(rr == NULL)
		{
			return NULL;
		}

		char* d_path = strdup(path);
		char* fname = NULL;
		fname = strtok(d_path, "/");
		while(fname != NULL)
		{
			rf = rr->GetFile(fname);
			rr->Release();

			if(rf == NULL)
			{
				break;
			}

			rr = rf;

			fname = strtok(NULL, "/");
		}

		free(d_path);

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

		riak_int32_t count = riak_get_get_n_content(robj_response);
		if(!count)
		{
			riak_get_response_free(m_cfg, &robj_response);
			return NULL;
		}

		riak_object** robjs = riak_get_get_content(robj_response);
		riak_object* robj = robjs[0];

		RiakFile* rf = new RiakFile();
		rf->Create(this, m_riak, m_cfg, robj);

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

	void RiakFS::Release(riak_get_response* response)
	{
		if(response != NULL)
		{
			riak_get_response_free(m_cfg, &response);	
		}
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


	RiakFile* RiakFS::CreateFile(const char* parent_key, const char* f_name, bool is_folder, const char* data_type/*="PGIS"*/)
	{
		if(parent_key==NULL || f_name==NULL)
		{
			return NULL;
		}

		// get parent objects
		riak_get_response* p_robj_response = NULL;
		p_robj_response = GetRiakObjects("rfs", parent_key);
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
		riak_object** p_robjs = riak_get_get_content(p_robj_response);
		riak_object* p_robj = p_robjs[0];

		// create riak file object
		char f_key[PATH_MAX];
		griak_uuid_generate(f_key, PATH_MAX);
		bool ret = CreateRiakFile(f_name, f_key, is_folder, data_type);
		if(!ret)
		{
			riak_get_response_free(m_cfg, &p_robj_response);
			return NULL;
		}

		AddLink(p_robj, "rfs", f_key, "parent");
		riak_get_response_free(m_cfg, &p_robj_response);

		return GetRiakFile("rfs", f_key);
	}

	bool RiakFS::AddLink(riak_object* r_obj, const char* bucket, const char* key, const char* tag)
	{
		// set bucket
		riak_binary* r_bucket = riak_binary_new_shallow(m_cfg, strlen(bucket), (riak_uint8_t*)bucket);
		riak_link* r_link = riak_object_new_link(m_cfg, r_obj);
		riak_link_set_bucket(m_cfg, r_link, r_bucket);

		// set key
		riak_binary* r_key = riak_binary_new_shallow(m_cfg, strlen(key), (riak_uint8_t*)key);
		riak_link_set_key(m_cfg, r_link, r_key);

		// set tag
		riak_binary* r_parent = riak_binary_new_shallow(m_cfg, strlen(tag), (riak_uint8_t*)tag);
		riak_link_set_tag(m_cfg, r_link, r_parent);		

		//-------------------------------------------------------------------------------
		// put file
		//-------------------------------------------------------------------------------
		riak_put_response *put_response = NULL;
		riak_put_options *put_options = riak_put_options_new(m_cfg);
		if(put_options==NULL)
		{
			riak_binary_free(m_cfg, &r_key);
			riak_binary_free(m_cfg, &r_bucket);			
			riak_binary_free(m_cfg, &r_parent);
			return false;			
		}
		riak_put_options_set_return_head(put_options, RIAK_TRUE);
		riak_put_options_set_return_body(put_options, RIAK_TRUE);
		riak_error err = riak_put(m_riak, r_obj, put_options, &put_response);

		if(!err)
		{
			char output[10240];
			riak_print_state print_state;
		  	riak_print_init(&print_state, output, sizeof(output));

		              printf("%s\n", output);
		}

		riak_put_options_free(m_cfg, &put_options);
		riak_put_response_free(m_cfg, &put_response);

		riak_binary_free(m_cfg, &r_key);
		riak_binary_free(m_cfg, &r_bucket);		
		riak_binary_free(m_cfg, &r_parent);

		return (!err);
	}

	bool RiakFS::CreateRiakFile(const char* f_name, const char* f_key,  bool is_folder, const char* data_type)
	{
		riak_binary* rf_name = NULL;
		riak_binary* rf_key = NULL;

		riak_object* r_obj = riak_object_new(m_cfg);
		if(r_obj==NULL)
		{
			return false;
		}

		// set bucket
		riak_error err = riak_object_set_bucket(m_cfg, r_obj, m_rfs_bucket);
		if(err)
		{
			riak_object_free(m_cfg, &r_obj);
			return false;
		}

		rf_name = riak_binary_new_shallow(m_cfg, strlen(f_name), (riak_uint8_t*)f_name);
		rf_key = riak_binary_new_shallow(m_cfg, strlen(f_key), (riak_uint8_t*)f_key);
		
		// set key
		err = riak_object_set_key(m_cfg, r_obj, rf_key);
		if(err)
		{
			riak_binary_free(m_cfg, &rf_name);
			riak_binary_free(m_cfg, &rf_key);
			riak_object_free(m_cfg, &r_obj);
			return false;
		}

		// set file name
		err = riak_object_set_value_shallow_copy(m_cfg, r_obj, rf_name);
		if(err)
		{
			riak_binary_free(m_cfg, &rf_name);
			riak_binary_free(m_cfg, &rf_key);
			riak_object_free(m_cfg, &r_obj);
			return false;			
		}

		// set content type
		const char* content_type = "text/plain";
		riak_binary* r_content_type = riak_binary_new_shallow(m_cfg, strlen(content_type), (riak_uint8_t*)content_type);
		if(r_content_type==NULL)
		{
			riak_binary_free(m_cfg, &r_content_type);
			riak_binary_free(m_cfg, &rf_name);
			riak_binary_free(m_cfg, &rf_key);
			riak_object_free(m_cfg, &r_obj);
			return false;
		}
		err = riak_object_set_content_type(m_cfg, r_obj, r_content_type);
		if(err)
		{
			riak_binary_free(m_cfg, &r_content_type);
			riak_binary_free(m_cfg, &rf_name);
			riak_binary_free(m_cfg, &rf_key);
			riak_object_free(m_cfg, &r_obj);
			return false;			
		}

		// set charset
		const char* charset = "UTF-8";
		riak_binary* r_charset = riak_binary_new_shallow(m_cfg, strlen(charset), (riak_uint8_t*)charset);
		if(r_charset==NULL)
		{
			riak_binary_free(m_cfg, &r_charset);
			riak_binary_free(m_cfg, &r_content_type);
			riak_binary_free(m_cfg, &rf_name);
			riak_binary_free(m_cfg, &rf_key);
			riak_object_free(m_cfg, &r_obj);
			return false;
		}
		err = riak_object_set_charset(m_cfg, r_obj, r_charset);
		if(err)
		{
			riak_binary_free(m_cfg, &r_charset);
			riak_binary_free(m_cfg, &r_content_type);
			riak_binary_free(m_cfg, &rf_name);
			riak_binary_free(m_cfg, &rf_key);
			riak_object_free(m_cfg, &r_obj);
			return false;	
		}

		//---------------------------------------------------------------------------------------------------
		// [meta_data]: set is_folder
		//---------------------------------------------------------------------------------------------------
		riak_pair* r_pair = riak_object_new_usermeta(m_cfg, r_obj);
		if(r_pair==NULL)
		{
			riak_binary_free(m_cfg, &r_charset);
			riak_binary_free(m_cfg, &r_content_type);
			riak_binary_free(m_cfg, &rf_name);
			riak_binary_free(m_cfg, &rf_key);
			riak_object_free(m_cfg, &r_obj);
			return false;
		}
		const char* is_folder_key = "IS_FOLDER";
		riak_binary* r_is_folder_key = riak_binary_new_shallow(m_cfg, strlen(is_folder_key), (riak_uint8_t*)is_folder_key);
		err = riak_pair_set_key(m_cfg, r_pair, r_is_folder_key);
		if(err)
		{
			riak_binary_free(m_cfg, &r_is_folder_key);
			riak_binary_free(m_cfg, &r_charset);
			riak_binary_free(m_cfg, &r_content_type);
			riak_binary_free(m_cfg, &rf_name);
			riak_binary_free(m_cfg, &rf_key);
			riak_object_free(m_cfg, &r_obj);
			return false;
		}
		const char* s_is_folder = is_folder?"true":"false";
		riak_binary* r_is_folder = riak_binary_new_shallow(m_cfg, strlen(s_is_folder), (riak_uint8_t*)s_is_folder);
		if(r_is_folder==NULL)
		{
			riak_binary_free(m_cfg, &r_is_folder_key);
			riak_binary_free(m_cfg, &r_is_folder);
			riak_binary_free(m_cfg, &r_charset);
			riak_binary_free(m_cfg, &r_content_type);
			riak_binary_free(m_cfg, &rf_name);
			riak_binary_free(m_cfg, &rf_key);
			riak_object_free(m_cfg, &r_obj);
			return false;
		}
		err = riak_pair_set_value(m_cfg, r_pair, r_is_folder);
		if(err)
		{
			riak_binary_free(m_cfg, &r_is_folder_key);
			riak_binary_free(m_cfg, &r_is_folder);
			riak_binary_free(m_cfg, &r_charset);
			riak_binary_free(m_cfg, &r_content_type);
			riak_binary_free(m_cfg, &rf_name);
			riak_binary_free(m_cfg, &rf_key);	
			riak_object_free(m_cfg, &r_obj);
			return false;
		}

		//---------------------------------------------------------------------------------------------------
		// [meta_data]: set storage type
		//---------------------------------------------------------------------------------------------------
		r_pair = riak_object_new_usermeta(m_cfg, r_obj);
		if(r_pair==NULL)
		{
			riak_binary_free(m_cfg, &r_is_folder_key);
			riak_binary_free(m_cfg, &r_is_folder);
			riak_binary_free(m_cfg, &r_charset);
			riak_binary_free(m_cfg, &r_content_type);
			riak_binary_free(m_cfg, &rf_name);
			riak_binary_free(m_cfg, &rf_key);	
			riak_object_free(m_cfg, &r_obj);
			return false;
		}
		const char* storage_key = "DATA_STORAGE_TYPE";
		riak_binary* r_storage_key = riak_binary_new_shallow(m_cfg, strlen(storage_key), (riak_uint8_t*)storage_key);
		err = riak_pair_set_key(m_cfg, r_pair, r_storage_key);
		if(err)
		{
			riak_binary_free(m_cfg, &r_storage_key);
			riak_binary_free(m_cfg, &r_is_folder_key);
			riak_binary_free(m_cfg, &r_is_folder);
			riak_binary_free(m_cfg, &r_charset);
			riak_binary_free(m_cfg, &r_content_type);
			riak_binary_free(m_cfg, &rf_name);
			riak_binary_free(m_cfg, &rf_key);	
			riak_object_free(m_cfg, &r_obj);
			return false;
		}
		const char* storage_type = is_folder ? "VALUE" : "BUCKET";
		riak_binary* r_storage = riak_binary_new_shallow(m_cfg, strlen(storage_type), (riak_uint8_t*)storage_type);
		if(r_storage==NULL)
		{
			riak_binary_free(m_cfg, &r_storage_key);
			riak_binary_free(m_cfg, &r_storage);

			riak_binary_free(m_cfg, &r_is_folder_key);
			riak_binary_free(m_cfg, &r_is_folder);
			riak_binary_free(m_cfg, &r_charset);
			riak_binary_free(m_cfg, &r_content_type);
			riak_binary_free(m_cfg, &rf_name);
			riak_binary_free(m_cfg, &rf_key);	
			riak_object_free(m_cfg, &r_obj);
			return false;
		}
		err = riak_pair_set_value(m_cfg, r_pair, r_storage);
		if(err)
		{
			riak_binary_free(m_cfg, &r_storage_key);
			riak_binary_free(m_cfg, &r_storage);

			riak_binary_free(m_cfg, &r_is_folder_key);
			riak_binary_free(m_cfg, &r_is_folder);
			riak_binary_free(m_cfg, &r_charset);
			riak_binary_free(m_cfg, &r_content_type);
			riak_binary_free(m_cfg, &rf_name);
			riak_binary_free(m_cfg, &rf_key);	
			riak_object_free(m_cfg, &r_obj);
			return false;
		}

		//---------------------------------------------------------------------------------------------------
		// [meta_data]: set FILE_NAME
		//---------------------------------------------------------------------------------------------------
		r_pair = riak_object_new_usermeta(m_cfg, r_obj);
		const char* fname_key = "FILE_NAME";
		riak_binary* r_fname_key = riak_binary_new_shallow(m_cfg, strlen(fname_key), (riak_uint8_t*)fname_key);
		err = riak_pair_set_key(m_cfg, r_pair, r_fname_key);
		err = riak_pair_set_value(m_cfg, r_pair, rf_name);

		//---------------------------------------------------------------------------------------------------
		// [meta_data]: set DESCRIBE
		//---------------------------------------------------------------------------------------------------
		r_pair = riak_object_new_usermeta(m_cfg, r_obj);
		const char* describe_key = "DESCRIBE";
		riak_binary* r_describe_key = riak_binary_new_shallow(m_cfg, strlen(describe_key), (riak_uint8_t*)describe_key);
		err = riak_pair_set_key(m_cfg, r_pair, r_describe_key);
		err = riak_pair_set_value(m_cfg, r_pair, rf_name);

		//---------------------------------------------------------------------------------------------------
		// [meta_data]: set CRATE_TIME
		//---------------------------------------------------------------------------------------------------
		char c_time[PATH_MAX] = {0};
		sprintf(c_time, "%ld", get_current_time_millis());
		r_pair = riak_object_new_usermeta(m_cfg, r_obj);
		const char* create_time_key = "CREATE_TIME";
		riak_binary* r_create_time_key = riak_binary_new_shallow(m_cfg, strlen(create_time_key), (riak_uint8_t*)create_time_key);
		err = riak_pair_set_key(m_cfg, r_pair, r_create_time_key);
		riak_binary* r_create_time = riak_binary_new_shallow(m_cfg, strlen(c_time), (riak_uint8_t*)c_time);
		err = riak_pair_set_value(m_cfg, r_pair, r_create_time);

		//---------------------------------------------------------------------------------------------------
		// [meta_data]: set MODIFY_TIME
		//---------------------------------------------------------------------------------------------------
		sprintf(c_time, "%ld", get_current_time_millis());
		r_pair = riak_object_new_usermeta(m_cfg, r_obj);
		const char* modify_time_key = "MODIFY_TIME";
		riak_binary* r_modify_time_key = riak_binary_new_shallow(m_cfg, strlen(modify_time_key), (riak_uint8_t*)modify_time_key);
		err = riak_pair_set_key(m_cfg, r_pair, r_modify_time_key);
		riak_binary* r_modify_time = riak_binary_new_shallow(m_cfg, strlen(c_time), (riak_uint8_t*)c_time);
		err = riak_pair_set_value(m_cfg, r_pair, r_modify_time);

		riak_binary* r_status_key = NULL;
		riak_binary* r_status = NULL;
		riak_binary* r_size_key = NULL;
		riak_binary* r_size = NULL;
		riak_binary* r_data_type_key = NULL;
		riak_binary* r_data_type = NULL;
		if(!is_folder)
		{
			//---------------------------------------------------------------------------------------------------
			// [meta_data]: set STATUS
			//---------------------------------------------------------------------------------------------------
			r_pair = riak_object_new_usermeta(m_cfg, r_obj);
			const char* status_key = "STATUS";
			r_status_key = riak_binary_new_shallow(m_cfg, strlen(status_key), (riak_uint8_t*)status_key);
			err = riak_pair_set_key(m_cfg, r_pair, r_status_key);
			const char* status = "COMPLETED";
			r_status = riak_binary_new_shallow(m_cfg, strlen(status), (riak_uint8_t*)status);
			err = riak_pair_set_value(m_cfg, r_pair, r_status);

			//---------------------------------------------------------------------------------------------------
			// [meta_data]: set SIZE
			//---------------------------------------------------------------------------------------------------
			r_pair = riak_object_new_usermeta(m_cfg, r_obj);
			const char* size_key = "SIZE";
			r_size_key = riak_binary_new_shallow(m_cfg, strlen(size_key), (riak_uint8_t*)size_key);
			err = riak_pair_set_key(m_cfg, r_pair, r_size_key);
			const char* ssize = "0";
			r_size = riak_binary_new_shallow(m_cfg, strlen(ssize), (riak_uint8_t*)ssize);
			err = riak_pair_set_value(m_cfg, r_pair, r_size);

			//---------------------------------------------------------------------------------------------------
			// [meta_data]: set DATA_TYPE
			//---------------------------------------------------------------------------------------------------
			if(data_type!=NULL)
			{
				r_pair = riak_object_new_usermeta(m_cfg, r_obj);
				const char* data_type_key = "DATA_TYPE";
				r_data_type_key = riak_binary_new_shallow(m_cfg, strlen(data_type_key), (riak_uint8_t*)data_type_key);
				err = riak_pair_set_key(m_cfg, r_pair, r_data_type_key);
				r_data_type = riak_binary_new_shallow(m_cfg, strlen(data_type), (riak_uint8_t*)data_type);
				err = riak_pair_set_value(m_cfg, r_pair, r_data_type);	
			}
			
		}
		
		// put file
		riak_put_response *put_response = NULL;
		riak_put_options *put_options = riak_put_options_new(m_cfg);
		if(put_options==NULL)
		{
			if(r_status_key!=NULL)
			{
				riak_binary_free(m_cfg, &r_status_key);
				riak_binary_free(m_cfg, &r_status);
			}
			if(r_size_key!=NULL)
			{
				riak_binary_free(m_cfg, &r_size_key);
				riak_binary_free(m_cfg, &r_size);
			}
			if(r_data_type_key!=NULL)
			{
				riak_binary_free(m_cfg, &r_data_type_key);
				riak_binary_free(m_cfg, &r_data_type);
			}

			riak_binary_free(m_cfg, &r_modify_time_key);
			riak_binary_free(m_cfg, &r_modify_time);

			riak_binary_free(m_cfg, &r_create_time_key);
			riak_binary_free(m_cfg, &r_create_time);

			riak_binary_free(m_cfg, &r_describe_key);
			riak_binary_free(m_cfg, &r_fname_key);

			riak_binary_free(m_cfg, &r_storage_key);
			riak_binary_free(m_cfg, &r_storage);

			riak_binary_free(m_cfg, &r_is_folder_key);
			riak_binary_free(m_cfg, &r_is_folder);
			riak_binary_free(m_cfg, &r_content_type);
			riak_binary_free(m_cfg, &rf_name);
			riak_binary_free(m_cfg, &rf_key);
			riak_object_free(m_cfg, &r_obj);
			return false;			
		}
		riak_put_options_set_return_head(put_options, RIAK_TRUE);
		riak_put_options_set_return_body(put_options, RIAK_TRUE);
		err = riak_put(m_riak, r_obj, put_options, &put_response);

		// if(!err)
		// {
		// 	char output[10240];
		// 	riak_print_state print_state;
		//   	riak_print_init(&print_state, output, sizeof(output));

		// 	riak_put_response_print(&print_state, put_response);
		//               printf("%s\n", output);
		// }

		riak_put_options_free(m_cfg, &put_options);
		riak_put_response_free(m_cfg, &put_response);

		if(r_status_key!=NULL)
		{
			riak_binary_free(m_cfg, &r_status_key);
			riak_binary_free(m_cfg, &r_status);
		}
		if(r_size_key!=NULL)
		{
			riak_binary_free(m_cfg, &r_size_key);
			riak_binary_free(m_cfg, &r_size);
		}

		riak_binary_free(m_cfg, &r_modify_time_key);
		riak_binary_free(m_cfg, &r_modify_time);

		riak_binary_free(m_cfg, &r_create_time_key);
		riak_binary_free(m_cfg, &r_create_time);

		riak_binary_free(m_cfg, &r_describe_key);
		riak_binary_free(m_cfg, &r_fname_key);

		riak_binary_free(m_cfg, &r_storage_key);
		riak_binary_free(m_cfg, &r_storage);

		riak_binary_free(m_cfg, &r_is_folder_key);
		riak_binary_free(m_cfg, &r_is_folder);
		riak_binary_free(m_cfg, &r_charset);
		riak_binary_free(m_cfg, &r_content_type);
		riak_binary_free(m_cfg, &rf_name);
		riak_binary_free(m_cfg, &rf_key);
		riak_object_free(m_cfg, &r_obj);

		return true;
	}

	RiakFile* RiakFS::Delete(RiakFile* file)
	{
		if(file==NULL)
		{
			return NULL;
		}

		if(file->IsRoot())
		{
			return NULL;
		}

		riak_get_response* robj_response = NULL;
		robj_response = GetRiakObjects("rfs", file->GetKey());
		if(robj_response==NULL)
		{
			return NULL;
		}
		riak_int32_t count = riak_get_get_n_content(robj_response);
		if(!count)
		{
			riak_get_response_free(m_cfg, &robj_response);
			return NULL;
		}
		riak_object** robjs = riak_get_get_content(robj_response);
		riak_object* r_obj = robjs[0];

		riak_error err;
		riak_pair* r_pair = NULL;
		riak_binary* r_status_key = NULL;
		riak_binary* r_status = NULL;

		//---------------------------------------------------------------------------------------------------
		// [meta_data]: set STATUS
		//---------------------------------------------------------------------------------------------------
		const char* status_key = "STATUS";
		r_pair = GetUserMeta(r_obj, status_key);
		if(r_pair==NULL)
		{
			r_pair = riak_object_new_usermeta(m_cfg, r_obj);
		}		
		r_status_key = riak_binary_new_shallow(m_cfg, strlen(status_key), (riak_uint8_t*)status_key);
		err = riak_pair_set_key(m_cfg, r_pair, r_status_key);
		const char* status = "DELETED";
		r_status = riak_binary_new_shallow(m_cfg, strlen(status), (riak_uint8_t*)status);
		err = riak_pair_set_value(m_cfg, r_pair, r_status);

		//---------------------------------------------------------------------------------------------------
		// [meta_data]: set MODIFY_TIME
		//---------------------------------------------------------------------------------------------------
		char c_time[PATH_MAX];
		sprintf(c_time, "%ld", get_current_time_millis());
		const char* modify_time_key = "MODIFY_TIME";
		r_pair = GetUserMeta(r_obj, modify_time_key);
		if(r_pair==NULL)
		{
			r_pair = riak_object_new_usermeta(m_cfg, r_obj);
		}
		
		riak_binary* r_modify_time_key = riak_binary_new_shallow(m_cfg, strlen(modify_time_key), (riak_uint8_t*)modify_time_key);
		err = riak_pair_set_key(m_cfg, r_pair, r_modify_time_key);
		riak_binary* r_modify_time = riak_binary_new_shallow(m_cfg, strlen(c_time), (riak_uint8_t*)c_time);
		err = riak_pair_set_value(m_cfg, r_pair, r_modify_time);

		// put riak_object
		riak_put_response *put_response = NULL;
		riak_put_options *put_options = riak_put_options_new(m_cfg);
		if(put_options==NULL)
		{
			
			riak_put_options_free(m_cfg, &put_options);
			riak_put_response_free(m_cfg, &put_response);

			riak_binary_free(m_cfg, &r_status_key); 
			riak_binary_free(m_cfg, &r_status);
			riak_binary_free(m_cfg, &r_modify_time_key);
			riak_binary_free(m_cfg, &r_modify_time);

			riak_get_response_free(m_cfg, &robj_response);
			return NULL;
		}
		riak_put_options_set_return_head(put_options, RIAK_TRUE);
		riak_put_options_set_return_body(put_options, RIAK_TRUE);
		err = riak_put(m_riak, r_obj, put_options, &put_response);
		if(err)
		{
			printf("[Error]:%s\n", riak_strerror(err));
		}

		riak_put_options_free(m_cfg, &put_options);
		riak_put_response_free(m_cfg, &put_response);

		riak_binary_free(m_cfg, &r_status_key); 
		riak_binary_free(m_cfg, &r_status);
		riak_binary_free(m_cfg, &r_modify_time_key);
		riak_binary_free(m_cfg, &r_modify_time);

		riak_get_response_free(m_cfg, &robj_response);

		file->SetStatus(RIAK_FILE_STATUS_DELETED);

		// add to trash
		MoveToTrash(file->GetKey());

		return file;
	}

	riak_pair* RiakFS::GetUserMeta(riak_object* r_obj, const char* m_key)
	{
		riak_pair* r_pair = NULL;
		riak_binary* r_key = NULL;

		riak_error err;
		riak_int32_t n_pair = riak_object_get_n_usermeta(r_obj);

		for(riak_int32_t i=0; i<n_pair; i++)
		{
			err = riak_object_get_usermeta(r_obj, &r_pair, i);
			if(!err)
			{
				r_key = riak_pair_get_key(r_pair);
				if(!riak_binary_compare_string(r_key, m_key))
				{
					return r_pair;
				}
			}
		}
		

		return NULL;
	}
		

	bool RiakFS::MoveToTrash(const char* f_key)
	{
		if(f_key==NULL)
		{
			return false;
		}

		riak_get_response* robj_response = NULL;
		robj_response = GetRiakObjects("rfs", f_key);
		if(robj_response==NULL)
		{
			return NULL;
		}
		riak_int32_t count = riak_get_get_n_content(robj_response);
		if(!count)
		{
			riak_get_response_free(m_cfg, &robj_response);
			return NULL;
		}
		riak_object** robjs = riak_get_get_content(robj_response);
		riak_object* r_obj = robjs[0];

		AddLink(r_obj, "rfs", f_key, "parent");

		riak_get_response_free(m_cfg, &robj_response);

		return true;
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
