#ifndef __GRIAK_FILE_SET_H__
#define __GRIAK_FILE_SET_H__

#include <vector>

namespace radi
{
	class RiakFile;

	class RiakFileSet
	{
	public:
		RiakFileSet();
		virtual ~RiakFileSet();
	public:
		unsigned int 	GetCount();
		RiakFile*	GetRiakFile(unsigned int i);
		void		Add(RiakFile* pFile);
		void		Release();

	private:
		std::vector<RiakFile*> m_files;
	};
}

#endif //__GRIAK_FILE_SET_H__