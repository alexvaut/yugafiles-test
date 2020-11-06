// Copyright (C) 2020 InfoVista S.A. All rights reserved.

using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace yugatests_files
{
	public interface IFileManager : IDisposable
	{
		Task Export(string filePath, string id = null, bool parallel = true);
		Task Export(IEnumerable<FileRef> files);
		Task Import(IEnumerable<FileRef> files);
		Task Import(string filePath, string id = null);

		void DisableDb();
		void RestoreDb();
	}

	public class FileRef
	{
		public string FilePath;
		public string Id;
	}
}
