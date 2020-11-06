// Copyright (C) 2020 InfoVista S.A. All rights reserved.
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;

namespace yugatests_files
{
	class Tester
	{
		private readonly IFileManager _fileManager;
		private int _fileCount;

		public Tester(IFileManager fileManager)
		{
			_fileManager = fileManager;
		}

		private int _runs;


		public long RunLoop(string msg, int fileCount, int count, Func<bool, long> a)
		{
			_fileCount = fileCount;
			//RunLoopSingle(msg, 2, true, a);
			long ret = RunLoopSingle(msg, count, false, a);
			Console.Out.WriteLine("------------------------------------");
			return ret;
		}

		public long RunLoopSingle(string msg, int count, bool disableDb, Func<bool, long> a)
		{
			_runs = 0;
			Console.Out.WriteLine($"{msg} [{_fileManager.GetType().Name}]");
			
			long tot = 0;

			for (int i = 0; i < count; i++)
			{
				long ms = a(disableDb);
				tot += ms;

				long size = _fileCount * FileSize;
				double speed = size * 1000.0 / ms / 1024 /1024;

				Console.Out.WriteLine($"Test {i}: {ms} ms ({speed:N3} MB/s)");
			}

			long totalSize = _fileCount * FileSize * count;
			double totalSpeed = totalSize * 1000.0 / tot / 1024 /1024;

			Console.Out.WriteLine($"Total: {tot} ms");
			Console.Out.WriteLine($"{msg} [{_fileManager.GetType().Name}] Average: {tot/count} ms ({totalSpeed:N3} MB/s)");

			return tot;
		}

		//public static string FilePath = "F:/tmp/Site_12_1_1070_20000_M.ppf";
		//public static string OutFilePath = "F:/tmp/test.ppf";

		public static string FilePath = "file.test";
		public static string OutFilePath = "file.test.imported";

		public long Export(bool disableDb)
		{
			Stopwatch sw = new Stopwatch();
			sw.Start();
			if (disableDb) _fileManager.DisableDb();

			if (_fileCount == 1)
			{
				string filePath = FilePath;
				string id = filePath +_runs;
				_runs++;
				_fileManager.Export(filePath, id).Wait();
			}
			else
			{
				List<FileRef> l = new List<FileRef>();
				for (int i = 0; i < _fileCount; i++)
				{
					string filePath = FilePath;
					string id = filePath + _runs;
					_runs++;

					l.Add(new FileRef
					{
						FilePath = FilePath,
						Id = id
					});	
				}

				_fileManager.Export(l).Wait();
			}

			_fileManager.RestoreDb();
			sw.Stop();

			return sw.ElapsedMilliseconds;
		}

		public long Import(bool disableDb)
		{
			Stopwatch sw = new Stopwatch();
			sw.Start();
			if (disableDb) _fileManager.DisableDb();


			if (_fileCount == 1)
			{
				string filePath = FilePath;
				string id = filePath;
				_fileManager.Import(OutFilePath + _runs, id + _runs).Wait();
				_runs++;
			}
			else
			{
				List<FileRef> l = new List<FileRef>();
				for (int i = 0; i < _fileCount; i++)
				{
					string filePath = FilePath;
					string id = filePath;
					l.Add(new FileRef
					{
						FilePath = OutFilePath+_runs,
						Id = id+_runs
					});
					_runs++;
				}
				_fileManager.Import(l).Wait();
			}
			
			_fileManager.RestoreDb();
			sw.Stop();

			return sw.ElapsedMilliseconds;
		}

		long FileSize => new FileInfo(FilePath).Length;
	}
}
