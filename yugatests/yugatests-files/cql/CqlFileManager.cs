// Copyright (C) 2020 InfoVista S.A. All rights reserved.

using Cassandra;
using Microsoft.Extensions.Configuration;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace yugatests_files.cql
{
	class CqlFileManager : IFileManager
	{
		private ISession _session;
		private readonly CqlConfig _cqlConfig;
		private bool _disableDb;

		public CqlFileManager(IConfiguration config)
		{
			_cqlConfig = new CqlConfig();
			var section = config.GetSection("CqlConfig");
			section.Bind(_cqlConfig);

			var cluster = Cluster.Builder().AddContactPoints(_cqlConfig.HostName).Build();
			_session = cluster.Connect();

			_session.Execute("CREATE KEYSPACE IF NOT EXISTS yugatests_files");
			_session.Execute("USE yugatests_files");

			_session.Execute("DROP TABLE IF EXISTS files");
			//_session.Execute("CREATE TABLE files (id text PRIMARY KEY, part int, chunk blob)");
			_session.Execute("CREATE TABLE files (id text, part int, chunk blob, PRIMARY KEY (id,part))");
		}

		public void Dispose()
		{
			_session?.Dispose();
			_session = null;
		}

		public async Task Export(string filePath, string id = null, bool parallel = true)
		{
			List<Task> listOfTasks = new List<Task>();
			var ps = _session.Prepare("INSERT INTO files (id, part, chunk) VALUES (?, ?, ?)");

			await using var fs = new FileStream(filePath, FileMode.Open);
			var len = (int)fs.Length;

			int part = 0;
			for (int i = 0; i < len; i+=_cqlConfig.ChunkSize)
			{
				int size = _cqlConfig.ChunkSize;
				if (i + size > len)
				{
					size = len - i;
				}
				var bits = new byte[size];
				fs.Read(bits, 0, size);

				if (!_disableDb)
				{
					var statement = ps.Bind(id ?? filePath, part, bits);
					listOfTasks.Add(_session.ExecuteAsync(statement));
				}
				
				part++;
			}

			await Task.WhenAll(listOfTasks);
		}

		public async Task Export(IEnumerable<FileRef> files)
		{
			foreach (FileRef fileRef in files)
			{
				await Export(fileRef.FilePath, fileRef.Id);
			}
		}

		public async Task Import(IEnumerable<FileRef> files)
		{
			foreach (FileRef fileRef in files)
			{
				await Import(fileRef.FilePath, fileRef.Id);
			}
		}

		public async Task Import(string filePath, string id = null)
		{
			List<Task> listOfTasks = new List<Task>();

			var ret = _session.Execute($"SELECT max(part) FROM files where id = '{id}'");
			int partCount = ret.First().GetValue<int>(0) + 1;

			SemaphoreSlim fileLock = new SemaphoreSlim(1,1);
			var fs = new FileStream(filePath, FileMode.Create);

			for (int i = 0; i < partCount; i++)
			{
				var statement = new SimpleStatement($"SELECT part, chunk FROM files where id = '{id}' AND part = {i}");
				//convert to await
				var task = _session.ExecuteAsync(statement).ContinueWith(
					async rows =>
					{
						var row = rows.Result.First();
						int part = row.GetValue<int>("part");
						byte[] bits = row.GetValue<byte[]>("chunk");

						await fileLock.WaitAsync();
						try
						{
							fs.Position = part * _cqlConfig.ChunkSize;
							await fs.WriteAsync(bits, 0, bits.Length);
						}
						finally
						{
							fileLock.Release();
						}
						
					});
				listOfTasks.Add(task);
			}

			await Task.WhenAll(listOfTasks).ContinueWith(_ => fs.DisposeAsync());
		}

		//public Task ImportSingle(string filePath, string id = null)
		//{

		//	var ret = _session.Execute($"SELECT max(part) FROM files where id = '{id}'");
		//	int partCount = ret.First().GetValue<int>(0) + 1;

		//	using var fs = new FileStream(filePath, FileMode.Create);

		//	for (int i = 0; i < partCount; i++)
		//	{
		//		var statement = new SimpleStatement($"SELECT part, chunk FROM files where id = '{id}' AND part = {i}");

		//		var rs = _session.Execute(statement);
		//		var l = rs.GetRows().ToList();

		//		//convert to await
		//		var rows = _session.Execute(statement);
		//		var row = rows.First();
		//		int part = row.GetValue<int>("part");
		//		byte[] bits = row.GetValue<byte[]>("chunk");
		//		fs.Position = part * _cqlConfig.ChunkSize;
		//		fs.Write(bits, 0, bits.Length);
		//	}

		//	return Task.CompletedTask;
		//}

		public void DisableDb()
		{
			_disableDb = true;
		}

		public void RestoreDb()
		{
			_disableDb = false;
		}
	}
}
