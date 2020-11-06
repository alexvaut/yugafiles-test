// Copyright (C) 2020 InfoVista S.A. All rights reserved.
using Microsoft.Extensions.Configuration;
using Npgsql;
using NpgsqlTypes;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace yugatests_files.sql
{
	public class SqlFileManager : IFileManager
	{
		private bool _disableDb;
		private readonly SqlConfig _sqlConfig;
		private NpgsqlConnection _connection;

		public SqlFileManager(IConfiguration config)
		{
			_sqlConfig = new SqlConfig();
			var section = config.GetSection("SqlConfig");
			section.Bind(_sqlConfig);

			_connection = new NpgsqlConnection(_sqlConfig.ConnectionString);
			_connection.Open();

			NpgsqlCommand cmd = new NpgsqlCommand("DROP TABLE IF EXISTS files;", _connection);
			cmd.ExecuteNonQuery();

			cmd = new NpgsqlCommand("CREATE TABLE files (id varchar, part int, chunk bytea, PRIMARY KEY (id, part));", _connection);
			cmd.ExecuteNonQuery();
		}

		public void Dispose()
		{
			_connection?.Dispose();
			_connection = null;
		}

		public async Task Export(string filePath, string id = null, bool parallel = true)
		{
			if (parallel)
			{
				await ExportSingle(filePath, id);
			}
			else
			{
				await ExportSingle(filePath, id);
			}
		}

		private Task ExportSingle(string filePath, string id = null)
		{
			using var fs = new FileStream(filePath, FileMode.Open);
			var len = (int)fs.Length;

			NpgsqlCommand insertCmd = new NpgsqlCommand("INSERT INTO files (id, part, chunk) VALUES(:id, :part, :chunk)", _connection);
			NpgsqlParameter paramId = new NpgsqlParameter("id", NpgsqlDbType.Varchar);
			insertCmd.Parameters.Add(paramId);

			NpgsqlParameter paramPart = new NpgsqlParameter("part", NpgsqlDbType.Integer);
			insertCmd.Parameters.Add(paramPart);

			NpgsqlParameter paramChunk = new NpgsqlParameter("chunk", NpgsqlDbType.Bytea);
			insertCmd.Parameters.Add(paramChunk);

			paramId.Value = id ?? filePath;

			int part = 0;
			for (int i = 0; i < len; i+=_sqlConfig.ChunkSize)
			{
				int size = _sqlConfig.ChunkSize;
				if (i + size > len)
				{
					size = len - i;
				}
				var bits = new byte[size];
				fs.Read(bits, 0, size);

				if (!_disableDb)
				{
					paramPart.Value = part;
					paramChunk.Value = bits;
					insertCmd.ExecuteNonQuery();
				}
				
				part++;
			}

			return Task.CompletedTask;
		}

		private async Task ExportParallel(string filePath, string id = null)
		{
			List<Task> listOfTasks = new List<Task>();
			await using var fs = new FileStream(filePath, FileMode.Open);
			var len = (int)fs.Length;

			int part = 0;
			for (int i = 0; i < len; i+= _sqlConfig.ChunkSize)
			{
				await using var connection = new NpgsqlConnection(_sqlConfig.ConnectionString);
				if (!_disableDb)
				{
					connection.Open();
				}
				NpgsqlCommand insertCmd = new NpgsqlCommand("INSERT INTO files (id, part, chunk) VALUES(:id, :part, :chunk)", connection);
				
				NpgsqlParameter paramId = new NpgsqlParameter("id", NpgsqlDbType.Varchar);
				insertCmd.Parameters.Add(paramId);

				NpgsqlParameter paramPart = new NpgsqlParameter("part", NpgsqlDbType.Integer);
				insertCmd.Parameters.Add(paramPart);

				NpgsqlParameter paramChunk = new NpgsqlParameter("chunk", NpgsqlDbType.Bytea);
				insertCmd.Parameters.Add(paramChunk);
				paramId.Value = id ?? filePath;

				int size = _sqlConfig.ChunkSize;
				if (i + size > len)
				{
					size = len - i;
				}
				var bits = new byte[size];
				fs.Read(bits, 0, size);

				if (!_disableDb)
				{
					paramPart.Value = part;
					paramChunk.Value = bits;
					listOfTasks.Add(insertCmd.ExecuteNonQueryAsync());
				}
				
				part++;
			}

			await Task.WhenAll(listOfTasks);
		}

		public Task Export(IEnumerable<FileRef> files)
		{
			foreach (FileRef fileRef in files)
			{
				Export(fileRef.FilePath, fileRef.Id).Wait();
			}

			return Task.CompletedTask;
		}

		public Task Import(IEnumerable<FileRef> files)
		{
			foreach (FileRef fileRef in files)
			{
				Import(fileRef.FilePath, fileRef.Id).Wait();
			}

			return Task.CompletedTask;
		}

		public Task Import(string filePath, string id = null)
		{
			NpgsqlCommand cmd = new NpgsqlCommand($"SELECT part,chunk FROM files WHERE id = '{id}'", _connection);
			using NpgsqlDataReader dr = cmd.ExecuteReader();

			using var fs = new FileStream(filePath, FileMode.Create);

			while (dr.Read())
			{
				int part = (int)dr[0];
				byte[] bits = (byte[]) dr[1];
				fs.Position = part * _sqlConfig.ChunkSize;
				fs.Write(bits, 0, bits.Length);
			}

			return Task.CompletedTask;
		}

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
