// Copyright (C) 2020 InfoVista S.A. All rights reserved.

namespace yugatests_files.sql
{
	public class SqlConfig
	{
		public string ConnectionString { get; set; } = "host=localhost;port=5433;user id=yugabyte;password=";

		public int ChunkSize { get; set; } = 4*1024*1024;
	}
}
