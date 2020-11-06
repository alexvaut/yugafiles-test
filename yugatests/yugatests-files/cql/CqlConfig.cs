// Copyright (C) 2020 InfoVista S.A. All rights reserved.

namespace yugatests_files.cql
{
	public class CqlConfig
	{
		public string HostName { get; set; } = "localhost";

		public int ChunkSize { get; set; } = 1*1024*1024;
	}
}
