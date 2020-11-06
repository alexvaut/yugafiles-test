// Copyright (C) 2020 InfoVista S.A. All rights reserved.

using System.Globalization;
using Microsoft.Extensions.Configuration;
using yugatests_files.cql;
using yugatests_files.sql;

namespace yugatests_files
{

	/// <summary>
	/// docker run -d --name yugabyte  -p7000:7000 -p9000:9000 -p5433:5433 -p9042:9042 yugabytedb/yugabyte:2.2.2.0-b15 bin/yugabyted start --daemon=false
	/// </summary>
	class Program
	{
		static void Main(string[] args)
		{
			IConfiguration config = new ConfigurationBuilder().AddJsonFile("appsettings.json",optional:true).AddEnvironmentVariables().Build();

			int testCount = 5;
			config.GetSection("TestCount").Get<int>();

			TestConfig testConfig = new TestConfig();
			var section = config.GetSection("TestConfig");
			section.Bind(testConfig);

			CqlFileManager cqlFileManager = new CqlFileManager(config);
			var tcql = new Tester(cqlFileManager);
			tcql.RunLoop("Export", testConfig.TestFiles, testConfig.TestCount, tcql.Export);
			tcql.RunLoop("Import", testConfig.TestFiles, testConfig.TestCount, tcql.Import);

			//TestOneFile(cqlFileManager, "F:/tmp/test_sql.ppf");

			SqlFileManager sqlFileManager = new SqlFileManager(config);
			var tsql = new Tester(sqlFileManager);
			tsql.RunLoop("Export", testConfig.TestFiles, testConfig.TestCount, tsql.Export);
			tsql.RunLoop("Import", testConfig.TestFiles, testConfig.TestCount, tsql.Import);

			//TestOneFile(cqlFileManager, "F:/tmp/test_cql.ppf");
		}


		private static void TestOneFile(IFileManager fileManager, string f)
		{
			fileManager.Export(Tester.FilePath).Wait();
			fileManager.Import(f,Tester.FilePath).Wait();
		}
	}

	public class TestConfig
	{
		public int TestCount { get; set; } = 5;
		public int TestFiles { get; set; } = 5;
	}
}
