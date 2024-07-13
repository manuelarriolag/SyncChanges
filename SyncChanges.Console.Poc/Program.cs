using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Mono.Options;
using Newtonsoft.Json;
using NLog;

namespace SyncChanges.Console.Poc;

public class Program
{

	static readonly Logger Log = LogManager.GetCurrentClassLogger();

	List<string> ConfigFiles;
	bool DryRun;
	bool Error = false;
	int Timeout = 0;
	bool Loop = false;
	int Interval = 60;  //1 min


	public static async Task Main(string[] args)
	{
		Log.Info("Started Program");

		try {

			System.Console.OutputEncoding = Encoding.UTF8;
			System.Console.Title = "SyncChanges Console";
			var program = new Program();
			var showHelp = false;

			//GlobalConfig = new ConfigurationBuilder()
			//    .AddJsonFile("appsettings.json")
			//    .Build();

			try
			{

				var options = new OptionSet {
					{ "h|help", "Show this message and exit", v => showHelp = v != null }, {
						"d|dryrun", "Do not alter target databases, only perform a test run",
						v => program.DryRun = v != null
					},
					{ "t|timeout=", "Database command timeout in seconds", (int v) => program.Timeout = v }, {
						"l|loop", "Perform replication in a loop, periodically checking for changes",
						v => program.Loop = v != null
					}, {
						"i|interval=", "Replication interval in seconds (default is 60); only relevant in loop mode",
						(int v) => program.Interval = v
					},
				};

				program.ConfigFiles = options.Parse(args);

				if (showHelp) {
					ShowHelp(options);
					System.Environment.ExitCode = 0;
					return;
				}

			} catch (Exception ex) {
				Log.Error(ex, "Error parsing command line arguments");
				System.Environment.ExitCode = 1;
				return;
			}

			if (!program.ConfigFiles.Any()) {
				Log.Error("No config files supplied");
				System.Environment.ExitCode = 2;
				return;
			}

			await program.Sync();

			//System.Console.ReadKey();

			System.Environment.ExitCode = program.Error ? 3 : 0;


		} catch (Exception ex) {
			Log.Error(ex);
			System.Environment.ExitCode = 4;

		} finally {
			Log.Info($"Finished Program {GetWithError(System.Environment.ExitCode)}");
		}

	}

	private static string GetWithError(int exitCode) {
		if (exitCode == 0) return String.Empty;
		return "(with error)";
	}
	private static string GetWithError(bool hasErrors)
	{
		if (!hasErrors) return String.Empty;
		return "(with error)";
	}

	private Task Sync()
	{
		Log.Info("Started Sync");

		foreach (var configFile in ConfigFiles) {
			Config config = null;

			try
			{
				config = JsonConvert.DeserializeObject<Config>(File.ReadAllText(configFile));
			} catch (Exception ex)
			{
				Log.Error(ex, $"Error reading configuration file {configFile}");
				Error = true;
				continue;
			} // try...

			try
			{
				//var synchronizer = new LocalToLocalSynchronizer(config) { DryRun = DryRun, Timeout = Timeout };
				var synchronizer = new LocalToRemoteSynchronizer(config) { DryRun = DryRun, Timeout = Timeout };
				//var synchronizer = new RemoteToLocalSynchronizer(config) { DryRun = DryRun, Timeout = Timeout };

				using var cancellationTokenSource = new CancellationTokenSource();
				System.Console.CancelKeyPress += (s, e) =>
				{
					cancellationTokenSource.Cancel();
					e.Cancel = true;
				};

				if (!Loop)
				{
					var success = synchronizer.Sync(cancellationTokenSource.Token);
					Error = Error || !success;
				} else
				{
					synchronizer.Interval = Interval;
					synchronizer.SyncLoop(cancellationTokenSource.Token);
				}
			} catch (Exception ex)
			{
				Log.Error(ex, $"Error synchronizing databases for configuration {configFile}");
				Error = true;
			}

		} // foreach...

		Log.Info($"Finished Sync {GetWithError(Error)}");

		return Task.CompletedTask;
	}

	private static void ShowHelp(OptionSet p)
	{
		System.Console.WriteLine("Usage: SyncChanges [OPTION]... CONFIGFILE...");
		System.Console.WriteLine("Replicate database changes.");
		System.Console.WriteLine();
		System.Console.WriteLine("Options:");
		p.WriteOptionDescriptions(System.Console.Out);
	}

}