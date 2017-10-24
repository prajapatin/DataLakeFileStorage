using Microsoft.Azure.Management.DataLake.Store;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.Rest.Azure.Authentication;
using Microsoft.WindowsAzure.Storage; // Namespace for CloudStorageAccount
using Microsoft.WindowsAzure.Storage.Table; // Namespace for Table storage types
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace DataLakeStorageSample
{

    class Program
    {
        static string adlsAccountName = "{Your data lake storage account name}";
        static string datalakeStorageFolder = "drivesdata";
        static string domain = "{Your azure active directory domain}";
        static string webApp_clientId = "{Your client app id for which permission has been granted on azure}";
        static string clientSecret = "{Client secret for above app}";
        static string storageConnectionString = "{Azure storage connection string for logging}";
        static ClientCredential clientCredential = new ClientCredential(webApp_clientId, clientSecret);
        static DataLakeStoreFileSystemManagementClient fileSystemClient;

        static void Main(string[] args)
        {
            try
            {
                // Parse the connection string and return a reference to the storage account.
                // Retrieve the storage account from the connection string.

                CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageConnectionString);
                CloudTableClient tableClient = storageAccount.CreateCloudTableClient();

                // Retrieve a reference to the table.
                CloudTable table = tableClient.GetTableReference("datalogger");

                SynchronizationContext.SetSynchronizationContext(new SynchronizationContext());
                var credTask = ApplicationTokenProvider.LoginSilentAsync(domain, clientCredential);

                string sourceDirectory = @"F:\OutputJson\";
                DirectoryInfo dir = new DirectoryInfo(sourceDirectory);
                int iteration = 0;
                while (dir.EnumerateFiles().Skip(iteration * 1000).Take(1000).Count() > 0)
                {
                    try
                    {
                        Console.WriteLine(string.Format("Processing Iteration: {0}" + Environment.NewLine, iteration));
                        List<DriveFile> files = new List<DriveFile>();
                        using (fileSystemClient = new DataLakeStoreFileSystemManagementClient(credTask.Result))
                        {
                            foreach (var file in dir.EnumerateFiles().Skip(iteration * 1000).Take(1000))
                            {
                                DriveFile driveFile = new DriveFile { FilePath = sourceDirectory + Path.GetFileName(file.ToString()), FileType = FileType.Signal };
                                files.Add(driveFile);
                            }

                            Task.WaitAll(StoreFiles(files, table).ToArray());
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(string.Format("Error on Batch Processing: Batch: {0} Error :{1}", iteration, ex.Message));
                    }
                    iteration++;
                }

            }
            catch (Exception ex)
            {
                Console.WriteLine("Error at " + ex.InnerException);
            }
        }

        private static List<Task> StoreFiles(List<DriveFile> files, CloudTable table)
        {
            List<Task> taskList = new List<Task>();
            if (files != null && files.Count > 0)
            {

                Parallel.ForEach(files, file =>
                {
                    string[] fileNameTokens = ExtractFileDetail(file);
                    if (fileNameTokens != null)
                    {
                        taskList.Add(StoreFileInDataLake(fileNameTokens, file.FilePath, File.OpenRead(file.FilePath), FileType.Signal, table));
                    }
                });
            }
            return taskList;
        }

        private static string[] ExtractFileDetail(DriveFile driveFile)
        {
            FileInfo fileInfo = new FileInfo(driveFile.FilePath);
            if (driveFile.FileType == FileType.Signal || driveFile.FileType == FileType.Event)
            {
                string[] fileNameTokens = fileInfo.Name.Split('.')[0].Split('_');
                if (fileNameTokens.Length == 3)
                {
                    return fileNameTokens;
                }
            }
            return null;
        }

        private static async Task StoreFileInDataLake(string[] fileNameTokens, string fullFileName, Stream fileContent, FileType fileType, CloudTable table)
        {
            try
            {
                string driveId = fileNameTokens[0] + "_" + fileNameTokens[1];
                DateTime dt = DateTime.MinValue;
                //File/folder format as per the requirement
                DateTime.TryParseExact(fileNameTokens[2], "yyyy-MM-dd-HH-mm-ss-fff", null, DateTimeStyles.None, out dt);
                string filePath = String.Format("/" + datalakeStorageFolder + "/{0}/{1}/{2}/{3}/{4}/{5}", driveId, dt.Year, dt.Month, dt.Day, dt.Hour, fileType.ToString());
                FileInfo fileInfo = new FileInfo(fullFileName);
                if (!fileSystemClient.FileSystem.PathExists(adlsAccountName, filePath))
                {
                    await fileSystemClient.FileSystem.CreateAsync(adlsAccountName, filePath + " / " + fileInfo.Name, fileContent, permission: 777, overwrite: true);
                    //Logger(fileInfo.Name, table).Wait();
                    //Console.WriteLine(fullFileName + " Upload Complete");
                }
               
            }
            catch (Exception ex)
            {
                Console.WriteLine(string.Format("Error on File Processing: {0}, Error :{1}", fullFileName, ex.Message));
            }
        }
        public static bool CheckforEntry(string sourceFile, CloudTable table)
        {
            // Create a retrieve operation that takes a customer entity.
            TableOperation retrieveOperation = TableOperation.Retrieve<DataLoggerEntity>("DataLogger", sourceFile);

            // Execute the retrieve operation.
            TableResult retrievedResult = table.Execute(retrieveOperation);

            if (retrievedResult.Result != null)
            {
                return true;
            }
            else
            {
                return false;
            }
        }
        public static async Task Logger(string sourceFile, CloudTable table)
        {
            DataLoggerEntity datalog = new DataLoggerEntity(sourceFile);
            TableOperation insertOrReplaceOperation = TableOperation.InsertOrReplace(datalog);
            TableResult result = await table.ExecuteAsync(insertOrReplaceOperation);
            DataLoggerEntity insertedLog = result.Result as DataLoggerEntity;
        }
    }
}