using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure; // Namespace for CloudConfigurationManager
using Microsoft.WindowsAzure.Storage; // Namespace for CloudStorageAccount
using Microsoft.WindowsAzure.Storage.Table; // Namespace for Table storage types

namespace DataLakeStorageSample
{
    public class DriveFile
    {
        private string _filePath;
        private FileType _fileType;

        public string FilePath { get => _filePath; set => _filePath = value; }
        public FileType FileType { get => _fileType; set => _fileType = value; }
    }

    public enum FileType
    {
        None,
        Signal,
        Event,
        DataLogger
    }

    public class DataLoggerEntity : TableEntity
    {
        public DataLoggerEntity(string fileName)
        {
            this.PartitionKey = "DataLogger";
            this.RowKey = fileName;
        }
        public DataLoggerEntity() { }
    }
}
