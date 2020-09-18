// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Microsoft.CloudMine.Core.Collectors.Context;
using Microsoft.CloudMine.Core.Collectors.Telemetry;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Queue;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.CloudMine.Core.Collectors.IO
{
    public class AzureBlobRecordWriter<T> : RecordWriterCore<T> where T : FunctionContext
    {
        private const long FileSizeLimit = 1024 * 1024 * 512; // 512 MB.
        private const long RecordSizeLimit = 1024 * 1024 * 4; // 4 MB.
        private const long RecordCountLimit = 0; // 0 means don't roll over based directly on # of records

        private readonly string blobRoot;
        private readonly string outputQueueName;
        private readonly string storageConnectionEnvironmentVariable;
        private readonly string notificationQueueConnectionEnvironmentVariable;
        private readonly bool notifyDownstream;

        private CloudBlobContainer outContainer;
        private CloudQueue queue;

        public AzureBlobRecordWriter(string blobRoot,
                                     string outputQueueName,
                                     string identifier,
                                     ITelemetryClient telemetryClient,
                                     T functionContext,
                                     ContextWriter<T> contextWriter,
                                     string storageConnectionEnvironmentVariable = "AzureWebJobsStorage",
                                     string notificationQueueConnectionEnvironmentVariable = "AzureWebJobsStorage",
                                     RecordWriterMode mode = RecordWriterMode.LineDelimited,
                                     string outputPathLayout = "",
                                     bool notifyDownstream = true)
            : this(blobRoot, outputQueueName, identifier, telemetryClient, functionContext, contextWriter, outputPathPrefix: null, storageConnectionEnvironmentVariable, notificationQueueConnectionEnvironmentVariable, mode, outputPathLayout: outputPathLayout, notifyDownstream: notifyDownstream)
        {
        }

        public AzureBlobRecordWriter(string blobRoot,
                                     string outputQueueName,
                                     string identifier,
                                     ITelemetryClient telemetryClient,
                                     T functionContext,
                                     ContextWriter<T> contextWriter,
                                     string outputPathPrefix,
                                     string storageConnectionEnvironmentVariable = "AzureWebJobsStorage",
                                     string notificationQueueConnectionEnvironmentVariable = "AzureWebJobsStorage",
                                     RecordWriterMode mode = RecordWriterMode.LineDelimited,
                                     string outputPathLayout = "",
                                     bool notifyDownstream = true)
            : base(identifier, telemetryClient, functionContext, contextWriter, RecordSizeLimit, FileSizeLimit, RecordCountLimit, source: RecordWriterSource.AzureBlob, mode: mode, outputPathLayout: outputPathLayout)
        {
            this.blobRoot = blobRoot;
            this.outputQueueName = outputQueueName;
            this.storageConnectionEnvironmentVariable = storageConnectionEnvironmentVariable;
            this.notificationQueueConnectionEnvironmentVariable = notificationQueueConnectionEnvironmentVariable;
            this.notifyDownstream = notifyDownstream;
        }

        protected override async Task InitializeInternalAsync()
        {
            this.queue = await AzureHelpers.GetStorageQueueAsync(this.outputQueueName, this.notificationQueueConnectionEnvironmentVariable).ConfigureAwait(false);
            this.outContainer = await AzureHelpers.GetStorageContainerAsync(this.blobRoot, this.storageConnectionEnvironmentVariable).ConfigureAwait(false);
        }

        protected override async Task<StreamWriter> NewStreamWriterAsync(string recordType, int fileIndex = 0, string uniqueId = "")
        {
            var outputBlob = this.outContainer.GetBlockBlobReference(this.BuildOutputPath(recordType, fileIndex, uniqueId));
            CloudBlobStream cloudBlobStream = await outputBlob.OpenWriteAsync().ConfigureAwait(false);
            return new StreamWriter(cloudBlobStream, Encoding.UTF8);
        }

        protected override async Task NotifyCurrentOutputAsync(string fileName)
        {
            if(this.notifyDownstream)
            {
                CloudBlob blob = this.outContainer.GetBlockBlobReference(fileName);

                // ToDo: How will this behave with "single" write mode, or with new files per record type?
                string notificationMessage = AzureHelpers.GenerateNotificationMessage(blob);
                await this.queue.AddMessageAsync(new CloudQueueMessage(notificationMessage)).ConfigureAwait(false);

                this.AddOutputPath(blob.Name);
            }
        }
    }
}
