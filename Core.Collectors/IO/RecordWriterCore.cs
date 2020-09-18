// Copyright (c) Microsoft Corporation. All rights reserved.

using Microsoft.CloudMine.Core.Collectors.Context;
using Microsoft.CloudMine.Core.Collectors.Error;
using Microsoft.CloudMine.Core.Collectors.Telemetry;
using Microsoft.CloudMine.Core.Collectors.Utility;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace Microsoft.CloudMine.Core.Collectors.IO
{
    public abstract class RecordWriterCore<T> : IRecordWriter where T : FunctionContext
    {
        private readonly string identifier;
        private readonly T functionContext;
        private readonly ContextWriter<T> contextWriter;
        private readonly long recordSizeLimit;
        private readonly long fileSizeLimit;
        private readonly long recordCountLimit;
        private readonly List<string> outputPaths;
        private readonly RecordWriterSource source;

        private bool initialized;

        // private StreamWriter currentWriter;

        // private string currentOutputSuffix;

        private int currentFileIndex;

        private int currentRecordIndex;

        // private string currentRecordType;

        private Dictionary<string, StreamWriter> currentWriteStreams { get; set; }

        protected ITelemetryClient TelemetryClient { get; private set; }

        protected long SizeInBytes { get; private set; }

        public ConcurrentDictionary<string, int> RecordStats { get; }

        /// <summary>
        /// Allows the consumer to specify a write mode for the output files
        /// </summary>
        public RecordWriterMode Mode { get; private set; }

        /// <summary>
        /// Allows the consumer to specify a layout path for output files
        /// </summary>
        public string OutputPathLayout { get; private set; }

        public Dictionary<string, string> OutputPathParts { get; private set; }

        protected RecordWriterCore(string identifier,
                                   ITelemetryClient telemetryClient,
                                   T functionContext,
                                   ContextWriter<T> contextWriter,
                                   long recordSizeLimit,
                                   long fileSizeLimit,
                                   long recordCountLimit,
                                   RecordWriterSource source,
                                   RecordWriterMode mode = RecordWriterMode.LineDelimited,
                                   string outputPathLayout = "")
        {
            this.identifier = identifier;
            this.TelemetryClient = telemetryClient;
            this.functionContext = functionContext;
            this.contextWriter = contextWriter;
            this.recordSizeLimit = recordSizeLimit;
            this.fileSizeLimit = fileSizeLimit;
            this.recordCountLimit = recordCountLimit;
            this.outputPaths = new List<string>();

            this.currentFileIndex = 0;
            this.source = source;

            this.RecordStats = new ConcurrentDictionary<string, int>();

            this.Mode = mode;

            // Populate with some known values that are available at this point
            OutputPathParts = new Dictionary<string, string>()
            {
                { "SessionId", this.functionContext.SessionId },
                { "Identifier", this.identifier },
                { "MM", this.functionContext.FunctionStartDate.ToString("MM") },
                { "dd", this.functionContext.FunctionStartDate.ToString("dd") },
                { "yyyy", this.functionContext.FunctionStartDate.ToString("yyyy") },
                { "HH.mm.ss", this.functionContext.FunctionStartDate.ToString("HH:mm:ss") }
            };

            // Unless otherwise specified, use the default path
            // $"{this.outputPathPrefix}/{dateTimeUtc:yyyy/MM/dd/HH.mm.ss}_{this.identifier}_{this.functionContext.SessionId}";
            OutputPathLayout = !string.IsNullOrWhiteSpace(outputPathLayout)
                ? outputPathLayout
                : "{OrganizationId}/{RepositoryId}/{yyyy}/{MM}/{dd}/{HH.mm.ss}_{Identifier}_{SessionId}_{RecordType}{FileIndex}{UniqueId}.json";
        }

        protected abstract Task InitializeInternalAsync();

        protected abstract Task<StreamWriter> NewStreamWriterAsync(string recordType, int fileIndex = 0, string uniqueId = "");

        protected abstract Task NotifyCurrentOutputAsync(string fileName);

        public IEnumerable<string> OutputPaths => this.outputPaths;

        protected void AddOutputPath(string outputPath)
        {
            this.outputPaths.Add(outputPath);
        }

        public void AddOutputPathPart(string key, string value)
        {
            // Make sure OutputPathParts is initialized
            if (OutputPathParts == null)
            {
                OutputPathParts = new Dictionary<string, string>();
            }

            OutputPathParts.Add(key, value);
        }

        // public void SetOutputPathPrefix(string outputPathPrefix)
        // {
        //     // Do nothing
        // }

        //protected string GetOutputPathFull(string recordType, int fileIndex = 0)
        //{
        //    string fileIndexPart = (fileIndex == 0 ? "" : $"_{fileIndex}");

        //    string recortTypePart = (string.IsNullOrWhiteSpace(recordType) ? "" : $"{recordType}/");

        //    var dateTimeUtc = this.functionContext.FunctionStartDate;

        //    var outputPath = $"{this.outputPathPrefix}/{recortTypePart}{dateTimeUtc:yyyy/MM/dd/HH.mm.ss}_{this.identifier}_{this.functionContext.SessionId}{fileIndexPart}.json";

        //    return outputPath;
        //}

        /// <summary>
        /// Replaces all the output file path placeholders with runtime values and does validation base on the configuration input
        /// </summary>
        /// <param name="recordType">The type of the record being written</param>
        /// <param name="fileIndex">Index of the current file</param>
        /// <returns></returns>
        protected string BuildOutputPath(string recordType, int fileIndex = 0, string uniqueId = "")
        {
            string finalOutputPath = OutputPathLayout;

            // Replace each of the
            foreach (var outputPathPart in OutputPathParts)
            {
                finalOutputPath = finalOutputPath.Replace($"{{{outputPathPart.Key}}}", outputPathPart.Value);
            }

            // Replace the RecordType placeholder, which is specific to the type of data being collected
            finalOutputPath = finalOutputPath.Replace("{RecordType}", recordType);

            // Append the file index (if > 0)
            finalOutputPath = finalOutputPath.Replace("{FileIndex}", fileIndex > 0 ? $"_{fileIndex}" : "");

            // Check to find any values that are defined in the layout template that aren't defined in the list of output variables
            var regex = new Regex(@"\{(.*?)\}");

            var matches = regex.Matches(finalOutputPath);

            var containsUniqueIdPlaceholder = matches.Any(match => match.Groups[1].Value == "UniqueId");

            // When record writer mode == Single, make sure that the value uniqueId is found in the string, or throw an exception
            if (Mode == RecordWriterMode.Single && !containsUniqueIdPlaceholder)
            {
                // Throw new exception - single mode requires a UniqueId placeholder that will be used later
                throw new RequiredPlaceholderException("UniqueId");
            }
            else if (containsUniqueIdPlaceholder && Mode != RecordWriterMode.Single)
            {
                // If UniqueId placeholder is present, but the record writer mode != Single, throw an exception
                throw new UnexpectedPlaceholderException("UniqueId");
            }

            if (this.Mode == RecordWriterMode.Single && containsUniqueIdPlaceholder)
            {
                finalOutputPath = finalOutputPath.Replace("{UniqueId}", uniqueId);
            }

            if (matches.Count > 0)
            {
                // At least 1 match was found, indicating the the code is not properly setting all of the values that the layout expects

                // Check for the "uniqueId" value - that is OK to be leftover
                // since it will get used to finalize the file name to write when
                // the record mode == Single.

                if(matches.Count >= 1 && !containsUniqueIdPlaceholder)
                {
                    List<Exception> innerExceptions = new List<Exception>();

                    foreach(Match match in matches)
                    {
                        innerExceptions.Add(new UnexpectedPlaceholderException(match.Groups[1].Value));
                    }

                    if (innerExceptions.Count > 0)
                    {
                        AggregateException agex = new AggregateException("Unexpected placeholders found in output path layout", innerExceptions.ToArray());
                        throw agex;
                    }
                }
            }

            return finalOutputPath;
        }

        private async Task InitializeAsync(string outputSuffix)
        {
            //if (this.outputPathPrefix == null)
            //{
            //    string message = "Cannot initialize record writer before the context is set.";
            //    this.TelemetryClient.LogCritical(message);
            //    throw new FatalTerminalException(message);
            //}

            await this.InitializeInternalAsync().ConfigureAwait(false);
            // this.currentWriter = await this.NewStreamWriterAsync(outputSuffix).ConfigureAwait(false);
            // this.currentOutputSuffix = outputSuffix;

            // New up the Dictionary that will hold each of the stream writers
            currentWriteStreams = new Dictionary<string, StreamWriter>();

            this.initialized = true;
        }

        public async Task NewOutputAsync(string recordType, int fileIndex = 0, string uniqueId = "")
        {
            // Make sure the instance is initialized before trying to create a stream
            if (!this.initialized)
            {
                await this.InitializeAsync(recordType).ConfigureAwait(false);
            }

            // If the dictionary contains a stream for the record type, grab it and dispose of it, then remove it
            if(currentWriteStreams.ContainsKey(recordType))
            {
                var stream = currentWriteStreams[recordType];

                // this.currentWriter.Dispose();
                await stream.DisposeAsync();

                currentWriteStreams.Remove(recordType);
            }

            var newStream = await this.NewStreamWriterAsync(recordType, fileIndex, uniqueId).ConfigureAwait(false);

            // ToDo: Need to figure out what to do here - not sure if this is really needed?
            // await this.NotifyCurrentOutputAsync().ConfigureAwait(false);

            currentWriteStreams.Add(recordType, newStream);

            // this.currentOutputSuffix = recordType;
            //currentRecordType = recordType;
        }

        public async Task WriteRecordAsync(JObject record, RecordContext recordContext)
        {
            var recordType = recordContext.RecordType;

            if (!this.initialized)
            {
                await this.InitializeAsync(outputSuffix: string.Empty).ConfigureAwait(false);
            }

            // Augment the metadata to the record only if not done by another record writer.
            JToken metadataToken = record.SelectToken("$.Metadata");
            if (recordContext.MetadataAugmented)
            {
                // Confirm (double check) that this is case and fail execution if not.
                if (metadataToken == null)
                {
                    Dictionary<string, string> properties = new Dictionary<string, string>()
                    {
                        { "RecordType", recordType },
                        { "RecordMetadata", record.SelectToken("$.Metadata").ToString(Formatting.None) },
                        { "RecordPrefix", record.ToString(Formatting.None).Substring(0, 1024) },
                    };
                    this.TelemetryClient.TrackEvent("RecordWithoutMetadata", properties);

                    throw new FatalTerminalException("Detected a record without metadata. Investigate 'RecordWithoutMetadata' custom event for details.");
                }
            }
            else
            {
                this.AugmentRecordMetadata(record, recordContext);
                this.AugmentRecord(record);

                recordContext.MetadataAugmented = true;
            }

            // Add WriterSource to Metadata after the other metadata is augmented. This is because this value changes between writers and need to be updated before the record is emitted.
            metadataToken = record.SelectToken("$.Metadata");
            JObject metadataObject = (JObject)metadataToken;
            JToken writerSourceToken = metadataObject.SelectToken("$.WriterSource");
            if (writerSourceToken == null)
            {
                // This is the first time we are adding writer source.
                metadataObject.Add("WriterSource", this.source.ToString());
            }
            else
            {
                // Override the existing value.
                writerSourceToken.Replace(this.source.ToString());
            }

            string content = record.ToString(Formatting.None);
            if (content.Length >= this.recordSizeLimit)
            {
                Dictionary<string, string> properties = new Dictionary<string, string>()
                {
                    { "RecordType", recordContext.RecordType },
                    { "RecordMetadata", record.SelectToken("$.Metadata").ToString(Formatting.None) },
                    { "RecordPrefix", content.Substring(0, 1024) },
                };

                this.TelemetryClient.TrackEvent("DroppedRecord", properties);
                return;
            }

            if (this.Mode == RecordWriterMode.Single)
            {
                // For single record writer modes, we don't need to open a stream, we'll just write a file
                JObject recordObject = JObject.Parse(content);

                // Get the Sha for the record and pass it to generate a new unique file
                string uniqueId = recordObject["Metadata"]["RecordSha"].Value<string>();

                // Always regenerate the file path and file when in single writer mode
                // protected string BuildOutputPath(string recordType, int fileIndex = 0, string uniqueId = "")
                await CreateBlobAsync(recordType, uniqueId, recordObject.ToString(Formatting.None));
            }
            else
            {
                // Get the current record writer
                var writer = currentWriteStreams[recordType];

                // this.SizeInBytes = this.currentWriter.BaseStream.Position;

                var sizeInBytes = writer.BaseStream.Position;

                // Check if the current file needs to be rolled over.
                if (this.SizeInBytes > this.fileSizeLimit || (this.currentRecordIndex > 0 && this.currentRecordIndex > this.recordCountLimit))
                {
                    // Roll over the file with a new file index
                    this.currentFileIndex++;
                    await this.NewOutputAsync(recordType, this.currentFileIndex).ConfigureAwait(false);

                    // Now that the stream has been re-created, grab it again
                    writer = currentWriteStreams[recordType];

                    // Reset the record index in the case that that's being used to determine when to roll over a file
                    currentRecordIndex = 0;
                }

                await writer.WriteLineAsync(content).ConfigureAwait(false);
                currentRecordIndex++;

                this.RegisterRecord(recordContext.RecordType);
            }
        }

        private async Task CreateBlobAsync(string recordType, string uniqueId, string blobContent)
        {
            var stream = await NewStreamWriterAsync(recordType, 0, uniqueId);

            await stream.WriteAsync(blobContent);

            stream.Close();
        }

        private void RegisterRecord(string recordType)
        {
            if (!this.RecordStats.TryGetValue(recordType, out int recordCount))
            {
                recordCount = 0;
            }
            this.RecordStats[recordType] = recordCount + 1;
        }

        protected virtual void AugmentRecord(JObject record)
        {
            // Default implementation does not do anything.
        }

        public async Task FinalizeAsync()
        {
            if (!this.initialized)
            {
                return;
            }

            // Close and dispose of each open stream
            foreach (var dict in currentWriteStreams)
            {
                var stream = dict.Value;

                await stream.FlushAsync();
                stream.Close();
                await stream.DisposeAsync();

                // ToDo: Need to figure out what to do here - not sure if this is really needed?
                // await this.NotifyCurrentOutputAsync().ConfigureAwait(false);
            }

            this.initialized = false;
        }

        public void Dispose()
        {
            if (!this.initialized)
            {
                TelemetryClient.LogWarning("RecordWriter.Dispose was called before RecordWriter was initialized. Ignoring the call.");
                return;
            }

            // Dispose all of the streams
            foreach (var streamPair in currentWriteStreams)
            {
                streamPair.Value.Dispose();
            }
        }

        private void AugmentRecordMetadata(JObject record, RecordContext recordContext)
        {
            string serializedRecord = record.ToString(Formatting.None);

            JToken metadataToken = record.SelectToken("Metadata");
            if (metadataToken == null)
            {
                metadataToken = new JObject();
                record.AddFirst(new JProperty("Metadata", metadataToken));
            }

            JObject metadata = (JObject)metadataToken;

            metadata.Add("FunctionStartDate", this.functionContext.FunctionStartDate);
            metadata.Add("RecordType", recordContext.RecordType);
            metadata.Add("SessionId", this.functionContext.SessionId);
            metadata.Add("CollectorType", this.functionContext.CollectorType.ToString());
            metadata.Add("CollectionDate", DateTime.UtcNow);

            Dictionary<string, JToken> additionalMetadata = recordContext.AdditionalMetadata;
            if (additionalMetadata != null)
            {
                foreach (KeyValuePair<string, JToken> metadataItem in additionalMetadata)
                {
                    metadata.Add(metadataItem.Key, metadataItem.Value);
                }
            }

            this.contextWriter.AugmentMetadata(metadata, this.functionContext);

            // ToDo: Create hashing strategy for each record type
            // For some cases, we may not want to use the entirety of the JSON blob to hash.  Instead, we may want
            // to just use a Record Id, or a series of values concatenated together, which would allow us to de-dupe
            // records

            metadata.Add("RecordSha", HashUtility.ComputeSha512(serializedRecord));
        }
    }

    public static class RecordWriterExtensions
    {
        public static string GetOutputPaths(List<IRecordWriter> recordWriters)
        {
            string result = string.Join(";", recordWriters.Select(recordWriter => string.Join(";", recordWriter.OutputPaths)));
            return result;
        }

        public static string GetOutputPaths(IRecordWriter recordWriter)
        {
            return GetOutputPaths(new List<IRecordWriter> { recordWriter });
        }
    }

    public enum RecordWriterSource
    {
        AzureBlob = 0,
        AzureDataLake = 1,
    }

    public class UnexpectedPlaceholderException : Exception
    {
        public UnexpectedPlaceholderException(string placeholder) : base($"The unexpected placeholder '{placeholder}' was found in output path.  Please make sure values are being passed for all placeholder values.")
        {

        }
    }

    public class RequiredPlaceholderException : Exception
    {
        public RequiredPlaceholderException(string placeholder) : base($"The placeholder '{placeholder}' was expected in output path but was not found")
        {

        }
    }
}
