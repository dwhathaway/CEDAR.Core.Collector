﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Microsoft.CloudMine.Core.Collectors.Context;
using Microsoft.CloudMine.Core.Collectors.Telemetry;
using Microsoft.Azure.DataLake.Store;
using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Azure.DataLake.Store.FileTransfer;
using Microsoft.CloudMine.Core.Collectors.Error;
using System.Diagnostics;
using System.Collections.Generic;

namespace Microsoft.CloudMine.Core.Collectors.IO
{
    public class AdlsBulkRecordWriter<T> : RecordWriterCore<T> where T : FunctionContext
    {
        private const long FileSizeLimit = 1024 * 1024 * 512; // 512 MB.
        private const long RecordSizeLimit = 1024 * 1024 * 4; // 4 MB.
        private const long RecordCountLimit = 0; // 0 means don't roll over based directly on # of records

        private readonly static TimeSpan MaxUploadDelay = TimeSpan.FromMinutes(10);

        private readonly string uniqueId;
        private readonly string adlsRoot;
        private readonly AdlsClient adlsClient;

        private string version;
        private string localRoot;

        private string currentLocalPath;

        private readonly bool notifyDownstream;

        // public AdlsBulkRecordWriter(AdlsClient adlsClient,
        //                             string identifier,
        //                             ITelemetryClient telemetryClient,
        //                             T functionContext,
        //                             ContextWriter<T> contextWriter,
        //                             string root)
        //     : this(adlsClient, identifier, telemetryClient, functionContext, contextWriter, root)
        // {
        // }

        // ToDo: outputPathLayout needs to be revisited here.  Adls writer uses a local file path historically.
        public AdlsBulkRecordWriter(AdlsClient adlsClient,
                                    string identifier,
                                    ITelemetryClient telemetryClient,
                                    T functionContext,
                                    ContextWriter<T> contextWriter,
                                    string root,
                                    string outputPathLayout = "",
                                    bool notifyDownstream = true)
            : base(identifier, telemetryClient, functionContext, contextWriter, RecordSizeLimit, FileSizeLimit, RecordCountLimit, source: RecordWriterSource.AzureDataLake, outputPathLayout: outputPathLayout)
        {
            this.adlsClient = adlsClient;
            this.adlsRoot = root;
            this.uniqueId = functionContext.SessionId;
            this.notifyDownstream = notifyDownstream;
        }

        protected override Task InitializeInternalAsync()
        {
            string appEnvironment = Environment.GetEnvironmentVariable("AppEnv");
            this.version = string.IsNullOrWhiteSpace(appEnvironment) ? "vLocal" : (appEnvironment.Equals("Staging") ? "vDev" : (appEnvironment.Equals("Production") ? "v3" : "vLocal"));
            this.localRoot = Path.Combine(Path.GetTempPath(), this.uniqueId);
            return Task.CompletedTask;
        }

        protected override Task<StreamWriter> NewStreamWriterAsync(string fileName)
        {
            // ToDo: make sure this is working properly after refactoring
            this.currentLocalPath = Path.Combine(this.localRoot, fileName);
            Directory.CreateDirectory(Path.GetDirectoryName(this.currentLocalPath));

            StreamWriter result = File.CreateText(this.currentLocalPath);
            return Task.FromResult(result);
        }

        protected override Task NotifyCurrentOutputAsync(string fileName)
        {
            if (notifyDownstream)
            {
                // Assume that upload will take at most 10 minutes.
                string finalOutputPath = Path.Combine(this.localRoot, fileName);

                // Rename local file.
                try
                {
                    File.Move(this.currentLocalPath, finalOutputPath);
                }
                catch (Exception)
                {
                    // Retry once, just in case.
                    try
                    {
                        File.Move(this.currentLocalPath, finalOutputPath);
                    }
                    catch (Exception exception)
                    {
                        string message =
                            $"ADLS Bulk Record Writer: cannot move file '{currentLocalPath}' to '{finalOutputPath}'.";
                        this.TelemetryClient.TrackException(exception, message);
                        throw new FatalException(message);
                    }
                }

                Stopwatch uploadTimer = Stopwatch.StartNew();
                string adlsDirectory = $"/local/Private/Upload/{this.adlsRoot}/{this.version}";
                try
                {
                    TransferStatus status = this.adlsClient.BulkUpload(this.localRoot, adlsDirectory);
                    bool retried = false;
                    if (status.EntriesFailed.Count != 0)
                    {
                        retried = true;
                        // Retry once.
                        status = this.adlsClient.BulkUpload(this.localRoot, adlsDirectory,
                            shouldOverwrite: IfExists.Fail);
                        if (status.EntriesFailed.Count != 0)
                        {
                            throw new FatalException($"Cannot bulk upload '{finalOutputPath}'.");
                        }
                    }

                    uploadTimer.Stop();
                    TimeSpan uploadDuration = uploadTimer.Elapsed;

                    Dictionary<string, string> properties = new Dictionary<string, string>()
                    {
                        {"Duration", uploadDuration.ToString()},
                        {"Retried", retried.ToString()},
                        {"SizeBytes", this.SizeInBytes.ToString()},
                    };
                    this.TelemetryClient.TrackEvent("AdlsUploadStats", properties);
                }
                finally
                {
                    try
                    {
                        File.Delete(finalOutputPath);
                    }
                    catch (Exception)
                    {
                        // Retry once, just in case.
                        try
                        {
                            File.Delete(finalOutputPath);
                        }
                        catch (Exception exception)
                        {
                            string message =
                                $"ADLS Bulk Record Writer: cannot delete file '{finalOutputPath}' after upload.";
                            this.TelemetryClient.TrackException(exception, message);
                            throw new FatalException(message);
                        }
                    }
                }

                string finalAdlsOutputPath = finalOutputPath.Replace($"{this.localRoot}\\", $"{adlsDirectory}/");
                this.AddOutputPath(finalAdlsOutputPath);
            }

            return Task.CompletedTask;
        }
    }
}
