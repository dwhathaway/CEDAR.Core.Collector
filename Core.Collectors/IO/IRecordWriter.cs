// Copyright (c) Microsoft Corporation. All rights reserved.

using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Microsoft.CloudMine.Core.Collectors.IO
{
    public interface IRecordWriter : IDisposable
    {
        IEnumerable<string> OutputPaths { get; }

        RecordWriterMode Mode { get; }

        string OutputPathLayout { get; }

        Dictionary<string, string> OutputPathParts { get; }

        void AddOutputPathPart(string key, string value);

        // void SetOutputPathPrefix(string outputPathPrefix);

        Task FinalizeAsync();

        Task WriteRecordAsync(JObject record, RecordContext context);

        Task NewOutputAsync(string recordType, int fileIndex = 0, string uniqueId = "");
    }
}