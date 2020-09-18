// Copyright (c) Microsoft Corporation. All rights reserved.

using Microsoft.CloudMine.Core.Collectors.IO;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Text.RegularExpressions;

namespace Microsoft.CloudMine.Core.Collectors.IO.Tests
{
    [TestClass]
    public class RecordWriterTests
    {
        [TestMethod]
        public void OutputPathsGenerateProperly()
        {
            // var blobWriter = new AzureBlobRecordWriter("github", "")

            var regex = new Regex(@"\{(.*?)\}");

            var OutputPathLayout = "{OrganizationId}/{RepositoryId}/{yyyy}/{MM}/{dd}/{HH.mm.ss}_{Identifier}_{SessionId}_{RecordType}{FileIndex}";

            var matches = regex.Matches(OutputPathLayout);

            foreach(Match match in matches)
            {
                Console.Write(match.Groups[1].Value);
            }
        }
    }
}