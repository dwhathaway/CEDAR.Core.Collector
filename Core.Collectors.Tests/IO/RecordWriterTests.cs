// Copyright (c) Microsoft Corporation. All rights reserved.

using System;
using System.Text.RegularExpressions;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Microsoft.CloudMine.Core.Collectors.Tests.IO
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