using System;
using System.Collections.Generic;
using System.Text;

namespace Microsoft.CloudMine.Core.Collectors.IO
{
    public enum RecordWriterMode
    {
        // Writes a single record per file
        Single,

        // Writes multiple records per file using new line as delimiter
        LineDelimited,

        // Writes multiple records per file using comma as delimiter
        CommaDelimited,

        // Writes multiple records per file using comma as delimiter and opening/closing []
        CommaDelimitedArray
    }
}