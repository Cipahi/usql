// 
// Copyright (c) Microsoft and contributors.  All rights reserved.
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// 
// See the License for the specific language governing permissions and
// limitations under the License.
// 
using Microsoft.Analytics.Interfaces;
using Microsoft.Analytics.Types.Sql;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;

namespace Microsoft.Analytics.Samples.Formats.FixedLength
{
    [SqlUserDefinedExtractor(AtomicFileProcessing = false)]
    public class FixedLengthExtractor : IExtractor
    {
        SqlMap<int, int> _fieldMap;

        public FixedLengthExtractor(Dictionary<int, int> fieldMap)
        {
            _fieldMap = new SqlMap<int, int>(fieldMap);
        }

        public FixedLengthExtractor(SqlMap<int, int> fieldMap)
        {
            _fieldMap = fieldMap;
        }

        public override IEnumerable<IRow> Extract(IUnstructuredReader input, IUpdatableRow output)
        {
            using(var reader = new StreamReader(input.BaseStream))
            {
                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    LineToRow(line, output);
                    yield return output.AsReadOnly();
                }
            }
        }

        protected virtual void LineToRow(string line, IUpdatableRow row)
        {
            int index = 0;
            foreach(var map in _fieldMap)
            {
                if (line.Length < map.Key + map.Value)
                {
                    index++;
                    continue;
                }

                if (index < row.Schema.Count && row.Schema[index].Type != typeof(string))
                {
                    var typeConverter = TypeDescriptor.GetConverter(row.Schema[index].Type);
                    if (typeConverter != null && typeConverter.CanConvertFrom(typeof(string)))
                    {
                        row.Set(index, typeConverter.ConvertFromString(line.Substring(map.Key, map.Value)));
                    }
                }
                else                    
                    row.Set(index, line.Substring(map.Key, map.Value));
                index++;
            }
        }
    }
}
