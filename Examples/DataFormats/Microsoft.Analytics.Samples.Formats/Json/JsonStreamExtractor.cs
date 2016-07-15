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

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Text;
using Microsoft.Analytics.Interfaces;
using Newtonsoft.Json;

namespace Microsoft.Analytics.Samples.Formats.Json
{
    /// <summary>
    /// JsonExtractor (sample)
    ///
    ///     [
    ///         { c1:r1v1, c2:r1v2, ...},
    ///         { c1:r2v2, c2:r2v2, ...},
    ///         ...
    ///     ]
    ///     => IEnumerable[IRow]
    ///
    /// </summary>
    [SqlUserDefinedExtractor(AtomicFileProcessing=true)]
    public class JsonStreamExtractor : IExtractor
    {
        /// <summary/>
        public override IEnumerable<IRow> Extract(IUnstructuredReader input, IUpdatableRow output)
        {
            if (input.Length == 0)
                yield break;

            using (var reader = new JsonTextReader(new StreamReader(input.BaseStream)))
            {
                IColumn currentColumn = null;
                StringBuilder valueBuilder = null;
                JsonTextWriter writer = null;
                var startedGlobalObjects = 0;
                var startedLocalObjects = 0;
                var startedGlobalArrays = 0;
                var startedLocalArrays = 0;

                while (reader.Read())
                {
                    switch (reader.TokenType)
                    {
                        case JsonToken.StartArray:
                            startedGlobalArrays++;
                            if (currentColumn != null && currentColumn.Type == typeof(string))
                            {
                                if (writer == null)
                                {
                                    valueBuilder = new StringBuilder();
                                    writer = new JsonTextWriter(new StringWriter(valueBuilder));
                                }
                                startedLocalArrays++;
                                writer.WriteStartArray();
                            }
                            break;
                        case JsonToken.EndArray:
                            startedGlobalArrays--;
                            if (writer != null)
                            {
                                startedLocalArrays--;
                                writer.WriteEndArray();
                            }
                            if (currentColumn != null && valueBuilder != null
                                && startedLocalArrays == 0 && startedLocalObjects == 0)
                            {
                                output.Set(currentColumn.Name, valueBuilder.ToString());
                                writer = null;
                                valueBuilder = null;
                                currentColumn = null;
                            }
                            if (startedGlobalArrays == 0)
                            {
                                yield break;
                            }
                            break;

                        case JsonToken.StartObject:
                            startedGlobalObjects++;
                            if (currentColumn != null && currentColumn.Type == typeof(string))
                            {
                                if (writer == null)
                                {
                                    valueBuilder = new StringBuilder();
                                    writer = new JsonTextWriter(new StringWriter(valueBuilder));
                                }
                                startedLocalObjects++;
                                writer.WriteStartObject();
                            }
                            break;
                        case JsonToken.EndObject:
                            startedGlobalObjects--;
                            if (writer != null)
                            {
                                startedLocalObjects--;
                                writer.WriteEndObject();
                            }
                            if (currentColumn != null && valueBuilder != null
                                && startedLocalArrays == 0 && startedLocalObjects == 0)
                            {
                                output.Set(currentColumn.Name, valueBuilder.ToString());
                                writer = null;
                                valueBuilder = null;
                                currentColumn = null;
                            }
                            if (startedGlobalObjects == 0)
                                yield return output.AsReadOnly();
                            break;

                        case JsonToken.PropertyName:
                            if (writer != null)
                            {
                                writer.WritePropertyName(reader.Value.ToString());
                            }
                            else
                            {
                                var currentPropertyName = reader.Value.ToString();
                                currentColumn = output.Schema
                                    .FirstOrDefault(s => s.Name == currentPropertyName);
                                if (currentColumn == null)
                                    reader.Skip();
                            }
                            break;

                        case JsonToken.String:
                        case JsonToken.Boolean:
                        case JsonToken.Bytes:
                        case JsonToken.Date:
                        case JsonToken.Integer:
                        case JsonToken.Float:
                            if (writer != null)
                            {
                                writer.WriteValue(reader.Value);
                            }
                            else if (currentColumn != null)
                            {
                                var typeConverter = TypeDescriptor.GetConverter(currentColumn.Type);
                                if (typeConverter != null && typeConverter.CanConvertFrom(reader.ValueType))
                                {
                                    output.Set(currentColumn.Name, typeConverter.ConvertFrom(reader.Value));
                                }
                                else
                                    output.Set(currentColumn.Name, reader.Value);
                                currentColumn = null;
                            }
                            break;
                        case JsonToken.Null:
                            if (writer != null)
                            {
                                writer.WriteNull();
                            }
                            else if (currentColumn != null)
                            {
                                output.Set(currentColumn.Name, currentColumn.DefaultValue);
                                currentColumn = null;
                            }
                            break;

                        case JsonToken.StartConstructor:
                            writer?.WriteStartConstructor(reader.Value.ToString());
                            break;
                        case JsonToken.EndConstructor:
                            writer?.WriteEndConstructor();
                            break;
                        case JsonToken.Comment:
                            writer?.WriteComment(reader.Value.ToString());
                            break;
                        case JsonToken.Raw:
                            writer?.WriteRaw(reader.Value.ToString());
                            break;
                        case JsonToken.None:
                        case JsonToken.Undefined:
                            // ignore
                            break;
                        default:
                            throw new NotImplementedException();
                    }
                } while (reader.TokenType != JsonToken.None);
            }
        }
    }
}
