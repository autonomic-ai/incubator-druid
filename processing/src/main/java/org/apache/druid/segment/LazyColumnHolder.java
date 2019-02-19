/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.segment;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.ByteBuffer;
import javax.annotation.Nullable;
import org.apache.druid.common.utils.SerializerUtils;
import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import org.apache.druid.segment.column.BaseColumn;
import org.apache.druid.segment.column.BitmapIndex;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.column.ColumnDescriptor;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.SpatialIndex;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.selector.settable.SettableColumnValueSelector;

public class LazyColumnHolder implements ColumnHolder
{
  private String columnName;
  private ObjectMapper mapper;
  private SmooshedFileMapper smooshedFiles;
  private SerializerUtils serializerUtils;
  private ColumnConfig columnConfig;
  private ColumnCapabilities capabilities;

  LazyColumnHolder(String columnName,
      ObjectMapper objectMapper,
      SmooshedFileMapper smooshedFileMapper,
      ColumnConfig columnConfig,
      SerializerUtils serializerUtils)
  {
    this.columnName = columnName;
    this.mapper = objectMapper;
    this.smooshedFiles = smooshedFileMapper;
    this.columnConfig = columnConfig;
    this.serializerUtils = serializerUtils;
    this.capabilities = new ColumnCapabilitiesImpl()
        .setType(ValueType.valueOf("STRING"))
        .setDictionaryEncoded(false)
        .setHasBitmapIndexes(false)
        .setHasSpatialIndexes(false)
        .setHasMultipleValues(false);
  }

  @Override
  public ColumnCapabilities getCapabilities()
  {
    return null;
  }

  @Override
  public int getLength()
  {
    return 0;
  }

  @Override
  public BaseColumn getColumn()
  {
    return null;
  }

  @Nullable
  @Override
  public BitmapIndex getBitmapIndex()
  {
    return null;
  }

  @Nullable
  @Override
  public SpatialIndex getSpatialIndex()
  {
    return null;
  }

  @Override
  public SettableColumnValueSelector makeNewSettableColumnValueSelector()
  {
    return null;
  }

  public ColumnHolder deserializeColumn()
      throws IOException
  {
    ByteBuffer byteBuffer = smooshedFiles.mapFile(columnName);
    ColumnDescriptor serde = mapper.readValue(
        serializerUtils.readString(byteBuffer), ColumnDescriptor.class
    );
    return serde.read(byteBuffer, columnConfig, smooshedFiles);
  }
}
