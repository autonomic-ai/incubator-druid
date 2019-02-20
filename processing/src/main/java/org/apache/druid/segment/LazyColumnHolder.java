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
import com.google.common.base.Throwables;
import java.io.IOException;
import java.nio.ByteBuffer;
import javax.annotation.Nullable;
import org.apache.druid.common.utils.SerializerUtils;
import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.segment.column.BaseColumn;
import org.apache.druid.segment.column.BitmapIndex;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.column.ColumnDescriptor;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.SpatialIndex;
import org.apache.druid.segment.selector.settable.SettableColumnValueSelector;

public class LazyColumnHolder implements ColumnHolder
{
  private String columnName;
  private ObjectMapper mapper;
  private SmooshedFileMapper smooshedFiles;
  private SerializerUtils serializerUtils;
  private ColumnConfig columnConfig;
  @Nullable
  private ColumnHolder columnHolder;
  private static final EmittingLogger log = new EmittingLogger(LazyColumnHolder.class);

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
  }

  @Override
  public ColumnCapabilities getCapabilities()
  {
    checkAndInit();
    return columnHolder.getCapabilities();
  }

  @Override
  public int getLength()
  {
    checkAndInit();
    return columnHolder.getLength();
  }

  @Override
  public BaseColumn getColumn()
  {
    checkAndInit();
    return columnHolder.getColumn();
  }

  @Nullable
  @Override
  public BitmapIndex getBitmapIndex()
  {
    checkAndInit();
    return columnHolder.getBitmapIndex();
  }

  @Nullable
  @Override
  public SpatialIndex getSpatialIndex()
  {
    checkAndInit();
    return columnHolder.getSpatialIndex();
  }

  @Override
  public SettableColumnValueSelector makeNewSettableColumnValueSelector()
  {
    checkAndInit();
    return columnHolder.makeNewSettableColumnValueSelector();
  }

  private void checkAndInit()
  {
    if (columnHolder == null) {
      columnHolder = deserializeColumn();
    }
  }

  public ColumnHolder deserializeColumn()
  {
    try {
      ByteBuffer byteBuffer = smooshedFiles.mapFile(columnName);
      ColumnDescriptor serde = mapper.readValue(
          serializerUtils.readString(byteBuffer), ColumnDescriptor.class
      );
      return serde.read(byteBuffer, columnConfig, smooshedFiles);
    }
    catch (IOException e) {
      log.makeAlert(e, "Lazy cache failed for column %s", columnName)
          .emit();
      throw Throwables.propagate(e);
    }
  }
}
