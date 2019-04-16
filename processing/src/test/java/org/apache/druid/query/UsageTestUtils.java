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

package org.apache.druid.query;

import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.FloatSumAggregatorFactory;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.junit.Assert;

import java.util.HashMap;

public class UsageTestUtils
{
  public static int numTestRows = 837;

  public static final String[] COLUMNS = {"qualityFloat", "qualityDouble", "qualityNumericString", "placement",
                                          "placementish", "indexMaxPlusTen", "quality_uniques"};
  public static final String NULL_COLUMN = "partial_null_column";

  public static final VirtualColumn EXPR_COLUMN =
      new ExpressionVirtualColumn("normal_virtual", "index * 2 + indexMin", ValueType.DOUBLE, TestExprMacroTable.INSTANCE);
  public static final VirtualColumn CONSTANT_COLUMN =
      new ExpressionVirtualColumn("constant_virtual", "1", ValueType.FLOAT, TestExprMacroTable.INSTANCE);
  public static final VirtualColumn EMPTY_COLUMN =
      new ExpressionVirtualColumn("empty_virtual", "strlen(" + NULL_COLUMN + ")", ValueType.STRING, TestExprMacroTable.INSTANCE);
  public static final VirtualColumn FOO_COLUMN =
      new ExpressionVirtualColumn("foo", "strlen(bar)", ValueType.STRING, TestExprMacroTable.INSTANCE);

  public static final DimFilter FILTER = new SelectorDimFilter("market", "spot", null);

  public static final AggregatorFactory AGGREGATOR_FACTORY = new FloatSumAggregatorFactory("normal_agge", "indexMinFloat");


  public static void verify(QueryRunner runner, Query query, int signals)
  {
    HashMap<String, Object> context = new HashMap<String, Object>();
    runner.run(QueryPlus.wrap(query), context).toList();

    Assert.assertEquals(signals, query.getUsageCollector().getNumAuSignals().get());
  }

}
