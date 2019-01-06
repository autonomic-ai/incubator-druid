/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.sql.calcite.rel;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import io.druid.java.util.common.ISE;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DimensionSpec;
import io.druid.sql.calcite.aggregation.Aggregation;
import io.druid.sql.calcite.aggregation.DimensionExpression;
import io.druid.sql.calcite.table.RowSignature;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class Windowing
{

  private final List<DimensionExpression> dimensions;
  private final List<Aggregation> aggregations;
  private final List<DimensionExpression> partitions;
  private final RowSignature outputRowSignature;

  private Windowing(
      final List<DimensionExpression> dimensions,
      final List<Aggregation> aggregations,
      final List<DimensionExpression> partitions,
      final RowSignature outputRowSignature
  )
  {
    this.dimensions = ImmutableList.copyOf(dimensions);
    this.aggregations = ImmutableList.copyOf(aggregations);
    this.partitions = partitions;
    this.outputRowSignature = outputRowSignature;

    // Verify no collisions.
    final Set<String> seen = Sets.newHashSet();
    for (DimensionExpression dimensionExpression : dimensions) {
      if (!seen.add(dimensionExpression.getOutputName())) {
        throw new ISE("Duplicate field name: %s", dimensionExpression.getOutputName());
      }
    }

    for (Aggregation aggregation : aggregations) {
      for (AggregatorFactory aggregatorFactory : aggregation.getAggregatorFactories()) {
        if (!seen.add(aggregatorFactory.getName())) {
          throw new ISE("Duplicate field name: %s", aggregatorFactory.getName());
        }
      }
      if (aggregation.getPostAggregator() != null && !seen.add(aggregation.getPostAggregator().getName())) {
        throw new ISE("Windowing aggregate function post aggregator missing field %s",
                      aggregation.getPostAggregator().getName());
      }
    }

    // Verify that items in the output signature exist.
    for (final String field : outputRowSignature.getRowOrder()) {
      if (!seen.contains(field)) {
        throw new ISE("Missing field in rowOrder: %s", field);
      }
    }
  }

  public static Windowing create(
      final List<DimensionExpression> dimensions,
      final List<Aggregation> aggregations,
      final List<DimensionExpression> partitions,
      final RowSignature outputRowSignature
  )
  {
    return new Windowing(dimensions, aggregations, partitions, outputRowSignature);
  }

  public List<DimensionExpression> getDimensions()
  {
    return dimensions;
  }

  public List<Aggregation> getAggregations()
  {
    return aggregations;
  }

  public List<PostAggregator> getPostAggregators()
  {
    return aggregations.stream()
                       .map(Aggregation::getPostAggregator)
                       .filter(Objects::nonNull)
                       .collect(Collectors.toList());
  }

  public RowSignature getOutputRowSignature()
  {
    return outputRowSignature;
  }

  public List<DimensionSpec> getDimensionSpecs()
  {
    return dimensions.stream()
                     .map(DimensionExpression::toDimensionSpec)
                     .collect(Collectors.toList());
  }

  public List<DimensionSpec> getPartitionSpecs()
  {
    return partitions.stream()
                     .map(DimensionExpression::toDimensionSpec)
                     .collect(Collectors.toList());
  }

  public List<AggregatorFactory> getAggregatorFactories()
  {
    return aggregations.stream()
                       .flatMap(aggregation -> aggregation.getAggregatorFactories().stream())
                       .collect(Collectors.toList());
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final Windowing window = (Windowing) o;
    return Objects.equals(dimensions, window.dimensions) &&
           Objects.equals(aggregations, window.aggregations) &&
           Objects.equals(partitions, window.partitions) &&
           Objects.equals(outputRowSignature, window.outputRowSignature);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dimensions, aggregations, partitions, outputRowSignature);
  }

  @Override
  public String toString()
  {
    return "Windowing{" +
           "dimensions=" + dimensions +
           ", aggregations=" + aggregations +
           ", partitions=" + partitions +
           ", outputRowSignature=" + outputRowSignature +
           '}';
  }
}
