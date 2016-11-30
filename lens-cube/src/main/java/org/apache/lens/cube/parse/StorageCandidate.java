/**
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
package org.apache.lens.cube.parse;

import java.util.*;

import org.apache.lens.cube.metadata.*;

import lombok.Getter;
import lombok.Setter;

/**
 * Represents a fact on a storage table and the dimensions it needs to be joined with to answer the query
 *
 */
public class StorageCandidate implements Candidate {

  /**
   * Participating fact, storage and dimensions for this StorageCandidate
   */
  @Getter
  private CubeFactTable fact;
  @Getter
  private String storage;
  private Map<Dimension, CandidateDim> dimensions;


  @Getter
  private CubeInterface cube;

  /**
   * Cached fact columns
   */
  private Collection<String> factColumns;

  /**
   * This map holds Tags (A tag refers to one or more measures) that have incomplete (below configured threshold) data.
   * Value is a map of date string and %completeness.
   */
  @Getter
  @Setter
  private Map<String, Map<String, Float>> incompleteDataDetails;

  @Override
  public String toHQL() {
    return null;
  }

  @Override
  public QueryAST getQueryAst() {
    return null;
  }

  @Override
  public Collection<String> getFactColumns() {
    if (factColumns == null) {
      factColumns = fact.getValidColumns();
      if (factColumns == null) {
        factColumns = fact.getAllFieldNames();
      }
    }
    return factColumns;
  }

  @Override
  public Date getStartTime() {
    return fact.getStartTime();
  }

  @Override
  public Date getEndTime() {
    return fact.getEndTime();
  }

  public boolean isValidForTimeRange(TimeRange timeRange) {
    return (!timeRange.getFromDate().before(fact.getStartTime())) && (!timeRange.getToDate().after(fact.getEndTime()));
  }

  private boolean isValidBeforeTimeRange(TimeRange timeRange) {
    return fact.getStartTime().before(timeRange.getFromDate());
  }

  private boolean isValidAfterTimeRange(TimeRange timeRange) {
    return fact.getEndTime().after(timeRange.getToDate());
  }

  public boolean isPartiallyValidForTimeRange(TimeRange range) {
    return  isValidAfterTimeRange(range) || isValidBeforeTimeRange(range);
  }

  @Override
  public double getCost() {
    return 0;
  }

  @Override
  public boolean evaluateCompleteness(TimeRange timeRange) {
    return false;
  }

  @Override
  public Set<FactPartition> getParticipatingPartitions() {
    return null;
  }

  @Override
  public boolean isExpresionEvaluable(ExpressionResolver.ExpressionContext exptCtx) {
    return false;
  }

}
