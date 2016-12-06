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
public class StorageCandidate implements Candidate,CandidateTable {

  /**
   * Participating fact, storage and dimensions for this StorageCandidate
   */
  @Getter
  private CubeFactTable fact;
  @Getter
  private String storageName;
  private Map<Dimension, CandidateDim> dimensions;

  public StorageCandidate(CubeInterface cube, CubeFactTable fact, String storageName) {
    if ((cube == null) || (fact == null) || (storageName == null)) {
      throw new IllegalArgumentException("Cube,fact and storageName should be non null");
    }
    this.cube = cube;
    this.fact = fact;
    this.storageName = storageName;
  }

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
  public String getStorageString(String alias) {
    return null;
  }

  @Override
  public AbstractCubeTable getTable() {
    return fact;
  }

  @Override
  public AbstractCubeTable getBaseTable() {
    return (AbstractCubeTable)cube;
  }

  @Override
  public String getName() {
    return null;
  }

  @Override
  public Collection<String> getColumns() {
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

  @Override
  public double getCost() {
    return 0;
  }

  @Override
  public String getAlias() {
    return null;
  }

  @Override
  public boolean contains(Candidate candidate) {
    //TODO union : decide if we need to override equals and hashcode for StorageCandidate
    return this.equals(candidate);
  }

  @Override
  public Collection<Candidate> getChildren() {
    return null;
  }

  @Override
  public boolean evaluateCompleteness(TimeRange timeRange, boolean failOnPartialData) {
    return false;
  }

  @Override
  public Set<FactPartition> getParticipatingPartitions() {
    return null;
  }

  @Override
  public boolean isExpressionEvaluable(ExpressionResolver.ExpressionContext expr) {
    return expr.isEvaluable(this);
  }

  @Override
  public boolean equals(Object obj) {
    if (super.equals(obj)) {
      return true;
    }

    if (obj == null || !(obj instanceof StorageCandidate)) {
      return false;
    }

    StorageCandidate storageCandidateObj = (StorageCandidate) obj;
    //Assuming that same instance of cube and fact will be used across StorageCandidate s and hence relying directly
    //on == check for these.
    return (this.cube == storageCandidateObj.cube && this.fact == storageCandidateObj.fact
      && this.storageName.equals(storageCandidateObj.storageName));
  }

  @Override
  public int hashCode() {
    return this.cube.hashCode() + this.fact.hashCode() + this.storageName.hashCode();
  }
}
