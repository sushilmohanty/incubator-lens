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

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.antlr.runtime.CommonToken;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.lens.cube.metadata.*;

import lombok.Getter;
import lombok.Setter;
import org.apache.lens.server.api.error.LensException;

import static org.apache.hadoop.hive.ql.parse.HiveParser.Identifier;

/**
 * Represents a fact on a storage table and the dimensions it needs to be joined with to answer the query
 *
 */
@Slf4j
public class StorageCandidate implements Candidate, CandidateTable {

  /**
   * Participating fact, storage and dimensions for this StorageCandidate
   */
  @Getter
  private CubeFactTable fact;
  @Getter
  private String storageName;
  private Map<Dimension, CandidateDim> dimensions;
  private String name;
  @Getter
  private String alias;
  @Getter
  private final Map<String, String> storgeWhereStringMap = new HashMap<>();
  @Getter
  private final Map<TimeRange, Map<String, String>> rangeToStorageWhereMap = new HashMap<>();
  @Getter
  private final Map<String, ASTNode> storgeWhereClauseMap = new HashMap<>();
  @Getter
  @Setter
  private QueryAST queryAst;
  private final List<Integer> selectIndices = Lists.newArrayList();
  private final List<Integer> dimFieldIndices = Lists.newArrayList();
  @Getter
  private String fromString;

  public StorageCandidate(CubeInterface cube, CubeFactTable fact, String storageName, String alias) {
    if ((cube == null) || (fact == null) || (storageName == null) || (alias == null)) {
      throw new IllegalArgumentException("Cube,fact and storageName should be non null");
    }
    this.cube = cube;
    this.fact = fact;
    this.storageName = storageName;
    this.alias = alias;

    this.name = cube.getName() + "." + fact.getName() + "." + storageName;
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
    return this.name;
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
    return fact.weight();
  }

  @Override
  public boolean contains(Candidate candidate) {
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

  @Override
  public String toString() {
    return getName();
  }

  public String getStorageWhereString(String storageTable) {
    return storgeWhereStringMap.get(storageTable);
  }
  /**
   * Update the ASTs to include only the fields queried from this fact, in all the expressions
   *
   * @param cubeql
   * @throws LensException
   */
  public void updateASTs(CubeQueryContext cubeql) throws LensException {
    // update select AST with selected fields
    int currentChild = 0;
    for (int i = 0; i < cubeql.getSelectAST().getChildCount(); i++) {
      ASTNode selectExpr = (ASTNode) queryAst.getSelectAST().getChild(currentChild);
      Set<String> exprCols = HQLParser.getColsInExpr(cubeql.getAliasForTableName(cubeql.getCube()), selectExpr);
      if (getColumns().containsAll(exprCols)) {
        selectIndices.add(i);
        if (exprCols.isEmpty() // no direct fact columns
            // does not have measure names
            || (!containsAny(cubeql.getCube().getMeasureNames(), exprCols))) {
          dimFieldIndices.add(i);
        }
        ASTNode aliasNode = HQLParser.findNodeByPath(selectExpr, Identifier);
        String alias = cubeql.getSelectPhrases().get(i).getSelectAlias();
        if (aliasNode != null) {
          String queryAlias = aliasNode.getText();
          if (!queryAlias.equals(alias)) {
            // replace the alias node
            ASTNode newAliasNode = new ASTNode(new CommonToken(HiveParser.Identifier, alias));
            queryAst.getSelectAST().getChild(currentChild).replaceChildren(selectExpr.getChildCount() - 1,
                selectExpr.getChildCount() - 1, newAliasNode);
          }
        } else {
          // add column alias
          ASTNode newAliasNode = new ASTNode(new CommonToken(HiveParser.Identifier, alias));
          queryAst.getSelectAST().getChild(currentChild).addChild(newAliasNode);
        }
      } else {
        queryAst.getSelectAST().deleteChild(currentChild);
        currentChild--;
      }
      currentChild++;
    }

    // don't need to update where ast, since where is only on dim attributes and dim attributes
    // are assumed to be common in multi fact queries.

    // push down of having clauses happens just after this call in cubequerycontext
  }

  static boolean containsAny(Collection<String> srcSet, Collection<String> colSet) {
    if (colSet == null || colSet.isEmpty()) {
      return true;
    }
    for (String column : colSet) {
      if (srcSet.contains(column)) {
        return true;
      }
    }
    return false;
  }

  public void updateFromString(CubeQueryContext query, Set<Dimension> queryDims,
                               Map<Dimension, CandidateDim> dimsToQuery) throws LensException {
    fromString = "%s"; // to update the storage alias later
    if (query.isAutoJoinResolved()) {
      fromString =
          query.getAutoJoinCtx().getFromString(fromString, this, queryDims, dimsToQuery,
              query, this.getQueryAst());
    }
  }
  public ASTNode getStorageWhereClause(String storageTable) {
    return storgeWhereClauseMap.get(storageTable);
  }


  public String toHQL() throws LensException {
    //setMissingExpressions();
    String qfmt = getQueryFormat();
    Object[] queryTreeStrings = getQueryTreeStrings();
    if (log.isDebugEnabled()) {
      log.debug("qfmt: {} Query strings: {}", qfmt, Arrays.toString(queryTreeStrings));
    }
    String baseQuery = String.format(qfmt, queryTreeStrings);
    return baseQuery;
  }

  private String[] getQueryTreeStrings() throws LensException {
    List<String> qstrs = new ArrayList<String>();
    qstrs.add(queryAst.getSelectString());
    qstrs.add(queryAst.getFromString());
    if (!StringUtils.isBlank(queryAst.getWhereString())) {
      qstrs.add(queryAst.getWhereString());
    }
    if (!StringUtils.isBlank(queryAst.getGroupByString())) {
      qstrs.add(queryAst.getGroupByString());
    }
    if (!StringUtils.isBlank(queryAst.getHavingString())) {
      qstrs.add(queryAst.getHavingString());
    }
    if (!StringUtils.isBlank(queryAst.getOrderByString())) {
      qstrs.add(queryAst.getOrderByString());
    }
    if (queryAst.getLimitValue() != null) {
      qstrs.add(String.valueOf(queryAst.getLimitValue()));
    }
    return qstrs.toArray(new String[0]);
  }

  private final String baseQueryFormat = "SELECT %s FROM %s";

  private String getQueryFormat() {
    StringBuilder queryFormat = new StringBuilder();
    queryFormat.append(baseQueryFormat);
    if (!StringUtils.isBlank(queryAst.getWhereString())) {
      queryFormat.append(" WHERE %s");
    }
    if (!StringUtils.isBlank(queryAst.getGroupByString())) {
      queryFormat.append(" GROUP BY %s");
    }
    if (!StringUtils.isBlank(queryAst.getHavingString())) {
      queryFormat.append(" HAVING %s");
    }
    if (!StringUtils.isBlank(queryAst.getOrderByString())) {
      queryFormat.append(" ORDER BY %s");
    }
    if (queryAst.getLimitValue() != null) {
      queryFormat.append(" LIMIT %s");
    }
    return queryFormat.toString();
  }

}
