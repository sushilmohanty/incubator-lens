package org.apache.lens.cube.parse;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.lens.cube.metadata.Dimension;
import org.apache.lens.cube.metadata.TimeRange;
import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.hive.ql.parse.ASTNode;

public interface Candidate {

  String toHQL();
  QueryAST getQueryAst();

  Collection<String> getColumns();

  boolean isValidForTimeRange(TimeRange timeRange);

  //Called during having push down
  boolean isExpressionAnswerable(ASTNode node, CubeQueryContext context) throws LensException;

  // NO caller Set<String> getTimePartCols(CubeQueryContext query) throws LensException;

  //TODO add methods to update AST in this candidate . Example push down having clause for join
  void updateFromString(CubeQueryContext query) throws LensException;

  /**
   * Update the ASTs to include only the fields queried from this fact, in all the expressions
   *
   * @param cubeql
   * @throws LensException
   */
  void updateASTs(CubeQueryContext cubeql) throws LensException;

  /**
   * Update Having clause. Required during pushdown of having conditions.
   * @param ast
   * @throws LensException
   */
  void addToHaving(ASTNode ast)  throws LensException;

  //Used Having push down flow
  String addAndGetAliasFromSelect(ASTNode ast, AliasDecider aliasDecider);

}
