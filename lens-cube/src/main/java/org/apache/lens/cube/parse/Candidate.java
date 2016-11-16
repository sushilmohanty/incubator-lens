package org.apache.lens.cube.parse;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.lens.cube.metadata.Dimension;
import org.apache.lens.cube.metadata.TimeRange;
import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.hive.ql.parse.ASTNode;

/**
 * This interface represents the main entity/data-structure that is involved in different phases of
 * query rewriting. At the lowest level, Candidate is represented by a StorageCandidate that has a
 * fact on a storage and other joined dimensions (if any) that will be required to answer the query
 * or part of the query. At a higher level Candidate can also be a Join or a Union Candidate representing
 * join or union between two other candidates
 *
 * Different Re-writers will work on applicable candidates to produce a final candidate which will be used
 * for generating the re-written query.
 *
 *
 */
public interface Candidate {

  /**
   * Returns String representation of this Candidate
   * @return
   */
  String toHQL();

  /**
   * Returns Query AST
   * @return
   */
  QueryAST getQueryAst();

  /**
   * Returns all the fact columns
   * TODO decide if we need to return Dimension columns too in a separate method
   * @return
   */
  Collection<String> getAllFactColumns();


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
