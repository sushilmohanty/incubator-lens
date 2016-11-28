package org.apache.lens.cube.parse;

import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.Set;

import org.apache.lens.cube.metadata.Dimension;
import org.apache.lens.cube.metadata.FactPartition;
import org.apache.lens.cube.metadata.TimeRange;
import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.hive.ql.parse.ASTNode;

/**
 * This interface represents candidates that are involved in different phases of query rewriting.
 * At the lowest level, Candidate is represented by a StorageCandidate that has a fact on a storage
 * and other joined dimensions (if any) that are required to answer the query or part of the query.
 * At a higher level Candidate can also be a Join or a Union Candidate representing join or union
 * between other candidates
 *
 * Different Re-writers will work on applicable candidates to produce a final candidate which will be used
 * for generating the re-written query.
 */
public interface Candidate {

  /**
   * Returns String representation of this Candidate
   * TODO decide if this method should be moved to QueryAST instead
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
   * TODO decide if we need to return the participating Dimension columns too in a separate method
   * @return
   */
  Collection<String> getFactColumns();

  /**
   * Start Time for this candidate (calculated based on schema)
   * @return
   */
  Date getStartTime();

  /**
   * End Time for this candidate (calculated based on schema)
   * @return
   */
  Date getEndTime();

  /**
   * Returns the cost of this candidate
   * @return
   */
  double getCost();

  /**
   * Calculates if this candidate can answer the query for given time range based on actual data registerad with
   * the underlying candidate storages. This method will also update any internal candidate data structures that are
   * required for writing the re-written query and to answer {@link #getParticipatingPartitions()}.
   *
   * @param timeRange
   * @return true if this Candidate can answer query for the given time range.
   */
  boolean evaluateCompleteness(TimeRange timeRange);


  /**
   * Returns the set of fact partitions that will participate in this candidate.
   * Note: This method can be called only after call to {@link #evaluateCompleteness(TimeRange)}
   * @return
   */
  Set<FactPartition> getParticipatingPartitions();

  // Moved to CandidateUtil boolean isValidForTimeRange(TimeRange timeRange);
  // Moved to CandidateUtil boolean isExpressionAnswerable(ASTNode node, CubeQueryContext context) throws LensException;
  // NO caller Set<String> getTimePartCols(CubeQueryContext query) throws LensException;



  //TODO add methods to update AST in this candidate in this class of in CandidateUtil.
  //void updateFromString(CubeQueryContext query) throws LensException;

  //void updateASTs(CubeQueryContext cubeql) throws LensException;

  //void addToHaving(ASTNode ast)  throws LensException;

  //Used Having push down flow
  //String addAndGetAliasFromSelect(ASTNode ast, AliasDecider aliasDecider);

}
