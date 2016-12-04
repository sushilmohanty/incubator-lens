package org.apache.lens.cube.parse;

import java.util.*;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import org.apache.lens.cube.metadata.CubeMetastoreClient;
import org.apache.lens.cube.metadata.MetastoreUtil;
import org.apache.lens.cube.metadata.TimeRange;
import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.parse.ASTNode;

/**
 * Placeholder for Util methods that will be required for {@link Candidate}
 */
public class CandidateUtil {

  /**
   * Is calculated measure expression answerable by the Candidate
   * @param exprNode
   * @param candidate
   * @param context
   * @return
   * @throws LensException
   */
  public static boolean isMeasureExpressionAnswerable(ASTNode exprNode, Candidate candidate, CubeQueryContext context)

    throws LensException {
    return candidate.getColumns().containsAll(HQLParser.getColsInExpr(
      context.getAliasForTableName(context.getCube()), exprNode));
  }

  /**
   * Returns true if the Candidate is valid for the given time range based on its start and end times.
   * @param candidate
   * @param timeRange
   * @return
   */
  public boolean isValidForTimeRange(Candidate candidate, TimeRange timeRange) {
    return (!timeRange.getFromDate().before(candidate.getStartTime()))
        && (!timeRange.getToDate().after(candidate.getEndTime()));
  }

  /**
   * Gets the time partition columns for a storage candidate
   * TODO decide is this needs to be supported for all Candidate types.
   *
   * @param candidate : Stoarge Candidate
   * @param metastoreClient : Cube metastore client
   * @return
   * @throws LensException
   */
  public Set<String> getTimePartitionCols(StorageCandidate candidate, CubeMetastoreClient metastoreClient)
      throws LensException {
    Set<String> cubeTimeDimensions = candidate.getCube().getTimedDimensions();
    Set<String> timePartDimensions = new HashSet<String>();
    String singleStorageTable = candidate.getStorageName();
    List<FieldSchema> partitionKeys = null;
    partitionKeys = metastoreClient.getTable(singleStorageTable).getPartitionKeys();
    for (FieldSchema fs : partitionKeys) {
      if (cubeTimeDimensions.contains(CubeQueryContext.getTimeDimOfPartitionColumn(candidate.getCube(),
          fs.getName()))) {
        timePartDimensions.add(fs.getName());
      }
    }
    return timePartDimensions;
  }

  /*
  static boolean checkForFactColumnExistsAndValidForRange(Set<CandidateFact> candidates,
                                                          Collection<QueriedPhraseContext> colSet,
                                                          CubeQueryContext cubeql) throws LensException {
    if (colSet == null || colSet.isEmpty()) {
      return true;
    }
    for (CandidateFact cfact : candidates) {
      for (QueriedPhraseContext qur : colSet) {
        if (!qur.isEvaluable(cubeql, cfact)) {
        return false;
        }
      }
    }
    return true;
  }
*/
  
  static boolean allEvaluableInSet(Set<Candidate> candidates, Collection<QueriedPhraseContext> colSet,
                              CubeQueryContext cubeql) throws LensException {

    for (Candidate cand : candidates) {
      if (!allEvaluableInSingleCandidate(cand, colSet, cubeql)) {
          return false;
      }
      }
    return true;
  }


  static boolean allEvaluableInSingleCandidate(Candidate cand, Collection<QueriedPhraseContext> colSet,
                                               CubeQueryContext cubeql) throws LensException {
      if (colSet == null || colSet.isEmpty()) {
        return true;
      }
        for (QueriedPhraseContext qur : colSet) {
          if (!qur.isEvaluable(cubeql, (StorageCandidate) cand)) {
            return true;
          }
        }
      return false;
  }

  /**
   * Copy Query AST from sourceAst to targetAst
   *
   * @param sourceAst
   * @param targetAst
   * @throws LensException
   */
  public void copyASTs(QueryAST sourceAst, QueryAST targetAst) throws LensException {
    targetAst.setSelectAST(MetastoreUtil.copyAST(sourceAst.getSelectAST()));
    targetAst.setWhereAST(MetastoreUtil.copyAST(sourceAst.getWhereAST()));
    if (sourceAst.getJoinAST() != null) {
      targetAst.setJoinAST(MetastoreUtil.copyAST(sourceAst.getJoinAST()));
    }
    if (sourceAst.getGroupByAST() != null) {
      targetAst.setGroupByAST(MetastoreUtil.copyAST(sourceAst.getGroupByAST()));
    }
  }

  public static Set<StorageCandidate> getStorageCandidates(Candidate candidate) {
    //TODO union : add implementation
    return null;
  }

  public static boolean checkForFactColumnExistsAndValidForRange(Collection<Candidate> candSet,
                                                                  Collection<QueriedPhraseContext> colSet,
                                                                  CubeQueryContext cubeql) throws LensException {
    if (colSet == null || colSet.isEmpty()) {
      return true;
    }
    boolean colExists = false;
    for (QueriedPhraseContext qur : colSet) {
      for (Candidate c : candSet) {
        if (!qur.isEvaluable(cubeql, (StorageCandidate) c)) {
          break;
        }
      }
    }
    return true;
  }



  public static boolean allEvaluable(StorageCandidate sc, Collection<QueriedPhraseContext> colSet,
                              CubeQueryContext cubeql) throws LensException {
    if (colSet == null || colSet.isEmpty()) {
      return true;
    }
    for (QueriedPhraseContext qur : colSet) {
      if (!qur.isEvaluable(cubeql, sc)) {
        return false;
      }
    }
    return true;
  }

  public static Set<QueriedPhraseContext> coveredMeasures(List<Candidate> candSet, Collection<QueriedPhraseContext> msrs,
                                                   CubeQueryContext cubeql) throws LensException {
    Set<QueriedPhraseContext> coveringSet = new HashSet<>();
    for (QueriedPhraseContext msr : msrs) {
      for (Candidate cand : candSet) {
        if (msr.isEvaluable(cubeql, (StorageCandidate) cand)) {
          coveringSet.add(msr);
        }
      }
    }
    return coveringSet;
  }
  
  /**
   * Returns true is the Candidates cover the entire time range.
   * @param candidates
   * @param startTime
   * @param endTime
   * @return
   */
  public static boolean isTimeRangeCovered(Collection<Candidate> candidates, Date startTime, Date endTime) {
    RangeSet<Date> set = TreeRangeSet.create();
    for (Candidate candidate : candidates) {
      set.add(Range.range(candidate.getStartTime(), BoundType.CLOSED, candidate.getEndTime(), BoundType.CLOSED));
    }
    return set.encloses(Range.range(startTime, BoundType.CLOSED, endTime, BoundType.OPEN));
  }

  public static Set<String> getColumns(Collection<QueriedPhraseContext> queriedPhraseContexts) {
    Set<String> cols = new HashSet<>();
    for (QueriedPhraseContext qur : queriedPhraseContexts) {
      cols.addAll(qur.getColumns());
    }
    return cols;
  }

}
