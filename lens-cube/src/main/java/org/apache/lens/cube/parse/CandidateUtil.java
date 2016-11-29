package org.apache.lens.cube.parse;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
    String singleStorageTable = candidate.getStorageTable();
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
}
