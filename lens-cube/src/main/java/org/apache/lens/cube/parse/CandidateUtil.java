package org.apache.lens.cube.parse;

import org.apache.lens.cube.metadata.TimeRange;
import org.apache.lens.server.api.error.LensException;

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
    return candidate.getFactColumns().containsAll(HQLParser.getColsInExpr(
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


}
