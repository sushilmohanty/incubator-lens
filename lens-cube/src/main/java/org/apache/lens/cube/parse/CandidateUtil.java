package org.apache.lens.cube.parse;

import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.hive.ql.parse.ASTNode;

/**
 * Created by puneet.gupta on 11/17/16.
 */
public class CandidateUtil {

  public static boolean isMeasureExpressionAnswerable(ASTNode exprNode, Candidate candidate, CubeQueryContext context)
    throws LensException {
    return candidate.getFactColumns().containsAll(HQLParser.getColsInExpr(
      context.getAliasForTableName(context.getCube()), exprNode));
  }



}
