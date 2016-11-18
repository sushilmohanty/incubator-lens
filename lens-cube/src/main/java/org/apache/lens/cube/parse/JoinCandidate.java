package org.apache.lens.cube.parse;

import java.util.Collection;

import org.apache.lens.cube.metadata.TimeRange;

/**
 * Represents a join of two candidates
 */
public class JoinCandidate implements Candidate {

  /**
   * Child candidates that will participate in the join
   */
   private Candidate childCandidate1;
   private Candidate childCandidate2;

   private String getJoinCondition() {
      return null;
   }


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
      return null;
   }

   @Override
   public boolean isValidForTimeRange(TimeRange timeRange) {
      return false;
   }
}
