package org.apache.lens.cube.parse;

import java.util.Collection;
import java.util.List;

import org.apache.lens.cube.metadata.TimeRange;

public class JoinCandidate implements Candidate {

   private Candidate joinCandidate1;
   private Candidate joinCandidate2;

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
