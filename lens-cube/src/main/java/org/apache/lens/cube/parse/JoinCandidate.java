package org.apache.lens.cube.parse;

import java.util.Collection;
import java.util.List;

import org.apache.lens.cube.metadata.TimeRange;

public class JoinCandidate implements Candidate {

   List<Candidate> candidatesForJoin;

   private String getJoinCondition(Candidate candidate1, Candidate candidate2) {
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
   public Collection<String> getColumns() {
      return null;
   }

   @Override
   public boolean isValidForTimeRange(TimeRange timeRange) {
      return false;
   }

}
