package org.apache.lens.cube.parse;

import java.util.List;

/**
 * Created by puneet.gupta on 11/14/16.
 */
public class JoinCandidate implements Candidate {
   List<Candidate> candidates;

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
}
