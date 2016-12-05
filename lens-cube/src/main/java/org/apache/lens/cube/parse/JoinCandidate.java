package org.apache.lens.cube.parse;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Set;

import org.apache.lens.cube.metadata.FactPartition;
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

   public JoinCandidate(Candidate childCandidate1, Candidate childCandidate2) {
      this.childCandidate1 = childCandidate1;
      this.childCandidate2 = childCandidate2;
   }

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
   public Collection<String> getColumns() {
      return null;
   }

   @Override
   public Date getStartTime() {
      return childCandidate1.getStartTime().after(childCandidate2.getStartTime())
          ? childCandidate1.getStartTime() : childCandidate2.getStartTime();
   }

   @Override
   public Date getEndTime() {
      return childCandidate1.getEndTime().before(childCandidate2.getEndTime())
          ? childCandidate1.getEndTime() : childCandidate2.getEndTime();
   }

  @Override
  public double getCost() {
    return 0;
  }

  @Override
  public String getAlias() {
    return null;
  }

  @Override
  public boolean contains(Candidate candidate) {
    if (this.equals(candidate)) {
      return true;
    }
    else return childCandidate1.contains(candidate) && childCandidate2.contains(candidate);
  }


  /**
   * TODO union : call evaluateCompleteness for child candidates and retrun false if either call returns false.
   * @param timeRange
   * @return
   */
  @Override
  public boolean evaluateCompleteness(TimeRange timeRange, boolean failOnPartialData) {
    return false;
  }

  @Override
  public Set<FactPartition> getParticipatingPartitions() {
    return null;
  }

  @Override
  public boolean isExpressionEvaluable(ExpressionResolver.ExpressionContext expr) {
    return false;
  }
}