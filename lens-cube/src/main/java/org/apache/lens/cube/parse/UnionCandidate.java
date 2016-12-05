package org.apache.lens.cube.parse;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Set;

import lombok.Getter;

import org.apache.lens.cube.metadata.FactPartition;
import org.apache.lens.cube.metadata.TimeRange;

/**
 * Represents a union of two candidates
 */
public class UnionCandidate implements Candidate {

  /**
   * Caching start and end time calculated for this candidate as it may have many child candidates.
   */
  Date startTime = null;
  Date endTime = null;
  /**
   * List of child candidates that will be union-ed
   */
  @Getter
  private List<Candidate> childCandidates;

  public UnionCandidate(List<Candidate> childCandidates) {
    this.childCandidates = childCandidates;
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
    //Note: concurrent calls not handled specifically (This should not be a problem even if we do
    //get concurrent calls).

    if (startTime == null) {
      Date minStartTime = childCandidates.get(0).getStartTime();
      for (Candidate child : childCandidates) {
        if (child.getStartTime().before(minStartTime)) {
          minStartTime = child.getStartTime();
        }
      }
      startTime = minStartTime;
    }
    return startTime;
  }

  @Override
  public Date getEndTime() {
    if (endTime == null) {
      Date maxEndTime = childCandidates.get(0).getEndTime();
      for (Candidate child : childCandidates) {
        if (child.getEndTime().after(maxEndTime)) {
          maxEndTime = child.getEndTime();
        }
      }
      endTime = maxEndTime;
    }
    return endTime;
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

    for (Candidate child : childCandidates) {
      if (!child.contains((candidate)))
        return false;
    }
    return true;
  }

  /**
   * TODO union : break the timerange into candidate specific time ranges and call evaluateCompleteness() for them.
   * TODO union : If any of the candidates returns false, this method should return false.
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