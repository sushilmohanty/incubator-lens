package org.apache.lens.cube.parse;

import java.util.*;

import org.apache.lens.cube.metadata.FactPartition;
import org.apache.lens.cube.metadata.TimeRange;
import org.apache.lens.server.api.error.LensException;

import lombok.Getter;

/**
 * Represents a union of two candidates
 */
public class UnionCandidate implements Candidate {

  /**
   * Caching start and end time calculated for this candidate as it may have many child candidates.
   */
  Date startTime = null;
  Date endTime = null;
  String toStr;
  @Getter
  String alias;
  /**
   * List of child candidates that will be union-ed
   */
  private List<Candidate> childCandidates;

  public UnionCandidate(List<Candidate> childCandidates, String alias) {
    this.childCandidates = childCandidates;
    this.alias = alias;
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
    double cost = 0.0;
    for (Candidate cand : childCandidates) {
      cost += cand.getCost();
    }
    return cost;
  }

  @Override
  public boolean contains(Candidate candidate) {
    if (this.equals(candidate)) {
      return true;
    }

    for (Candidate child : childCandidates) {
      if (child.contains((candidate)))
        return true;
    }
    return false;
  }

  @Override
  public Collection<Candidate> getChildren() {
    return childCandidates;
  }

  /**
   *
   * @param timeRange
   * @return
   */
  @Override
  public boolean evaluateCompleteness(TimeRange timeRange, boolean failOnPartialData) throws LensException {
    List<TimeRange> candidateRange = getTimeRangeForChildren(timeRange);
    boolean ret = true;
    for (int i = 0; i < childCandidates.size(); i++) {
      ret &= childCandidates.get(i).evaluateCompleteness(candidateRange.get(i), failOnPartialData);
    }
    return ret;
  }

  private List<TimeRange> getRangeForCandidates(List<Candidate> childCandidates, TimeRange timeRange) {
    return null;
  }

  @Override
  public Set<FactPartition> getParticipatingPartitions() {
    return null;
  }

  @Override
  public boolean isExpressionEvaluable(ExpressionResolver.ExpressionContext expr) {
    for (Candidate cand : childCandidates) {
      if (!cand.isExpressionEvaluable(expr)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public String toString() {
    if (this.toStr == null) {
      this.toStr = getToString();
    }
    return this.toStr;
  }

  private String getToString() {
    StringBuilder builder = new StringBuilder(10 * childCandidates.size());
    builder.append("UNION[");
    for (Candidate candidate : childCandidates) {
      builder.append(candidate.toString());
      builder.append(", ");
    }
    builder.delete(builder.length() - 2, builder.length());
    builder.append("]");
    return builder.toString();
  }

  private List<TimeRange> getTimeRangeForChildren(TimeRange timeRange) {
    Collections.sort(childCandidates, new Comparator<Candidate>() {
      @Override
      public int compare(Candidate o1, Candidate o2) {
        return o1.getCost() < o2.getCost() ? -1 : o1.getCost() == o2.getCost() ? 0 : 1;
      }
    });
    return getRangeForCandidates(childCandidates, timeRange);
  }

}