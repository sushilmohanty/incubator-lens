package org.apache.lens.cube.parse;

import java.util.Collection;
import java.util.List;

import org.apache.lens.cube.metadata.TimeRange;

/**
 * Represents a union of two candidates
 */
public class UnionCandidate implements Candidate {

  /**
   * List of child candidates that will be union-ed
   */
  private List<Candidate> childCandidates;

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
