package org.apache.lens.cube.parse;

import java.util.Collection;
import java.util.List;

import org.apache.lens.cube.metadata.TimeRange;

/**
 * Created by puneet.gupta on 11/14/16.
 */
public class UnionCandidate implements Candidate {
  List<Candidate> candidates;

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
