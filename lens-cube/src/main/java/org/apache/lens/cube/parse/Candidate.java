package org.apache.lens.cube.parse;

import java.util.Collection;

import org.apache.lens.cube.metadata.TimeRange;

import org.apache.hadoop.hive.ql.parse.ASTNode;

/**
 * Created by puneet.gupta on 11/14/16.
 */
public interface Candidate {

  public String toHQL();
  public QueryAST getQueryAst();

  public Collection<String> getColumns();
  public boolean isValidForTimeRange(TimeRange timeRange);

  //TODO add methods to update AST in this candidate . Example push down having clause for join

}
