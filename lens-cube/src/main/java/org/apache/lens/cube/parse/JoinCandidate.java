package org.apache.lens.cube.parse;

import java.util.*;

import com.google.common.collect.Lists;
import org.antlr.runtime.CommonToken;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.util.StringUtils;
import org.apache.lens.cube.error.LensCubeErrorCode;
import org.apache.lens.cube.metadata.Cube;
import org.apache.lens.cube.metadata.FactPartition;
import org.apache.lens.cube.metadata.TimeRange;
import org.apache.lens.server.api.error.LensException;

import lombok.Getter;

import static org.apache.hadoop.hive.ql.parse.HiveParser.Identifier;

/**
 * Represents a join of two candidates
 */
public class JoinCandidate implements Candidate {

  /**
   * Child candidates that will participate in the join
   */
  private Candidate childCandidate1;
  private Candidate childCandidate2;
  private String toStr;
  @Getter
  private String alias;
  private QueryAST queryAST;
  private CubeQueryContext cubeql;

  public JoinCandidate(Candidate childCandidate1, Candidate childCandidate2, String alias, CubeQueryContext cubeql) {
    this.childCandidate1 = childCandidate1;
    this.childCandidate2 = childCandidate2;
    this.alias = alias;
    this.cubeql = cubeql;
  }

  private String getJoinCondition() {
    return null;
  }

  @Override
  public String toHQL() throws LensException {
    return CandidateUtil.createHQLQuery(getSelectString(), getFromString(), cubeql.getWhereString(),
        null, cubeql.getOrderByString(),
        null ,null);
  }

  @Override
  public QueryAST getQueryAst() {
    return null;
  }

  @Override
  public Collection<String> getColumns() {
    Set<String> columns = new HashSet<>();
    columns.addAll(childCandidate1.getColumns());
    columns.addAll(childCandidate2.getColumns());
    return columns;
  }

  @Override
  public Date getStartTime() {
    return childCandidate1.getStartTime().after(childCandidate2.getStartTime())
        ? childCandidate1.getStartTime()
        : childCandidate2.getStartTime();
  }

  @Override
  public Date getEndTime() {
    return childCandidate1.getEndTime().before(childCandidate2.getEndTime())
        ? childCandidate1.getEndTime()
        : childCandidate2.getEndTime();
  }

  @Override
  public double getCost() {
    return childCandidate1.getCost() + childCandidate2.getCost();
  }

  @Override
  public boolean contains(Candidate candidate) {
    if (this.equals(candidate)) {
      return true;
    } else
      return childCandidate1.contains(candidate) || childCandidate2.contains(candidate);
  }

  @Override
  public Collection<Candidate> getChildren() {
    return new ArrayList() {{
      add(childCandidate1);
      add(childCandidate2);
    }};
  }

  /**
   * @param timeRange
   * @return
   */
  @Override
  public boolean evaluateCompleteness(TimeRange timeRange, TimeRange parentTimeRange, boolean failOnPartialData)
      throws LensException {
    return this.childCandidate1.evaluateCompleteness(timeRange, parentTimeRange, failOnPartialData)
        && this.childCandidate2.evaluateCompleteness(timeRange, parentTimeRange, failOnPartialData);
  }

  @Override
  public Set<FactPartition> getParticipatingPartitions() {
    return null;
  }

  @Override
  public boolean isExpressionEvaluable(ExpressionResolver.ExpressionContext expr) {
    return childCandidate1.isExpressionEvaluable(expr) || childCandidate2.isExpressionEvaluable(expr);
  }

  @Override
  public void updateAnswerableQueriedColumns(CubeQueryContext cubeql) {

  }

  @Override
  public ArrayList<Integer> getAnswerableMeasureIndices() {
    Set<Integer> mesureIndices = new HashSet<>();
    Set<StorageCandidate> scs = new HashSet<>();
    scs.addAll(CandidateUtil.getStorageCandidates(getChildren()));
    // All children in the UnionCandiate will be having common quriable measure
    for (StorageCandidate sc : scs) {
      mesureIndices.addAll(sc.getAnswerableMeasureIndices());
    }
    return new ArrayList<>(mesureIndices);
  }

  @Override
  public String toString() {
    if (this.toStr == null) {
      this.toStr = getToString();
    }
    return this.toStr;
  }

  private String getToString() {
    return this.toStr = "JOIN[" + childCandidate1.toString() + ", " + childCandidate2.toString() + "]";
  }

  private String getSelectString() throws LensException {
    List<String> selectPhrases = new ArrayList<>();
    for (SelectPhraseContext spc : cubeql.getSelectPhrases()) {
      StringBuilder select = new StringBuilder();
      if (!spc.hasMeasures(cubeql)
          || isMeasureAnswerableByAllCandidates(spc.getPosition())) {
        select.append("COALESCE(")
            .append(childCandidate1.getAlias())
            .append(".")
            .append(spc.getSelectAlias())
            .append(",")
            .append(childCandidate2.getAlias())
            .append(".")
            .append(spc.getSelectAlias())
            .append(") as ")
            .append(spc.getFinalAlias());
        selectPhrases.add(select.toString());
      } else if (spc.hasMeasures(cubeql)) {
        Candidate cand = getMeasureAnswerableCandidate(spc.getPosition());
        select.append(cand.getAlias())
            .append(".")
            .append(spc.getSelectAlias())
            .append(" as ")
            .append(spc.getFinalAlias());
        selectPhrases.add(select.toString());
      }
    }
    return "SELECT " + StringUtils.join(", ", selectPhrases).toString();
  }

  private Candidate getMeasureAnswerableCandidate(int position) {
    if (childCandidate1.getAnswerableMeasureIndices().contains(position)) {
      return childCandidate1;
    } else {
      return childCandidate2;
    }
  }
  private boolean isMeasureAnswerableByAllCandidates(int position) {
    return  childCandidate1.getAnswerableMeasureIndices().contains(position) &&
        childCandidate2.getAnswerableMeasureIndices().contains(position);
  }

  private String getFromString() throws LensException {
    StringBuilder from = new StringBuilder();
    List<String> hqlQueries = new ArrayList<>();
    for (Candidate c : getChildren()) {
      hqlQueries.add(" ( " + c.toHQL() + " )  as " + c.getAlias());
    }
    return  from.append(" ( ")
        .append(StringUtils.join(" FULL OUTER JOIN ", hqlQueries))
        .append(" ) as " + alias).toString();
  }

}