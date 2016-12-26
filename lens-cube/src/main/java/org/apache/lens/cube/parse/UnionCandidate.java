package org.apache.lens.cube.parse;

import java.util.*;

import org.antlr.runtime.CommonToken;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.optimizer.calcite.functions.CanAggregateDistinct;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.util.StringUtils;
import org.apache.lens.cube.metadata.FactPartition;
import org.apache.lens.cube.metadata.MetastoreUtil;
import org.apache.lens.cube.metadata.TimeRange;
import org.apache.lens.server.api.error.LensException;

import lombok.Getter;

import static org.apache.hadoop.hive.ql.parse.HiveParser.*;
import static org.apache.hadoop.hive.ql.parse.HiveParser.DOT;
import static org.apache.lens.cube.parse.HQLParser.*;

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
  CubeQueryContext cubeql;
  /**
   * List of child candidates that will be union-ed
   */
  private List<Candidate> childCandidates;
  @Getter
  private QueryAST queryAst;

  private Map<HashableASTNode, ASTNode> innerToOuterASTs = new HashMap<>();
  private AliasDecider aliasDecider = new DefaultAliasDecider();

  public UnionCandidate(List<Candidate> childCandidates, String alias, CubeQueryContext cubeql) {
    this.childCandidates = childCandidates;
    this.alias = alias;
    this.cubeql = cubeql;
  }

  @Override
  public String toHQL() throws LensException {
    queryAst = new DefaultQueryAST();
    updateAsts();
    if (this == cubeql.getPickedCandidate()) {
      CandidateUtil.updateFinalAlias(queryAst.getSelectAST(), cubeql);
    }
    return CandidateUtil.createHQLQuery(queryAst.getSelectString(), getFromString(), null,
        queryAst.getGroupByString(), queryAst.getOrderByString(),
        queryAst.getHavingString() ,queryAst.getLimitValue());
  }


  private void updateQueriableMeasureInAST(ASTNode node) {
    if (node == null) {
      return;
    }

    List<QueriedPhraseContext> contexts = cubeql.getQueriedPhrases();
    for (int i = 0; i < contexts.size(); i++) {
      if (contexts.get(i).hasMeasures(cubeql) &&
          !childMeasureIndices().contains(i) ){
        removeChildAST(node, contexts.get(i).getExprAST());
      }
    }
  }

  private void removeChildAST(ASTNode node, ASTNode child) {
    for (int i = 0; i < node.getChildCount(); i++) {
      if (node.getChild(i).equals(child)) {
        node.deleteChild(i);
      }
    }
  }

  private ArrayList<Integer> childMeasureIndices() {
    ArrayList<Integer> mesureIndices = new ArrayList<>();
    List<StorageCandidate> scs = new ArrayList<StorageCandidate>();
    scs.addAll(CandidateUtil.getStorageCandidates(childCandidates));
    // All children in the UnionCandiate will be having common quriable measure
    for (StorageCandidate sc : scs) {
      mesureIndices = (ArrayList<Integer>) sc.getMeasureIndices();
    }
    return mesureIndices;
  }

  private void updateAsts() {
    for (Candidate child : childCandidates) {
      if (child.getQueryAst().getHavingAST() != null) {
        child.getQueryAst().setHavingAST(null);
      }
      if (child.getQueryAst().getOrderByAST() != null) {
        child.getQueryAst().setOrderByAST(null);
      }
      if (child.getQueryAst().getLimitValue() != null) {
        child.getQueryAst().setLimitValue(null);
      }
    }
    processSelectAST();
    queryAst.setGroupByAST(processExpression(cubeql.getGroupByAST()));
    setHavingAST();
    setOrderByAST();
    setLimit();

    // update union candidate alias
    updateUnionCandidateAlias(queryAst.getSelectAST());
    updateUnionCandidateAlias(queryAst.getGroupByAST());
    updateUnionCandidateAlias(queryAst.getOrderByAST());
    updateUnionCandidateAlias(queryAst.getHavingAST());

    // update non quriable measure in ASTs
    updateQueriableMeasureInAST(queryAst.getSelectAST());
    updateQueriableMeasureInAST(queryAst.getGroupByAST());
    updateQueriableMeasureInAST(queryAst.getOrderByAST());
    updateQueriableMeasureInAST(queryAst.getHavingAST());
  }

  private void setHavingAST() {
    if (cubeql.getHavingAST() != null) {
      queryAst.setHavingAST(cubeql.getHavingAST());
    }
  }

  private void setOrderByAST() {
    if (cubeql.getOrderByAST() != null) {
      queryAst.setOrderByAST(cubeql.getOrderByAST());
    }
  }

  private void setLimit() {
    if (cubeql.getLimitValue() != null) {
      queryAst.setLimitValue(cubeql.getLimitValue());
    }
  }


  private String getFromString() throws LensException {
    StringBuilder from = new StringBuilder();
    List<String> hqlQueries = new ArrayList<>();
    for (Candidate c : childCandidates) {
      hqlQueries.add(c.toHQL());
    }
    return  from.append(" ( ")
        .append(StringUtils.join(" UNION ALL ", hqlQueries))
        .append(" ) as " + alias).toString();
  }

  @Override
  public Collection<String> getColumns() {
    Set<String> columns = new HashSet<>();
    for (Candidate cand : childCandidates) {
      columns.addAll(cand.getColumns());
    }
    return columns;
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
   * @param timeRange
   * @return
   */
  @Override
  public boolean evaluateCompleteness(TimeRange timeRange, TimeRange parentTimeRange, boolean failOnPartialData)
    throws LensException {
    Map<Candidate, TimeRange> candidateRange = splitTimeRangeForChildren(timeRange);
    boolean ret = true;
    for (Map.Entry<Candidate, TimeRange> entry : candidateRange.entrySet()) {
      ret &= entry.getKey().evaluateCompleteness(entry.getValue(), parentTimeRange, failOnPartialData);
    }
    return ret;
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
  public void updateAnswerableQueriedColumns(CubeQueryContext cubeql) {

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

  /**
   * Splits the parent time range for each candidate.
   * The candidates are sorted based on their costs.
   *
   * @param timeRange
   * @return
   */
  private Map<Candidate, TimeRange> splitTimeRangeForChildren(TimeRange timeRange) {
    Collections.sort(childCandidates, new Comparator<Candidate>() {
      @Override
      public int compare(Candidate o1, Candidate o2) {
        return o1.getCost() < o2.getCost() ? -1 : o1.getCost() == o2.getCost() ? 0 : 1;
      }
    });
    Map<Candidate, TimeRange> childrenTimeRangeMap = new HashMap<>();
    // Sorted list based on the weights.
    Set<TimeRange> ranges = new HashSet<>();
    ranges.add(timeRange);
    for (Candidate c : childCandidates) {
      TimeRange.TimeRangeBuilder builder = getClonedBuiler(timeRange);
      TimeRange tr = resolveTimeRangeForChildren(c, ranges, builder);
      if (tr != null) {
        // If the time range is not null it means this child candidate is valid for this union candidate.
        childrenTimeRangeMap.put(c, tr);
      }
    }
    return childrenTimeRangeMap;
  }

  /**
   * Resolves the time range for this candidate based on overlap.
   *
   * @param candidate : Candidate for which the time range is to be calculated
   * @param ranges    : Set of time ranges from which one has to be choosen.
   * @param builder   : TimeRange builder created by the common AST.
   * @return Calculated timeRange for the candidate. If it returns null then there is no suitable time range split for
   * this candidate. This is the correct behaviour because an union candidate can have non participating child
   * candidates for the parent time range.
   */
  private TimeRange resolveTimeRangeForChildren(Candidate candidate, Set<TimeRange> ranges,
    TimeRange.TimeRangeBuilder builder) {
    Iterator<TimeRange> it = ranges.iterator();
    Set<TimeRange> newTimeRanges = new HashSet<>();
    TimeRange ret = null;
    while (it.hasNext()) {
      TimeRange range = it.next();
      // Check for out of range
      if (candidate.getStartTime().getTime() >= range.getToDate().getTime() || candidate.getEndTime().getTime() <= range
        .getFromDate().getTime()) {
        continue;
      }
      // This means overlap.
      if (candidate.getStartTime().getTime() <= range.getFromDate().getTime()) {
        // Start time of the new time range will be range.getFromDate()
        builder.fromDate(range.getFromDate());
        if (candidate.getEndTime().getTime() <= range.getToDate().getTime()) {
          // End time is in the middle of the range is equal to c.getEndTime().
          builder.toDate(candidate.getEndTime());
        } else {
          // End time will be range.getToDate()
          builder.toDate(range.getToDate());
        }
      } else {
        builder.fromDate(candidate.getStartTime());
        if (candidate.getEndTime().getTime() <= range.getToDate().getTime()) {
          builder.toDate(candidate.getEndTime());
        } else {
          builder.toDate(range.getToDate());
        }
      }
      // Remove the time range and add more time ranges.
      it.remove();
      ret = builder.build();
      if (ret.getFromDate().getTime() == range.getFromDate().getTime()) {
        checkAndUpdateNewTimeRanges(ret, range, newTimeRanges);
      } else {
        TimeRange.TimeRangeBuilder b1 = getClonedBuiler(ret);
        b1.fromDate(range.getFromDate());
        b1.toDate(ret.getFromDate());
        newTimeRanges.add(b1.build());
        checkAndUpdateNewTimeRanges(ret, range, newTimeRanges);

      }
      break;
    }
    ranges.addAll(newTimeRanges);
    return ret;
  }

  private void checkAndUpdateNewTimeRanges(TimeRange ret, TimeRange range, Set<TimeRange> newTimeRanges) {
    if (ret.getToDate().getTime() < range.getToDate().getTime()) {
      TimeRange.TimeRangeBuilder b2 = getClonedBuiler(ret);
      b2.fromDate(ret.getToDate());
      b2.toDate(range.getToDate());
      newTimeRanges.add(b2.build());
    }
  }

  private TimeRange.TimeRangeBuilder getClonedBuiler(TimeRange timeRange) {
    TimeRange.TimeRangeBuilder builder = new TimeRange.TimeRangeBuilder();
    builder.astNode(timeRange.getAstNode());
    builder.childIndex(timeRange.getChildIndex());
    builder.parent(timeRange.getParent());
    builder.partitionColumn(timeRange.getPartitionColumn());
    return builder;
  }

  private void updateUnionCandidateAlias(ASTNode node) {
    if (node == null) {
      return;
    }
    if (node.getToken().getType() == DOT) {
        ASTNode table = HQLParser.findNodeByPath(node, TOK_TABLE_OR_COL, Identifier);
        if (table.getText().equals(cubeql.getCube().toString())) {
          table.getToken().setText(alias);
        }
      }
    for (int i = 0; i < node.getChildCount(); i++) {
      ASTNode child = (ASTNode) node.getChild(i);
      updateUnionCandidateAlias(child);
    }
  }

  private void processSelectAST() {
    ASTNode originalSelectAST = MetastoreUtil.copyAST(cubeql.getSelectAST());
    queryAst.setSelectAST(new ASTNode(originalSelectAST.getToken()));
    ASTNode outerSelectAST = processSelectExpression(originalSelectAST);
    queryAst.setSelectAST(outerSelectAST);
  }

  private ASTNode processExpression(ASTNode astNode) {
    if (astNode == null) {
      return null;
    }
    ASTNode outerExpression = new ASTNode(astNode);
    // iterate over all children of the ast and get outer ast corresponding to it.
    for (Node child : astNode.getChildren()) {
      outerExpression.addChild(getOuterAST((ASTNode)child));
    }
    return outerExpression;
  }

  private ASTNode processSelectExpression(ASTNode astNode) {
    if (astNode == null) {
      return null;
    }
    ASTNode outerExpression = new ASTNode(astNode);
    // iterate over all children of the ast and get outer ast corresponding to it.
    for (Node node : astNode.getChildren()) {
      ASTNode child = (ASTNode)node;
      ASTNode outerSelect = new ASTNode(child);
      ASTNode selectExprAST = (ASTNode)child.getChild(0);
      ASTNode outerAST = getOuterAST(selectExprAST);
      outerSelect.addChild(outerAST);

      // has an alias? add it
      if (child.getChildCount() > 1) {
        outerSelect.addChild(child.getChild(1));
      }
      outerExpression.addChild(outerSelect);
    }
    return outerExpression;
  }

  /*

 Perform a DFS on the provided AST, and Create an AST of similar structure with changes specific to the
 inner query - outer query dynamics. The resultant AST is supposed to be used in outer query.

 Base cases:
  1. ast is null => null
  2. ast is aggregate_function(table.column) => add aggregate_function(table.column) to inner select expressions,
           generate alias, return aggregate_function(cube.alias). Memoize the mapping
           aggregate_function(table.column) => aggregate_function(cube.alias)
           Assumption is aggregate_function is transitive i.e. f(a,b,c,d) = f(f(a,b), f(c,d)). SUM, MAX, MIN etc
           are transitive, while AVG, COUNT etc are not. For non-transitive aggregate functions, the re-written
           query will be incorrect.
  3. ast has aggregates - iterate over children and add the non aggregate nodes as is and recursively get outer ast
  for aggregate.
  4. If no aggregates, simply select its alias in outer ast.
  5. If given ast is memorized as mentioned in the above cases, return the mapping.
  */
  private ASTNode getOuterAST(ASTNode astNode) {
    if (astNode == null) {
      return null;
    }
    if (innerToOuterASTs.containsKey(new HQLParser.HashableASTNode(astNode))) {
      return innerToOuterASTs.get(new HQLParser.HashableASTNode(astNode));
    }
    if (isAggregateAST(astNode)) {
      return processAggregate(astNode);
    } else if (hasAggregate(astNode)) {
      ASTNode outerAST = new ASTNode(astNode);
      for (Node child : astNode.getChildren()) {
        ASTNode childAST = (ASTNode) child;
        if (hasAggregate(childAST)) {
          outerAST.addChild(getOuterAST(childAST));
        } else {
          outerAST.addChild(childAST);
        }
      }
      return outerAST;
    } else {
      ASTNode innerSelectASTWithoutAlias = MetastoreUtil.copyAST(astNode);
      ASTNode innerSelectExprAST = new ASTNode(new CommonToken(HiveParser.TOK_SELEXPR));
      innerSelectExprAST.addChild(innerSelectASTWithoutAlias);
      String alias = aliasDecider.decideAlias(astNode);
      ASTNode aliasNode = new ASTNode(new CommonToken(Identifier, alias));
      innerSelectExprAST.addChild(aliasNode);
      addToInnerSelectAST(innerSelectExprAST);
      ASTNode outerAST = getDotAST(cubeql.getCube().getName(), alias);
      innerToOuterASTs.put(new HashableASTNode(innerSelectASTWithoutAlias), outerAST);
      return outerAST;
    }
  }

  private ASTNode processAggregate(ASTNode astNode) {
    ASTNode innerSelectASTWithoutAlias = MetastoreUtil.copyAST(astNode);
    ASTNode innerSelectExprAST = new ASTNode(new CommonToken(HiveParser.TOK_SELEXPR));
    innerSelectExprAST.addChild(innerSelectASTWithoutAlias);
    String alias = aliasDecider.decideAlias(astNode);
    ASTNode aliasNode = new ASTNode(new CommonToken(Identifier, alias));
    innerSelectExprAST.addChild(aliasNode);
    addToInnerSelectAST(innerSelectExprAST);
    ASTNode dotAST = getDotAST(cubeql.getCube().getName(), alias);
    ASTNode outerAST = new ASTNode(new CommonToken(TOK_FUNCTION));
    //TODO: take care or non-transitive aggregate functions
    outerAST.addChild(new ASTNode(new CommonToken(Identifier, astNode.getChild(0).getText())));
    outerAST.addChild(dotAST);
    innerToOuterASTs.put(new HashableASTNode(innerSelectASTWithoutAlias), outerAST);
    return outerAST;
  }

  private void addToInnerSelectAST(ASTNode selectExprAST) {
    if (queryAst.getSelectAST() == null) {
      queryAst.setSelectAST(new ASTNode(new CommonToken(TOK_SELECT)));
    }
    queryAst.getSelectAST().addChild(selectExprAST);
  }

}