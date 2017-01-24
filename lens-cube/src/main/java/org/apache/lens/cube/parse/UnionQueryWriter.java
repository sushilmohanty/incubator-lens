/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.lens.cube.parse;


import org.antlr.runtime.CommonToken;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.util.StringUtils;
import org.apache.lens.cube.metadata.MetastoreUtil;
import org.apache.lens.server.api.error.LensException;

import java.util.*;

import static org.apache.hadoop.hive.ql.parse.HiveParser.*;
import static org.apache.lens.cube.parse.HQLParser.*;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UnionQueryWriter {

  private QueryAST queryAst;
  private Map<HQLParser.HashableASTNode, ASTNode> innerToOuterSelectASTs = new HashMap<>();
  private Map<HQLParser.HashableASTNode, ASTNode> innerToOuterHavingASTs = new HashMap<>();
  private AliasDecider aliasDecider = new DefaultAliasDecider();
  private Candidate cand;
  private CubeQueryContext cubeql;
  Set<StorageCandidate> storageCandidates;
  public static final String DEFAULT_MEASURE = "0.0";

  public UnionQueryWriter(Candidate cand, CubeQueryContext cubeql) {
    this.cand = cand;
    this.cubeql = cubeql;
    storageCandidates = CandidateUtil.getStorageCandidates(cand);
  }

  public String toHQL() throws LensException {
    // Set the default value for the non queriable measures. If a measure is not
    // answerable from a StorageCandidate set it as 0.0
    for (StorageCandidate sc : storageCandidates) {
      updateSelectASTWithDefault(sc);
    }
    queryAst = DefaultQueryAST.fromStorageCandidate(storageCandidates.iterator().next(),
        storageCandidates.iterator().next().getQueryAst());
    // Get not default outer select expr asts
    processSelectAndHavingAST();
    processGroupByAST();
    processOrderByAST();
    updateAsts();
    CandidateUtil.updateFinalAlias(queryAst.getSelectAST(), cubeql);
    return CandidateUtil.buildHQLString(queryAst.getSelectString(), getFromString(), null,
        queryAst.getGroupByString(), queryAst.getOrderByString(),
        queryAst.getHavingString(), queryAst.getLimitValue());
  }

  private void updateAsts() {
    for (StorageCandidate sc : storageCandidates) {
      if (sc.getQueryAst().getHavingAST() != null) {
        sc.getQueryAst().setHavingAST(null);
      }
      if (sc.getQueryAst().getOrderByAST() != null) {
        sc.getQueryAst().setOrderByAST(null);
      }
      if (sc.getQueryAst().getLimitValue() != null) {
        sc.getQueryAst().setLimitValue(null);
      }
    }
  }

  private void processGroupByAST() throws LensException {
    if (queryAst.getGroupByAST() != null) {
      queryAst.setGroupByAST(processGroupByExpression(queryAst.getGroupByAST()));
    }
  }

  private ASTNode processHavingAST(ASTNode innerAst, AliasDecider aliasDecider, StorageCandidate sc)
      throws LensException {

    if (cubeql.getHavingAST() != null) {
      ASTNode havingCopy = MetastoreUtil.copyAST(cubeql.getHavingAST());
      Set<ASTNode> havingAggChildrenASTs = new LinkedHashSet<>();
      getAggregateHavingChildren(havingCopy, havingAggChildrenASTs);
      processHavingExpression(innerAst,havingAggChildrenASTs ,aliasDecider, sc);
      updateOuterHavingAST(havingCopy);
      queryAst.setHavingAST(havingCopy);
      HQLParser.getString(havingCopy);
    }
    return null;
  }

  private ASTNode updateOuterHavingAST(ASTNode node) {
    if (node.getToken().getType() == HiveParser.TOK_FUNCTION
        && (HQLParser.isAggregateAST(node))) {
        if (innerToOuterSelectASTs.containsKey(new HQLParser.HashableASTNode(node))
            || innerToOuterHavingASTs.containsKey(new HQLParser.HashableASTNode(node))) {
          ASTNode expr = innerToOuterSelectASTs.containsKey(new HQLParser.HashableASTNode(node)) ?
              innerToOuterSelectASTs.get(new HQLParser.HashableASTNode(node)) :
              innerToOuterHavingASTs.get(new HQLParser.HashableASTNode(node));
          node.getParent().setChild(0, expr);
        }
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      ASTNode child = (ASTNode) node.getChild(i);
      updateOuterHavingAST(child);
    }
    return node;
  }


  private void processOrderByAST() throws LensException {
    if (queryAst.getOrderByAST() != null) {
      queryAst.setOrderByAST(processOrderbyExpression(queryAst.getOrderByAST()));
    }
  }

  private ASTNode processOrderbyExpression(ASTNode astNode) throws LensException {
    if (astNode == null) {
      return null;
    }
    ASTNode outerExpression = new ASTNode(astNode);
    // sample orderby AST looks the following :
    /*
    TOK_ORDERBY
   TOK_TABSORTCOLNAMEDESC
      TOK_NULLS_LAST
         .
            TOK_TABLE_OR_COL
               testcube
            cityid
   TOK_TABSORTCOLNAMEASC
      TOK_NULLS_FIRST
         .
            TOK_TABLE_OR_COL
               testcube
            stateid
   TOK_TABSORTCOLNAMEASC
      TOK_NULLS_FIRST
         .
            TOK_TABLE_OR_COL
               testcube
            zipcode
     */
    for (Node node : astNode.getChildren()) {
      ASTNode child = (ASTNode) node;
      ASTNode outerOrderby = new ASTNode(child);
      ASTNode tokNullsChild = (ASTNode) child.getChild(0);
      ASTNode outerTokNullsChild = new ASTNode(tokNullsChild);
      outerTokNullsChild.addChild(getOuterAST((ASTNode) tokNullsChild.getChild(0), null, aliasDecider, null, true));
      outerOrderby.addChild(outerTokNullsChild);
      outerExpression.addChild(outerOrderby);
    }
    return outerExpression;
  }

  private ASTNode getDefaultNode(ASTNode aliasNode) throws LensException {
    ASTNode defaultNode = getSelectExprAST();
    defaultNode.addChild(HQLParser.parseExpr(DEFAULT_MEASURE));
    defaultNode.addChild(aliasNode);
    return defaultNode;
  }

  private ASTNode getSelectExprAST() {
    return new ASTNode(new CommonToken(HiveParser.TOK_SELEXPR, "TOK_SELEXPR"));
  }

  private void updateSelectASTWithDefault(StorageCandidate sc) throws LensException {
    ASTNode innerSelect = new ASTNode(new CommonToken(TOK_SELECT, "TOK_SELECT"));
    for (int i = 0; i < cubeql.getSelectPhrases().size(); i++) {
      SelectPhraseContext phrase = cubeql.getSelectPhrases().get(i);
      ASTNode aliasNode = new ASTNode(new CommonToken(Identifier, phrase.getSelectAlias()));
      // Check whether phrase is dimensions or answerable measure in StorageCandidate
      if (!phrase.hasMeasures(cubeql)
          || sc.getAnswerableMeasurePhraseIndices().contains(phrase.getPosition())) {
        ASTNode node = getSelectExprAST();
        node.addChild(getInnerSelectExprForAlias(sc, aliasNode.getText()));
        node.addChild(aliasNode);
        innerSelect.addChild(node);
      } else {
        innerSelect.addChild(getDefaultNode(aliasNode));
      }
    }
    sc.getQueryAst().setSelectAST(innerSelect);
  }


  private ASTNode getInnerSelectExprForAlias(StorageCandidate sc, String alias) {
    ASTNode expr = new ASTNode();
    ASTNode select = sc.getQueryAst().getSelectAST();
    for (int i = 0; i < select.getChildCount(); i++) {
      String innerAlias = select.getChild(i).getChild(1).getText();
      if (innerAlias.equals(alias)) {
        expr = (ASTNode) select.getChild(i).getChild(0);
        break;
      }
    }
    return expr;
  }

  private void processSelectAndHavingAST() throws LensException {
    ASTNode outerSelectAst = new ASTNode(queryAst.getSelectAST());
    ASTNode outerHavingAst = new ASTNode(new CommonToken(TOK_HAVING, "TOK_HAVING"));
    DefaultAliasDecider aliasDecider = new DefaultAliasDecider();//ASTNode originalSelectAST = MetastoreUtil.copyAST(sc.getQueryAst().getSelectAST());
    int selectAliasCounter = 0;
    for (StorageCandidate sc : storageCandidates) {
      aliasDecider.setCounter(0);
      ASTNode innerSelectAST = new ASTNode(new CommonToken(TOK_SELECT, "TOK_SELECT"));
      processSelectExpression(sc, outerSelectAst, innerSelectAST, aliasDecider);
      selectAliasCounter = aliasDecider.getCounter();
    }
    queryAst.setSelectAST(outerSelectAst);

    // Iterate over the StorageCandidates and add non projected having columns in inner select ASTs
    for (StorageCandidate sc : storageCandidates) {
      aliasDecider.setCounter(selectAliasCounter);
      processHavingAST(sc.getQueryAst().getSelectAST(), aliasDecider, sc);
    }
  }
 private void processSelectExpression(StorageCandidate sc, ASTNode outerSelectAst,
                                       ASTNode innerSelectAST, AliasDecider aliasDecider) throws LensException {
    ASTNode selectAST = sc.getQueryAst().getSelectAST();
    if (selectAST == null) {
      return;
    }
    // iterate over all children of the ast and get outer ast corresponding to it.
    for (int i = 0; i < selectAST.getChildCount(); i++) {
      ASTNode child = (ASTNode) selectAST.getChild(i);
      ASTNode outerSelect = new ASTNode(child);
      ASTNode selectExprAST = (ASTNode) child.getChild(0);
      ASTNode outerAST = getOuterAST(selectExprAST, innerSelectAST, aliasDecider, sc, true);
      outerSelect.addChild(outerAST);
      // has an alias? add it
      if (child.getChildCount() > 1) {
        outerSelect.addChild(child.getChild(1));
      }
      if (outerSelectAst.getChildCount() <= selectAST.getChildCount()) {
        if (outerSelectAst.getChild(i) == null) {
          outerSelectAst.addChild(outerSelect);
        } else if (HQLParser.getString((ASTNode) outerSelectAst.getChild(i).getChild(0)).equals(DEFAULT_MEASURE)) {
          outerSelectAst.replaceChildren(i, i, outerSelect);
        }
      }
    }
    sc.getQueryAst().setSelectAST(innerSelectAST);
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
  private ASTNode getOuterAST(ASTNode astNode, ASTNode innerSelectAST,
      AliasDecider aliasDecider, StorageCandidate sc, boolean isSelectAst) throws LensException {
    if (astNode == null) {
      return null;
    }
    Set<String> msrCols = new HashSet<>();
    getAllColumnsOfNode(astNode, msrCols);
    if (isAggregateAST(astNode) && sc.getColumns().containsAll(msrCols)) {
      return processAggregate(astNode, innerSelectAST, aliasDecider, isSelectAst);
    } else if (isAggregateAST(astNode) && !sc.getColumns().containsAll(msrCols)) {
      ASTNode outerAST = new ASTNode(new CommonToken(HiveParser.TOK_SELEXPR, "TOK_SELEXPR"));
      outerAST.addChild(getOuterAST(getDefaultNode(null), innerSelectAST, aliasDecider, sc, isSelectAst));
      return outerAST;
    } else {
      if (hasAggregate(astNode)) {
        ASTNode outerAST = new ASTNode(astNode);
        for (Node child : astNode.getChildren()) {
          ASTNode childAST = (ASTNode) child;
          if (hasAggregate(childAST) && sc.getColumns().containsAll(msrCols)) {
            outerAST.addChild(getOuterAST(childAST, innerSelectAST, aliasDecider, sc, isSelectAst));
          } else if (hasAggregate(childAST) && !sc.getColumns().containsAll(msrCols)) {
            childAST.replaceChildren(1, 1, getDefaultNode(null));
            outerAST.addChild(getOuterAST(childAST, innerSelectAST, aliasDecider, sc, isSelectAst));
          } else {
            outerAST.addChild(childAST);
          }
        }
        return outerAST;
      } else {
        ASTNode innerSelectASTWithoutAlias = MetastoreUtil.copyAST(astNode);
        ASTNode innerSelectExprAST = new ASTNode(new CommonToken(HiveParser.TOK_SELEXPR, "TOK_SELEXPR"));
        innerSelectExprAST.addChild(innerSelectASTWithoutAlias);
        String alias = aliasDecider.decideAlias(astNode);
        ASTNode aliasNode = new ASTNode(new CommonToken(Identifier, alias));
        innerSelectExprAST.addChild(aliasNode);
        innerSelectAST.addChild(innerSelectExprAST);
        if (astNode.getText().equals(DEFAULT_MEASURE)) {
          ASTNode outerAST = new ASTNode(new CommonToken(HiveParser.TOK_SELEXPR, "TOK_SELEXPR"));
          outerAST.addChild(astNode);
          return outerAST;
        } else {
          ASTNode outerAST = getDotAST(cubeql.getCube().getName(), alias);
          if (isSelectAst) {
            innerToOuterSelectASTs.put(new HashableASTNode(innerSelectASTWithoutAlias), outerAST);
          } else {
            innerToOuterHavingASTs.put(new HashableASTNode(innerSelectASTWithoutAlias), outerAST);
          }
          return outerAST;
        }
      }
    }
  }

  private ASTNode processAggregate(ASTNode astNode, ASTNode innerSelectAST,
      AliasDecider aliasDecider, boolean isSelectAst) {
    ASTNode innerSelectASTWithoutAlias = MetastoreUtil.copyAST(astNode);
    ASTNode innerSelectExprAST = new ASTNode(new CommonToken(HiveParser.TOK_SELEXPR, "TOK_SELEXPR"));
    innerSelectExprAST.addChild(innerSelectASTWithoutAlias);
    String alias = aliasDecider.decideAlias(astNode);
    ASTNode aliasNode = new ASTNode(new CommonToken(Identifier, alias));
    innerSelectExprAST.addChild(aliasNode);
    innerSelectAST.addChild(innerSelectExprAST);
    ASTNode dotAST = getDotAST(cubeql.getCube().getName(), alias);
    ASTNode outerAST = new ASTNode(new CommonToken(TOK_FUNCTION, "TOK_FUNCTION"));
    //TODO: take care or non-transitive aggregate functions
    outerAST.addChild(new ASTNode(new CommonToken(Identifier, astNode.getChild(0).getText())));
    outerAST.addChild(dotAST);
    if (isSelectAst) {
      innerToOuterSelectASTs.put(new HashableASTNode(innerSelectASTWithoutAlias), outerAST);
    } else {
      innerToOuterHavingASTs.put(new HashableASTNode(innerSelectASTWithoutAlias), outerAST);
    }
    return outerAST;
  }


  private ASTNode processGroupByExpression(ASTNode astNode) throws LensException {
    ASTNode outerExpression = new ASTNode(astNode);
    // iterate over all children of the ast and get outer ast corresponding to it.
    for (Node child : astNode.getChildren()) {
      // Columns in group by should have been projected as they are dimension columns
      if (innerToOuterSelectASTs.containsKey(new HQLParser.HashableASTNode((ASTNode) child))) {
        outerExpression.addChild(innerToOuterSelectASTs.get(new HQLParser.HashableASTNode((ASTNode) child)));
      }
    }
    return outerExpression;
  }

  private void processHavingExpression(ASTNode innerSelectAst,Set<ASTNode> havingAggASTs,
      AliasDecider aliasDecider, StorageCandidate sc) throws LensException {
    // iterate over all children of the ast and get outer ast corresponding to it.
    for (ASTNode child : havingAggASTs) {
      if (!innerToOuterSelectASTs.containsKey(new HQLParser.HashableASTNode(child))) {
        getOuterAST(child, innerSelectAst, aliasDecider, sc, false);
      }
    }
  }

  private Set<ASTNode> getAggregateHavingChildren(ASTNode node, Set<ASTNode> havingClauses) {
    if (node.getToken().getType() == HiveParser.TOK_FUNCTION
        && (HQLParser.isAggregateAST(node)
        )) {
      havingClauses.add(node);
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      ASTNode child = (ASTNode) node.getChild(i);
      getAggregateHavingChildren(child, havingClauses);
    }
    return havingClauses;
  }

  private Set<String> getAllColumnsOfNode(ASTNode node, Set<String> msrs) {
    if (node.getToken().getType() == HiveParser.DOT) {
      String table = HQLParser.findNodeByPath(node, TOK_TABLE_OR_COL, Identifier).toString();
      msrs.add( node.getChild(1).toString());
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      ASTNode child = (ASTNode) node.getChild(i);
      getAllColumnsOfNode(child, msrs);
    }
    return msrs;
  }

  private String getFromString() throws LensException {
    StringBuilder from = new StringBuilder();
    List<String> hqlQueries = new ArrayList<>();
    for (StorageCandidate sc : storageCandidates) {
      hqlQueries.add(" ( " + sc.toHQL() + " ) ");
    }
    return from.append(" ( ")
        .append(StringUtils.join(" UNION ALL ", hqlQueries))
        .append(" ) as " + cubeql.getBaseCube()).toString();
  }

}
