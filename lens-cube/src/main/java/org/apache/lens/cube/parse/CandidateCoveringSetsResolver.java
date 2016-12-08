package org.apache.lens.cube.parse;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.lens.cube.error.LensCubeErrorCode;
import org.apache.lens.cube.metadata.TimeRange;

import org.apache.lens.server.api.error.LensException;

import java.util.*;

@Slf4j
public class CandidateCoveringSetsResolver implements ContextRewriter {

  private List<UnionCandidate> unionCandidates = new ArrayList<>();
  private List<Candidate> finalCandidates = new ArrayList<>();
  private int unionCandidatealiasCounter = 0;
  private int joinCandidatealiasCounter = 0;

  public CandidateCoveringSetsResolver(Configuration conf) {
  }

  @Override
  public void rewriteContext(CubeQueryContext cubeql) throws LensException {

    Set<QueriedPhraseContext> queriedMsrs = new HashSet<>();
    for (QueriedPhraseContext qur : cubeql.getQueriedPhrases()) {
      if (qur.hasMeasures(cubeql)) {
        queriedMsrs.add(qur);
      }
    }
    // if no measures are queried, add all StorageCandidates individually as single covering sets
    if (queriedMsrs.isEmpty()) {
      finalCandidates.addAll(cubeql.getCandidates());
    }

    List<TimeRange> ranges = cubeql.getTimeRanges();
    resolveRangeCoveringFactSet(cubeql, ranges, queriedMsrs);
    List<List<UnionCandidate>> measureCoveringSets = resolveJoinCandidates(unionCandidates, queriedMsrs, cubeql);
    updateFinalCandidates(measureCoveringSets);
    log.info("Covering candidate sets :{}", finalCandidates);

    String msrString = CandidateUtil.getColumns(queriedMsrs).toString();
    if (finalCandidates.isEmpty()) {
      throw new LensException(LensCubeErrorCode.NO_FACT_HAS_COLUMN.getLensErrorInfo(), msrString);
    }
    // update final candidate sets
    cubeql.getCandidates().clear();
    cubeql.getCandidates().addAll(finalCandidates);
    // TODO : we might need to prune if we maintian two data structures in CubeQueryContext.
    //cubeql.pruneCandidateFactWithCandidateSet(CandidateTablePruneCause.columnNotFound(getColumns(queriedMsrs)));

    if (cubeql.getCandidateFacts().size() == 0) {
      throw new LensException(LensCubeErrorCode.NO_FACT_HAS_COLUMN.getLensErrorInfo(), msrString);
    }
  }

  private Candidate createJoinCandidateFromUnionCandidates(List<UnionCandidate> ucs) {
    Candidate cand;
    if (ucs.size() >= 2) {
      UnionCandidate first = ucs.get(0);
      UnionCandidate second = ucs.get(1);
      cand = new JoinCandidate(first, second, "jc" + joinCandidatealiasCounter++);
      for (int i = 2; i < ucs.size(); i++) {
        cand = new JoinCandidate(cand, ucs.get(i), "jc" + joinCandidatealiasCounter++);
      }
    } else {
      cand = ucs.get(0);
    }
    return cand;
  }

  private void updateFinalCandidates(List<List<UnionCandidate>> jcs) {
    int aliasCounter = 0;
    for (Iterator<List<UnionCandidate>> itr = jcs.iterator(); itr.hasNext(); ) {
      List<UnionCandidate> jc = itr.next();
      if (jc.size() == 1 && jc.iterator().next().getChildCandidates().size() == 1) {
        finalCandidates.add(jc.iterator().next().getChildCandidates().iterator().next());
      } else {
        finalCandidates.add(createJoinCandidateFromUnionCandidates(jc));
      }
    }
  }

  private boolean isCandidateCoveringTimeRanges(UnionCandidate uc, List<TimeRange> ranges) {
    for (Iterator<TimeRange> itr = ranges.iterator(); itr.hasNext(); ) {
      TimeRange range = itr.next();
      if (!CandidateUtil.isTimeRangeCovered(uc.getChildCandidates(), range.getFromDate(), range.getToDate())) {
        return false;
      }
    }
    return true;
  }

  private void pruneUnionCandidatesNotCoveringAllRanges(List<UnionCandidate> ucs, List<TimeRange> ranges) {
    for (Iterator<UnionCandidate> itr = ucs.iterator(); itr.hasNext(); ) {
      UnionCandidate uc = itr.next();
      if (!isCandidateCoveringTimeRanges(uc, ranges)) {
        itr.remove();
      }
    }
  }

  private void resolveRangeCoveringFactSet(CubeQueryContext cubeql, List<TimeRange> ranges,
                                           Set<QueriedPhraseContext> queriedMsrs) throws LensException {
    // All Candidates
    List<Candidate> allCandidates = new ArrayList<Candidate>(cubeql.getCandidates());
    // Partially valid candidates
    List<Candidate> allCandidatesPartiallyValid = new ArrayList<>();
    for (Candidate cand : allCandidates) {
      // Assuming initial list of candidates populated are StorageCandidate
      if (cand instanceof StorageCandidate) {
        StorageCandidate sc = (StorageCandidate) cand;
        if (CandidateUtil.isValidForTimeRanges(sc, ranges)) {
          List<Candidate> one = new ArrayList<Candidate>(Arrays.asList(CandidateUtil.cloneStorageCandidate(sc)));
          unionCandidates.add(new UnionCandidate(one, "uc" + unionCandidatealiasCounter++));
          continue;
        } else if (CandidateUtil.isPartiallyValidForTimeRanges(sc, ranges)) {
          allCandidatesPartiallyValid.add(CandidateUtil.cloneStorageCandidate(sc));
        }
      } else {
        throw new LensException("Not a StorageCandidate!!");
      }
    }
    // Get all covering fact sets
    List<UnionCandidate> unionCoveringSet =
        getCombinations(new ArrayList<Candidate>(allCandidatesPartiallyValid));
    // Sort the Collection based on no of elements
    Collections.sort(unionCoveringSet);
    // prune non covering sets
    pruneUnionCandidatesNotCoveringAllRanges(unionCoveringSet, ranges);
    // prune candidate set without common measure
    pruneUnionCoveringSetWithoutAnyCommonMeasure(unionCoveringSet, queriedMsrs, cubeql);
    // prune redundant covering sets
    pruneRedundantUnionCoveringSets(unionCoveringSet);
    // pruing done in the previous steps, now create union candidates
    unionCandidates.addAll(unionCoveringSet);

  }

  private boolean isMeasureAnswerableForCandidates(QueriedPhraseContext msr, UnionCandidate uc,
                                                   CubeQueryContext cubeql) throws LensException {
    for (Candidate cand : uc.getChildCandidates()) {
      if (!msr.isEvaluable(cubeql, (StorageCandidate) cand)) {
        return false;
      }
    }
    return true;
  }

  private void pruneUnionCoveringSetWithoutAnyCommonMeasure(List<UnionCandidate> ucs,
                                                       Set<QueriedPhraseContext> queriedMsrs,
                                                       CubeQueryContext cubeql) throws LensException {
    for (ListIterator<UnionCandidate> itr = ucs.listIterator(); itr.hasNext(); ) {
      boolean toRemove = true;
      UnionCandidate uc = itr.next();
      for (QueriedPhraseContext msr : queriedMsrs) {
        if (isMeasureAnswerableForCandidates(msr, uc, cubeql)) {
          toRemove = false;
          break;
        }
      }
      if (toRemove) {
        itr.remove();
      }
    }
  }

  private void pruneRedundantUnionCoveringSets(List<UnionCandidate> candidates) {
    for (int i = 0; i < candidates.size(); i++) {
      UnionCandidate current = candidates.get(i);
      int j = i + 1;
      for (ListIterator<UnionCandidate> itr = candidates.listIterator(j); itr.hasNext(); ) {
        UnionCandidate next = itr.next();
        if (next.getChildCandidates().containsAll(current.getChildCandidates())) {
          itr.remove();
        }
      }
    }
  }

  public List<UnionCandidate> getCombinations(final List<Candidate> candidates) {
    int aliasCounter = 0;
    List<UnionCandidate> combinations = new LinkedList<UnionCandidate>();
    int size = candidates.size();
    int threshold = Double.valueOf(Math.pow(2, size)).intValue() - 1;

    for (int i = 1; i <= threshold; ++i) {
      LinkedList<Candidate> individualCombinationList = new LinkedList<Candidate>();
      int count = size - 1;
      int clonedI = i;
      while (count >= 0) {
        if ((clonedI & 1) != 0) {
          individualCombinationList.addFirst(candidates.get(count));
        }
        clonedI = clonedI >>> 1;
        --count;
      }
      combinations.add(new UnionCandidate(individualCombinationList, "uc" + unionCandidatealiasCounter++ ));
    }
    return combinations;
  }

  private List<List<UnionCandidate>> resolveJoinCandidates(List<UnionCandidate> unionCandidates,
                                                           Set<QueriedPhraseContext> msrs,
                                                           CubeQueryContext cubeql) throws LensException {
    List<List<UnionCandidate>> msrCoveringSets = new ArrayList<>();
    List<UnionCandidate> ucSet = new ArrayList<>(unionCandidates);
    boolean evaluable = false;
    // Check if a single set can answer all the measures and exprsWithMeasures
    for (Iterator<UnionCandidate> i = ucSet.iterator(); i.hasNext(); ) {
      UnionCandidate uc = i.next();
      for (QueriedPhraseContext msr : msrs) {
        evaluable = isMeasureAnswerableForCandidates(msr, uc, cubeql) ? true : false;
        if (!evaluable) {
          break;
        }
      }
      if (evaluable) {
        // single set can answer all the measures as an UnionCandidate
        List<UnionCandidate> one = new ArrayList<>();
        one.add(uc);
        msrCoveringSets.add(one);
        i.remove();
      }
    }
    // Sets that contain all measures or no measures are removed from iteration.
    // find other facts
    for (Iterator<UnionCandidate> i = ucSet.iterator(); i.hasNext(); ) {
      UnionCandidate uc = i.next();
      i.remove();
      // find the remaining measures in other facts
      if (i.hasNext()) {
        Set<QueriedPhraseContext> remainingMsrs = new HashSet<>(msrs);
        Set<QueriedPhraseContext> coveredMsrs = CandidateUtil.coveredMeasures(uc.getChildCandidates(), msrs, cubeql);
        remainingMsrs.removeAll(coveredMsrs);

        List<List<UnionCandidate>> coveringSets = resolveJoinCandidates(ucSet, remainingMsrs, cubeql);
        if (!coveringSets.isEmpty()) {
          for (List<UnionCandidate> candSet : coveringSets) {
            candSet.add(uc);
            msrCoveringSets.add(candSet);
          }
        } else {
          log.info("Couldnt find any set containing remaining measures:{} {} in {}", remainingMsrs,
              ucSet);
        }
      }
    }
    log.info("Covering set {} for measures {} with factsPassed {}", msrCoveringSets, msrs, ucSet);
    return msrCoveringSets;
  }
}