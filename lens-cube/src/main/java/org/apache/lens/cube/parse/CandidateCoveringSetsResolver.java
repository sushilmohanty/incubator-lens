package org.apache.lens.cube.parse;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.lens.cube.metadata.Storage;
import org.apache.lens.cube.metadata.TimeRange;

import org.apache.lens.server.api.error.LensException;

import java.util.*;

@Slf4j
public class CandidateCoveringSetsResolver implements ContextRewriter{

  private Set<CandidateSets> rangeCoveringCandidates;

  public CandidateCoveringSetsResolver(Configuration conf) {}

  @Override
  public void rewriteContext(CubeQueryContext cubeql) throws LensException {
    List<TimeRange> ranges = cubeql.getTimeRanges();
    for (TimeRange range : ranges) {
      resolveRangeCoveringFactSet(cubeql, range);
    }

    Set<QueriedPhraseContext> queriedMsrs = new HashSet<>();
    for (QueriedPhraseContext qur : cubeql.getQueriedPhrases()) {
      if (qur.hasMeasures(cubeql)) {
        queriedMsrs.add(qur);
      }
    }
    findMeasureCoveringSets(rangeCoveringCandidates, queriedMsrs, cubeql);
    //Set<Set<Candidate>> measureCoveringSets = new HashSet<>();
    //pruneNonMeasureCoveringSets(rangeCoveringCandidates, queriedMsrs, cubeql);
    //cubeql.getCandidateSet().addAll(measureCoveringSets);
  }

  private void resolveRangeCoveringFactSet(CubeQueryContext cubeql, TimeRange range) throws LensException {
    // All facts
    Set<Candidate> allCandidates = cubeql.getCandidateSet();
    // Partially valid facts
    Set<Candidate> allCandidatesPartiallyValid = new HashSet<>();
    //Set<CandidateSets> rangeCoveringCandidates  = new HashSet<>();
    for (Candidate cand : allCandidates) {
      // Assuming initial list of candidates populated are StorageCandidate
      if (cand instanceof StorageCandidate) {
        StorageCandidate sc = (StorageCandidate) cand;
        if (sc.isValidForTimeRange(range)) {
          Set<Candidate> one = new HashSet<>(Arrays.asList((Candidate) sc));
          rangeCoveringCandidates.add(new CandidateSets(one));
        } else if (sc.isPartiallyValidForTimeRange(range)) {
          allCandidatesPartiallyValid.add(sc);
        }
      } else {
        throw new LensException("Not a StorageCandidate!!");
      }
    }
    // Get all covering fact sets
    List<List<Candidate>> coveringFactSets =
        getCombinations(new ArrayList<Candidate>(allCandidatesPartiallyValid));
    for (List<Candidate> sets : coveringFactSets) {
      CandidateSets set  =  new CandidateSets(new HashSet<Candidate>(sets));
      setMinAndMacDateForAllCandidateSets(set);
      rangeCoveringCandidates.add(set);
    }

    // Iterate over the covering set and remove the set which can't answer the time range
    for (Iterator<CandidateSets> itr = rangeCoveringCandidates.iterator(); itr.hasNext();) {
      CandidateSets set = itr.next();
      if (!(set.getStartTime().before(range.getFromDate()) && set.getEndTime().after(range.getToDate()))) {
        itr.remove();
      }
    }
    // Remove redundant covering sets
    removeRedundantCoveringSets(rangeCoveringCandidates);
  }



  public void removeRedundantCoveringSets(Set<CandidateSets> candidates) {
    Set<CandidateSets> set1 = new HashSet<>(candidates);
    Set<CandidateSets> set2 = new HashSet<>(candidates);
    for (CandidateSets set1Set : set1) {
      for (CandidateSets set2Set : set2) {
        if (set2Set.getCandidates().containsAll(set1Set.getCandidates())) {
          set2.remove(set2Set);
        }
      }
    }
  }

/*
  public Set<Set<CandidateFact>> pruneNonMeasureCoveringSets(Set<CandidateSets> rangeCoveringSets,
                                      Set<QueriedPhraseContext> msrs, CubeQueryContext cubeql) throws LensException {
      for (Iterator<CandidateSets> itr = rangeCoveringSets.iterator(); itr.hasNext();) {
        CandidateSets set = itr.next();
        for (QueriedPhraseContext msr : msrs) {
          for (Candidate cand : set.getCandidates()) {
            if (msr.isEvaluable(cubeql, cand)) {
              break;
            } else {
              itr.remove();
            }
          }
        }
      }
    return rangeCoveringSets;
  }
*/

  public List<List<Candidate>> getCombinations(final List<Candidate> candidates) {
    List<List<Candidate>> combinations = new LinkedList<List<Candidate>>();
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
      combinations.add(individualCombinationList);
    }
    return combinations;
  }

  private void setMinAndMacDateForAllCandidateSets(CandidateSets set) {
    Set<Date> startDates = new HashSet<>();
    Set<Date> endDates = new HashSet<>();
      for (Candidate candidate : set.getCandidates()) {
        startDates.add(candidate.getStartTime());
        endDates.add(candidate.getEndTime());
      }
      set.setStartTime(Collections.min(startDates));
      set.setEndTime(Collections.max(endDates));
  }

  private class CandidateSets {
    @Getter
    Set<Candidate> candidates ;
    @Getter
    @Setter
    Date startTime;
    @Getter
    @Setter
    Date endTime;
    public CandidateSets(Set<Candidate> candidates ) {
      this.candidates = candidates;
    }
  }


  static Set<Set<CandidateFact>> findMeasureCoveringSets(Set<CandidateSets> rangeCoveringSets,
                                                          Set<QueriedPhraseContext> msrs,
                                                          CubeQueryContext cubeql) throws LensException {
    Set<CandidateSets> candSets = new HashSet<>();
    //List<CandidateFact> cfacts = new ArrayList<>(cfactsPassed);
    for (Iterator<CandidateSets> i = rangeCoveringSets.iterator(); i.hasNext();) {
      CandidateSets candSet = i.next();
      if (CandidateUtil.allEvaluableInSet(candSet.getCandidates(), msrs, cubeql)) {
        // Set is answerable
        candSets.add(candSet);
        i.remove();
      } else {
        i.remove();
      }
    }
    // Sets that contain all measures or no measures are removed from iteration.
    // find other facts
    for (Iterator<CandidateSets> i = rangeCoveringSets.iterator(); i.hasNext();) {
      CandidateSets candSet = i.next();
      i.remove();
      // find the remaining measures in other facts
      if (i.hasNext()) {
        Set<QueriedPhraseContext> remainingMsrs = new HashSet<>(msrs);
        Set<QueriedPhraseContext> coveredMsrs  = CandidateUtil.coveredMeasures(candSet, msrs, cubeql);
        remainingMsrs.removeAll(coveredMsrs);

        Set<Set<CandidateFact>> coveringSets = findMeasureCoveringSets(rangeCoveringSets, remainingMsrs, cubeql);
        if (!coveringSets.isEmpty()) {
          //for (Set<CandidateFact> set : coveringSets) {
            cfactset.add(candSet);
         // }
        } else {
          log.info("Couldnt find any set containing remaining measures:{} {} in {}", remainingMsrs,
              rangeCoveringSets);
        }
      }
    }
    log.info("Covering set {} for measures {} with factsPassed {}", cfactset, msrs, rangeCoveringSets);
    return cfactset;
  }
}