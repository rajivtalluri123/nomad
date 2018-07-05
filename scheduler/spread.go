package scheduler

import (
	"fmt"
	"math"

	"github.com/hashicorp/go-memdb"
	"github.com/hashicorp/nomad/nomad/structs"
)

type SpreadConfig struct {
	scoreBooster float64
	weights      map[string]float64
	attribute    string
}

// SpreadIterator is used to spread allocations across a specified attribute
// according to preset weights that sum up to 1.0
type SpreadIterator struct {
	ctx           Context
	source        RankIterator
	namespace     string
	jobID         string
	tg            *structs.TaskGroup
	spreadConfigs []SpreadConfig
	errorBuilding error
	countMap      map[string]uint64
}

func NewSpreadIterator(ctx Context, source RankIterator, spreadConfigs []SpreadConfig) *SpreadIterator {
	iter := &SpreadIterator{
		ctx:           ctx,
		source:        source,
		spreadConfigs: spreadConfigs,
	}
	return iter
}

func (iter *SpreadIterator) Reset() {
	iter.source.Reset()
}

func (iter *SpreadIterator) SetJob(job *structs.Job) {
	iter.jobID = job.ID
	iter.namespace = job.Namespace
}

func (iter *SpreadIterator) SetTaskGroup(tg *structs.TaskGroup) {
	iter.tg = tg
	iter.computeCounts()
}

func (iter *SpreadIterator) Next() *RankedNode {
	for {
		option := iter.source.Next()
		if option == nil {
			return nil
		}

		// Nothing to do if the count map is empty
		if iter.countMap == nil || len(iter.countMap) == 0 {
			return option
		}

		// Resolve this node's attribute values from the spread config
		nodeProps := make(map[string]string)
		for _, sc := range iter.spreadConfigs {
			nProperty, ok := getProperty(option.Node, sc.attribute)
			if !ok {
				continue
			}
			nodeProps[sc.attribute] = nProperty
		}
		// Iterate over each spread config and add a weighted score
		for _, sc := range iter.spreadConfigs {
			nodeValue := nodeProps[sc.attribute]
			count, _ := iter.countMap[nodeValue]
			weight, ok := sc.weights[nodeValue]
			if !ok {
				// Warn about missing weight
				iter.ctx.Logger().Printf("[WARN] sched: missing weight for value %v in spread stanza for job %v", nodeValue, iter.jobID)
				continue
			}
			desiredCount := math.Round(float64(iter.tg.Count) * weight)
			if float64(count) < desiredCount {
				scoreBoost := ((desiredCount - float64(count)) / desiredCount) * sc.scoreBooster
				option.Score += scoreBoost
				iter.ctx.Metrics().ScoreNode(option.Node, "spread-factor", sc.scoreBooster)
			}
		}
		return option
	}
}

// computeCounts computes total matching counts across all
func (iter *SpreadIterator) computeCounts() {
	allocs, err := iter.ctx.State().AllocsByJob(memdb.NewWatchSet(), iter.namespace, iter.jobID, false)

	// Filter to the correct set of allocs
	//TODO will there be different spread rules at job level?
	// right now its assumed to be set only at task group level, and can inherit from the job
	allocs = filterAllocs(allocs, true, iter.tg.Name)

	// Get all the nodes that have been used by the allocs
	nodes, err := buildNodeMap(iter.ctx.State(), allocs)
	if err != nil {
		iter.errorBuilding = fmt.Errorf("failed to get job's allocations: %v", err)
		iter.ctx.Logger().Printf("[ERR] scheduler.dynamic-constraint: %v", iter.errorBuilding)
		return
	}
	existingValues := make(map[string]uint64)
	combinedValues := make(map[string]uint64)
	for _, sc := range iter.spreadConfigs {
		populateProperties(sc.attribute, allocs, nodes, existingValues)
		iter.computeProposedCounts(sc.attribute, existingValues, combinedValues)
	}

	iter.countMap = combinedValues

}

func (iter *SpreadIterator) computeProposedCounts(attribute string, existingValues map[string]uint64, combinedValues map[string]uint64) {

	// Reset the proposed properties
	proposedValues := make(map[string]uint64)
	clearedValues := make(map[string]uint64)

	// Gather the set of proposed stops.
	var stopping []*structs.Allocation
	for _, updates := range iter.ctx.Plan().NodeUpdate {
		stopping = append(stopping, updates...)
	}
	stopping = filterAllocs(stopping, false, iter.tg.Name)

	// Gather the proposed allocations
	var proposed []*structs.Allocation
	for _, pallocs := range iter.ctx.Plan().NodeAllocation {
		proposed = append(proposed, pallocs...)
	}
	proposed = filterAllocs(proposed, true, iter.tg.Name)

	// Get the used nodes
	both := make([]*structs.Allocation, 0, len(stopping)+len(proposed))
	both = append(both, stopping...)
	both = append(both, proposed...)
	nodes, err := buildNodeMap(iter.ctx.State(), both)
	if err != nil {
		iter.errorBuilding = err
		iter.ctx.Logger().Printf("[ERR] Error building spread: %v", err)
		return
	}

	// Populate the cleared values
	populateProperties(attribute, stopping, nodes, clearedValues)

	// Populate the proposed values
	populateProperties(attribute, proposed, nodes, proposedValues)

	// Remove any cleared value that is now being used by the proposed allocs
	for value := range proposedValues {
		current, ok := clearedValues[value]
		if !ok {
			continue
		} else if current == 0 {
			delete(clearedValues, value)
		} else if current > 1 {
			clearedValues[value]--
		}
	}

	// combine the counts of how many times the property has been used by
	// existing and proposed allocations
	for _, usedValues := range []map[string]uint64{existingValues, proposedValues} {
		for propertyValue, usedCount := range usedValues {
			combinedValues[propertyValue] += usedCount
		}
	}

	// Go through and discount the combined count when the value has been
	// cleared by a proposed stop.
	for propertyValue, clearedCount := range clearedValues {
		combined, ok := combinedValues[propertyValue]
		if !ok {
			continue
		}

		// Don't clear below 0.
		if combined >= clearedCount {
			combinedValues[propertyValue] = combined - clearedCount
		} else {
			combinedValues[propertyValue] = 0
		}
	}
	return
}
