package scheduler

import (
	"testing"

	"github.com/hashicorp/nomad/helper/uuid"
	"github.com/hashicorp/nomad/nomad/mock"
	"github.com/hashicorp/nomad/nomad/structs"
	"github.com/stretchr/testify/require"
)

func TestSpreadIterator(t *testing.T) {
	state, ctx := testContext(t)
	dcs := []string{"dc1", "dc2", "dc1", "dc1"}
	var nodes []*RankedNode

	// Add these nodes to the state store
	for i, dc := range dcs {
		node := mock.Node()
		node.Datacenter = dc
		if err := state.UpsertNode(uint64(100+i), node); err != nil {
			t.Fatalf("failed to upsert node: %v", err)
		}
		nodes = append(nodes, &RankedNode{Node: node})
	}

	static := NewStaticRankIterator(ctx, nodes)

	job := mock.Job()
	tg := job.TaskGroups[0]
	job.TaskGroups[0].Count = 5
	// add allocs to nodes in dc1
	upserting := []*structs.Allocation{
		{
			Namespace: structs.DefaultNamespace,
			TaskGroup: tg.Name,
			JobID:     job.ID,
			Job:       job,
			ID:        uuid.Generate(),
			EvalID:    uuid.Generate(),
			NodeID:    nodes[0].Node.ID,
		},
		{
			Namespace: structs.DefaultNamespace,
			TaskGroup: tg.Name,
			JobID:     job.ID,
			Job:       job,
			ID:        uuid.Generate(),
			EvalID:    uuid.Generate(),
			NodeID:    nodes[2].Node.ID,
		},
	}

	if err := state.UpsertAllocs(1000, upserting); err != nil {
		t.Fatalf("failed to UpsertAllocs: %v", err)
	}

	spreadConfig := SpreadConfig{
		scoreBooster: 0.8,
		weights: map[string]float64{
			"dc1": 0.8,
			"dc2": 0.2,
		},
		attribute: "${node.datacenter}",
	}
	spreadIter := NewSpreadIterator(ctx, static, []SpreadConfig{spreadConfig})
	spreadIter.SetJob(job)
	spreadIter.SetTaskGroup(tg)

	out := collectRanked(spreadIter)

	// Expect nodes in dc1 with existing allocs to get a boost
	// Boost should be ((expected-actual)/expected)*scoreBooster
	// For this test, that becomes dc1 = ((4-2)/4 )* 0.8 = 0.4, and dc2=1/1*0.8
	expectedScores := map[string]float64{
		"dc1": 0.4,
		"dc2": 0.8,
	}
	for _, rn := range out {
		require.Equal(t, expectedScores[rn.Node.Datacenter], rn.Score)
	}

	// Update the plan to add more allocs to nodes in dc1
	ctx.plan.NodeAllocation[nodes[0].Node.ID] = []*structs.Allocation{
		{
			Namespace: structs.DefaultNamespace,
			TaskGroup: tg.Name,
			JobID:     job.ID,
			Job:       job,
			ID:        uuid.Generate(),
			NodeID:    nodes[0].Node.ID,
		},
		// Should be ignored as it is a different job.
		{
			Namespace: structs.DefaultNamespace,
			TaskGroup: "bbb",
			JobID:     "ignore 2",
			Job:       job,
			ID:        uuid.Generate(),
			NodeID:    nodes[0].Node.ID,
		},
	}
	ctx.plan.NodeAllocation[nodes[3].Node.ID] = []*structs.Allocation{
		{
			Namespace: structs.DefaultNamespace,
			TaskGroup: tg.Name,
			JobID:     job.ID,
			Job:       job,
			ID:        uuid.Generate(),
			NodeID:    nodes[3].Node.ID,
		},
	}
	// After this step there are enough allocs to meet the desired count in dc1

	// Reset the scores back to 0
	for _, node := range nodes {
		node.Score = 0
	}
	static = NewStaticRankIterator(ctx, nodes)
	spreadIter = NewSpreadIterator(ctx, static, []SpreadConfig{spreadConfig})
	spreadIter.SetJob(job)
	spreadIter.SetTaskGroup(tg)
	out = collectRanked(spreadIter)

	// Expect nodes in dc2 with existing allocs to get a boost
	// DC1 nodes are not boosted because there are enough allocs to meet
	// the desired count
	expectedScores = map[string]float64{
		"dc1": 0,
		"dc2": 0.8,
	}
	for _, rn := range out {
		require.Equal(t, expectedScores[rn.Node.Datacenter], rn.Score)
	}
}
