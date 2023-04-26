package main

import (
	"context"
	"fmt"
	"time"

	"github.com/risingwavelabs/risingwave-cloud/rpc/meta"
)

type myClient struct {
	inner meta.MetaRpcClient
}

func (c *myClient) DeleteWorkerNode(ctx context.Context, host string, port int32) {

	// this gets stuck
	// I assume it gets stuck, because meta marks the node as deleted, but does not delete it
	err := c.inner.DeleteWorkerNode(ctx, host, port)
	if err != nil {
		fmt.Printf("DeleteWorkerNode Error: %s", err.Error())
	}
}

func (c *myClient) GetSchedule(ctx context.Context) (*meta.Schedule, error) {
	return c.inner.GetSchedule(ctx)
}

// Adds or removes actors to or from parallel units.
func (c *myClient) Reschedule(ctx context.Context, addPUs, removePUs map[meta.FragmentID][]meta.ParallelUnitID) (bool, error) {
	return c.inner.Reschedule(ctx, addPUs, removePUs)
}

// Removes all actors from the given worker.
// This is required before shutting down a worker.
func (c *myClient) ClearWorkerNode(ctx context.Context, host string, port int32) (bool, error) {
	return c.inner.ClearWorkerNode(ctx, host, port)
}

func main() {
	clientTmp, err := meta.NewMetaRpcClient(context.TODO(), "localhost", 5690, nil)
	if err != nil {
		fmt.Printf("NewMetaRpcClient Error: %s\n", err.Error())
		return
	}
	client := &myClient{inner: *clientTmp}

	schedule := printSchedule(client)
	if schedule == nil {
		return
	}

	fmt.Printf("\n----------\n\n")

	worker := schedule.Workers[0]

	fmt.Printf("Clear Worker %s:%d\n", worker.Host, worker.Port)
	client.ClearWorkerNode(context.TODO(), worker.Host, worker.Port)

	fmt.Printf("\n----------\n\n")

	_ = printSchedule(client)

	fmt.Printf("\n----------\n\n")

	println(time.Now().String())
	fmt.Printf("Delete Worker %s:%d\n", worker.Host, worker.Port)
	client.DeleteWorkerNode(context.TODO(), worker.Host, worker.Port)

	fmt.Printf("\n----------\n\n")

	time.Sleep(5 * time.Second)
	_ = printSchedule(client)
}

func printSchedule(client meta.MetaNodeClient) *meta.Schedule {
	schedule, err := client.GetSchedule(context.TODO())
	if err != nil {
		fmt.Printf("GetSchedule Error: %s\n", err.Error())
		return nil
	}

	for _, fragment := range schedule.Fragments {
		printFragment(fragment)
	}
	for _, worker := range schedule.Workers {
		printWorker(worker)
	}

	return schedule
}

func printFragment(fragment meta.Fragment) {
	fmt.Printf("\n*** Fragment %d\n", fragment.ID)

	fmt.Printf("    flags:")
	if fragment.IsSource() {
		fmt.Printf(" Source")
	}
	if fragment.IsMaterialisedView() {
		fmt.Printf(" Materialised-View")
	}
	if fragment.IsSink() {
		fmt.Printf(" Sink")
	}
	if fragment.IsNow() {
		fmt.Printf(" Now")
	}
	if fragment.IsChainNode() {
		fmt.Printf(" Chain")
	}
	fmt.Println()

	fmt.Printf("  workers:")
	for _, actor := range fragment.Actors {
		fmt.Printf(" (%d -> %d)", actor.ID, actor.ParallelUnit)
	}
	fmt.Println()
}

func printWorker(worker meta.Worker) {
	fmt.Printf("\n*** Worker %d\n", worker.ID)

	fmt.Printf("  address: %s:%d", worker.Host, worker.Port)
	fmt.Printf("      PUs:")
	for _, puId := range worker.ParallelUnits {
		fmt.Printf(" %d", puId)
	}
	fmt.Println()
}

// onebox shutdown && \
// make start && \
// onebox setup monitor && \
// rwc register -account arne -password Password1! && \
// rwc login -account arne -password Password1! && \
// rwc tenant create -name my-tenant

// rwc tenant create-user -n my-tenant -u arne -p pwd && \
// psql 'postgres://arne:pwd@localhost:30010/dev?options=--tenant%3Drwc-1-my-tenant' < testData.sql

/*
create table s1 (v1 int, v2 float) with (
    connector = 'datagen',
    fields.v1.min = '1',
    fields.v1.max  = '100000',
    fields.v2.min = '1',
    fields.v2.max = '100000',
    datagen.rows.per.second='25',
    datagen.split.num = '12'
) row format json;

// kubectl port-forward risingwave-meta-default-0 -n rwc-1-my-tenant 5690:5690

// go run scaling.go

/*


first schedule. One CN, no nothing
address: 127.0.0.1:5688      PUs: 0 1 2 3 4 5 6 7 8 9

*/
