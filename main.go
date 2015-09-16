package main

import (
    "flag"
    "log"
    "time"
    "fmt"

    "github.com/AdRoll/goamz/aws"
    "github.com/AdRoll/goamz/dynamodb"
)

var numIn = flag.Int("numIn", 4, "Number of DynamoDB read thread")
var numOut = flag.Int("numOut", 4, "Number of DynamoDB read thread")
var batchSize = flag.Int("batchSize", 10, "size for DynamoDB batch write")
var tps = flag.Int("tps", 1000, "Default TPS")

var tableSrc = flag.String("src", "", "Src table name")
var tableDst = flag.String("dst", "", "Dst table name")
var region = flag.String("r", "ap-southeast-2", "Region. Default would be Sydney.")

const READ_BATCH = 100
const WORK2_BUFFER = 10000

func finish(done chan bool){
    done <- true
}


func regulator_thread(desire_tps int, 
    work chan map[string]*dynamodb.Attribute,
    work2 chan map[string]*dynamodb.Attribute,
    done chan bool){

    defer finish(&done)

    desire_tp_01s := desire_tps / 10
    duration_01s := 100 * time.Millisecond 
    fmt.Printf("desire_tp_01s: %v, duration_01s: %v\n",
        desire_tp_01s, duration_01s)
    
    //defer close(tick) // cannot close receive only channel
    i := 0
    t1 := time.Now()
    for w := range work {
        work2 <- w
        i++
        if i % desire_tp_01s == 0 {
            t2 := time.Now()
            delta := duration_01s - t2.Sub(t1)
            if delta > 0 {
                time.Sleep(delta)
            }
            t1 = time.Now()
        }
    }
}



func read(tableName string, auth *aws.Auth, region aws.Region,
        attributeComparisons []dynamodb.AttributeComparison, 
        segid int, totalSeg int, 
        work chan map[string]*dynamodb.Attribute, 
        done chan bool){
    defer finish(done) // To signal that job is done
    server := dynamodb.New(*auth, region)

    //just test the connection to dyno table
    tableDesc, err := server.DescribeTable(tableName)
    if err != nil {
        log.Fatal("Could not DescribeTable", err)
    }
    pk, err := tableDesc.BuildPrimaryKey()
    if err != nil {
        log.Fatal("Could not BuildPrimaryKey", err)
    }

    table := server.NewTable(tableName, pk)

    items, startKey, err := table.ParallelScanPartialLimit(
        attributeComparisons, nil, segid, totalSeg, READ_BATCH)

    count := 0 
    if err == nil {
        for _, item := range items {
            work <- item
            count += 1
        }

        for startKey != nil {
            items, startKey, err = table.ParallelScanPartialLimit(
                attributeComparisons, startKey, segid, totalSeg, READ_BATCH)
            if err == nil {
                for _, item := range items {
                    work <- item
                    count += 1
                }
            }else{
                // fixme
                log.Fatal("failed to scan", err)
            }
        }
    }else{
        // fixme
        log.Fatal("failed to scan", err)
    }
}

func batch_shoot(table *dynamodb.Table, batch [][]dynamodb.Attribute) error {
    m := map[string][][]dynamodb.Attribute{
        "Put": batch,
    }

    bw := table.BatchWriteItems(m)
    unprocessed, err := bw.Execute()

    if err != nil {
        fmt.Printf("unprocessed: %v\n", len(unprocessed))
    }else{
        fmt.Printf(".")
    }
    return err
}

func write(
    tableName string, auth *aws.Auth, region aws.Region,
    batchSize int,
    work chan map[string]*dynamodb.Attribute, done chan bool){
    defer finish(done) // To signal that job is done
    server := dynamodb.New(*auth, region)

    //just test the connection to dyno table
    tableDesc, err := server.DescribeTable(tableName)
    if err != nil {
        log.Fatal("Could not DescribeTable", err)
    }
    pk, err := tableDesc.BuildPrimaryKey()
    if err != nil {
        log.Fatal("Could not BuildPrimaryKey", err)
    }


    table := server.NewTable(tableName, pk)
    batch := [][]dynamodb.Attribute{}

    for w := range work {
        item := map_to_item(w)
        batch = append(batch, *item)
        
        if len(batch) == batchSize {
            err = batch_shoot(table, batch)
            if err != nil {
                // fixme
                log.Printf("Failed to save DB(1): %v\n", err.Error())
                //log.Printf("Batch =: %v\n", batch)
            }
            batch = nil
        }
    }

    if len(batch) > 0 {
        // will do that again if batch isn't empty
        err = batch_shoot(table, batch)
        if err != nil {
            // fixme
            log.Printf("Failed to save DB(2): %v\n", err.Error())
            //log.Printf("Batch =: %v\n", batch)
        }
        batch = nil
    }
}

func map_to_item(obj map[string]*dynamodb.Attribute) *[]dynamodb.Attribute {
    items := []dynamodb.Attribute{}
    for _, v := range obj {
        items = append(items, *v)
    }

    return &items
}

func write_2_std(
    work chan map[string]*dynamodb.Attribute, done chan bool){
    defer finish(done)
    for i := range work{
        log.Printf("%v", map_to_item(i))
    }
}


func main(){
    flag.Parse()
    default_cond  := []dynamodb.AttributeComparison{}
    auth, err := aws.GetAuth("", "", "", time.Now())
    aws_region := aws.Regions[*region]

    if err != nil {
        log.Fatal("Failed to auth", err)
    }

    work := make(chan map[string]*dynamodb.Attribute, *numIn)
    work2 := make(chan map[string]*dynamodb.Attribute, WORK2_BUFFER)
    done := make(chan bool)

    // read (4) => speed regulator (1) => write (4)

    for i := 0; i < *numIn; i++ {
        go read(
            *tableSrc, &auth, aws_region, default_cond, i, *numIn,  work, done)
    }

    // start a regulator to control the speed
    go regulator_thread(*tps, work, work2)

    for j := 0; j < *numOut; j++ {
        go write(
            *tableDst, &auth, aws_region, *batchSize, work2, done)
    }

    //wait for read
    for i :=0 ; i < *numIn; i ++ {
        <-done
    }
    close(work)

    // wait for regulator
    <-done

    close(work2)

    //wait for write
    for j := 0; j < *numOut; j++ {
        <-done
    }
    close(done)
}
