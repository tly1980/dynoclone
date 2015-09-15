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
const ONEfloat64 float64 = 1.0

func finish(done chan bool){
    done <- true
}

type Regulator struct {
    desire_tps float64
    batch_size float64
    desire_duration float64
    last_timestamp time.Time
}

func newRegulator(desire_tps, batch_size int) *Regulator{
    desire_duration := ONEfloat64 / float64(desire_tps)
    log.Printf("desire_tps: %v, desire_duration: %v, batch_size: %v\n", 
        desire_tps, desire_duration, batch_size)
    return &Regulator{ 
            float64(desire_tps),
            float64(batch_size),
            desire_duration,
            time.Now(),
        }

}

func (self *Regulator) time_it() (float64, float64) {
    now := time.Now()
    delta := now.Sub(self.last_timestamp)
    tps := self.batch_size / delta.Seconds()
    // return current_tps, sleep_duration
    return tps, self.desire_duration - tps / ONEfloat64
}

func (self *Regulator) sleep(){
    _, duration := self.time_it()
    if duration > 0.0 {
        log.Printf("sleep: %v", duration)
        to_sleep := time.Duration(duration) * time.Second
        log.Printf("sleep: %v", to_sleep)
        time.Sleep(to_sleep)
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
    work chan map[string]*dynamodb.Attribute, done chan bool,
    regulator *Regulator){
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
            regulator.sleep()
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
        regulator.sleep()
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

    reg := newRegulator(*tps, *batchSize)

    if err != nil {
        log.Fatal("Failed to auth", err)
    }

    work := make(chan map[string]*dynamodb.Attribute, *numIn)
    done := make(chan bool)

    for i := 0; i < *numIn; i++ {
        go read(*tableSrc, &auth, aws_region, default_cond, i, *numIn,  work, done)
    }
    //go write_2_std(work, done)
    for j := 0; j < *numOut; j++ {
        go write(
            *tableDst, &auth, aws_region,
            *batchSize, work, done, reg)
    }

    //wait for read
    for i :=0 ; i < *numIn; i ++ {
        <-done
    }
    

    close(work)
    //wiat for write
    for j := 0; j < *numOut; j++ {
        <-done
    }
    close(done)
}
