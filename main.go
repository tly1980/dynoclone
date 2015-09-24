package main

import (
    "flag"
    "log"
    "time"
    "fmt"
    "sync"

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

var GLOBAL_STATS = map[string]int64{}

const READ_BATCH = 100
const WORK2_BUFFER = 10000
var RETRY_SLEEP_SEQUENCE = []time.Duration{
    time.Millisecond * 400,
    time.Millisecond * 800,
    time.Millisecond * 1600, 
    time.Millisecond * 3200,
    time.Millisecond * 6400,
}

type Stat struct {
    src string
    op_type string
    count int
}

func finish(
    done chan string, role string){
    done <- role
}


func regulator_thread(desire_tps int, 
    work chan map[string]*dynamodb.Attribute,
    work2 chan map[string]*dynamodb.Attribute,
    done chan string){
    defer finish(done, "regulator")

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


func stat_collect_thread(stats chan Stat, 
    done chan string){
    for s := range stats {
        key := fmt.Sprint("%s.%s", s.src, s.op_type)
        total, ok := GLOBAL_STATS[key]

        if ok != true {
            total = int64(0)
        }

        total += int64(s.count)
        GLOBAL_STATS[key] = total
    }

    done <- "stat_collector"
}




func read(tableName string, auth *aws.Auth, region aws.Region,
        attributeComparisons []dynamodb.AttributeComparison, 
        segid int, totalSeg int, 
        work chan map[string]*dynamodb.Attribute,
        done chan string,
        stats chan Stat){
    src := fmt.Sprintf("reader [%v]", segid)
    defer finish(done, src) // To signal that job is done
    
    optype := "ok:read"
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

        if count >= 1000 {
            // send stat
            stats <- Stat { src, optype, count }
            count = 0
        }


        for startKey != nil {
            items, startKey, err = table.ParallelScanPartialLimit(
                attributeComparisons, startKey, segid, totalSeg, READ_BATCH)
            if err == nil {
                for _, item := range items {
                    work <- item
                    count += 1

                    if count >= 1000 {
                        // send stat
                        stats <- Stat { src, optype, count }
                        count = 0
                    }

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

    // send stat
    stats <- Stat{src, optype, count}
}


func batch_shoot(src string, stats chan Stat, table *dynamodb.Table, batch [][]dynamodb.Attribute) error {
 
    m := map[string][][]dynamodb.Attribute{
        "Put": batch,
    }

    bw := table.BatchWriteItems(m)

    var err error
    var unprocessed map[string]interface {}
    max_retry_seq := len(RETRY_SLEEP_SEQUENCE) - 1


    for i, t := range RETRY_SLEEP_SEQUENCE {
        unprocessed, err = bw.Execute()

        if err == nil && len(unprocessed) == 0 {
            stats <- Stat {src, "ok:write", len(batch)}
            return nil
        }

        if err != nil && len(unprocessed) > 0{
            // unprocessed is not [][]dynamodb.Attribute 
            // So we just resent the whole batch 
            time.Sleep(t)
            bw = table.BatchWriteItems(m)

            if i < max_retry_seq {
                stats <- Stat { src, "resend", 1}
            }
        }
    }

    stats <- Stat { 
        src, 
        fmt.Sprintf("err:%s", err.Error()), 
        1,
    }

    return err
}

func write(
    writer_id int,
    tableName string, auth *aws.Auth, region aws.Region,
    batchSize int,
    work chan map[string]*dynamodb.Attribute,
    done chan string,
    stats chan Stat){
    src := fmt.Sprintf("writer [%d]", writer_id)
    defer finish(done, src) // To signal that job is done
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
            err = batch_shoot(src, stats, table, batch)
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
        err = batch_shoot(src, stats, table, batch)
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

func monitor_thread(){
    c := time.Tick(1000 * time.Second)
    for _ = range c {
        show_stat()
    }
}

var SHOW_STAT = &sync.Mutex{}

func show_stat(){
    SHOW_STAT.Lock()
    defer SHOW_STAT.Unlock()
    fmt.Printf("\033[H\033[2J")
    for k, v := range GLOBAL_STATS{
        fmt.Printf("%s: %v\n", k, v)
    }
}

func drain(done chan string){
    who := <- done
    log.Printf("%s finished", who)
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
    done := make(chan string)
    stats := make(chan Stat)

    go stat_collect_thread(stats, done)

    // this one never requires a done signal
    go monitor_thread()

    // read (4) => speed regulator (1) => write (4)
    for i := 0; i < *numIn; i++ {
        go read(
            *tableSrc, &auth, aws_region, 
            default_cond, i, *numIn,  work, done, stats)
    }

    // start a regulator to control the speed
    go regulator_thread(*tps, 
        work, work2, done)


    for j := 0; j < *numOut; j++ {
        go write(
            j,
            *tableDst, &auth, aws_region, 
            *batchSize, work2, done, stats)
    }

    show_stat()

    //wait for read
    for i :=0 ; i < *numIn; i ++ {
        drain(done)
    }
    close(work)

    // wait for regulator
    drain(done)

    close(work2)

    //wait for write
    for j := 0; j < *numOut; j++ {
        drain(done)
    }

    close(stats)
    // for stat_collector
    drain(done)

    show_stat()
}
