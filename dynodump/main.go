package main

import (
    "flag"
    "log"
    "time"

    "github.com/AdRoll/goamz/aws"
    "github.com/AdRoll/goamz/dynamodb"

    "dynoclone/utils"
    "dynoclone/reader"
    "dynoclone/writer"
    "dynoclone/base"
)

var numIn = flag.Int("numIn", 4, "Number of DynamoDB read thread")
var numOut = flag.Int("numOut", 1, "Number of DynamoDB read thread")
var batchSize = flag.Int("batchSize", 10, "size for DynamoDB batch write")
var tps = flag.Int("tps", 1000, "Default TPS")

var tableSrc = flag.String("src", "", "Src table name")
var dstPath = flag.String("dst", "", "Dst table name")
var region = flag.String("r", "ap-southeast-2", "Region. Default would be Sydney.")


func main(){
    flag.Parse()
    default_cond  := []dynamodb.AttributeComparison{}
    auth, err := aws.GetAuth("", "", "", time.Now())
    aws_region := aws.Regions[*region]
    

    if err != nil {
        log.Fatal("Failed to auth", err)
    }

    srcDesc, _, err := utils.Sniff(
        &auth, aws_region, *tableSrc, "")

    if err != nil {
        log.Fatal("Failed to describe table.", err)
    }

    work := make(chan map[string]*dynamodb.Attribute, *numIn)
    work2 := make(chan map[string]*dynamodb.Attribute, utils.WORK2_BUFFER)
    done := make(chan string)
    events := make(chan utils.Event, utils.EVENTS_BUFFER)
    var readers = make([]base.Reader, *numIn)
    var writers = make([]base.Writer, *numOut)
    mon := utils.NewMonitor(*tableSrc, *dstPath,
        srcDesc.ItemCount, *tps,
        done, events, readers, writers)

    // this one never requires a done signal
    go mon.Run()

    // read (4) => speed regulator (1) => write (4)
    for i := 0; i < *numIn; i++ {
        r := reader.NewDynoReader( *tableSrc, &auth, aws_region,
            default_cond, 
            i, *numIn, utils.READ_BATCH,
            work,
            done, events)

        readers[i] = r
        go r.Run(work)
    }

    // start a regulator to control the speed
    go utils.RegulatorThread(*tps, work, work2, done)


    for j := 0; j < *numOut; j++ {
        w := writer.NewJsonWriter(
            *dstPath,
            j,
            work2, done, events)
        writers[j] = w
        go w.Run(work2)
    }

    mon.Show()

    //wait for reader
    for i :=0 ; i < *numIn; i ++ {
        mon.Drain()
    }
    close(work)

    // wait for regulator
    mon.Drain()

    close(work2)

    //wait for writer
    for j := 0; j < *numOut; j++ {
        mon.Drain()
    }

    close(events)
    // for mon
    mon.Drain()

    mon.Show()
}
