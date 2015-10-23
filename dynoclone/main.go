package main

import (
    "flag"
    "log"
    "time"

    "github.com/AdRoll/goamz/aws"
    "github.com/AdRoll/goamz/dynamodb"

    "github.com/tly1980/dynoclone/utils"
    "github.com/tly1980/dynoclone/reader"
    "github.com/tly1980/dynoclone/writer"
    "github.com/tly1980/dynoclone/base"
)

var numIn = flag.Int("numIn", 4, "Number of DynamoDB read thread")
var numOut = flag.Int("numOut", 4, "Number of DynamoDB read thread")
var batchSize = flag.Int("batchSize", 10, "size for DynamoDB batch write")
var tps = flag.Int("tps", 1000, "Default TPS")

var tableSrc = flag.String("src", "", "Src table name")
var tableDst = flag.String("dst", "", "Dst table name")
var region = flag.String("r", "ap-southeast-2", "Region. Default would be Sydney.")

func regulator_thread(desire_tps int, 
    work chan map[string]*dynamodb.Attribute,
    work2 chan map[string]*dynamodb.Attribute,
    done chan string){
    defer utils.Finish(done, "Regulator")

    desire_tp_01s := desire_tps / 10
    duration_01s := 100 * time.Millisecond 
    //fmt.Printf("desire_tp_01s: %v, duration_01s: %v\n",
    //    desire_tp_01s, duration_01s)
    
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

func sniff(auth *aws.Auth,
        region aws.Region, 
        src string,
        dst string) (*dynamodb.TableDescriptionT, *dynamodb.TableDescriptionT, error ) {
    server := dynamodb.New(*auth, region)

    //just test the connection to dyno table
    srcDesc, err := server.DescribeTable(src)

    if err != nil {
        return nil, nil, err
    }

    dstDesc, err := server.DescribeTable(dst)

    if err != nil {
        return nil, nil, err
    }

    return srcDesc, dstDesc, nil
}


func main(){
    flag.Parse()
    default_cond  := []dynamodb.AttributeComparison{}
    auth, err := aws.GetAuth("", "", "", time.Now())
    aws_region := aws.Regions[*region]
    

    if err != nil {
        log.Fatal("Failed to auth", err)
    }

    srcDesc, _, err := sniff(
        &auth, aws_region, *tableSrc, *tableDst)

    if err != nil {
        log.Fatal("Failed to describe table.", err)
    }

    work := make(chan map[string]*dynamodb.Attribute, *numIn)
    work2 := make(chan map[string]*dynamodb.Attribute, utils.WORK2_BUFFER)
    done := make(chan string)
    events := make(chan utils.Event, utils.EVENTS_BUFFER)
    var readers = make([]base.Reader, *numIn)
    var writers = make([]base.Writer, *numOut)
    mon := utils.NewMonitor(*tableSrc, *tableDst,
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
    go regulator_thread(*tps, 
        work, work2, done)


    for j := 0; j < *numOut; j++ {
        w := writer.NewDynoWriter(j,
            *tableDst, &auth, aws_region, 
            *batchSize, work2, done, events)
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
