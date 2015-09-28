package main

import (
    "flag"
    "log"
    "time"
    "fmt"
    "sync"

    "github.com/AdRoll/goamz/aws"
    "github.com/AdRoll/goamz/dynamodb"
    "text/template"
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
const EVENTS_BUFFER = 10000
var RETRY_SLEEP_SEQUENCE = []time.Duration{
    time.Millisecond * 400,
    time.Millisecond * 800,
    time.Millisecond * 1600, 
    time.Millisecond * 3200,
    time.Millisecond * 6400,
}

type Event struct {
    src string
    category string
    detail string
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

type Reader struct {
    src string
    tableName string
    auth *aws.Auth
    region aws.Region
    attributeComparisons []dynamodb.AttributeComparison
    seg_id int
    total_seg int
    batch_size int
    count int64
    work chan map[string]*dynamodb.Attribute
    done chan string
    events chan Event
}

func (self *Reader) run(){
    defer finish(self.done, self.src) // To signal that job is done

    server := dynamodb.New(*self.auth, self.region)

    //just test the connection to dyno table
    tableDesc, err := server.DescribeTable(self.tableName)
    if err != nil {
        log.Fatal("Could not DescribeTable", err)
    }
    pk, err := tableDesc.BuildPrimaryKey()
    if err != nil {
        log.Fatal("Could not BuildPrimaryKey", err)
    }

    table := server.NewTable(self.tableName, pk)

    items, startKey, err := table.ParallelScanPartialLimit(
        self.attributeComparisons, nil, 
        self.seg_id, self.total_seg, int64(self.batch_size))

    if err == nil {
        for _, item := range items {
            self.work <- item
            self.count += 1
        }

        for startKey != nil {
            items, startKey, err = table.ParallelScanPartialLimit(
                self.attributeComparisons, startKey, 
                self.seg_id, self.total_seg, int64(self.batch_size))

            if err == nil {
                for _, item := range items {
                    self.work <- item
                    self.count += 1
                }

            }else{
                e := Event {
                    self.src,
                    "error",
                    err.Error(),
                }
                self.events <- e
            }
        }

    }else{
        e := Event {
            self.src,
            "error",
            err.Error(),
        }
        self.events <- e
    }

}

func newReader(
    tableName string,
    auth *aws.Auth,
    region aws.Region,
    attributeComparisons []dynamodb.AttributeComparison,
    seg_id int,
    total_seg int,
    batch_size int,
    work chan map[string]*dynamodb.Attribute,
    done chan string,
    events chan Event) *Reader {

    return &Reader{
        fmt.Sprintf("Reader [id=%d]", seg_id),
        tableName,
        auth,
        region,
        attributeComparisons,
        seg_id,
        total_seg,
        batch_size,
        int64(0),
        work,
        done,
        events,
    }
}



type Writer struct {
    src string
    writer_id int
    tableName string
    auth *aws.Auth
    region aws.Region
    batch_size int
    count int64
    work chan map[string]*dynamodb.Attribute
    done chan string
    events chan Event
}

func newWriter (
    writer_id int,
    tableName string, 
    auth *aws.Auth, 
    region aws.Region,
    batch_size int,
    work chan map[string]*dynamodb.Attribute,
    done chan string,
    events chan Event) *Writer {

    return &Writer{
        fmt.Sprintf("Writer [id=%d]", writer_id),
        writer_id,
        tableName, 
        auth, 
        region,
        batch_size,
        0,
        work,
        done,
        events,
    }
}

func (self *Writer) batch_shoot(
    table *dynamodb.Table,
    batch [][]dynamodb.Attribute) error {
 
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
            self.count += int64(len(batch))
            return nil
        }

        if err != nil && len(unprocessed) > 0{
            // unprocessed is not [][]dynamodb.Attribute 
            // So we just resent the whole batch 
            time.Sleep(t)
            bw = table.BatchWriteItems(m)

            if i < max_retry_seq {
                self.events <- Event { self.src, "resend", err.Error()}
            }
        }
    }

    return err
}

func (self *Writer) run(){
    defer finish(self.done, self.src) // To signal that job is done
    server := dynamodb.New(*self.auth, self.region)

    //just test the connection to dyno table
    tableDesc, err := server.DescribeTable(self.tableName)
    if err != nil {
        log.Fatal("Could not DescribeTable", err)
    }
    pk, err := tableDesc.BuildPrimaryKey()
    if err != nil {
        log.Fatal("Could not BuildPrimaryKey", err)
    }

    table := server.NewTable(self.tableName, pk)
    batch := [][]dynamodb.Attribute{}

    for w := range self.work {
        item := map_to_item(w)
        batch = append(batch, *item)
        
        if len(batch) == self.batch_size {
            err = self.batch_shoot(table, batch)
            if err != nil {
                self.events <- Event { 
                    self.src, 
                    "error",
                    err.Error(),
                }
            }
            batch = nil
        }
    }

    if len(batch) > 0 {
        // will do that again if batch isn't empty
        err = self.batch_shoot(table, batch)
        if err != nil {
            self.events <- Event { 
                self.src,
                "error",
                err.Error(),
            }
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


type Monitor struct {
    lock *sync.Mutex

    Src string
    Dst string
    SrcTotal int64
    DesireTps int
    last_call time.Time

    ReadCount int64
    WriteCount int64
    CurrentReadTps float64
    CurrentWriteTps float64

    ReadersTps []TpsStat
    WritersTps []TpsStat
    readers []*Reader
    writers []*Writer

    events chan Event
    event_map map[string]int64
    template *template.Template
}

type TpsStat struct {
    Src string
    Count int64
}


func newMonitor(
    src, dst string, 
    src_total int64, desire_tps int,
    events chan Event, readers []*Reader, writers []*Writer) *Monitor {
    tpl := `
Src: {{ .Src}}\t\tDst: {{ .Dst }}
DesireTps: {{ DesireTps }}
CurrentReadTps: {{ CurrentReadTps }}
CurrentWriteTps: {{ CurrentWriteTps }}
Readers: {{range .ReadersTps}} {{ .Src }}: {{ .Count }}\t{{end}}
Writers: {{range .WritersTps}} {{ .Src }}: {{ .Count }}\t{{end}}
`

    return &Monitor {
        lock: &sync.Mutex{},

        Src: src,
        Dst: dst,
        SrcTotal: src_total,
        DesireTps: desire_tps,
        last_call: time.Now(),

        ReadCount: 0,
        WriteCount: 0,
        CurrentReadTps: float64(0),
        CurrentWriteTps: float64(0),

        ReadersTps: nil,
        WritersTps: nil,
        readers: readers,
        writers: writers,

        events: events,
        event_map: make(map[string]int64),
        template: template.Must(template.New("out").Parse(tpl)),
    }
}


func (self *Monitor) run(){
    c := time.Tick(1 * time.Second)
    for _ = range c {
        self.show()
    }
}


func (self *Monitor) show(){
    self.lock.Lock()
    defer self.lock.Unlock()
    self.collect_rw_stat()
    self.collect_event()
    fmt.Printf("\033[H\033[2J")
}

func (self *Monitor) collect_event(){
    for e := range self.events {
        // ignoring the detail error
        k := fmt.Sprintf("%s|%s", e.src, e.category)
        count, _ := self.event_map[k]
        count += 1
        self.event_map[k] = count
    }
}


func (self *Monitor) collect_rw_stat() {
    // this function should only be called in show
    // as show() accquired the lock.
    // or before you call it, just accquire the lock first.

    write_count := int64(0)
    read_count := int64(0)

    writers_tps := []TpsStat {}
    readers_tps := []TpsStat {}

    for _, w := range self.writers {
        writers_tps = append(writers_tps, TpsStat{w.src, w.count})
        write_count += w.count
    }

    self.WritersTps = writers_tps

    for _, r := range self.readers {
        readers_tps = append(readers_tps, TpsStat{r.src, r.count})
        read_count += r.count
    }

    self.ReadersTps = readers_tps

    now := time.Now()
    duration := now.Sub(self.last_call)
    self.CurrentWriteTps = float64( write_count - self.WriteCount ) / duration.Seconds()
    self.CurrentReadTps = float64( read_count - self.ReadCount ) / duration.Seconds()

    self.last_call = now
    self.WriteCount = write_count
    self.ReadCount = read_count
}

func drain(done chan string){
    who := <- done
    fmt.Printf("%s finished", who)
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
    work2 := make(chan map[string]*dynamodb.Attribute, WORK2_BUFFER)
    done := make(chan string)
    events := make(chan Event, EVENTS_BUFFER)
    var readers = make([]*Reader, *numIn)
    var writers = make([]*Writer, *numOut)
    mon := newMonitor(*tableSrc, *tableDst,
        srcDesc.ItemCount, *tps,
        events, readers, writers)

    // this one never requires a done signal
    go mon.run()

    // read (4) => speed regulator (1) => write (4)
    for i := 0; i < *numIn; i++ {
        r := newReader( *tableSrc, &auth, aws_region,
            default_cond, 
            i, *numIn, READ_BATCH,
            work,
            done, events)

        readers[i] = r
        go r.run()
    }

    // start a regulator to control the speed
    go regulator_thread(*tps, 
        work, work2, done)


    for j := 0; j < *numOut; j++ {
        w := newWriter(j,
            *tableDst, &auth, aws_region, 
            *batchSize, work2, done, events)
        writers[j] = w
        go w.run()
    }

    mon.show()

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

    close(events)
    // for stat_collector
    drain(done)

    mon.show()
}
