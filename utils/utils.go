package utils 

import (
    "sync"
    "text/template"
    "time"
    "strings"
    "fmt"
    "os"

    "github.com/AdRoll/goamz/aws"
    "github.com/AdRoll/goamz/dynamodb"

    "github.com/tly1980/dynoclone/base"
)

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
    Src string
    Category string
    Detail string
}

type Monitor struct {
    lock *sync.Mutex
    started_at time.Time
    Elapsed time.Duration
    Remain time.Duration
    TitleBar string

    Src string
    Dst string
    SrcTotal int64
    DesireTps int
    last_call time.Time

    ReadCount int64
    WriteCount int64
    Progress float64
    CurrentReadTps float64
    CurrentWriteTps float64

    ReadCounters [] base.Reader
    WriteCounters [] base.Writer
    Doners []string

    events chan Event
    event_stats map[string]int64
    EventStats map[string]int64
    template *template.Template

    done chan string
}


func NewMonitor(
    src, dst string, 
    src_total int64, desire_tps int,
    done chan string, events chan Event,
    readers [] base.Reader, writers [] base.Writer) *Monitor {
    tpl := `{{ .Src}} => {{ .Dst }} 
{{ .TitleBar }}
Time:
    Elapsed: {{ .Elapsed }}
    Remain: {{ .Remain }}
Progress:
    {{ .Progress | progressbar }} {{ .Progress | printf "%.2f" }} %
    Written/Total: {{ .WriteCount }} / {{ .SrcTotal }}, 
    Read: {{ .ReadCount }}      Write: {{ .WriteCount }}
TPS:
    Desire: {{ .DesireTps }}
    Instant Read: {{ .CurrentReadTps | printf "%.2f"  }}
    Instant Write: {{ .CurrentWriteTps | printf "%.2f"  }}
TPS Breakdown:
    Readers: {{range .ReadCounters}} {{ .Src }}: {{ .Count }}, {{end}}
    Writers: {{range .WriteCounters}} {{ .Src }}: {{ .Count }}, {{end}}
{{ if .EventStats }}
Events:
    {{ range $key, $value := .EventStats }}{{ $key }}: {{ $value }}
    {{ end }}
{{ end }}
{{ if .Doners }}
Done:
    {{range .Doners}} {{.}}, {{end}}
{{ end }}`
    t := template.New("monitorOutput")
    t = t.Funcs(template.FuncMap{"progressbar": ProgressBar})
    t = template.Must(t.Parse(tpl))

    return &Monitor {
        lock: &sync.Mutex{},
        started_at: time.Now(),

        Src: src,
        Dst: dst,
        SrcTotal: src_total,
        DesireTps: desire_tps,
        last_call: time.Now(),

        ReadCount: 0,
        WriteCount: 0,
        CurrentReadTps: float64(0),
        CurrentWriteTps: float64(0),
        Doners: []string{},

        ReadCounters: readers,
        WriteCounters: writers,

        events: events,
        event_stats: make(map[string]int64),
        EventStats: make(map[string]int64),
        template: t,

        done: done,
        TitleBar: strings.Repeat("=", len(src) + len(dst) + 4),
    }
}

func (self *Monitor) Run(){
    go self.collect_event()
    c := time.Tick(1 * time.Second)
    for _ = range c {
        self.Show()
    }
}


func (self *Monitor) Show(){
    self.lock.Lock()
    defer self.lock.Unlock()
    self.collect_rw_stat()
    fmt.Printf("\033[H\033[2J")
    self.template.Execute(os.Stdout, *self)
}

func ProgressBar(progress float64) string {
    completed := int(progress / 10)
    if completed < 10{
        return fmt.Sprintf("[%s>%s]", strings.Repeat("=", completed), strings.Repeat(".", 10-completed))
    }else{
        return fmt.Sprintf("[%s]", strings.Repeat("=", 12))
    }
}

func (self *Monitor) update_EventStat(){
    self.lock.Lock()
    defer self.lock.Unlock()
    for k,v := range self.event_stats {
        self.EventStats[k] = v
    }
}

func (self *Monitor) collect_event(){
    defer Finish(self.done, "Monitor")
    for e := range self.events {
        // ignoring the detail error
        k := fmt.Sprintf("%s|%s", e.Src, e.Category)
        count, _ := self.event_stats[k]
        count += 1
        self.event_stats[k] = count
        self.update_EventStat()
    }
}


func (self *Monitor) collect_rw_stat() {
    // this function should only be called in show
    // as show() accquired the lock.
    // or before you call it, just accquire the lock first.

    write_count := int64(0)
    read_count := int64(0)

    for _, w := range self.WriteCounters {
        write_count += w.Count()
    }

    for _, r := range self.ReadCounters {
        read_count += r.Count()
    }

    now := time.Now()
    duration := now.Sub(self.last_call)
    self.CurrentWriteTps = float64( write_count - self.WriteCount ) / duration.Seconds()
    self.CurrentReadTps = float64( read_count - self.ReadCount ) / duration.Seconds()
    self.Progress = float64(write_count * 100) / float64(self.SrcTotal )

    self.last_call = now
    self.WriteCount = write_count
    self.ReadCount = read_count

    self.Elapsed = time.Now().Sub(self.started_at)
    if self.CurrentWriteTps > 0 && self.SrcTotal != self.WriteCount {
        self.Remain = time.Duration(int64(float64( self.SrcTotal - self.WriteCount ) / self.CurrentWriteTps)) * time.Second
    } else if self.SrcTotal == self.WriteCount {
        self.Remain = time.Duration(0)
    }else{
        self.Remain = time.Duration(9999) * time.Hour
    }

}

func (self *Monitor) Drain(){
    who := <- self.done
    self.Doners = append(self.Doners, who)
    //fmt.Printf("%s finished", who)
}


func Finish(
    done chan string, role string){
    done <- role
}

func RegulatorThread(desire_tps int, 
    work chan map[string]*dynamodb.Attribute,
    work2 chan map[string]*dynamodb.Attribute,
    done chan string){
    defer Finish(done, "Regulator")

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


func Sniff(auth *aws.Auth,
        region aws.Region, src string) (*dynamodb.TableDescriptionT, error) {
    server := dynamodb.New(*auth, region)

    //just test the connection to dyno table
    srcDesc, err := server.DescribeTable(src)

    if err != nil {
        return nil, err
    }

    return srcDesc, nil
}


