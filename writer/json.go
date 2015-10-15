package writer

import (
    //"bufio"
    //"os"
    "fmt"
    "encoding/json"
    "strings"

    "github.com/AdRoll/goamz/dynamodb"

    "dynoclone/utils"
    "dynoclone/base"
)

type JsonWriter struct {
    dst string
    writer_id int
    count int64
    work chan map[string]*dynamodb.Attribute
    done chan string
    events chan utils.Event
    src string
}

func NewJsonWriter(
    dstPath string,
    writer_id int,
    work chan map[string]*dynamodb.Attribute,
    done chan string,
    events chan utils.Event) base.Writer {

    return &JsonWriter {
        dst: dstPath,
        writer_id: writer_id,
        work: work,
        done: done,
        events: events,
        src: fmt.Sprintf("%v-%v", dstPath, writer_id),
    }
}

func (self *JsonWriter) Run(work chan map[string]*dynamodb.Attribute){
    kvp := []string{}
    for w := range self.work {
        for k, v := range w {
            json_val, _ := json.Marshal(v)
            s := fmt.Sprintf(`"%s":%s`, k, json_val)
            kvp = append(kvp, s)
        }
        self.count += 1
        json_str := fmt.Sprintf(
            "{ %s }\n", strings.Join(kvp, " "))

        fmt.Printf("%s", json_str)
    }
}

func (self *JsonWriter) Src() string {
    return self.src
}

func (self *JsonWriter) Count() int64{
    return self.count
}
