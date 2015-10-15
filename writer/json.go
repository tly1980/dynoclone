package writer

import (
    //"bufio"
    //"os"
    "fmt"
    "encoding/json"
    "strings"
    "bufio"
    "os"
    "log"

    "github.com/AdRoll/goamz/dynamodb"

    "dynoclone/utils"
    "dynoclone/base"

)

type JsonWriter struct {
    jsonfmt string
    dst string
    writer_id int
    count int64
    work chan map[string]*dynamodb.Attribute
    done chan string
    events chan utils.Event
    src string
    is_dyno bool
    batch_size int64
    writer *bufio.Writer
}

func NewJsonWriter(
    jsonfmt string,
    dstPath string,
    writer_id int,
    work chan map[string]*dynamodb.Attribute,
    done chan string,
    events chan utils.Event) base.Writer {
    var buf_writer *bufio.Writer
    if len(dstPath) == 0 {
        buf_writer = bufio.NewWriter(os.Stdout)
    }else {
        f, err := os.Create(dstPath)
        if err != nil {
            log.Fatal("Failed to describe table.", err)
        }
        buf_writer = bufio.NewWriter(f)
    }

    return &JsonWriter {
        jsonfmt: jsonfmt,
        dst: dstPath,
        writer_id: writer_id,
        work: work,
        done: done,
        events: events,
        src: fmt.Sprintf("[%v]-%v", dstPath, writer_id),
        is_dyno: false,
        batch_size: 20,
        writer: buf_writer,
    }
}

func simple_render(w map[string]*dynamodb.Attribute) string {
    kvp := []string{}
    for k, v := range w {
        var json_val []byte

        if v.Value != "" {
            json_val, _ = json.Marshal(v.Value)

        }else if v.ListValues != nil {
            json_val, _ = json.Marshal(v.ListValues)

        }else if v.MapValues != nil {
            json_val, _ = json.Marshal(v.MapValues)
        }
        s := fmt.Sprintf(`"%s":%s`, k, json_val)
        kvp = append(kvp, s)
    }

    return fmt.Sprintf(
            "{ %s }", strings.Join(kvp, ", "))
}

func dynoformat_render(w map[string]*dynamodb.Attribute) string {
    kvp := []string{}
    for k, v := range w {
        _, json_val := json.Marshal(v)
        s := fmt.Sprintf(`"%s":%s`, k, json_val)
        kvp = append(kvp, s)
    }

    return fmt.Sprintf(
            "{ %s }", strings.Join(kvp, ", "))
}


func (self *JsonWriter) Run(work chan map[string]*dynamodb.Attribute){
    defer self.writer.Flush()

    renderf := simple_render

    if self.jsonfmt == "dynojson" {
        renderf = dynoformat_render
    }


    linebreak := []byte("\n")
    for w := range self.work {
        self.writer.Write([]byte(renderf(w)))
        self.writer.Write(linebreak)
        self.count += 1
        if self.count % self.batch_size == 0 {
            self.writer.Flush()
        }
    }
}

func (self *JsonWriter) Src() string {
    return self.src
}

func (self *JsonWriter) Count() int64{
    return self.count
}
