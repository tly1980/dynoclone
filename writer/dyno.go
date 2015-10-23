package writer 

import (
    "log"
    "time"
    "fmt"

    "github.com/AdRoll/goamz/aws"
    "github.com/AdRoll/goamz/dynamodb"

    "github.com/tly1980/dynoclone/utils"
    "github.com/tly1980/dynoclone/base"
)

type DynoWriter struct {
    src string
    writer_id int
    tableName string
    auth *aws.Auth
    region aws.Region
    batch_size int
    count int64
    work chan map[string]*dynamodb.Attribute
    done chan string
    events chan utils.Event
}


func NewDynoWriter (
    writer_id int,
    tableName string,
    auth *aws.Auth,
    region aws.Region,
    batch_size int,
    work chan map[string]*dynamodb.Attribute,
    done chan string,
    events chan utils.Event) base.Writer {

    return &DynoWriter{
        fmt.Sprintf("W%04d", writer_id),
        writer_id,
        tableName, 
        auth, 
        region,
        batch_size,
        0,
        nil,
        done,
        events,
    }
}

func (self *DynoWriter) batch_shoot(
    table *dynamodb.Table,
    batch [][]dynamodb.Attribute) error {
 
    m := map[string][][]dynamodb.Attribute{
        "Put": batch,
    }

    bw := table.BatchWriteItems(m)

    var err error
    var unprocessed map[string]interface {}
    max_retry_seq := len(utils.RETRY_SLEEP_SEQUENCE) - 1

    for i, t := range utils.RETRY_SLEEP_SEQUENCE {
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
                self.events <- utils.Event { self.src, "resend", err.Error()}
            }
        }
    }

    return err
}

func (self *DynoWriter) Run(work chan map[string]*dynamodb.Attribute){
    self.work = work
    defer utils.Finish(self.done, self.src) // To signal that job is done
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
                self.events <- utils.Event { 
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
            self.events <- utils.Event { 
                self.src,
                "error",
                err.Error(),
            }
        }
        batch = nil
    }
}

func (self *DynoWriter) Src() string {
    return self.src
}

func (self *DynoWriter) Count() int64{
    return self.count
}
