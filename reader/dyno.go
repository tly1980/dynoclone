package reader

import (
    "log"
    "fmt"

    "github.com/AdRoll/goamz/aws"
    "github.com/AdRoll/goamz/dynamodb"

    "dynoclone/utils"
    "dynoclone/base"
)

type DynoReader struct {
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
    events chan utils.Event
}

// implementing reader
func (self *DynoReader) Run(work chan map[string]*dynamodb.Attribute){
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
                e := utils.Event {
                    self.src,
                    "error",
                    err.Error(),
                }
                self.events <- e
            }
        }

    }else{
        e := utils.Event {
            self.src,
            "error",
            err.Error(),
        }
        self.events <- e
    }

}

func NewDynoReader(
    tableName string,
    auth *aws.Auth,
    region aws.Region,
    attributeComparisons []dynamodb.AttributeComparison,
    seg_id int,
    total_seg int,
    batch_size int,
    work chan map[string]*dynamodb.Attribute,
    done chan string,
    events chan utils.Event) base.Reader {

    return &DynoReader{
        fmt.Sprintf("R%04d", seg_id),
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

func (self *DynoReader) Src() string {
    return self.src
}

func (self *DynoReader) Count() int64 {
    return self.count
}
