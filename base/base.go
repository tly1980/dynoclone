package base

import  "github.com/AdRoll/goamz/dynamodb"


type Reader interface{
    Count() int64
    Src() string
    Run(work chan map[string]*dynamodb.Attribute)
}

type Writer interface {
    Count() int64
    Src() string
    Run(work chan map[string]*dynamodb.Attribute)
}