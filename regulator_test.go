package main

import (
    "testing"
    "time"
    "fmt"

    //"github.com/stretchr/testify/assert"
    "github.com/AdRoll/goamz/dynamodb"
)



func check(work chan map[string]*dynamodb.Attribute){
    j := 0
    t1 := time.Now()
    check_point := 300
    for _ = range work {
        j += 1
        if j % check_point == 0 {
            t2 := time.Now()
            delta := t2.Sub(t1)
            fmt.Printf("count: %v, TPS: %v, %v\n", 
                j, float64(check_point) / delta.Seconds(), delta.Seconds())
            t1 = time.Now()
        }
    }
}

func Test1(t *testing.T){
    w1 := make(chan map[string]*dynamodb.Attribute)
    w2 := make(chan map[string]*dynamodb.Attribute)
    go check(w2)
    go regulator_thread(1000, w1, w2)

    for i := 0; i<10000; i++ {
        w1 <- nil
    }
    close(w1)

    time.Sleep(time.Second * 5)
    close(w2)
}