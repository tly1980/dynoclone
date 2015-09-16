## A DynamoDB Clone tool

A simple, multi-threaded, highly efficient DynamoDB copy solution.

Powered by [golang](http://www.golang.org), implemented with GO 

### Why ?

I had a terrible expierence with AWS Data-pipeline. I tried clone a DynamoDB with roughly 12 millions records, however the up-to-date ( 2015-09-15 ) data-pipeline cannot fullfilled that, roughly only 300 K records were copied. 

I raised a ticket to AWS and I ended up spent 30 ~ 40 minutes with AWS tech support by creating a new pipeline, and the issue still persists. After a while, they get back to me say that was a known issue with EMR blah blah blah and product team is working really hard on that, blah blah blah ... 

And they came up with a solution which is to use AWS data pipeline to dump the data to S3, and use another pipeline to import from S3.

The data-pipeline EMR I used has two medium nodes and can merely push the throughput to 150 TPS.

Fair enough ... 

Accessing DynamoDB is nothing but sending HTTP requests. I had a fantasty expierence with Golang in implementing high efficient HTTP client / server. Why not try to see what it can do ?

So here it is, dynoclone, aimed to be easy to use and highly efficient. 

Thanks to golang 

### Usage

```bash
dynoclone -src SRC_DYNO -dst DST_DYNO
```

Or you want to configure more. Let say 4 reader thread, 2 writer thread and with a write batch size equal 5.

```bash
dynoclone -src SRC_DYNO -dst DST_DYNO -numIn 4 -numOut 2 -batchSize 5
```

### Benchmark

my-src-dyno has 12 million records.

```bash
$ time nk dynoclone -src my-src-dyno -dst my-dst-dyno -numIn 4 -numOut 4 -batchSize 10 &> out2

real    60m23.424s
user    25m43.864s
sys     2m36.700s
```
