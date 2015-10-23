package main

import (
    "os"
    "fmt"

    "github.com/tly1980/dynoclone/clone"
    "github.com/tly1980/dynoclone/dump"
)

func main() {
    argv := os.Args[2:]
    action := os.Args[1]

    switch action {
      case "dump":
        dump.Main(argv)
      case "clone":
        clone.Main(argv)
      default:
        fmt.Fprintf(
            os.Stderr, "Unsupported action: %s\n", action)
        os.Exit(-1)
    }

}