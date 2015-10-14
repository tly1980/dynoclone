package writer

import (
	"github.com/AdRoll/goamz/dynamodb"
)

func map_to_item(obj map[string]*dynamodb.Attribute) *[]dynamodb.Attribute {
    items := []dynamodb.Attribute{}
    for _, v := range obj {
        items = append(items, *v)
    }
    return &items
}