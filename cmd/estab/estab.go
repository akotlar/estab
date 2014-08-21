package main

import (
	"flag"
	"fmt"
	"log"
	"strings"

	"github.com/belogik/goes"
)

func main() {

	host := flag.String("host", "localhost", "elasticsearch host")
	port := flag.String("port", "9200", "elasticsearch port")
	indicesString := flag.String("indices", "", "indices to search (or all)")
	fieldsString := flag.String("f", "content.245.a", "field or fields space separated")
	timeout := flag.String("timeout", "10m", "scroll timeout")
	size := flag.Int("size", 10000, "scroll batch size")
	nullValue := flag.String("null", "NOT_AVAILABLE", "value for empty fields")
	separator := flag.String("separator", "|", "separator to use for multiple field values")
	delimiter := flag.String("delimiter", "\t", "column delimiter")

	flag.Parse()

	var indices []string
	trimmed := strings.TrimSpace(*indicesString)
	if len(trimmed) > 0 {
		indices = strings.Fields(trimmed)
	}

	fields := strings.Fields(*fieldsString)
	conn := goes.NewConnection(*host, *port)
	var query = map[string]interface{}{
		"query": map[string]interface{}{
			"match_all": map[string]interface{}{},
		},
		"fields": fields,
	}

	scanResponse, err := conn.Scan(query, indices, []string{""}, *timeout, *size)
	if err != nil {
		log.Fatalln(err)
	}

	for {
		scrollResponse, err := conn.Scroll(scanResponse.ScrollId, *timeout)
		if err != nil {
			log.Fatalln(err)
		}
		if len(scrollResponse.Hits.Hits) == 0 {
			break
		}
		for _, hit := range scrollResponse.Hits.Hits {
			var columns []string
			for _, f := range fields {
				var c []string
				switch value := hit.Fields[f].(type) {
				case nil:
					c = []string{*nullValue}
				case []interface{}:
					for _, e := range value {
						c = append(c, e.(string))
					}
				default:
					log.Fatalf("unknown field type in response: %+v", hit.Fields[f])
				}
				columns = append(columns, strings.Join(c, *separator))
			}
			fmt.Println(strings.Join(columns, *delimiter))
		}
	}
}