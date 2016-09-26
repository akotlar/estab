// estab exports elasticsearch fields as tab separated values
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"

	// "github.com/bitly/go-simplejson"
	"github.com/davecgh/go-spew/spew"
	"github.com/miku/estab"
	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"
	"gopkg.in/cheggaaa/pb.v1"
	"gopkg.in/olivere/elastic.v3"

	"io"
	"log"
	"math"
	"os"
	"runtime/pprof"
	"strconv"
	"strings"
)

func main() {
	host := flag.String("host", "localhost", "elasticsearch host")
	port := flag.String("port", "9200", "elasticsearch port")
	indicesString := flag.String("indices", "", "comma-separated indices to search (or all)")
	typesString := flag.String("types", "", "comma-separated types to search (or all)")
	fieldsString := flag.String("f", "", "field or fields space separated")
	// timeout := flag.String("timeout", "10m", "scroll timeout")
	size := flag.Int("size", 10000, "scroll batch size")
	nullValue := flag.String("null", "NA", "value for empty fields")
	separator := flag.String("separator", "|", "separator to use for multiple field values")
	delimiter := flag.String("delimiter", "\t", "column delimiter")
	// limit := flag.Int("limit", -1, "maximum number of docs to return (return all by default)")
	version := flag.Bool("v", false, "prints current program version")
	cpuprofile := flag.String("cpuprofile", "", "write cpu profile to file")
	queryString := flag.String("query", "", "custom query to run")
	raw := flag.Bool("raw", false, "stream out the raw json records")
	header := flag.Bool("header", false, "output header row with field names")
	singleValue := flag.Bool("1", false, "one value per line (works only with a single column in -f)")
	zeroAsNull := flag.Bool("zero-as-null", false, "treat zero length strings as null values")
	precision := flag.Int("precision", 2, "precision for numeric output")
	outPath := flag.String("out", "", "output file path (defualt: stdout)")
	flag.Parse()

	filePath := (*os.File)(nil)

	if *outPath != "" {
		var err error
		filePath, err = os.Create(*outPath)

		if err != nil {
			log.Fatal(err)
		}
	}

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	if *fieldsString == "" {
		log.Fatal("Fields required")
		os.Exit(0)
	}

	if *version {
		fmt.Println(estab.Version)
		os.Exit(0)
	}

	// indices := strings.Fields(*indicesString)
	fields := strings.Fields(*fieldsString)

	spew.Dump(fields)

	if *raw && *singleValue {
		log.Fatal("-1 xor -raw ")
	}

	if *singleValue && len(fields) > 1 {
		log.Fatalf("-1 works only with a single column, %d given: %s\n", len(fields), strings.Join(fields, " "))
	}

	var query map[string]interface{}
	err := json.Unmarshal([]byte(*queryString), &query)
	if err != nil {
		log.Fatal(err)
	}

	if *fieldsString != "" {
		// Why does this work, without declaring a type?
		query["fields"] = fields
	}

	jsonByteArray, err := json.Marshal(query)

	if err != nil {
		log.Fatal(err)
	}

	spew.Dump(query)

	println("\n\n")

	queryStringWithFields := string(jsonByteArray) //string(jsonByteArray)

	client, err := elastic.NewClient(elastic.SetURL(fmt.Sprintf("%s:%s", *host, *port)))

	if err != nil {
		log.Fatal(err)
	}

	f := elastic.NewRawStringQuery(queryStringWithFields)

	spew.Dump(f)

	// Count total and setup progress
	total, err := client.Count().Index(*indicesString).Type(*typesString).Query(elastic.NewRawStringQuery(*queryString)).Do()

	if err != nil {
		log.Fatal(err)
	}

	bar := pb.StartNew(int(total))
	// 1st goroutine sends individual hits to channel.
	hits := make(chan map[string]interface{})
	// resultsChannel := make(chan [string])

	g, ctx := errgroup.WithContext(context.Background())

	g.Go(func() error {
		defer close(hits)
		// Initialize scroller. Just don't call Do yet.
		cursor, err := client.Scan(*indicesString).Fields(fields...).Query(elastic.NewRawStringQuery(*queryString)).Type(*typesString).Size(*size).Do()

		if err != nil {
			log.Fatal(err)
		}

		for {
			results, err := cursor.Next()
			if err == io.EOF {
				return nil // all results retrieved
			}
			if err != nil {
				return err // something went wrong
			}

			// Send the hits to the hits channel
			for _, hit := range results.Hits.Hits {
				hits <- hit.Fields
			}

			// Check if we need to terminate early
			select {
			default:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})

	w := bufio.NewWriter(os.Stdout)
	if filePath != nil {
		w = bufio.NewWriter(filePath)
	}

	defer w.Flush()

	if *header {
		fmt.Fprintln(w, strings.Join(fields, *delimiter))
	}

	// 2nd goroutine receives hits and deserializes them.
	//
	// If you want, setup a number of goroutines handling deserialization in parallel.
	g.Go(func() error {
		for hit := range hits {

			// Each hit is a row
			var columns []string
			// Deserialize
			// var item map[string]interface{}
			// err := json.Unmarshal(hit, &item)

			// if err != nil {
			// 	return err
			// }

			foundData := false
			for _, f := range fields {
				var c []string
				// Do something with the product here, e.g. send it to another channel
				// for further processing.
				switch value := hit[f].(type) {
				case string:
					foundData = true
					if value == "" && *zeroAsNull {
						c = append(c, *nullValue)
					} else {
						c = append(c, value)
					}
				case float64:
					foundData = true
					if !isFloatingPoint(value) {
						c = append(c, fmt.Sprintf("%.0f", value))
					} else {
						c = append(c, strconv.FormatFloat(value, 'G', *precision, 64))
					}
				case bool:
					foundData = true
					c = append(c, strconv.FormatBool(value))
				case nil:
					c = []string{*nullValue}
				case []interface{}:
					// fmt.Printf("%s is interface\n", f)
					for _, e := range value {
						switch e.(type) {
						case nil:
							c = []string{*nullValue}
						case string:
							foundData = true
							// fmt.Printf("%s is string %s\n", f, e.(string))
							s := e.(string)
							if s == "" && *zeroAsNull {
								c = append(c, *nullValue)
							} else {
								c = append(c, e.(string))
							}
						case float64:
							foundData = true
							// fmt.Printf("%s is a float %f\n", f, e.(float64))
							if !isFloatingPoint(e.(float64)) {
								c = append(c, fmt.Sprintf("%.0f", e.(float64)))
							} else {
								c = append(c, strconv.FormatFloat(e.(float64), 'G', *precision, 64))
							}
						case bool:
							foundData = true
							// fmt.Printf("%s is bool %b", f, e.(bool))
							c = append(c, strconv.FormatBool(e.(bool)))
						default:
							fmt.Printf("For %s none of the types match", f)
						}
						// case []interface{}:
						// 	fmt.Printf("%s is inner interface\n", f)
						// 	for _, e := range value {
						// 		switch e.(type) {
						// 		case nil:
						// 			c = []string{*nullValue}
						// 		case string:
						// 			foundData = true
						// 			fmt.Printf("%s is string %s\n", f, e.(string))
						// 			s := e.(string)
						// 			if s == "" && *zeroAsNull {
						// 				c = append(c, *nullValue)
						// 			} else {
						// 				c = append(c, e.(string))
						// 			}
						// 		case float64:
						// 			foundData = true
						// 			fmt.Printf("%s is a float %f\n", f, e.(float64))
						// 			if !isFloatingPoint(e.(float64)) {
						// 				c = append(c, fmt.Sprintf("%.0f", e.(float64)))
						// 			} else {
						// 				c = append(c, strconv.FormatFloat(e.(float64), 'G', *precision, 64))
						// 			}
						// 		case bool:
						// 			foundData = true
						// 			fmt.Printf("%s is bool %b\n", f, e.(bool))
						// 			c = append(c, strconv.FormatBool(e.(bool)))
						// 		}
						// 	}
						// default:
						// 	// spew.Dump(value)
						// 	// fmt.Printf("unknown field type in response: %+v\n", item.Fields[f])
						// 	// log.Fatalf("unknown field type in response: %+v\n", item.Fields[f])
						// }
					}
				default:
					// spew.Dump(value)
					// fmt.Printf("unknown field type in response: %+v\n", item.Fields[f])
					// log.Fatalf("unknown field type in response: %+v\n", item.Fields[f])
				}
				if foundData != false {
					columns = append(columns, strings.Join(c, *separator))
				}
				// else {
				// 	println("found no data for desired rows")
				// 	spew.Dump(fields, item, hit)
				// }

			}

			if len(columns) > 0 {
				// resultsChannel <- strings.Join(columns, *delimiter)
				// fmt.Fprintln(w, strings.Join(columns, *delimiter))
			}

			bar.Increment()

			// Terminate early?
			select {
			default:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})

	// Check whether any goroutines failed.
	if err := g.Wait(); err != nil {
		panic(err)
	}

	// Done.
	bar.FinishPrint("Done")

	// for {
	//   scrollResponse, err := conn.Scroll(scanResponse.ScrollId, *timeout)
	//   if err == io.EOF {
	//     break
	//   }
	//   if err != nil {
	//     log.Fatal(err)
	//   }
	//   if len(scrollResponse.Hits.Hits) == 0 {
	//     break
	//   }
	//   for _, hit := range scrollResponse.Hits.Hits {
	//     if i == *limit {
	//       return
	//     }
	//     if *raw {
	//       b, err := json.Marshal(hit)
	//       if err != nil {
	//         log.Fatal(err)
	//       }
	//       fmt.Fprintln(w, string(b))
	//       continue
	//     }

	//     var columns []string
	//     for _, f := range fields {
	//       var c []string
	//       switch f {
	//       case "_id":
	//         c = append(c, hit.Id)
	//       case "_index":
	//         c = append(c, hit.Index)
	//       case "_type":
	//         c = append(c, hit.Type)
	//       case "_score":
	//         c = append(c, strconv.FormatFloat(hit.Score, 'f', 6, 64))
	//       default:
	//         switch value := hit.Fields[f].(type) {
	//         case nil:
	//           c = []string{*nullValue}
	//         case []interface{}:
	//           for _, e := range value {
	//             switch e.(type) {
	//             case string:
	//               s := e.(string)
	//               if s == "" && *zeroAsNull {
	//                 c = append(c, *nullValue)
	//               } else {
	//                 c = append(c, e.(string))
	//               }
	//             case float64:
	//               if !isFloatingPoint(e.(float64)) {
	//                 c = append(c, fmt.Sprintf("%.0f", e.(float64)))
	//               } else {
	//                 c = append(c, strconv.FormatFloat(e.(float64), 'G', *precision, 64))
	//               }
	//             case bool:
	//               c = append(c, strconv.FormatBool(e.(bool)))
	//             }
	//           }
	//         default:
	//           fmt.Printf("unknown field type in response: %+v\n", hit.Fields[f])
	//           log.Fatalf("unknown field type in response: %+v\n", hit.Fields[f])
	//         }
	//       }
	//       if *singleValue {
	//         for _, value := range c {
	//           fmt.Fprintln(w, value)
	//         }
	//       } else {
	//         columns = append(columns, strings.Join(c, *separator))
	//       }
	//     }
	//     if !*singleValue {
	//       fmt.Fprintln(w, strings.Join(columns, *delimiter))
	//     }
	//     i++
	//   }
	// }
}

func isFloatingPoint(value float64) bool {
	return float64(int64(value)) != value
}

//https://play.golang.org
func round(val float64, roundOn float64, places int) (newVal float64) {
	var round float64
	pow := math.Pow(10, float64(places))
	digit := pow * val
	_, div := math.Modf(digit)
	if div >= roundOn {
		round = math.Ceil(digit)
	} else {
		round = math.Floor(digit)
	}
	newVal = round / pow
	return
}

// type Response struct {
// 	// Acknowledged bool
// 	// Error        string
// 	// Errors       bool
// 	// Status       uint64
// 	// Took         uint64
// 	// TimedOut     bool  `json:"timed_out"`
// 	// Shards       Shard `json:"_shards"`
// 	// Hits         Hits
// 	// Index        string `json:"_index"`
// 	// Id           string `json:"_id"`
// 	// Type         string `json:"_type"`
// 	// Version      int    `json:"_version"`
// 	// Found        bool
// 	// Count        int

// 	// Used by the _stats API
// 	// All All `json:"_all"`

// 	// Used by the _bulk API
// 	// Items []map[string]Item `json:"items,omitempty"`

// 	// Used by the GET API
// 	Source map[string]interface{} `json:"_source"`
// 	Fields map[string]interface{} `json:"fields"`

// 	// Used by the _status API
// 	// Indices map[string]IndexStatus

// 	// Scroll id for iteration
// 	// ScrollId string `json:"_scroll_id"`

// 	// Aggregations map[string]Aggregation `json:"aggregations,omitempty"`

// 	Raw map[string]interface{}
// }
