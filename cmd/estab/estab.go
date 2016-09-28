// estab exports elasticsearch fields as tab separated values
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"

	// "github.com/davecgh/go-spew/spew"
	"github.com/miku/estab"
	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"
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
	size := flag.Int("size", 1000, "scroll batch size")
	nullValue := flag.String("null", "NA", "value for empty fields")
	separator := flag.String("separator", ";", "separator to use for multiple field values")
	secondarySeparator := flag.String("secondarySeparator", "|", "separator to represent  the inner array of an array of values as a string")
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

	client, err := elastic.NewClient(elastic.SetURL(fmt.Sprintf("%s:%s", *host, *port)))

	if err != nil {
		log.Fatal(err)
	}

	// TODO: reactivate, with progress reports via redis
	// f := elastic.NewRawStringQuery(queryStringWithFields)

	// spew.Dump(f)

	// Count total and setup progress
	// total, err := client.Count().Index(*indicesString).Type(*typesString).Query(elastic.NewRawStringQuery(*queryString)).Do()

	if err != nil {
		log.Fatal(err)
	}

	// bar := pb.StartNew(int(total))
	// 1st goroutine sends individual hits to channel.
	hits := make(chan json.RawMessage)

	g, ctx := errgroup.WithContext(context.Background())

	g.Go(func() error {
		defer close(hits)
		// Initialize scroller. Just don't call Do yet.
		cursor, err := client.Scan().Source(*queryString).Index(*indicesString).Type(*typesString).Size(*size).Do()

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
				hits <- *hit.Source
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
		for jsonHit := range hits {

			var hit map[string]interface{}
			err := json.Unmarshal(jsonHit, &hit)

			if err != nil {
				log.Fatal(err)
				return err
			}

			var columns []string

			for _, f := range fields {
				var c []string
				foundInnerArray := false

				var fieldArray []string
				var fieldValue interface{}

				// The value we're looking for may be deeply nested
				var buriedObject map[string]interface{}
				buriedObject = hit

				if strings.Contains(f, ".") {
					fieldArray = strings.Split(f, ".")
				} else {
					fieldArray = []string{f}
				}

				// Iterate over the nested json structure
				// We expect that any Seqant field is only a scalar or array
				// Because we expect that this package is provided the fully qualified
				// Path to the value (e.g if { refSeq => {geneSymbol: DLG1}}
				// we are given "refSeq.geneSymbol")
				for _, fieldNamePart := range fieldArray {
					if val, ok := buriedObject[fieldNamePart]; ok {
						switch item := val.(type) {
						case map[string]interface{}:
							buriedObject = item
						default:
							fieldValue = item
						}
					}
				}

				//TODO: write into recursive function
				switch value := fieldValue.(type) {
				case nil:
					c = []string{*nullValue}
				case string:
					if value == "" && *zeroAsNull {
						c = append(c, *nullValue)
					} else {
						c = append(c, value)
					}
				case float64:
					if !isFloatingPoint(value) {
						c = append(c, fmt.Sprintf("%.0f", value))
					} else {
						c = append(c, strconv.FormatFloat(value, 'G', *precision, 64))
					}
				case bool:
					c = append(c, strconv.FormatBool(value))
				case []interface{}:
					for _, e := range value {
						switch e.(type) {
						case nil:
							c = []string{*nullValue}
						case string:
							s := e.(string)
							if s == "" && *zeroAsNull {
								c = append(c, *nullValue)
							} else {
								c = append(c, e.(string))
							}
						case float64:
							if !isFloatingPoint(e.(float64)) {
								c = append(c, fmt.Sprintf("%.0f", e.(float64)))
							} else {
								c = append(c, strconv.FormatFloat(e.(float64), 'G', *precision, 64))
							}
						case bool:
							c = append(c, strconv.FormatBool(e.(bool)))

						// This is an inner array
						case []interface{}:
							foundInnerArray = true
							var innerArray []string
							// fmt.Printf("%s is inner interface\n", f)

							for _, innerVal := range e.([]interface{}) {
								switch innerVal.(type) {
								case nil:
									innerArray = []string{*nullValue}
								case string:
									// fmt.Printf("%s is string %s\n", f, innerVal.(string))
									s := innerVal.(string)
									if s == "" && *zeroAsNull {
										innerArray = append(innerArray, *nullValue)
									} else {
										innerArray = append(innerArray, innerVal.(string))
									}
								case float64:
									// fmt.Printf("%s is a float %f\n", f, innerVal.(float64))
									if !isFloatingPoint(innerVal.(float64)) {
										innerArray = append(innerArray, fmt.Sprintf("%.0f", innerVal.(float64)))
									} else {
										innerArray = append(innerArray, strconv.FormatFloat(innerVal.(float64), 'G', *precision, 64))
									}
								case bool:
									innerArray = append(innerArray, strconv.FormatBool(innerVal.(bool)))
								default:
									log.Fatal(fmt.Errorf("Invalid type found %+v\n", innerVal))
								}
							}

							if len(innerArray) > 0 {
								c = append(c, strings.Join(innerArray, *separator))
							}
						default:
							log.Fatal(fmt.Errorf("Invalid type found %+v\n", e))
						}
					}
				default:
					log.Fatal(fmt.Errorf("Invalid type found %+v\n", value))
				}

				var outerSeparator string
				if foundInnerArray == true {
					outerSeparator = *secondarySeparator
				} else {
					outerSeparator = *separator
				}
				columns = append(columns, strings.Join(c, outerSeparator))
			}

			if len(columns) > 0 {
				fmt.Fprintln(w, strings.Join(columns, *delimiter))
			}

			// bar.Increment()

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
	// bar.FinishPrint("Done")
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
