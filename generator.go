package main

import (
	"bufio"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
)

type Entry struct {
	key   uint64
	value uint64
}

type QueryType int

const (
	ReadQuery QueryType = iota
	WriteQuery
)

type Result struct {
	isPresent bool
	value     uint64
}

type Query struct {
	queryType QueryType
	key       uint64
	value     uint64
	result    Result
}

func (q Query) String() string {
	switch q.queryType {
	case ReadQuery:
		if q.result.isPresent {
			return fmt.Sprintf("READ,%d,%d\n", q.key, q.result.value)
		} else {
			return fmt.Sprintf("READ,%d\n", q.key)
		}

	case WriteQuery:
		return fmt.Sprintf("WRITE,%d,%d\n", q.key, q.value)
	}

	return ""
}

func (e Entry) String() string {
	return fmt.Sprintf("%d,%d\n", e.key, e.value)
}

type KeyDistribution int
type ValueDistribution int

const (
	KeyUniform KeyDistribution = iota
	// KeyNormal
	KeySequential
)

const (
	ValueUniform ValueDistribution = iota
	ValueSame
)

var entriesSet = make(map[uint64]bool)
var entries = make([]Entry, 0)

func uniformKey(i int) uint64 {
	return rand.Uint64()
}

// func normalKey(i int) uint64 {
// 	return uint64(rand.NormFloat64()*(math.MaxUint64/6) + (math.MaxUint64 / 2))
// }

func sequentialKey(i int) uint64 {
	return uint64(i)
}

func uniformValue(key uint64) uint64 {
	return rand.Uint64()
}

func sameValue(key uint64) uint64 {
	return key
}

var keyDistributions = map[KeyDistribution](func(int) uint64){0: uniformKey, 1: sequentialKey}
var valueDistributions = map[ValueDistribution](func(uint64) uint64){0: uniformValue, 1: sameValue}

func main() {
	// parse options for the generator
	N := flag.Int("N", int(math.Pow(10, 7)), "the number of entries to start the db with")
	numQueries := flag.Int("queries", int(math.Pow(10, 7)), "the number of queries to generate")
	keyDistribution := flag.Int("keyDistribution", 0, "the distribution of the keys; 0 for uniform, 1 for normal, 2 for sequential")
	valueDistribution := flag.Int("valueDistribution", 0, "the distribution of the values; 0 for uniform, 1 for same as key")
	selectivity := flag.Int("selectivity", 100, "the selectivity of reads; integer 0 to 100")
	dataFileName := flag.String("dataFile", "../data", "the file to write data to")
	queryFileName := flag.String("queryFile", "../queries", "the file to write queries to")
	readPercentageFlag := flag.Int("readPercentage", 100, "the percent of queries that are reads; integer 0 to 100")
	flag.Parse()

	f, err := os.Create(*dataFileName)
	if err != nil {
		panic(err)
	}

	defer f.Close()

	w := bufio.NewWriterSize(f, 10000000)

	keyFunc := keyDistributions[KeyDistribution(*keyDistribution)]
	valueFunc := valueDistributions[ValueDistribution(*valueDistribution)]
	selectivityPct := float64(*selectivity) / 100
	readPct := float64(*readPercentageFlag) / 100

	var keyCount int

	for keyCount = 0; keyCount < *N; keyCount++ {
		var key uint64

		for key = keyFunc(keyCount); ; {
			_, present := entriesSet[key]

			if !present {
				break
			}
		}

		value := valueFunc(key)
		entriesSet[key] = true

		entries = append(entries, Entry{key: key, value: value})
	}

	for _, v := range entries {
		w.WriteString(v.String())
	}

	w.Flush()

	queryFile, err := os.Create(*queryFileName)
	if err != nil {
		panic(err)
	}
	defer queryFile.Close()

	queryWriter := bufio.NewWriter(queryFile)

	queries := make([]Query, 0)

	for i := 0; i < *numQueries; i++ {
		queryChoice := rand.Float64()

		if queryChoice < readPct {
			choose := rand.Float64()

			if choose < selectivityPct {
				randIdx := rand.Intn(len(entries))

				queries = append(queries, Query{
					ReadQuery,
					entries[randIdx].key,
					uint64(0),
					Result{
						true,
						entries[randIdx].value,
					},
				})
			} else {
				var key uint64
				for key = rand.Uint64(); ; {
					_, present := entriesSet[key]

					if !present {
						break
					}
				}

				queries = append(queries, Query{
					ReadQuery,
					key,
					uint64(0),
					Result{
						false,
						uint64(0),
					},
				})
			}
		} else {
			key := keyFunc(keyCount)
			keyCount++
			value := valueFunc(key)

			entriesSet[key] = true
			entries = append(entries, Entry{key, value})

			queries = append(queries, Query{
				WriteQuery,
				key,
				value,
				Result{
					false,
					uint64(0),
				},
			})
			queries = append(queries, Query{
				ReadQuery,
				key,
				uint64(0),
				Result{
					true,
					value,
				},
			})
		}
	}

	for _, v := range queries {
		queryWriter.WriteString(v.String())
	}

	queryWriter.Flush()
}
