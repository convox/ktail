package main

import (
	"fmt"
	"os"
	"time"

	"github.com/convox/ktail/Godeps/_workspace/src/github.com/awslabs/aws-sdk-go/aws"
	"github.com/convox/ktail/Godeps/_workspace/src/github.com/awslabs/aws-sdk-go/aws/credentials"
	"github.com/convox/ktail/Godeps/_workspace/src/github.com/awslabs/aws-sdk-go/service/kinesis"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "usage: ktail <stream>\n")
		os.Exit(1)
	}

	stream := os.Args[1]

	out := make(chan []byte)
	done := make(chan bool)

	go subscribeKinesis(stream, stream, out, done)

	for {
		select {
		case data := <-out:
			fmt.Print(string(data))
		case <-done:
			os.Exit(0)
		}
	}
}

func Kinesis() *kinesis.Kinesis {
	return kinesis.New(&aws.Config{
		Credentials: credentials.NewStaticCredentials(os.Getenv("AWS_ACCESS"), os.Getenv("AWS_SECRET"), ""),
		Region:      os.Getenv("AWS_REGION"),
	})
}

func subscribeKinesis(prefix, stream string, output chan []byte, quit chan bool) {
	sreq := &kinesis.DescribeStreamInput{
		StreamName: aws.String(stream),
	}
	sres, err := Kinesis().DescribeStream(sreq)

	if err != nil {
		fmt.Printf("err1 %+v\n", err)
		// panic(err)
		return
	}

	shards := make([]string, len(sres.StreamDescription.Shards))

	for i, s := range sres.StreamDescription.Shards {
		shards[i] = *s.ShardID
	}

	done := make([](chan bool), len(shards))

	for i, shard := range shards {
		done[i] = make(chan bool)
		go subscribeKinesisShard(prefix, stream, shard, output, done[i])
	}
}

func subscribeKinesisShard(prefix, stream, shard string, output chan []byte, quit chan bool) {
	ireq := &kinesis.GetShardIteratorInput{
		ShardID:           aws.String(shard),
		ShardIteratorType: aws.String("LATEST"),
		StreamName:        aws.String(stream),
	}

	ires, err := Kinesis().GetShardIterator(ireq)

	if err != nil {
		fmt.Printf("err2 %+v\n", err)
		// panic(err)
		return
	}

	iter := *ires.ShardIterator

	for {
		select {
		case <-quit:
			fmt.Println("quitting")
			return
		default:
			greq := &kinesis.GetRecordsInput{
				ShardIterator: aws.String(iter),
			}
			gres, err := Kinesis().GetRecords(greq)

			if err != nil {
				fmt.Printf("err3 %+v\n", err)
				// panic(err)
				return
			}

			iter = *gres.NextShardIterator

			for _, record := range gres.Records {
				output <- []byte(fmt.Sprintf("%s: %s\n", prefix, string(record.Data)))
			}

			time.Sleep(500 * time.Millisecond)
		}
	}
}
