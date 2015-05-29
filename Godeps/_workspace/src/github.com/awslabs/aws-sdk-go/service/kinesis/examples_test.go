package kinesis_test

import (
	"bytes"
	"fmt"
	"github.com/convox/ktail/Godeps/_workspace/src/github.com/awslabs/aws-sdk-go/aws"
	"github.com/convox/ktail/Godeps/_workspace/src/github.com/awslabs/aws-sdk-go/aws/awserr"
	"github.com/convox/ktail/Godeps/_workspace/src/github.com/awslabs/aws-sdk-go/aws/awsutil"
	"github.com/convox/ktail/Godeps/_workspace/src/github.com/awslabs/aws-sdk-go/service/kinesis"
	"time"
)

var _ time.Duration
var _ bytes.Buffer

func ExampleKinesis_AddTagsToStream() {
	svc := kinesis.New(nil)

	params := &kinesis.AddTagsToStreamInput{
		StreamName: aws.String("StreamName"), // Required
		Tags: &map[string]*string{ // Required
			"Key": aws.String("TagValue"), // Required
			// More values...
		},
	}
	resp, err := svc.AddTagsToStream(params)

	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			// Generic AWS Error with Code, Message, and original error (if any)
			fmt.Println(awsErr.Code(), awsErr.Message(), awsErr.OrigErr())
			if reqErr, ok := err.(awserr.RequestFailure); ok {
				// A service error occurred
				fmt.Println(reqErr.Code(), reqErr.Message(), reqErr.StatusCode(), reqErr.RequestID())
			}
		} else {
			// This case should never be hit, The SDK should alwsy return an
			// error which satisfies the awserr.Error interface.
			fmt.Println(err.Error())
		}
	}

	// Pretty-print the response data.
	fmt.Println(awsutil.StringValue(resp))
}

func ExampleKinesis_CreateStream() {
	svc := kinesis.New(nil)

	params := &kinesis.CreateStreamInput{
		ShardCount: aws.Long(1),              // Required
		StreamName: aws.String("StreamName"), // Required
	}
	resp, err := svc.CreateStream(params)

	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			// Generic AWS Error with Code, Message, and original error (if any)
			fmt.Println(awsErr.Code(), awsErr.Message(), awsErr.OrigErr())
			if reqErr, ok := err.(awserr.RequestFailure); ok {
				// A service error occurred
				fmt.Println(reqErr.Code(), reqErr.Message(), reqErr.StatusCode(), reqErr.RequestID())
			}
		} else {
			// This case should never be hit, The SDK should alwsy return an
			// error which satisfies the awserr.Error interface.
			fmt.Println(err.Error())
		}
	}

	// Pretty-print the response data.
	fmt.Println(awsutil.StringValue(resp))
}

func ExampleKinesis_DeleteStream() {
	svc := kinesis.New(nil)

	params := &kinesis.DeleteStreamInput{
		StreamName: aws.String("StreamName"), // Required
	}
	resp, err := svc.DeleteStream(params)

	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			// Generic AWS Error with Code, Message, and original error (if any)
			fmt.Println(awsErr.Code(), awsErr.Message(), awsErr.OrigErr())
			if reqErr, ok := err.(awserr.RequestFailure); ok {
				// A service error occurred
				fmt.Println(reqErr.Code(), reqErr.Message(), reqErr.StatusCode(), reqErr.RequestID())
			}
		} else {
			// This case should never be hit, The SDK should alwsy return an
			// error which satisfies the awserr.Error interface.
			fmt.Println(err.Error())
		}
	}

	// Pretty-print the response data.
	fmt.Println(awsutil.StringValue(resp))
}

func ExampleKinesis_DescribeStream() {
	svc := kinesis.New(nil)

	params := &kinesis.DescribeStreamInput{
		StreamName:            aws.String("StreamName"), // Required
		ExclusiveStartShardID: aws.String("ShardId"),
		Limit: aws.Long(1),
	}
	resp, err := svc.DescribeStream(params)

	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			// Generic AWS Error with Code, Message, and original error (if any)
			fmt.Println(awsErr.Code(), awsErr.Message(), awsErr.OrigErr())
			if reqErr, ok := err.(awserr.RequestFailure); ok {
				// A service error occurred
				fmt.Println(reqErr.Code(), reqErr.Message(), reqErr.StatusCode(), reqErr.RequestID())
			}
		} else {
			// This case should never be hit, The SDK should alwsy return an
			// error which satisfies the awserr.Error interface.
			fmt.Println(err.Error())
		}
	}

	// Pretty-print the response data.
	fmt.Println(awsutil.StringValue(resp))
}

func ExampleKinesis_GetRecords() {
	svc := kinesis.New(nil)

	params := &kinesis.GetRecordsInput{
		ShardIterator: aws.String("ShardIterator"), // Required
		Limit:         aws.Long(1),
	}
	resp, err := svc.GetRecords(params)

	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			// Generic AWS Error with Code, Message, and original error (if any)
			fmt.Println(awsErr.Code(), awsErr.Message(), awsErr.OrigErr())
			if reqErr, ok := err.(awserr.RequestFailure); ok {
				// A service error occurred
				fmt.Println(reqErr.Code(), reqErr.Message(), reqErr.StatusCode(), reqErr.RequestID())
			}
		} else {
			// This case should never be hit, The SDK should alwsy return an
			// error which satisfies the awserr.Error interface.
			fmt.Println(err.Error())
		}
	}

	// Pretty-print the response data.
	fmt.Println(awsutil.StringValue(resp))
}

func ExampleKinesis_GetShardIterator() {
	svc := kinesis.New(nil)

	params := &kinesis.GetShardIteratorInput{
		ShardID:                aws.String("ShardId"),           // Required
		ShardIteratorType:      aws.String("ShardIteratorType"), // Required
		StreamName:             aws.String("StreamName"),        // Required
		StartingSequenceNumber: aws.String("SequenceNumber"),
	}
	resp, err := svc.GetShardIterator(params)

	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			// Generic AWS Error with Code, Message, and original error (if any)
			fmt.Println(awsErr.Code(), awsErr.Message(), awsErr.OrigErr())
			if reqErr, ok := err.(awserr.RequestFailure); ok {
				// A service error occurred
				fmt.Println(reqErr.Code(), reqErr.Message(), reqErr.StatusCode(), reqErr.RequestID())
			}
		} else {
			// This case should never be hit, The SDK should alwsy return an
			// error which satisfies the awserr.Error interface.
			fmt.Println(err.Error())
		}
	}

	// Pretty-print the response data.
	fmt.Println(awsutil.StringValue(resp))
}

func ExampleKinesis_ListStreams() {
	svc := kinesis.New(nil)

	params := &kinesis.ListStreamsInput{
		ExclusiveStartStreamName: aws.String("StreamName"),
		Limit: aws.Long(1),
	}
	resp, err := svc.ListStreams(params)

	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			// Generic AWS Error with Code, Message, and original error (if any)
			fmt.Println(awsErr.Code(), awsErr.Message(), awsErr.OrigErr())
			if reqErr, ok := err.(awserr.RequestFailure); ok {
				// A service error occurred
				fmt.Println(reqErr.Code(), reqErr.Message(), reqErr.StatusCode(), reqErr.RequestID())
			}
		} else {
			// This case should never be hit, The SDK should alwsy return an
			// error which satisfies the awserr.Error interface.
			fmt.Println(err.Error())
		}
	}

	// Pretty-print the response data.
	fmt.Println(awsutil.StringValue(resp))
}

func ExampleKinesis_ListTagsForStream() {
	svc := kinesis.New(nil)

	params := &kinesis.ListTagsForStreamInput{
		StreamName:           aws.String("StreamName"), // Required
		ExclusiveStartTagKey: aws.String("TagKey"),
		Limit:                aws.Long(1),
	}
	resp, err := svc.ListTagsForStream(params)

	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			// Generic AWS Error with Code, Message, and original error (if any)
			fmt.Println(awsErr.Code(), awsErr.Message(), awsErr.OrigErr())
			if reqErr, ok := err.(awserr.RequestFailure); ok {
				// A service error occurred
				fmt.Println(reqErr.Code(), reqErr.Message(), reqErr.StatusCode(), reqErr.RequestID())
			}
		} else {
			// This case should never be hit, The SDK should alwsy return an
			// error which satisfies the awserr.Error interface.
			fmt.Println(err.Error())
		}
	}

	// Pretty-print the response data.
	fmt.Println(awsutil.StringValue(resp))
}

func ExampleKinesis_MergeShards() {
	svc := kinesis.New(nil)

	params := &kinesis.MergeShardsInput{
		AdjacentShardToMerge: aws.String("ShardId"),    // Required
		ShardToMerge:         aws.String("ShardId"),    // Required
		StreamName:           aws.String("StreamName"), // Required
	}
	resp, err := svc.MergeShards(params)

	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			// Generic AWS Error with Code, Message, and original error (if any)
			fmt.Println(awsErr.Code(), awsErr.Message(), awsErr.OrigErr())
			if reqErr, ok := err.(awserr.RequestFailure); ok {
				// A service error occurred
				fmt.Println(reqErr.Code(), reqErr.Message(), reqErr.StatusCode(), reqErr.RequestID())
			}
		} else {
			// This case should never be hit, The SDK should alwsy return an
			// error which satisfies the awserr.Error interface.
			fmt.Println(err.Error())
		}
	}

	// Pretty-print the response data.
	fmt.Println(awsutil.StringValue(resp))
}

func ExampleKinesis_PutRecord() {
	svc := kinesis.New(nil)

	params := &kinesis.PutRecordInput{
		Data:                      []byte("PAYLOAD"),          // Required
		PartitionKey:              aws.String("PartitionKey"), // Required
		StreamName:                aws.String("StreamName"),   // Required
		ExplicitHashKey:           aws.String("HashKey"),
		SequenceNumberForOrdering: aws.String("SequenceNumber"),
	}
	resp, err := svc.PutRecord(params)

	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			// Generic AWS Error with Code, Message, and original error (if any)
			fmt.Println(awsErr.Code(), awsErr.Message(), awsErr.OrigErr())
			if reqErr, ok := err.(awserr.RequestFailure); ok {
				// A service error occurred
				fmt.Println(reqErr.Code(), reqErr.Message(), reqErr.StatusCode(), reqErr.RequestID())
			}
		} else {
			// This case should never be hit, The SDK should alwsy return an
			// error which satisfies the awserr.Error interface.
			fmt.Println(err.Error())
		}
	}

	// Pretty-print the response data.
	fmt.Println(awsutil.StringValue(resp))
}

func ExampleKinesis_PutRecords() {
	svc := kinesis.New(nil)

	params := &kinesis.PutRecordsInput{
		Records: []*kinesis.PutRecordsRequestEntry{ // Required
			&kinesis.PutRecordsRequestEntry{ // Required
				Data:            []byte("PAYLOAD"),          // Required
				PartitionKey:    aws.String("PartitionKey"), // Required
				ExplicitHashKey: aws.String("HashKey"),
			},
			// More values...
		},
		StreamName: aws.String("StreamName"), // Required
	}
	resp, err := svc.PutRecords(params)

	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			// Generic AWS Error with Code, Message, and original error (if any)
			fmt.Println(awsErr.Code(), awsErr.Message(), awsErr.OrigErr())
			if reqErr, ok := err.(awserr.RequestFailure); ok {
				// A service error occurred
				fmt.Println(reqErr.Code(), reqErr.Message(), reqErr.StatusCode(), reqErr.RequestID())
			}
		} else {
			// This case should never be hit, The SDK should alwsy return an
			// error which satisfies the awserr.Error interface.
			fmt.Println(err.Error())
		}
	}

	// Pretty-print the response data.
	fmt.Println(awsutil.StringValue(resp))
}

func ExampleKinesis_RemoveTagsFromStream() {
	svc := kinesis.New(nil)

	params := &kinesis.RemoveTagsFromStreamInput{
		StreamName: aws.String("StreamName"), // Required
		TagKeys: []*string{ // Required
			aws.String("TagKey"), // Required
			// More values...
		},
	}
	resp, err := svc.RemoveTagsFromStream(params)

	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			// Generic AWS Error with Code, Message, and original error (if any)
			fmt.Println(awsErr.Code(), awsErr.Message(), awsErr.OrigErr())
			if reqErr, ok := err.(awserr.RequestFailure); ok {
				// A service error occurred
				fmt.Println(reqErr.Code(), reqErr.Message(), reqErr.StatusCode(), reqErr.RequestID())
			}
		} else {
			// This case should never be hit, The SDK should alwsy return an
			// error which satisfies the awserr.Error interface.
			fmt.Println(err.Error())
		}
	}

	// Pretty-print the response data.
	fmt.Println(awsutil.StringValue(resp))
}

func ExampleKinesis_SplitShard() {
	svc := kinesis.New(nil)

	params := &kinesis.SplitShardInput{
		NewStartingHashKey: aws.String("HashKey"),    // Required
		ShardToSplit:       aws.String("ShardId"),    // Required
		StreamName:         aws.String("StreamName"), // Required
	}
	resp, err := svc.SplitShard(params)

	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			// Generic AWS Error with Code, Message, and original error (if any)
			fmt.Println(awsErr.Code(), awsErr.Message(), awsErr.OrigErr())
			if reqErr, ok := err.(awserr.RequestFailure); ok {
				// A service error occurred
				fmt.Println(reqErr.Code(), reqErr.Message(), reqErr.StatusCode(), reqErr.RequestID())
			}
		} else {
			// This case should never be hit, The SDK should alwsy return an
			// error which satisfies the awserr.Error interface.
			fmt.Println(err.Error())
		}
	}

	// Pretty-print the response data.
	fmt.Println(awsutil.StringValue(resp))
}