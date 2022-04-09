package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func isBucketExist(bucketName string, existedBuckets []*s3.Bucket) bool {
	for _, existedBucket  := range existedBuckets {
		if *existedBucket.Name == bucketName {
			return true
		}
	}

	return false
}

func main() {
	var bucket string
	var key string
	var timeout time.Duration

	// CLI flag parsing
	flag.StringVar(&bucket, "b", "", "Bucket name")
	flag.StringVar(&key, "k", "", "Object key")
	flag.DurationVar(&timeout, "d", 0, "Timeout")
	flag.Parse()
	fmt.Printf("start upload file to %s/%s\n", bucket, key)

	// All clients require a Session. The Session provides the client with
	// shared configuration such as region, endpoint, and credentials. A
	// Session should be shared where possible to take advantage of
	// configuration and credential caching.
	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String("ap-southeast-1"),
		DisableSSL: aws.Bool(true),
		Endpoint: aws.String("http://localhost:4566"),
		S3ForcePathStyle: aws.Bool(true),
	}))
	fmt.Printf("session created\n")

	// Create a new instance of the service's client with a Session.
	// Optional aws.Config values can also be provided as variadic arguments
	// to the New function. This option allows you to provide service
	// specific configuration.
	s3Client := s3.New(sess)
	fmt.Printf("s3 client created\n")

	// Create a context with a timeout that will abort the upload if it takes
	// more than the passed in timeout.
	ctx := context.Background()
	var cancel func()
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, timeout)
	}
	if cancel != nil {
		defer cancel()
	}
	fmt.Printf("ctx created\n")

	// Make sure bucket exist
	listBucketsOutput, err := s3Client.ListBuckets(&s3.ListBucketsInput{})
	if err != nil {
		fmt.Printf("list buckets error %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("buckets %v\n", listBucketsOutput.Buckets)
	if !isBucketExist(bucket, listBucketsOutput.Buckets) {
		_, err = s3Client.CreateBucket(&s3.CreateBucketInput{
			Bucket: aws.String(bucket),
		})
		if err != nil {
			fmt.Printf("create bucket %v error %v\n", bucket, err)
			os.Exit(1)
		}
	}

	// Uploads the object to S3 with timeout
	file, err := os.OpenFile("./sample.txt", os.O_RDWR, 0644)
	if err != nil {
		fmt.Printf("open file error, %v\n", err)
		os.Exit(1)
	}
	time.Sleep(10 * time.Second)
	_, err = s3Client.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   io.ReadSeeker(file),
	})
	fmt.Printf("put object finished\n")

	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok && awsErr.Code() == request.CanceledErrorCode {
			fmt.Fprintf(os.Stderr, "upload canceled due to timeout, %v\n", err)
		} else {
			fmt.Fprintf(os.Stderr, "failed to upload object, %v\n", err)
		}
		os.Exit(1)
	}

	fmt.Printf("successfully uploaded file to %s/%s\n", bucket, key)
}