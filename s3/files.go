package s3

import (
	"context"
	"io"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type Client struct {
	s3Client *s3.Client
}

func ConnectS3(region string) (*Client, error) {

	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(region))

	if err != nil {
		log.Fatal(err)
	}

	client := s3.NewFromConfig(cfg)

	return &Client{
		s3Client: client,
	}, err
}

func (client *Client) ListBuckets() ([]string, error) {

	output, err := client.s3Client.ListBuckets(context.TODO(), &s3.ListBucketsInput{})

	if err != nil {
		log.Fatal(err)
	}

	var buckets []string

	for _, bucket := range output.Buckets {
		buckets = append(buckets, aws.ToString(bucket.Name))
	}

	return buckets, err
}

func (client *Client) ListObjects(bucket string) (objects []string, err error) {

	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket)}

	output, err := client.s3Client.ListObjectsV2(context.Background(), input)

	if err != nil {
		log.Fatal(err)
	}

	for _, object := range output.Contents {

		objects = append(objects, aws.ToString(object.Key))
	}

	return objects, err

}

func (client *Client) GetObject(bucket string, object string) ([]byte, error) {

	input := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
	}

	output, err := client.s3Client.GetObject(context.Background(), input)

	if err != nil {
		log.Fatal(err)
	}

	defer output.Body.Close()

	body, err := io.ReadAll(output.Body)

	return body, err

}

func (client *Client) DownloadObject(bucket string, object string, fileName string) (err error) {

	input := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
	}

	output, err := client.s3Client.GetObject(context.Background(), input)

	if err != nil {
		log.Fatal(err)
	}

	defer output.Body.Close()

	file, err := os.Create(fileName)

	if err != nil {
		log.Fatal(err)
	}

	defer file.Close()

	body, err := io.ReadAll(output.Body)

	if err != nil {
		log.Fatal(err)
	}

	_, err = file.Write(body)

	return err
}

func (client *Client) UploadObject(bucket string, object string, fileName string) (err error) {

	file, err := os.Open(fileName)

	if err != nil {
		log.Fatal(err)
	}

	defer file.Close()

	input := &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
		Body:   file,
	}

	_, err = client.s3Client.PutObject(context.Background(), input)

	return err
}
