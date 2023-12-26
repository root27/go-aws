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

func ConnectS3(region string) (*s3.Client, error) {

	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(region))

	if err != nil {
		log.Fatal(err)
	}

	client := s3.NewFromConfig(cfg)

	return client, err
}

func ListBuckets(client *s3.Client) ([]string, error) {

	output, err := client.ListBuckets(context.TODO(), &s3.ListBucketsInput{})

	if err != nil {
		log.Fatal(err)
	}

	var buckets []string

	for _, bucket := range output.Buckets {
		buckets = append(buckets, aws.ToString(bucket.Name))
	}

	return buckets, err
}

func ListObjects(client *s3.Client, bucket string) (objects []string, err error) {

	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket)}

	output, err := client.ListObjectsV2(context.Background(), input)

	if err != nil {
		log.Fatal(err)
	}

	for _, object := range output.Contents {

		objects = append(objects, aws.ToString(object.Key))
	}

	return objects, err

}

func GetObject(client *s3.Client, bucket string, object string) ([]byte, error) {

	input := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
	}

	output, err := client.GetObject(context.Background(), input)

	if err != nil {
		log.Fatal(err)
	}

	defer output.Body.Close()

	body, err := io.ReadAll(output.Body)

	return body, err

}

func DownloadObject(client *s3.Client, bucket string, object string, fileName string) (err error) {

	input := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
	}

	output, err := client.GetObject(context.Background(), input)

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

func UploadObject(client *s3.Client, bucket string, object string, fileName string) (err error) {

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

	_, err = client.PutObject(context.Background(), input)

	return err
}
