package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/wvanbergen/kafka/consumergroup"
	kazoo "github.com/wvanbergen/kazoo-go"
)

//DefaultKafkaTopics ..
const (
	DefaultKafkaTopics   = "TEST_TOPIC"
	DefaultConsumerGroup = "golang_test_consumer"
)

var (
	consumerGroup  = flag.String("group", DefaultConsumerGroup, "The name of the consumer group, used for coordination and load balancing")
	kafkaTopicsCSV = flag.String("topics", DefaultKafkaTopics, "The comma-separated list of topics to consume")
	zookeeper      = flag.String("zookeeper", "10.198.107.58:2181,10.198.107.59:2181,10.198.107.60:2181", "A comma-separated Zookeeper connection string (e.g. `zookeeper1.local:2181,zookeeper2.local:2181,zookeeper3.local:2181`)")

	zookeeperNodes []string
)

func init() {
	sarama.Logger = log.New(os.Stdout, "[Sarama] ", log.LstdFlags)
}

func main() {

	//https://github.com/wvanbergen/kafka/blob/master/examples/consumergroup/main.go

	flag.Parse()

	if *zookeeper == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	config := consumergroup.NewConfig()
	config.Offsets.Initial = sarama.OffsetNewest
	config.Offsets.ProcessingTimeout = 10 * time.Second

	zookeeperNodes, config.Zookeeper.Chroot = kazoo.ParseConnectionString(*zookeeper)

	kafkaTopics := strings.Split(*kafkaTopicsCSV, ",")

	consumer, consumerErr := consumergroup.JoinConsumerGroup(*consumerGroup, kafkaTopics, zookeeperNodes, config)
	if consumerErr != nil {
		log.Fatalln(consumerErr)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		if err := consumer.Close(); err != nil {
			sarama.Logger.Println("Error closing the consumer", err)
		}
	}()

	go func() {
		for err := range consumer.Errors() {
			log.Println(err)
		}
	}()

	eventCount := 0
	offsets := make(map[string]map[int32]int64)

	for message := range consumer.Messages() {
		if offsets[message.Topic] == nil {
			offsets[message.Topic] = make(map[int32]int64)
		}

		eventCount++
		if offsets[message.Topic][message.Partition] != 0 && offsets[message.Topic][message.Partition] != message.Offset-1 {
			log.Printf("Unexpected offset on %s:%d. Expected %d, found %d, diff %d.\n", message.Topic, message.Partition, offsets[message.Topic][message.Partition]+1, message.Offset, message.Offset-offsets[message.Topic][message.Partition]+1)
		}

		fmt.Printf("topic: %s\t Partition: %v\t Offset: %v Message:%v\n", message.Topic, message.Partition, message.Offset, string(message.Value))
		//Simulate processing time
		time.Sleep(10 * time.Millisecond)

		offsets[message.Topic][message.Partition] = message.Offset
		consumer.CommitUpto(message)
	}

	log.Printf("Processed %d events.", eventCount)
	log.Printf("%+v", offsets)
}
