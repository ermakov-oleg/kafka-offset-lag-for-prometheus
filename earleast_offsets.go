package main

import (
	"errors"
	"log"
	"strconv"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/prometheus/client_golang/prometheus"
)

var offsetsCache = &Cache{map[string]map[int32]OffsetTimestamp{}}

const MaxMessageSize = 1024 * 1024 * 5 // 5mb
const MaxWaitTime = 1000 * 1           // 1 sec
const MaxRequestTries = 10

type OffsetTimestamp struct {
	offset    int64
	timestamp int64
}

type Cache struct {
	offsets map[string]map[int32]OffsetTimestamp
}

func (c *Cache) get(topic string, partition int32) *OffsetTimestamp {
	partitions := c.offsets[topic]
	if partitions == nil {
		return nil
	}
	if value, ok := partitions[partition]; ok {
		return &value
	}
	return nil
}

func (c *Cache) set(topic string, partition int32, offsetTimestamp OffsetTimestamp) {
	partitions := c.offsets[topic]
	if partitions == nil {
		partitions = make(map[int32]OffsetTimestamp)
		c.offsets[topic] = partitions
	}
	partitions[partition] = offsetTimestamp
}

func getEarliestOffsets(client sarama.Client, topic string, partitions []int32, highOffsets map[int32]int64) {
	needFetch := make(map[int32]bool)
	timeStamps := make(map[int32]*OffsetTimestamp)

	for _, partition := range partitions {
		low, err := client.GetOffset(topic, partition, sarama.OffsetOldest)
		if err != nil {
			log.Printf("Problem fetching oldest offset for topic:partition %s:%d err:%v\n", topic, partition, err)
			continue
		}

		if highOffsets[partition] == low {
			timestamp := OffsetTimestamp{offset: low, timestamp: -1}
			offsetsCache.set(topic, partition, timestamp)
			timeStamps[partition] = &timestamp
			continue
		}
		cacheValue := offsetsCache.get(topic, partition)
		if cacheValue != nil && cacheValue.offset == low {
			timeStamps[partition] = cacheValue
			continue
		}
		needFetch[partition] = true
	}

	for i := 0; i < MaxRequestTries; i++ {
		if len(needFetch) == 0 {
			break
		}
		needFetchPartitions := make([]int32, 0, len(needFetch))
		for partition := range needFetch {
			needFetchPartitions = append(needFetchPartitions, partition)
		}
		result := getFirstMessagesTimestamps(client, topic, needFetchPartitions, highOffsets)
		for partition, timestamp := range result {
			timeStamps[partition] = timestamp
			offsetsCache.set(topic, partition, *timestamp)
			delete(needFetch, partition)
		}
	}

	for partition, timestamp := range timeStamps {
		if timestamp != nil && timestamp.timestamp != -1 {
			FirstMessageTimestamp.With(
				prometheus.Labels{
					"topic":     topic,
					"partition": strconv.FormatInt(int64(partition), 10),
				}).Set(float64(timestamp.timestamp))
			if *debug {
				log.Printf("topic:partition:firstMessageTimestamp %s:%d:%d", topic, partition, timestamp.timestamp)
			}
		}
	}

	failedFetches := 0
	for _, partition := range partitions {
		if _, ok := timeStamps[partition]; !ok {
			failedFetches++
		}
	}
	if failedFetches > 0 {
		log.Printf("Failed to fetch earliest offsets for %d partitions of topic %s", failedFetches, topic)
	}
}

func getFirstMessagesTimestamps(client sarama.Client, topic string, partitions []int32, offsets map[int32]int64) map[int32]*OffsetTimestamp {
	result := make(map[int32]*OffsetTimestamp)
	requests := make(map[*sarama.Broker]*sarama.FetchRequest)
	for _, partition := range partitions {
		broker, epoch, err := client.LeaderAndEpoch(topic, partition)
		if err != nil {
			log.Printf("Failed to obtain Epoch for topic:partition %s:%d", topic, partition)
		}
		if requests[broker] == nil {
			requests[broker] = newFetchRequest(client.Config())
		}
		requests[broker].AddBlock(topic, partition, offsets[partition], MaxMessageSize, epoch)
	}

	lock := sync.Mutex{}

	wg := sync.WaitGroup{}
	for broker, request := range requests {
		wg.Add(1)
		go func(broker *sarama.Broker, request *sarama.FetchRequest) {
			defer wg.Done()
			response, err := broker.Fetch(request)
			if err != nil {
				log.Printf("Failed to fetch earliest offsets for topic: %s broker:%s", topic, broker.Addr())
				return
			}
			if response.Blocks == nil {
				return
			}

			blocks := response.Blocks[topic]

			if blocks == nil {
				return
			}

			for partition, block := range blocks {
				firstMessageTimestamp := getFirstMessageTimeStampFromBlock(block, topic, partition)
				if firstMessageTimestamp != nil {
					lock.Lock()
					result[partition] = firstMessageTimestamp
					lock.Unlock()
				}

			}
		}(broker, request)
		wg.Wait()
	}
	return result
}

func newFetchRequest(config *sarama.Config) *sarama.FetchRequest {
	fetchRequest := &sarama.FetchRequest{
		MinBytes:    1,
		MaxBytes:    MaxMessageSize,
		MaxWaitTime: MaxWaitTime,
	}
	if config.Version.IsAtLeast(sarama.V0_9_0_0) {
		fetchRequest.Version = 1
	}
	if config.Version.IsAtLeast(sarama.V0_10_0_0) {
		fetchRequest.Version = 2
	}
	if config.Version.IsAtLeast(sarama.V0_10_1_0) {
		fetchRequest.Version = 3
		fetchRequest.MaxBytes = MaxMessageSize
	}
	if config.Version.IsAtLeast(sarama.V0_11_0_0) {
		fetchRequest.Version = 4
		fetchRequest.Isolation = config.Consumer.IsolationLevel
	}
	if config.Version.IsAtLeast(sarama.V1_1_0_0) {
		fetchRequest.Version = 7
		// We do not currently implement KIP-227 FetchSessions. Setting the id to 0
		// and the epoch to -1 tells the broker not to generate as session ID we're going
		// to just ignore anyway.
		fetchRequest.SessionID = 0
		fetchRequest.SessionEpoch = -1
	}
	if config.Version.IsAtLeast(sarama.V2_1_0_0) {
		fetchRequest.Version = 10
	}
	if config.Version.IsAtLeast(sarama.V2_3_0_0) {
		fetchRequest.Version = 11
		fetchRequest.RackID = config.RackID
	}
	return fetchRequest
}

func getFirstMessageTimeStampFromBlock(block *sarama.FetchResponseBlock, topic string, partition int32) *OffsetTimestamp {
	if block == nil {
		log.Printf("Failed fetch messages for topic: %s, partition: %d with error (empty block)", topic, partition)
		return nil
	}
	if !errors.Is(block.Err, sarama.ErrNoError) {
		log.Printf("Failed fetch messages for topic: %s, partition: %d with error %s", topic, partition, block.Err.Error())
		return nil
	}

	if block.RecordsSet == nil {
		return nil
	}

	for _, records := range block.RecordsSet {
		if records.MsgSet != nil {
			// legacy message type
			if len(records.MsgSet.Messages) == 0 {
				log.Printf("Failed fetch messages for topic: %s, partition: %d with error (empty MsgSet.Messages)", topic, partition)
				return nil
			}
			firstMessage := records.MsgSet.Messages[0]
			if firstMessage.Msg == nil {
				log.Printf("Failed fetch messages for topic: %s, partition: %d with error (empty MsgSet.Messages[0].Msg)", topic, partition)
				return nil
			}

			return &OffsetTimestamp{
				offset:    firstMessage.Offset,
				timestamp: firstMessage.Msg.Timestamp.Unix(),
			}

		}

		if records.RecordBatch != nil {
			// default message format
			return &OffsetTimestamp{
				offset:    records.RecordBatch.FirstOffset,
				timestamp: records.RecordBatch.FirstTimestamp.Unix(),
			}
		}
	}
	return nil
}
