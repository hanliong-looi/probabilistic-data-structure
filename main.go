package main

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
)

func main() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr: ":6379",
	})
	_ = rdb.FlushDB(ctx).Err()
	count := 1000000

	addChannel := make(chan int)
	countChannel := make(chan int)

	go addCounts(rdb, ctx, count, addChannel, countChannel)
	go poll(rdb, ctx, count, countChannel)
	<-addChannel
	keyHLLCount, err := rdb.PFCount(ctx, "key").Result()
	if err != nil {
		panic(err)
	}

	emptyHLLCount, err := rdb.PFCount(ctx, "random").Result()
	if err != nil {
		panic(err)
	}

	fmt.Println("key count from HLL: ", keyHLLCount)
	fmt.Println("actual count: ", count)
	fmt.Println("non-existent key count from HLL: ", emptyHLLCount)
}

func addPipelinedCounts(rdb *redis.Client, ctx context.Context, count int, addChannel chan int, countChannel chan int) error {
	_, err := rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		for i := 0; i < count; i++ {
			if err := pipe.PFAdd(ctx, "key", fmt.Sprint(i)).Err(); err != nil {
				return err
			}
			if i%10000 == 0 {
				countChannel <- i
			}
		}
		return nil
	})
	addChannel <- 1
	return err
}

func addCounts(rdb *redis.Client, ctx context.Context, count int, addChannel chan int, countChannel chan int) error {
	for i := 0; i < count; i++ {
		_, err := rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			//fmt.Println(i)
			if err := pipe.PFAdd(ctx, "key", fmt.Sprint(i)).Err(); err != nil {
				return err
			}
			if i%10000 == 0 {
				countChannel <- i
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	addChannel <- 1
	return nil
}

func poll(rdb *redis.Client, ctx context.Context, count int, countChannel chan int) {
	currentCount := <-countChannel
	keyHLLCount, err := rdb.PFCount(ctx, "key").Result()
	if err != nil {
		panic(err)
	}
	fmt.Println("Current count: ", keyHLLCount)
	if currentCount < count {
		go poll(rdb, ctx, count, countChannel)
	}
}
