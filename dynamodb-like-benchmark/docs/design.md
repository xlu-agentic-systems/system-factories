# Design Notes

## Baseline Write Path

For a like:

```text
PutItem Likes(user_id, post_id)
  condition: user has not already liked this post

UpdateItem PostCounters(post_id)
  ADD like_count 1
```

For an unlike:

```text
DeleteItem Likes(user_id, post_id)
  condition: user had liked this post

UpdateItem PostCounters(post_id)
  ADD like_count -1
```

The `Likes` table gives idempotency. The `PostCounters` table gives display counts.

## Hot Post Problem

The benchmark intentionally compares:

```text
hot:
  1,000,000 users -> 1 post counter item

distributed:
  1,000,000 users -> many post counter items
```

Both execute the same number of writes. The difference is write concentration. In the hot case, the counter item is the bottleneck. In the distributed case, DynamoDB can spread writes across many partition keys.

## Why Not Just Update One Row Forever?

One-at-a-time counter updates are correct but can be too slow for viral posts. They also create tail latency when many requests contend on the same logical resource.

Better production approaches:

- `PostCounterShards`: `post_id#shard_id` counters, sum on read or cache the sum.
- Stream aggregation: write like events, aggregate asynchronously.
- Hybrid: exact `Likes` table plus approximate display count during spikes.
- Adaptive mode: normal posts use direct counters, viral posts use sharded counters.

## Benchmark Caveat

DynamoDB Local is not AWS DynamoDB. It is useful for local behavior and relative comparisons, not absolute throughput claims. The benchmark answers:

```text
How much worse does one hot counter item look than spreading the same writes across many posts?
```

It does not measure AWS partition throughput limits directly.

