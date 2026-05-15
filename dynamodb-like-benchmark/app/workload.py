from __future__ import annotations

import random
from dataclasses import dataclass


@dataclass(frozen=True)
class LikeOperation:
    user_id: str
    post_id: str
    action: str


def post_ids(count: int) -> list[str]:
    return [f"post-{index:06d}" for index in range(1, count + 1)]


def generate_operations(
    total: int,
    mode: str,
    posts: int,
    seed: int = 42,
    unlike_ratio: float = 0.0,
) -> list[LikeOperation]:
    ids = post_ids(posts)
    rng = random.Random(seed)
    operations: list[LikeOperation] = []
    for index in range(total):
        if mode == "hot":
            post_id = ids[0]
        elif mode == "distributed":
            post_id = ids[index % posts]
        elif mode == "zipf":
            post_id = ids[_zipf_index(rng, posts)]
        else:
            raise ValueError("mode must be one of: hot, distributed, zipf")
        action = "unlike" if rng.random() < unlike_ratio else "like"
        operations.append(LikeOperation(user_id=f"user-{index:09d}", post_id=post_id, action=action))
    return operations


def _zipf_index(rng: random.Random, posts: int) -> int:
    # Small deterministic approximation: 80% of traffic goes to top 20% of posts.
    hot_posts = max(1, posts // 5)
    if rng.random() < 0.80:
        return rng.randrange(hot_posts)
    return rng.randrange(hot_posts, posts)

