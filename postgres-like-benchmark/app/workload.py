from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class LikeOperation:
    user_id: str
    post_id: str


def post_ids(count: int) -> list[str]:
    return [f"post-{index:06d}" for index in range(1, count + 1)]


def generate_operations(total: int, mode: str, posts: int) -> list[LikeOperation]:
    ids = post_ids(posts)
    operations: list[LikeOperation] = []
    for index in range(total):
        if mode == "hot":
            post_id = ids[0]
        elif mode == "distributed":
            post_id = ids[index % posts]
        else:
            raise ValueError("mode must be 'hot' or 'distributed'")
        operations.append(LikeOperation(user_id=f"user-{index:09d}", post_id=post_id))
    return operations

