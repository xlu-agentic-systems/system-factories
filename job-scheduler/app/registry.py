import logging
from inspect import isawaitable
from collections.abc import Awaitable, Callable
from typing import Any

from app.models import TaskResult

logger = logging.getLogger(__name__)

TaskHandler = Callable[[dict[str, Any]], Awaitable[TaskResult] | TaskResult]


class TaskRegistry:
    def __init__(self) -> None:
        self._handlers: dict[str, TaskHandler] = {}

    def register(self, task_id: str, handler: TaskHandler) -> None:
        self._handlers[task_id] = handler

    async def run(self, task_id: str, parameters: dict[str, Any]) -> TaskResult:
        handler = self._handlers.get(task_id)
        if handler is None:
            raise ValueError(f"unknown task_id: {task_id}")

        result = handler(parameters)
        if isawaitable(result):
            return await result
        return result


async def send_email(parameters: dict[str, Any]) -> TaskResult:
    logger.info("send_email task requested", extra={"parameters": parameters})
    return TaskResult(output={"delivered": True})


async def print_message(parameters: dict[str, Any]) -> TaskResult:
    logger.info("print_message task requested", extra={"parameters": parameters})
    return TaskResult(output={"printed": parameters.get("message")})


registry = TaskRegistry()
registry.register("send_email", send_email)
registry.register("print_message", print_message)
