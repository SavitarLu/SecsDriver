"""
后台任务工具。

统一管理临时任务，避免 asyncio.create_task 的引用丢失问题，
并为未处理异常提供日志出口。
"""

import asyncio
import logging
from typing import Coroutine, Optional, Set

logger = logging.getLogger(__name__)

_tasks: Set[asyncio.Task] = set()


def spawn(coro: Coroutine, name: Optional[str] = None) -> asyncio.Task:
    """
    创建后台任务并保存强引用。

    任务结束时自动从未完成集合移除；若抛出未处理异常则记入日志。
    """
    task = asyncio.ensure_future(coro)
    _tasks.add(task)

    def _on_done(done: asyncio.Task) -> None:
        _tasks.discard(done)
        if done.cancelled():
            return
        exc = done.exception()
        if exc is not None:
            logger.error("后台任务 %s 抛出未处理异常: %s", name or done.get_name(), exc)

    task.add_done_callback(_on_done)
    return task


def pending_count() -> int:
    """当前存活的后台任务数量（调试用）。"""
    return len(_tasks)


async def cancel_all() -> None:
    """取消所有后台任务（进程退出清理用）。"""
    tasks = [task for task in _tasks if not task.done()]
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
