"""
统一定时任务调度器
"""

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Callable, Awaitable, List, Tuple, Set
from astrbot.api import logger

# 全局变量：防止多个 Scheduler 同时运行
_global_scheduler_running = False
_global_scheduler_instance = None


class Scheduler:
    # 类级别共享变量：多个实例共享执行记录和锁
    _shared_executed_keys: Set[str] = set()
    _shared_lock: asyncio.Lock = None  # 会在第一次使用时初始化

    def __init__(self):
        self._tasks: List[Tuple[str, List[str], Callable]] = []
        self._running = False
        self._task: asyncio.Task = None

    def _get_lock(self) -> asyncio.Lock:
        """获取共享锁"""
        if Scheduler._shared_lock is None:
            Scheduler._shared_lock = asyncio.Lock()
        return Scheduler._shared_lock

    def _cn_tz(self) -> timezone:
        return timezone(timedelta(hours=8))

    def register(self, name: str, times: List[str], handler: Callable[[], Awaitable[None]]):
        # 检查是否已注册
        for existing_name, _, _ in self._tasks:
            if existing_name == name:
                logger.warning(f"[Scheduler] 任务已存在，跳过重复注册: {name}")
                return
        self._tasks.append((name, times, handler))
        logger.info(f"[Scheduler] 注册定时任务: {name}, 时间: {times}")
    
    def _parse_time(self, time_str: str, base: datetime) -> datetime:
        hour, minute = map(int, time_str.split(":"))
        return base.replace(hour=hour, minute=minute, second=0, microsecond=0)
    
    def _next_time(self, now: datetime, times: List[str]) -> datetime:
        for t in sorted(times):
            pt = self._parse_time(t, now)
            if pt > now:
                return pt
        next_day = now + timedelta(days=1)
        return self._parse_time(sorted(times)[0], next_day)
    
    def _make_exec_key(self, name: str, time_str: str, date: datetime) -> str:
        """生成唯一执行key: 任务名-时间-日期"""
        return f"{name}-{time_str}-{date.strftime('%Y-%m-%d')}"

    async def _run_handlers(self, now: datetime):
        # 使用共享锁防止多个 scheduler 同时执行
        lock = self._get_lock()
        if lock.locked():
            logger.debug("[Scheduler] 其他 scheduler 正在执行，跳过")
            return

        async with lock:
            for name, times, handler in self._tasks:
                for t in times:
                    pt = self._parse_time(t, now)
                    # 只匹配当前时间点（前60秒内），避免重复执行
                    if 0 <= (now - pt).total_seconds() < 60:
                        exec_key = self._make_exec_key(name, t, now)
                        if exec_key in Scheduler._shared_executed_keys:
                            logger.debug(f"[Scheduler] 跳过已执行任务: {exec_key}")
                            continue
                        try:
                            logger.info(f"[Scheduler] 执行定时任务: {name}")
                            await handler()
                            Scheduler._shared_executed_keys.add(exec_key)
                        except Exception as e:
                            logger.error(f"[Scheduler] 任务执行失败 {name}: {e}")

            # 清理过期的执行记录（保留最近2天的）
            today = now.strftime('%Y-%m-%d')
            yesterday = (now - timedelta(days=1)).strftime('%Y-%m-%d')
            Scheduler._shared_executed_keys = {
                k for k in Scheduler._shared_executed_keys
                if today in k or yesterday in k
            }
    
    async def _loop(self):
        while self._running:
            try:
                now = datetime.now(self._cn_tz())
                if now.tzinfo is None:
                    now = now.replace(tzinfo=self._cn_tz())
                
                all_times = []
                for _, times, _ in self._tasks:
                    all_times.extend(times)
                
                if not all_times:
                    await asyncio.sleep(60)
                    continue
                
                next_run = self._next_time(now, all_times)
                sleep_seconds = max(1, (next_run - now).total_seconds())
                
                await asyncio.sleep(sleep_seconds)
                
                now = datetime.now(self._cn_tz())
                await self._run_handlers(now)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[Scheduler] 循环异常: {e}")
                await asyncio.sleep(60)
    
    def start(self):
        global _global_scheduler_running, _global_scheduler_instance

        # 停止旧的实例（如果存在）
        if _global_scheduler_instance and _global_scheduler_instance != self:
            logger.info("[Scheduler] 停止旧的 scheduler 实例")
            _global_scheduler_instance._running = False
            if _global_scheduler_instance._task and not _global_scheduler_instance._task.done():
                _global_scheduler_instance._task.cancel()
            _global_scheduler_running = False

        if self._running:
            logger.debug("[Scheduler] 当前实例已运行")
            return

<<<<<<< HEAD
        # 检查是否有其他 scheduler 正在运行
        try:
            current_loop = asyncio.get_running_loop()
            all_tasks = asyncio.all_tasks(current_loop)

            scheduler_tasks = [
                t for t in all_tasks
                if t.get_name() == "scheduler_loop" and not t.done()
            ]

            if scheduler_tasks:
                logger.warning(f"[Scheduler] 已有 scheduler 任务在运行，停止旧任务")
                for t in scheduler_tasks:
                    t.cancel()

        except RuntimeError:
            pass

        self._running = True
        self._task = asyncio.create_task(self._loop(), name="scheduler_loop")
        logger.info(f"[Scheduler] 调度器已启动 (任务数: {len(self._tasks)})")
=======
        self._running = True
        _global_scheduler_running = True
        _global_scheduler_instance = self
        self._task = asyncio.create_task(self._loop())
        logger.info("[Scheduler] 调度器已启动")
>>>>>>> 29c6601ee0d42663783e6351ebda62fa825aaa00

    async def stop(self):
        global _global_scheduler_running, _global_scheduler_instance

        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

        if _global_scheduler_instance == self:
            _global_scheduler_running = False
            _global_scheduler_instance = None

        logger.info("[Scheduler] 调度器已停止")
