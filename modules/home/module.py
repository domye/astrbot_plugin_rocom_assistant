"""
家园种植提醒模块
"""

import asyncio
from typing import Dict, Any, List, Set
from datetime import datetime, timezone, timedelta

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, MessageChain
import astrbot.api.message_components as Comp

from ...core.base import BaseModule
from .api import HomeApi
from .subscription import HomeSubscriptionManager


class HomeModule(BaseModule):
    name = "home"
    description = "家园种植提醒"
    version = "2.0.0"
    
    EGG_CHECK_INTERVAL = 10800
    PLANT_TIMER_KEY = "plant_timer"
    EGG_TIMER_KEY = "egg_timer"
    
    def __init__(self, context, data_dir: str, config: dict = None):
        super().__init__(context, data_dir, config)
        config = config or {}
        api_key = config.get("api_key", "")
        self.api = HomeApi(api_key)
        self.subscription = HomeSubscriptionManager(data_dir)
        self._plant_timers: Dict[str, asyncio.Task] = {}
        self._egg_timer: asyncio.Task = None
        self._running = False
        self._tracked_eggs: Dict[str, Set[str]] = {}
    
    async def on_load(self):
        self.register_command("家园订阅", self._subscribe, "订阅家园种植提醒")
        self.register_command("家园取消", self._unsubscribe, "取消家园订阅")
        self.register_command("家园状态", self._status, "查看订阅状态")
        
        self._running = True
        asyncio.create_task(self._init_subscriptions())
        logger.info(f"[HomeModule] 模块加载完成")
    
    async def _init_subscriptions(self):
        await asyncio.sleep(1)
        
        subs = await self.subscription.get_all_subscriptions()
        for sub in subs:
            await self._setup_plant_timer(sub)
        
        if subs:
            self._start_egg_check_timer()
    
    async def on_unload(self):
        self._running = False
        
        for task in self._plant_timers.values():
            task.cancel()
        
        if self._egg_timer:
            self._egg_timer.cancel()
        
        await self.api.close()
    
    def _cn_tz(self) -> timezone:
        return timezone(timedelta(hours=8))
    
    def _now_ts(self) -> int:
        return int(datetime.now(self._cn_tz()).timestamp())
    
    def _sub_key(self, session_id: str, user_id: str) -> str:
        return f"{session_id}:{user_id}"
    
    async def _subscribe(self, event: AstrMessageEvent, uid: str = ""):
        if not uid:
            yield event.plain_result("请输入UID，例如: /家园订阅 123456")
            return
        
        added = await self.subscription.subscribe(
            event.unified_msg_origin,
            event.get_sender_id(),
            uid
        )
        
        if added:
            sub = await self.subscription.get_subscription(
                event.unified_msg_origin,
                event.get_sender_id()
            )
            if sub:
                await self._setup_plant_timer(sub)
                self._start_egg_check_timer()
            
            yield event.plain_result(f"已订阅家园 {uid}，将在作物成熟和精灵产蛋时提醒你")
        else:
            yield event.plain_result("你已订阅过该UID")
    
    async def _unsubscribe(self, event: AstrMessageEvent, _=None):
        sub = await self.subscription.get_subscription(
            event.unified_msg_origin,
            event.get_sender_id()
        )
        
        if sub:
            key = self._sub_key(event.unified_msg_origin, event.get_sender_id())
            if key in self._plant_timers:
                self._plant_timers[key].cancel()
                del self._plant_timers[key]
        
        removed = await self.subscription.unsubscribe(
            event.unified_msg_origin,
            event.get_sender_id()
        )
        yield event.plain_result("已取消家园订阅" if removed else "你未订阅任何家园")
    
    async def _status(self, event: AstrMessageEvent, _=None):
        sub = await self.subscription.get_subscription(
            event.unified_msg_origin,
            event.get_sender_id()
        )
        
        if not sub:
            yield event.plain_result("你未订阅任何家园")
            return
        
        uid = sub.get("uid")
        
        home_info = await self.api.get_home_info(uid)
        if not home_info:
            yield event.plain_result(f"订阅UID: {uid}\n查询家园信息失败")
            return
        
        plants = self.api.extract_plants(home_info)
        pets = self.api.extract_pets(home_info)
        now = self._now_ts()
        
        ripe_count = 0
        growing_count = 0
        max_rip_time = None
        
        for plant in plants:
            state = plant.get("plant_state", 0)
            rip_time = plant.get("plant_rip_time", 0)
            
            if state == 1:
                if rip_time <= now:
                    ripe_count += 1
                else:
                    growing_count += 1
                    if max_rip_time is None or rip_time > max_rip_time:
                        max_rip_time = rip_time
        
        egg_pets = [pet for pet in pets if pet.get("have_egg")]
        
        lines = [f"订阅UID: {uid}"]
        lines.append(f"种植园: 成熟: {ripe_count}株, 生长中: {growing_count}株")
        
        if growing_count > 0 and max_rip_time:
            remain = max_rip_time - now
            remain_str = self._format_remain(remain)
            lines.append(f"{remain_str}后全部成熟")
        
        if egg_pets:
            egg_names = [pet.get("name", "未知") for pet in egg_pets]
            lines.append(f"室内: {', '.join(egg_names)}")
        
        yield event.plain_result("\n".join(lines))
    
    def _format_remain(self, seconds: int) -> str:
        if seconds < 60:
            return f"{seconds}秒"
        elif seconds < 3600:
            return f"{seconds // 60}分钟"
        else:
            hours = seconds // 3600
            mins = (seconds % 3600) // 60
            return f"{hours}小时{mins}分钟"
    
    async def _setup_plant_timer(self, sub: Dict[str, Any]):
        uid = sub.get("uid")
        session_id = sub.get("session_id")
        user_id = sub.get("user_id")
        key = self._sub_key(session_id, user_id)
        
        if key in self._plant_timers:
            self._plant_timers[key].cancel()
        
        home_info = await self.api.get_home_info(uid)
        if not home_info:
            return
        
        plants = self.api.extract_plants(home_info)
        now = self._now_ts()
        
        ripe_count = 0
        growing_count = 0
        max_rip_time = None
        
        for plant in plants:
            state = plant.get("plant_state", 0)
            rip_time = plant.get("plant_rip_time", 0)
            
            if state == 1:
                if rip_time <= now:
                    ripe_count += 1
                else:
                    growing_count += 1
                    if max_rip_time is None or rip_time > max_rip_time:
                        max_rip_time = rip_time
        
        if ripe_count > 0:
            await self._notify_plant(session_id, user_id, uid, ripe_count, growing_count)
        
        if growing_count > 0 and max_rip_time:
            wait_seconds = max(0, max_rip_time - now)
            task = asyncio.create_task(
                self._plant_timer(session_id, user_id, uid, wait_seconds)
            )
            self._plant_timers[key] = task
        
        pets = self.api.extract_pets(home_info)
        egg_names = set(pet.get("name", "未知") for pet in pets if pet.get("have_egg"))
        self._tracked_eggs[key] = egg_names
        
        if egg_names:
            await self._notify_egg(session_id, user_id, uid, list(egg_names), is_new=True)
    
    async def _plant_timer(self, session_id: str, user_id: str, uid: str, wait_seconds: int):
        try:
            await asyncio.sleep(wait_seconds)
            
            home_info = await self.api.get_home_info(uid)
            if not home_info:
                return
            
            plants = self.api.extract_plants(home_info)
            now = self._now_ts()
            
            ripe_count = 0
            growing_count = 0
            next_rip_time = None
            
            for plant in plants:
                state = plant.get("plant_state", 0)
                rip_time = plant.get("plant_rip_time", 0)
                
                if state == 1:
                    if rip_time <= now:
                        ripe_count += 1
                    else:
                        growing_count += 1
                        if next_rip_time is None or rip_time > next_rip_time:
                            next_rip_time = rip_time
            
            if ripe_count > 0:
                await self._notify_plant(session_id, user_id, uid, ripe_count, growing_count)
            
            if growing_count > 0 and next_rip_time:
                key = self._sub_key(session_id, user_id)
                wait_seconds = max(0, next_rip_time - now)
                task = asyncio.create_task(
                    self._plant_timer(session_id, user_id, uid, wait_seconds)
                )
                self._plant_timers[key] = task
            
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"[HomeModule] 植物定时器异常: {e}")
    
    def _start_egg_check_timer(self):
        if self._egg_timer is None or self._egg_timer.done():
            self._egg_timer = asyncio.create_task(self._egg_check_loop())
    
    async def _egg_check_loop(self):
        while self._running:
            try:
                await asyncio.sleep(self.EGG_CHECK_INTERVAL)
                
                subs = await self.subscription.get_all_subscriptions()
                for sub in subs:
                    await self._check_eggs(sub)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[HomeModule] 蛋检查循环异常: {e}")
    
    async def _check_eggs(self, sub: Dict[str, Any]):
        uid = sub.get("uid")
        session_id = sub.get("session_id")
        user_id = sub.get("user_id")
        key = self._sub_key(session_id, user_id)
        
        home_info = await self.api.get_home_info(uid)
        if not home_info:
            return
        
        plants = self.api.extract_plants(home_info)
        pets = self.api.extract_pets(home_info)
        now = self._now_ts()
        
        ripe_count = 0
        growing_count = 0
        max_rip_time = None
        
        for plant in plants:
            state = plant.get("plant_state", 0)
            rip_time = plant.get("plant_rip_time", 0)
            
            if state == 1:
                if rip_time <= now:
                    ripe_count += 1
                else:
                    growing_count += 1
                    if max_rip_time is None or rip_time > max_rip_time:
                        max_rip_time = rip_time
        
        if ripe_count > 0:
            await self._notify_plant(session_id, user_id, uid, ripe_count, growing_count)
        
        if growing_count > 0 and max_rip_time:
            if key in self._plant_timers:
                self._plant_timers[key].cancel()
            wait_seconds = max(0, max_rip_time - now)
            task = asyncio.create_task(
                self._plant_timer(session_id, user_id, uid, wait_seconds)
            )
            self._plant_timers[key] = task
        
        current_eggs = set(pet.get("name", "未知") for pet in pets if pet.get("have_egg"))
        
        prev_eggs = self._tracked_eggs.get(key, set())
        
        new_eggs = current_eggs - prev_eggs
        
        if new_eggs:
            await self._notify_egg(session_id, user_id, uid, list(new_eggs), is_new=True)
        
        if current_eggs and not new_eggs:
            if len(current_eggs) != len(prev_eggs) or current_eggs != prev_eggs:
                await self._notify_egg(session_id, user_id, uid, list(current_eggs), is_new=False)
        
        self._tracked_eggs[key] = current_eggs
    
    async def _notify_plant(self, session_id: str, user_id: str, uid: str, ripe_count: int, growing_count: int):
        chain = MessageChain()
        chain.at(name="", qq=str(user_id)).message(f" 你的家园({uid})作物已成熟!\n种植园: 成熟: {ripe_count}株, 生长中: {growing_count}株")
        
        try:
            await self.context.send_message(session_id, chain)
            logger.info(f"[HomeModule] 已发送成熟提醒: user={user_id}, uid={uid}")
        except Exception as e:
            logger.error(f"[HomeModule] 发送提醒失败: {e}")
    
    async def _notify_egg(self, session_id: str, user_id: str, uid: str, egg_names: List[str], is_new: bool):
        if is_new:
            msg = f" 你的家园({uid})有新精灵蛋!\n室内: {', '.join(egg_names)}"
        else:
            msg = f" 你的家园({uid})有精灵蛋待收取!\n室内: {', '.join(egg_names)}"
        
        chain = MessageChain()
        chain.at(name="", qq=str(user_id)).message(msg)
        
        try:
            await self.context.send_message(session_id, chain)
            logger.info(f"[HomeModule] 已发送蛋提醒: user={user_id}, uid={uid}, eggs={egg_names}")
        except Exception as e:
            logger.error(f"[HomeModule] 发送提醒失败: {e}")
