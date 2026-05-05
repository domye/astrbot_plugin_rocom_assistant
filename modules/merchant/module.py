"""
远行商人查询模块
"""

from typing import Dict, Any, List
from datetime import datetime, timedelta, timezone

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, MessageChain
import astrbot.api.message_components as Comp

from ...core.base import BaseModule
from ...core.utils import check_admin
from .api import MerchantApi
from .subscription import MerchantSubscriptionManager


class MerchantModule(BaseModule):
    name = "merchant"
    description = "远行商人查询"
    version = "1.0.0"
    
    PUSH_TIMES = ["08:10", "12:10", "16:10", "20:10"]
    
    def __init__(self, context, data_dir: str, config: dict = None):
        super().__init__(context, data_dir, config)
        self.api = MerchantApi()
        self.subscription = MerchantSubscriptionManager(data_dir)
    
    async def on_load(self):
        self.register_command("商人", self._query, "查询当前远行商人商品")
        self.register_command("商人订阅开启", self._sub_open, "管理员开启群订阅")
        self.register_command("商人订阅关闭", self._sub_close, "管理员关闭群订阅")
        self.register_command("商人订阅添加", self._sub_add, "添加商品订阅")
        self.register_command("商人订阅删除", self._sub_remove, "删除商品订阅")
        self.register_command("商人订阅列表", self._sub_list, "查看订阅列表")
        self.register_command("商人订阅清空", self._sub_clear, "清空自己的订阅")
        
        self.register_schedule(self.PUSH_TIMES, self._do_push, "远行商人定时推送")
        logger.info(f"[MerchantModule] 模块加载完成，定时推送: {self.PUSH_TIMES}")
    
    async def on_unload(self):
        await self.api.close()
    
    def _cn_tz(self) -> timezone:
        return timezone(timedelta(hours=8))
    
    def _format_items(self, items: List[str]) -> str:
        return "、".join(items) if items else "暂无商品"
    
    def _get_round_id(self, data: Dict[str, Any]) -> str:
        round_info = data.get("roundInfo", {})
        date = round_info.get("date", datetime.now(self._cn_tz()).strftime("%Y-%m-%d"))
        current = round_info.get("current", "unknown")
        return f"{date}-{current}"
    
    async def _query(self, event: AstrMessageEvent, _=None):
        data = await self.api.fetch_data()
        if not data:
            yield event.plain_result("获取商人数据失败，请稍后重试")
            return
        
        items = data.get("items", [])
        round_info = data.get("roundInfo", {})
        msg = f"【远行商人】{round_info.get('current', '-')}\n当前商品: {self._format_items(items)}"
        yield event.plain_result(msg)
    
    async def _sub_open(self, event: AstrMessageEvent, _=None):
        if not await check_admin(event):
            yield event.plain_result("仅管理员可执行此操作")
            return
        
        await self.subscription.set_enabled(event.unified_msg_origin, True)
        yield event.plain_result(
            f"已开启远行商人订阅\n推送时间: 每天 {', '.join(self.PUSH_TIMES)}\n"
            "用户可使用 /商人订阅添加 <商品名> 订阅特定商品"
        )
    
    async def _sub_close(self, event: AstrMessageEvent, _=None):
        if not await check_admin(event):
            yield event.plain_result("仅管理员可执行此操作")
            return
        
        await self.subscription.set_enabled(event.unified_msg_origin, False)
        yield event.plain_result("已关闭远行商人订阅")
    
    async def _sub_add(self, event: AstrMessageEvent, item_name: str = ""):
        if not item_name:
            yield event.plain_result("请输入商品名称，例如: /商人订阅添加 国王球")
            return
        
        added = await self.subscription.add_user_item(
            event.unified_msg_origin,
            event.get_sender_id(),
            item_name
        )
        yield event.plain_result(
            f"已添加订阅: {item_name}" if added else f"你已订阅过该商品: {item_name}"
        )
    
    async def _sub_remove(self, event: AstrMessageEvent, item_name: str = ""):
        if not item_name:
            yield event.plain_result("请输入商品名称，例如: /商人订阅删除 国王球")
            return
        
        removed = await self.subscription.remove_user_item(
            event.unified_msg_origin,
            event.get_sender_id(),
            item_name
        )
        yield event.plain_result(
            f"已删除订阅: {item_name}" if removed else f"未找到订阅: {item_name}"
        )
    
    async def _sub_list(self, event: AstrMessageEvent, _=None):
        sub = await self.subscription.get_group(event.unified_msg_origin)
        
        if not sub:
            yield event.plain_result("当前群未开启订阅")
            return
        
        enabled = "已开启" if sub.get("enabled") else "已关闭"
        user_items = sub.get("user_items", {})
        
        if not user_items:
            yield event.plain_result(f"订阅状态: {enabled}\n暂无用户订阅商品")
            return
        
        lines = [f"订阅状态: {enabled}\n用户订阅:"]
        for user_id, items in user_items.items():
            lines.append(f"  - 用户{user_id}: {self._format_items(items)}")
        
        yield event.plain_result("\n".join(lines))
    
    async def _sub_clear(self, event: AstrMessageEvent, _=None):
        cleared = await self.subscription.clear_user_items(
            event.unified_msg_origin,
            event.get_sender_id()
        )
        yield event.plain_result(
            "已清空你的所有商品订阅" if cleared else "你没有订阅任何商品"
        )
    
    async def _do_push(self):
        logger.info("[MerchantModule] 开始执行定时推送...")
        enabled_groups = await self.subscription.get_all_enabled()
        if not enabled_groups:
            logger.info("[MerchantModule] 没有开启订阅的群")
            return
        
        data = await self.api.fetch_data()
        if not data:
            logger.warning("[MerchantModule] 获取商人数据失败，跳过推送")
            return
        
        items = data.get("items", [])
        round_info = data.get("roundInfo", {})
        logger.info(f"[MerchantModule] 商人数据: {round_info.get('current')}, 商品: {items}")
        
        for session_id, group_data in enabled_groups.items():
            try:
                user_items_map = group_data.get("user_items", {})
                matched_users: Dict[str, List[str]] = {}
                
                for user_id, subscribed_items in user_items_map.items():
                    matched = [item for item in subscribed_items if item in items]
                    if matched:
                        matched_users[user_id] = matched
                
                chain = MessageChain()
                chain.message(f"【远行商人】{round_info.get('current', '')}\n当前商品: {self._format_items(items)}")
                
                if matched_users:
                    chain.message("\n\n订阅提醒:")
                    for user_id, matched_items in matched_users.items():
                        chain.at(name="", qq=str(user_id)).message(f" 命中: {self._format_items(matched_items)}")
                
                await self.context.send_message(session_id, chain)
                logger.info(f"[MerchantModule] 已推送到 {session_id}")
            except Exception as e:
                logger.error(f"[MerchantModule] 推送到 {session_id} 失败: {e}")
