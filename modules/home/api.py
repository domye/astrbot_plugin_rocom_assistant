"""
家园API接口
"""

import aiohttp
import asyncio
from typing import Dict, Any, Optional, List
from astrbot.api import logger

API_BASE = "https://wegame.shallow.ink"


class HomeApi:
    API_BASE = "https://wegame.shallow.ink"
    
    def __init__(self, api_key: str = ""):
        self._api_key = api_key
        self._session: aiohttp.ClientSession = None
        self._pending_requests: Dict[str, asyncio.Future] = {}
    
    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session
    
    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
    
    async def get_task_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        """
        查询异步任务状态
        
        Args:
            task_id: 任务ID
            
        Returns:
            任务状态数据，包含 status 和 result 字段
        """
        try:
            session = await self._get_session()
            url = f"{API_BASE}/api/v1/games/rocom/ingame/tasks/{task_id}"
            headers = {}
            if self._api_key:
                headers["X-API-Key"] = self._api_key
            
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("code") == 0:
                        return data.get("data")
                    logger.warning(f"[HomeApi] 查询任务状态失败: code={data.get('code')}, msg={data.get('msg')}")
                else:
                    logger.warning(f"[HomeApi] 查询任务状态失败: status={resp.status}")
                return None
        except Exception as e:
            logger.error(f"[HomeApi] 查询任务状态异常: {e}")
            return None
    
    async def get_home_info(self, uid: str, timeout: int = 15) -> Optional[Dict[str, Any]]:
        """
        获取家园信息，支持异步任务轮询
        
        使用 single-flight 模式避免对同一 UID 的并发请求
        
        Args:
            uid: 用户ID
            timeout: 轮询超时时间（秒），默认15秒
            
        Returns:
            家园信息数据
        """
        if uid in self._pending_requests:
            logger.debug(f"[HomeApi] 等待已存在的请求: uid={uid}")
            return await self._pending_requests[uid]
        
        future = asyncio.get_running_loop().create_future()
        self._pending_requests[uid] = future
        
        try:
            result = await self._get_home_info_impl(uid, timeout)
            future.set_result(result)
            return result
        except Exception as e:
            future.set_exception(e)
            raise
        finally:
            del self._pending_requests[uid]
    
    async def _get_home_info_impl(self, uid: str, timeout: int) -> Optional[Dict[str, Any]]:
        try:
            session = await self._get_session()
            url = f"{API_BASE}/api/v1/games/rocom/ingame/home/info"
            params = {"uid": uid, "wait_ms": 5000}
            headers = {}
            if self._api_key:
                headers["X-API-Key"] = self._api_key
            
            async with session.get(url, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("code") == 0:
                        return data.get("data")
                    logger.warning(f"[HomeApi] 获取家园信息失败: code={data.get('code')}, msg={data.get('msg')}")
                    return None
                    
                elif resp.status == 202:
                    data = await resp.json()
                    task_id = data.get("data", {}).get("task_id")
                    if not task_id:
                        logger.error(f"[HomeApi] 202响应缺少task_id: {data}")
                        return None
                    
                    logger.info(f"[HomeApi] 收到异步任务: task_id={task_id}")
                    
                    start_time = asyncio.get_running_loop().time()
                    poll_interval = 2.5
                    
                    while True:
                        elapsed = asyncio.get_running_loop().time() - start_time
                        if elapsed >= timeout:
                            logger.warning(f"[HomeApi] 任务轮询超时: task_id={task_id}, elapsed={elapsed:.1f}s")
                            return None
                        
                        await asyncio.sleep(poll_interval)
                        
                        task_status = await self.get_task_status(task_id)
                        if task_status is None:
                            logger.warning(f"[HomeApi] 查询任务状态失败，继续轮询: task_id={task_id}")
                            continue
                        
                        status = task_status.get("status")
                        logger.debug(f"[HomeApi] 任务状态: task_id={task_id}, status={status}")
                        
                        if status == "completed":
                            result = task_status.get("result")
                            if result and result.get("code") == 0:
                                logger.info(f"[HomeApi] 任务完成: task_id={task_id}")
                                return result.get("data")
                            else:
                                logger.warning(f"[HomeApi] 任务结果异常: task_id={task_id}, result={result}")
                                return None
                                
                        elif status == "failed":
                            error_msg = task_status.get("error_message") or task_status.get("error") or task_status.get("message") or "未知错误"
                            logger.error(f"[HomeApi] 任务失败: task_id={task_id}, error={error_msg}")
                            return None
                        
                        if status in ("queued", "running"):
                            continue
                        
                        logger.warning(f"[HomeApi] 未知任务状态: task_id={task_id}, status={status}")
                        return None
                    
                else:
                    logger.warning(f"[HomeApi] 获取家园信息失败: status={resp.status}")
                    return None
                    
        except Exception as e:
            logger.error(f"[HomeApi] 请求异常: {e}")
            return None
    
    def extract_plants(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        plants = []
        try:
            home_info = data.get("home_info", {})
            brief = home_info.get("friend_cell_home_brief_info", {})
            plant_info = brief.get("home_plant_info", {})
            land_list = plant_info.get("home_plant_land_list", [])
            
            for land in land_list:
                land_index = land.get("land_index", 0)
                for plant in land.get("home_plant_list", []):
                    plant["land_index"] = land_index
                    plants.append(plant)
        except Exception as e:
            logger.error(f"[HomeApi] 解析植物数据失败: {e}")
        
        return plants
    
    def extract_pets(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        pets = []
        try:
            home_info = data.get("home_info", {})
            brief = home_info.get("friend_cell_home_brief_info", {})
            home_pets = brief.get("home_pets", [])
            for pet in home_pets:
                pet_info = pet.get("home_pet_info", {})
                display_info = pet.get("display_info", {})
                pets.append({
                    "pet_gid": pet_info.get("pet_gid"),
                    "pet_cfg_id": pet_info.get("pet_cfg_id"),
                    "name": display_info.get("name") or pet_info.get("name", ""),
                    "level": display_info.get("level", 0),
                    "have_egg": pet.get("have_egg", False)
                })
        except Exception as e:
            logger.error(f"[HomeApi] 解析宠物数据失败: {e}")
        
        return pets
