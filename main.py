"""
AstrBot 插件：自动同意 QQ 好友申请和邀请入群请求。

实现原理
--------
AstrBot 的插件事件系统（AstrMessageEvent / @filter.event_message_type 等）覆盖的是
"消息"类事件。而 QQ 的加好友请求、加群请求/邀请属于 OneBot v11 协议里的 "request" 类
事件，不会经过 AstrBot 的统一消息管道，也就拿不到对应的 AstrMessageEvent。

因此本插件的做法是：直接找到 aiocqhttp（OneBot v11）协议端的原始客户端对象，
挂载 on_request 监听器来接收这两类事件，再调用协议端提供的
set_friend_add_request / set_group_add_request API 完成"同意"操作。

这也意味着：本插件只对通过 aiocqhttp（OneBot v11，例如 NapCat / Lagrange / LLOneBot 等
协议实现）接入的 QQ 个人号生效；QQ 官方机器人接口没有这两类事件，装了也不会生效。
"""

import asyncio

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.platform import Platform
from astrbot.api.star import Context, Star, register

try:
    # 较新版本 AstrBot 推荐从 astrbot.api.platform 引用
    from astrbot.api.platform import AiocqhttpAdapter
except ImportError:
    # 兼容部分版本未在 astrbot.api.platform 中导出该类型的情况
    from astrbot.core.platform.sources.aiocqhttp.aiocqhttp_platform_adapter import (
        AiocqhttpAdapter,
    )

# 同意好友申请后，等待多久再发送打招呼消息（秒）。
# 紧接着立刻发送有时会因为好友关系还没同步完成而失败，稍微等一下更稳妥。
GREETING_DELAY_SECONDS = 2


def _parse_lines(text: str) -> list[str]:
    """把多行文本配置解析成去除首尾空白、忽略空行后的字符串列表。"""
    if not text:
        return []
    return [line.strip() for line in text.splitlines() if line.strip()]


@register(
    "astrbot_plugin_auto_accept",
    "author",
    "自动同意 QQ 好友申请和邀请入群请求（基于 aiocqhttp/OneBot v11）",
    "1.0.0",
    "https://github.com/yourname/astrbot_plugin_auto_accept",
)
class AutoAcceptPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config

        # 插件是否处于激活状态；terminate() 时置 False，作为兜底开关，
        # 即使事件监听器因为某些原因没能成功反注册，回调里也会先检查这个标记直接跳过。
        self._active = True
        # 已挂载过监听器的平台实例（用 id(platform) 去重，避免重复挂载导致重复处理）。
        self._hooked_platform_keys: set[int] = set()
        # 记录 (client, event_name, func)，terminate 时用于反注册。
        self._subscriptions: list[tuple] = []

        # 插件被"重载"时，各平台早已加载完毕，on_astrbot_loaded 不会再触发一次，
        # 所以这里也主动尝试挂载一次当前已存在的 aiocqhttp 平台实例，覆盖热重载场景。
        asyncio.create_task(self._hook_all_platforms())

    @filter.on_astrbot_loaded()
    async def on_astrbot_loaded(self):
        """AstrBot 冷启动完成时再尝试挂载一次，覆盖插件先于平台加载完成的时序情况。"""
        await self._hook_all_platforms()

    async def _hook_all_platforms(self):
        """遍历当前已加载的平台实例，给还没挂载过的 aiocqhttp 客户端挂上监听器。"""
        try:
            platforms: list[Platform] = self.context.platform_manager.get_insts()
        except Exception as e:
            logger.error(f"[自动同意好友/群请求] 获取平台实例列表失败：{e}")
            return

        for platform in platforms:
            if not isinstance(platform, AiocqhttpAdapter):
                continue

            key = id(platform)
            if key in self._hooked_platform_keys:
                continue

            try:
                client = platform.get_client()
            except Exception as e:
                logger.error(f"[自动同意好友/群请求] 获取 aiocqhttp client 失败：{e}")
                continue

            if client is None:
                # 客户端可能还没初始化完成，之后 on_astrbot_loaded 或下次重载再重试。
                continue

            self._hook_client(client)
            self._hooked_platform_keys.add(key)
            logger.info(
                f"[自动同意好友/群请求] 已挂载到平台实例：{self._describe_platform(platform)}"
            )

    @staticmethod
    def _describe_platform(platform) -> str:
        """仅用于日志展示的可读标识；取不到就退化成类名，不影响实际功能。"""
        for attr in ("platform_id", "id"):
            val = getattr(platform, attr, None)
            if val:
                return str(val)
        try:
            meta = platform.meta()
            name = getattr(meta, "id", None) or getattr(meta, "name", None)
            if name:
                return str(name)
        except Exception:
            pass
        return platform.__class__.__name__

    def _hook_client(self, client) -> None:
        """在给定的 aiocqhttp client 上挂载好友请求 / 群请求监听器。"""

        async def on_friend_request(event):
            await self._handle_friend_request(client, event)

        async def on_group_request(event):
            await self._handle_group_request(client, event)

        # aiocqhttp 的事件名按层级命名（type.detail_type[.sub_type]）。
        # 订阅 "request.group" 会同时收到 sub_type 为 add 和 invite 的两种事件，
        # 具体行为在 handler 内部再用 event.sub_type 区分处理。
        client.subscribe("request.friend", on_friend_request)
        client.subscribe("request.group", on_group_request)

        self._subscriptions.append((client, "request.friend", on_friend_request))
        self._subscriptions.append((client, "request.group", on_group_request))

    async def _handle_friend_request(self, client, event) -> None:
        if not self._active:
            return
        try:
            user_id = event.user_id
            comment = event.comment or ""
            flag = event.flag
            if not flag:
                logger.warning(f"[自动同意好友请求] 事件缺少 flag，无法处理：{event}")
                return

            if not self.config.get("accept_friend_request", True):
                return

            keywords = _parse_lines(self.config.get("friend_request_keywords", ""))
            if keywords and not any(kw in comment for kw in keywords):
                logger.info(
                    f"[自动同意好友请求] QQ {user_id} 的验证信息「{comment}」未命中关键词"
                    "白名单，跳过（不会自动拒绝，可在协议端/客户端手动处理）"
                )
                return

            await client.api.call_action(
                "set_friend_add_request", flag=flag, approve=True
            )
            logger.info(f"[自动同意好友请求] 已同意 QQ {user_id} 的好友请求，验证信息：{comment}")

            if self.config.get("friend_greeting_enable", False):
                greeting = (self.config.get("friend_greeting_message", "") or "").strip()
                if greeting:
                    asyncio.create_task(self._send_greeting(client, user_id, greeting))
        except Exception as e:
            logger.error(f"[自动同意好友请求] 处理时出错：{e}")

    async def _send_greeting(self, client, user_id, greeting: str) -> None:
        try:
            await asyncio.sleep(GREETING_DELAY_SECONDS)
            await client.api.call_action(
                "send_private_msg", user_id=user_id, message=greeting
            )
        except Exception as e:
            logger.error(f"[自动同意好友请求] 发送欢迎语失败：{e}")

    async def _handle_group_request(self, client, event) -> None:
        if not self._active:
            return
        try:
            sub_type = event.sub_type  # "add"（加群申请）或 "invite"（邀请入群）
            user_id = event.user_id
            group_id = event.group_id
            comment = event.comment or ""
            flag = event.flag
            if not flag:
                logger.warning(f"[自动同意群请求] 事件缺少 flag，无法处理：{event}")
                return

            if sub_type == "invite":
                feature_name = "邀请入群"
                if not self.config.get("accept_group_invite", True):
                    return
                whitelist = _parse_lines(self.config.get("group_invite_whitelist", ""))
                if whitelist and str(user_id) not in whitelist:
                    logger.info(
                        f"[自动同意群请求] QQ {user_id} 邀请加入群 {group_id}，不在白名单内，跳过"
                    )
                    return
            elif sub_type == "add":
                feature_name = "加群申请"
                if not self.config.get("accept_group_join_request", False):
                    return
            else:
                logger.info(f"[自动同意群请求] 未知的 sub_type：{sub_type}，忽略")
                return

            await client.api.call_action(
                "set_group_add_request",
                flag=flag,
                sub_type=sub_type,
                approve=True,
            )
            logger.info(
                f"[自动同意群请求] 已同意群 {group_id} 的{feature_name}"
                f"（来自 QQ {user_id}），验证信息：{comment}"
            )
        except Exception as e:
            logger.error(f"[自动同意群请求] 处理时出错：{e}")

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("autoaccept_status", alias={"自动同意状态"})
    async def autoaccept_status(self, event: AstrMessageEvent):
        '''查看"自动同意好友/群请求"插件当前的运行状态和配置（仅管理员可用）'''
        lines = [
            "【自动同意好友/群请求】",
            f"插件状态：{'运行中' if self._active else '已停用'}",
            f"已挂载的 aiocqhttp 平台数：{len(self._hooked_platform_keys)}",
            f"自动同意好友请求：{'开' if self.config.get('accept_friend_request', True) else '关'}",
            f"自动同意邀请入群：{'开' if self.config.get('accept_group_invite', True) else '关'}",
            f"自动同意加群申请：{'开' if self.config.get('accept_group_join_request', False) else '关'}",
        ]
        yield event.plain_result("\n".join(lines))

    async def terminate(self):
        '''插件被卸载/停用时调用：置位停用标记，并尽量反注册事件监听器。'''
        self._active = False
        for client, event_name, func in self._subscriptions:
            try:
                client.unsubscribe(event_name, func)
            except Exception as e:
                logger.warning(f"[自动同意好友/群请求] 反注册监听器失败（可忽略）：{e}")
        self._subscriptions.clear()
        logger.info("[自动同意好友/群请求] 插件已停用")
