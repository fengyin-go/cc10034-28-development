"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import delivery, resource

# delivery 含 /api/route_dispatcher/deliveries 等具体路径，必须先于
# 通用资源路由注册，否则会被 /api/{module}/{entry_id} 抢先匹配。
ROUTERS = [delivery, resource]
