"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import resource, run_receipt

# 专用回执路由需先注册，避免被通用资源路由的 /api/{module} 动态段抢先匹配。
ROUTERS = [run_receipt, resource]
