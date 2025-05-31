
"""
스케줄러 모듈 초기화
"""

# 스트리밍 아키텍처용 글로벌 스케줄러 import
from .global_scheduler import StreamingGlobalScheduler

# 하위 호환성을 위한 별칭 제공
GlobalScheduler = StreamingGlobalScheduler

# 헬스 모니터
from .health_monitor import HealthMonitor

__all__ = ['StreamingGlobalScheduler', 'GlobalScheduler', 'HealthMonitor']