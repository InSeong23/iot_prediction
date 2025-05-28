"""
Scheduler 모듈 - 글로벌 멀티 테넌트 스케줄링 및 헬스 모니터링
"""

from .global_scheduler import GlobalScheduler
from .health_monitor import HealthMonitor

__all__ = [
    'GlobalScheduler',
    'HealthMonitor'
]

# 버전 정보
__version__ = '3.0.0'  # 멀티 테넌트 지원으로 메이저 버전 업

# 호환성 알림
import warnings

def _deprecated_import_warning():
    warnings.warn(
        "BatchScheduler는 더 이상 사용되지 않습니다. GlobalScheduler를 사용하세요.",
        DeprecationWarning,
        stacklevel=3
    )

# BatchScheduler 접근 시 경고 출력
class _DeprecatedModule:
    def __getattr__(self, name):
        if name == 'BatchScheduler':
            _deprecated_import_warning()
            raise ImportError(
                "BatchScheduler는 제거되었습니다. "
                "GlobalScheduler를 사용하세요: "
                "from scheduler.global_scheduler import GlobalScheduler"
            )
        raise AttributeError(f"module 'scheduler' has no attribute '{name}'")