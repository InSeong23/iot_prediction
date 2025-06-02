"""
데이터 수집 및 전처리 파이프라인 - ConfigManager 연동
- 기존 DataCollector, DataPreprocessor를 파이프라인으로 래핑
- 초기 수집, 정기 수집, 전처리를 하나의 플로우로 관리
"""
import os
import sys
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Optional

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.config_manager import ConfigManager
from core.logger import logger
from core.db import DatabaseManager

class BasePipeline(ABC):
    """파이프라인 기본 클래스"""
    
    def __init__(self, config_dict=None):
        # dict 설정을 ConfigWrapper로 감싸기
        if isinstance(config_dict, dict):
            from core.config_manager import ConfigWrapper
            self.config = ConfigWrapper(config_dict)
        else:
            self.config = config_dict or {}
        self._db_manager = None
    
    def get_db_manager(self):
        """데이터베이스 매니저 지연 로딩"""
        if self._db_manager is None:
            self._db_manager = DatabaseManager()
        return self._db_manager
    
    @abstractmethod
    def execute(self, **kwargs) -> bool:
        """파이프라인 실행"""
        pass
    
    def __del__(self):
        if self._db_manager:
            self._db_manager.close()
