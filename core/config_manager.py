# config_manager.py 수정
"""
통합 설정 관리자 - 환경변수와 데이터베이스 설정을 통합 관리
"""
import os
from typing import Dict, Any, Optional
from core.logger import logger

class ConfigManager:
    """통합 설정 관리 클래스"""
    
    def __init__(self, config_dict=None):
        """초기화 - dict 설정도 지원"""
        self._config_cache: Dict[str, Any] = {}
        self._db_manager = None
        
        if config_dict:
            self._config_cache.update(config_dict)
        
        self._load_base_config()
    
    def _load_base_config(self):
        """기본 설정 로드 (환경변수 기반)"""
        base_config = {
            'company_domain': os.getenv('COMPANY_DOMAIN', 'javame'),
            'device_id': os.getenv('DEVICE_ID'),
            'server_id': None,
            
            'mysql_host': os.getenv('MYSQL_HOST', 's4.java21.net'),
            'mysql_port': int(os.getenv('MYSQL_PORT', '3306')),
            'mysql_user': os.getenv('MYSQL_USER', 'aiot02_team3'),
            'mysql_password': os.getenv('MYSQL_PASSWORD', 'ryL7LcSp@Yiz[bR7'),
            'mysql_database': os.getenv('MYSQL_DATABASE', 'aiot02_team3'),
            
            'influxdb_url': os.getenv('INFLUXDB_URL', 'http://localhost:8888'),
            'influxdb_token': os.getenv('INFLUXDB_TOKEN', 'g-W7W0j9AE4coriQfnhHGMDnDhTZGok8bgY1NnZ6Z0EnTOsFY3SWAqDTC5fYlQ9mYnbK_doR074-a4Dgck2AOQ=='),
            'influxdb_org': os.getenv('INFLUXDB_ORG', 'javame'),
            'influxdb_bucket': os.getenv('INFLUXDB_BUCKET', 'data'),
            
            'initial_data_period': os.getenv('INITIAL_DATA_PERIOD', '7d'),
            'data_collection_overlap': int(os.getenv('DATA_COLLECTION_OVERLAP', '10')),
            'time_interval_minutes': int(os.getenv('TIME_INTERVAL_MINUTES', '5')),
            
            'prediction_horizon': int(os.getenv('PREDICTION_HORIZON', '24')),
            'training_window': os.getenv('TRAINING_WINDOW', '3d'),
            'model_type': os.getenv('MODEL_TYPE', 'gradient_boosting'),
            'app_model_type': os.getenv('APP_MODEL_TYPE', 'random_forest'),
            
            'cpu_threshold': float(os.getenv('CPU_THRESHOLD', '80.0')),
            'memory_threshold': float(os.getenv('MEMORY_THRESHOLD', '80.0')),
            'disk_threshold': float(os.getenv('DISK_THRESHOLD', '85.0')),
            
            'data_collection_interval': int(os.getenv('DATA_COLLECTION_INTERVAL', '30')),
            'model_training_interval': int(os.getenv('MODEL_TRAINING_INTERVAL', '360')),
            'prediction_interval': int(os.getenv('PREDICTION_INTERVAL', '60')),
            'health_check_interval': int(os.getenv('HEALTH_CHECK_INTERVAL', '15')),
            
            'window_sizes': [5, 15, 30, 60],
            'statistics': ['mean', 'std', 'max', 'min'],
            'resample_interval': '5min',
            'validation_split': 0.2,
            'adaptive_interval': True,
            'prediction_interval_minutes': 5
        }
        
        for key, value in base_config.items():
            if key not in self._config_cache:
                self._config_cache[key] = value
        
        logger.info("기본 설정 로드 완료")
    
    def get_db_manager(self):
        """데이터베이스 매니저 가져오기 (지연 로딩)"""
        if self._db_manager is None:
            from core.db import DatabaseManager
            self._db_manager = DatabaseManager()
        return self._db_manager
    
    def get(self, key: str, default: Any = None) -> Any:
        """설정값 가져오기"""
        return self._config_cache.get(key, default)
    
    def set(self, key: str, value: Any):
        """설정값 설정하기"""
        self._config_cache[key] = value
    
    def get_server_id(self) -> Optional[int]:
        """서버 ID 가져오기 (자동 조회/생성)"""
        if self._config_cache.get('server_id'):
            return self._config_cache['server_id']
        
        device_id = self.get('device_id')
        if not device_id:
            logger.error("디바이스 ID가 설정되지 않았습니다")
            return None
        
        company_domain = self.get('company_domain')
        
        try:
            db = self.get_db_manager()
            server_id = db.get_server_by_device_id(company_domain, device_id)
            
            if not server_id:
                from scripts.setup import insert_server
                server_id = insert_server(db, company_domain, device_id, f"Auto {device_id}", "auto")
                logger.info(f"새 서버 자동 생성: {server_id}")
            
            self._config_cache['server_id'] = server_id
            return server_id
            
        except Exception as e:
            logger.error(f"서버 ID 조회/생성 실패: {e}")
            return None
    
    def get_system_resources(self) -> list:
        """시스템 리소스 목록 조회"""
        try:
            db = self.get_db_manager()
            return db.get_system_resources(self.get('company_domain'))
        except Exception as e:
            logger.warning(f"시스템 리소스 목록 조회 실패: {e}")
            return ['cpu', 'mem', 'disk']
    
    def get_excluded_devices(self) -> list:
        """제외 디바이스 목록 조회"""
        try:
            db = self.get_db_manager()
            return db.get_excluded_devices(self.get('company_domain'))
        except Exception as e:
            logger.warning(f"제외 디바이스 목록 조회 실패: {e}")
            return []
    
    def get_applications(self) -> list:
        """애플리케이션 목록 조회"""
        try:
            db = self.get_db_manager()
            company_domain = self.get('company_domain')
            server_id = self.get_server_id()
            device_id = self.get('device_id')
            
            if not server_id:
                return []
            
            device_filter = f" AND device_id = '{device_id}'" if device_id else ""
            
            query = f"""
            SELECT DISTINCT application 
            FROM jvm_metrics 
            WHERE company_domain = %s AND {db.server_id_field} = %s{device_filter}
            ORDER BY application
            """
            
            results = db.fetch_all(query, (company_domain, server_id))
            return [row[0] for row in results] if results else []
            
        except Exception as e:
            logger.warning(f"애플리케이션 목록 조회 실패: {e}")
            return ['javame-gateway', 'javame-frontend', 'javame-backend']
    
    def get_mysql_config(self) -> dict:
        """MySQL 설정 딕셔너리 반환"""
        return {
            'host': self.get('mysql_host'),
            'port': self.get('mysql_port'),
            'user': self.get('mysql_user'),
            'password': self.get('mysql_password'),
            'database': self.get('mysql_database')
        }
    
    def get_influxdb_config(self) -> dict:
        """InfluxDB 설정 딕셔너리 반환"""
        return {
            'url': self.get('influxdb_url'),
            'token': self.get('influxdb_token'),
            'org': self.get('influxdb_org'),
            'bucket': self.get('influxdb_bucket')
        }
    
    def get_model_config(self) -> dict:
        """모델 설정 딕셔너리 반환"""
        return {
            'prediction_horizon': self.get('prediction_horizon'),
            'training_window': self.get('training_window'),
            'model_type': self.get('model_type'),
            'app_model_type': self.get('app_model_type'),
            'validation_split': self.get('validation_split')
        }
    
    def get_alert_thresholds(self) -> dict:
        """알림 임계값 딕셔너리 반환"""
        return {
            'cpu': self.get('cpu_threshold'),
            'memory': self.get('memory_threshold'),
            'disk': self.get('disk_threshold')
        }
    
    def get_prediction_interval(self, hours: int = None) -> int:
        """예측 간격 조회 (분 단위)"""
        if self.get('adaptive_interval', True) and hours:
            from config.settings import PREDICTION_CONFIG
            interval_map = PREDICTION_CONFIG.get('interval_by_horizon', {})
            
            for horizon, interval in sorted(interval_map.items()):
                if hours <= horizon:
                    logger.info(f"예측 범위 {hours}시간에 대해 {interval}분 간격 적용")
                    return interval
            
            max_interval = max(interval_map.values()) if interval_map else 60
            logger.info(f"예측 범위 {hours}시간에 대해 최대 간격 {max_interval}분 적용")
            return max_interval
        
        return self.get('prediction_interval_minutes', 5)

    def get_prediction_config(self) -> dict:
        """예측 설정 딕셔너리 반환"""
        return {
            'prediction_horizon': self.get('prediction_horizon'),
            'training_window': self.get('training_window'),
            'model_type': self.get('model_type'),
            'prediction_interval_minutes': self.get('prediction_interval_minutes', 5),
            'adaptive_interval': self.get('adaptive_interval', True)
        }
    
    def validate_config(self) -> bool:
        """설정 유효성 검사"""
        required_keys = [
            'company_domain', 'mysql_host', 'mysql_user', 
            'mysql_password', 'mysql_database'
        ]
        
        missing_keys = []
        for key in required_keys:
            if not self.get(key):
                missing_keys.append(key)
        
        if missing_keys:
            logger.error(f"필수 설정이 없습니다: {missing_keys}")
            return False
        
        if not self.get('device_id') and not self.get('server_id'):
            logger.error("디바이스 ID 또는 서버 ID가 설정되어야 합니다")
            return False
        
        return True
    
    def __del__(self):
        """소멸자"""
        if self._db_manager:
            self._db_manager.close()