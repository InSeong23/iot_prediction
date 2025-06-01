"""
JVM 메트릭 기반 시스템 리소스 예측 - 간소화된 설정 파일
하드코딩 제거, 환경변수 기본값과 상수만 정의
"""
import os
from datetime import datetime

# ================================
# 환경 설정
# ================================
ENV = os.environ.get('ENVIRONMENT', 'development')
DEBUG = ENV == 'development'
VERSION = "2.0.0"
BUILD_DATE = datetime.now().strftime("%Y-%m-%d")

# ================================
# 데이터베이스 스키마 상수 (변경되지 않는 값들)
# ================================
DB_SCHEMA = {
    "server_table": "servers",
    "server_id_field": "server_no", 
    "server_ip_field": "server_iphost",
    "server_domain_field": "company_domain"
}

# ================================
# 기본 JVM 메트릭 목록 (상수)
# ================================
JVM_METRICS = [
    'cpu_utilization_percent',
    'gc_g1_young_generation_count',
    'memory_old_gen_used_bytes',
    'memory_total_heap_used_bytes',
    'process_open_file_descriptors_count',
    'thread_active_count'
]

# ================================
# 시스템 리소스 매핑 (상수)
# ================================
RESOURCE_MEASUREMENTS = {
    "cpu": ["usage_idle"],      # 수집 후 변환: 100 - usage_idle = usage_user
    "mem": ["used_percent"],
    "disk": ["used_percent"]
}

# ================================
# 환경변수 기본값 (ConfigManager에서 사용)
# ================================
DEFAULT_CONFIG = {
    # 기본 정보
    'company_domain': 'javame',
    'device_id': None,  # 필수 설정
    
    # 데이터베이스 
    'mysql_host': 's4.java21.net',
    'mysql_port': 13306,
    'mysql_user': 'aiot02_team3',
    'mysql_password': 'ryL7LcSp@Yiz[bR7',
    'mysql_database': 'aiot02_team3',
    
    # InfluxDB
    'influxdb_url': 'http://localhost:8888',
    'influxdb_token': 'g-W7W0j9AE4coriQfnhHGMDnDhTZGok8bgY1NnZ6Z0EnTOsFY3SWAqDTC5fYlQ9mYnbK_doR074-a4Dgck2AOQ==',  # 필수 설정
    'influxdb_org': 'javame',
    'influxdb_bucket': 'data',
    'influxdb_timeout': 60,
    
    # 데이터 수집
    'initial_data_period': '7d',
    'data_collection_overlap': 10,
    'time_interval_minutes': 5,
    'resource_location': 'server_resource_data',
    'service_location': 'service_resource_data',
    'gateway_field': 'gatewayId',
    
    # 전처리
    'resample_interval': '5min',
    'window_sizes': [5, 15, 30, 60],
    'statistics': ['mean', 'std', 'max', 'min'],
    'impact_calculation': 'correlation',
    'time_features': True,
    
    # 모델
    'app_model_type': 'random_forest',
    'prediction_model_type': 'gradient_boosting',
    'prediction_horizon': 24,
    'training_window': '3d',
    'validation_split': 0.2,
    
    # 예측 간격 설정
    'prediction_interval_minutes': int(os.getenv('PREDICTION_INTERVAL_MINUTES', '5')),  # 기본 5분
    'adaptive_interval': os.getenv('ADAPTIVE_INTERVAL', 'true').lower() == 'true',
    
    # 임계값
    'cpu_threshold': 80.0,
    'memory_threshold': 80.0,
    'disk_threshold': 85.0,
    
    # 스케줄 (분 단위)
    'data_collection_interval': 30,
    'model_training_interval': 360,
    'prediction_interval': 60,
    'health_check_interval': 15,
    
    # 로깅
    'log_level': 'INFO',
    'log_file': 'logs/jvm_metrics.log',
    'log_max_bytes': 10485760,  # 10MB
    'log_backup_count': 5,
    
    # 배치 처리
    'batch_size': 1000,
    'query_timeout': 60,
    'retry_count': 3,
    'connection_pool_size': 5
}

# ================================
# 기본 제외 목록 (MySQL에서 관리하되 기본값 제공)
# ================================
DEFAULT_EXCLUDED_GATEWAYS = [
    "diskio", "net", "sensors", "swap", "system", 
    "http", "unknown_service", "입구", "jvm"
]

DEFAULT_EXCLUDED_DEVICES = [
    "nhnacademy-Inspiron-14-5420",
    "24e124743d011875", 
    "24e124136d151368"
]

# ================================
# 리소스별 기본 집계 방식
# ================================
DEFAULT_RESOURCE_AGGREGATION = {
    "cpu": {
        "method": "max",
        "reason": "피크 사용률이 시스템 부하 예측에 중요"
    },
    "mem": {
        "method": "last",
        "reason": "메모리는 현재 점유 상태가 가장 의미있음"
    },
    "disk": {
        "method": "last",
        "reason": "디스크 사용률은 누적적 특성으로 현재 상태가 중요"
    }
}

DEFAULT_AGGREGATION_METHOD = "average"

# ================================
# 모델 하이퍼파라미터 기본값
# ================================
DEFAULT_HYPERPARAMETERS = {
    "random_forest": {
        "n_estimators": 100,
        "max_depth": 10,
        "random_state": 42
    },
    "gradient_boosting": {
        "n_estimators": 100,
        "learning_rate": 0.1,
        "random_state": 42
    }
}

# ================================
# 유효성 검사 규칙
# ================================
VALIDATION_RULES = {
    "cpu_utilization_percent": {"min": 0, "max": 100},
    "memory_usage_percent": {"min": 0, "max": 100},
    "disk_usage_percent": {"min": 0, "max": 100},
    "thread_count": {"min": 0, "max": 10000},
    "memory_bytes": {"min": 0, "max": 1e12}  # 1TB 제한
}
# 시간 정렬 및 집계 설정
TIME_ALIGNMENT_CONFIG = {
    # 시간 정렬 설정
    'time_alignment_minutes': int(os.getenv('TIME_ALIGNMENT_MINUTES', '1')),  # 1분 단위 정렬
    'aggregation_window_seconds': int(os.getenv('AGGREGATION_WINDOW_SECONDS', '30')),  # 30초 집계 윈도우
    
    # 예측 정확도 설정
    'time_tolerance_minutes': int(os.getenv('TIME_TOLERANCE_MINUTES', '2')),  # 2분 허용 오차
    'max_prediction_age_hours': int(os.getenv('MAX_PREDICTION_AGE_HOURS', '48')),  # 48시간 최대 나이
    
    # 개선된 로직 활성화 플래그
    'use_improved_collector': os.getenv('USE_IMPROVED_COLLECTOR', 'true').lower() == 'true',
    'use_improved_prediction_accuracy': os.getenv('USE_IMPROVED_PREDICTION_ACCURACY', 'true').lower() == 'true',
    
    # 집계 방식 설정
    'default_aggregation_method': os.getenv('DEFAULT_AGGREGATION_METHOD', 'average'),
    'cpu_aggregation_method': os.getenv('CPU_AGGREGATION_METHOD', 'max'),
    'memory_aggregation_method': os.getenv('MEMORY_AGGREGATION_METHOD', 'last'),
    'disk_aggregation_method': os.getenv('DISK_AGGREGATION_METHOD', 'last'),
}
# ================================
# 헬퍼 함수
# ================================
def get_default_config():
    """기본 설정 딕셔너리 반환"""
    return DEFAULT_CONFIG.copy()

def get_required_env_vars():
    """필수 환경변수 목록 반환"""
    return [
        'MYSQL_HOST', 'MYSQL_USER', 'MYSQL_PASSWORD', 'MYSQL_DATABASE',
        'INFLUXDB_URL', 'INFLUXDB_TOKEN', 'COMPANY_DOMAIN'
    ]

def get_db_schema():
    """데이터베이스 스키마 정보 반환"""
    return DB_SCHEMA.copy()

def get_default_aggregation():
    """기본 집계 설정 반환"""
    return DEFAULT_RESOURCE_AGGREGATION.copy()

def get_hyperparameters(model_type):
    """모델별 하이퍼파라미터 반환"""
    return DEFAULT_HYPERPARAMETERS.get(model_type, {})

def validate_value(metric_type, value):
    """값 유효성 검사"""
    if metric_type in VALIDATION_RULES:
        rules = VALIDATION_RULES[metric_type]
        return rules["min"] <= value <= rules["max"]
    return True

# ================================
# 호환성 유지 (기존 코드에서 사용하는 변수들)
# ================================
# 기존 코드에서 import해서 사용하는 변수들은 유지
COMPANY_DOMAIN = os.environ.get('COMPANY_DOMAIN', DEFAULT_CONFIG['company_domain'])
SERVER_CODE = os.environ.get('DEVICE_ID', None)  # device_id를 server_code로 사용
VISUALIZATION_ENABLED = os.environ.get('VISUALIZATION_ENABLED', 'false').lower() == 'true'

# 기존 MYSQL, INFLUXDB 딕셔너리 호환성 유지
MYSQL = {
    "host": os.environ.get('MYSQL_HOST', DEFAULT_CONFIG['mysql_host']),
    "port": int(os.environ.get('MYSQL_PORT', str(DEFAULT_CONFIG['mysql_port']))),
    "user": os.environ.get('MYSQL_USER', DEFAULT_CONFIG['mysql_user']),
    "password": os.environ.get('MYSQL_PASSWORD', DEFAULT_CONFIG['mysql_password']),
    "database": os.environ.get('MYSQL_DATABASE', DEFAULT_CONFIG['mysql_database'])
}

INFLUXDB = {
    "url": os.environ.get('INFLUXDB_URL', DEFAULT_CONFIG['influxdb_url']),
    "token": os.environ.get('INFLUXDB_TOKEN', DEFAULT_CONFIG['influxdb_token']),
    "org": os.environ.get('INFLUXDB_ORG', DEFAULT_CONFIG['influxdb_org']),
    "bucket": os.environ.get('INFLUXDB_BUCKET', DEFAULT_CONFIG['influxdb_bucket']),
    "timeout": int(os.environ.get('INFLUXDB_TIMEOUT', str(DEFAULT_CONFIG['influxdb_timeout'])))
}

# 기존 COLLECTION, PREPROCESSING, MODEL 등은 ConfigManager에서 동적으로 관리
# 호환성을 위해 기본값만 제공
COLLECTION = {
    "initial_period": DEFAULT_CONFIG['initial_data_period'],
    "regular_interval": DEFAULT_CONFIG['data_collection_interval'],
    "overlap_minutes": DEFAULT_CONFIG['data_collection_overlap'],
    "system_resources": ["cpu", "mem", "disk"],
    "excluded_locations": DEFAULT_EXCLUDED_GATEWAYS,
    "resource_location": DEFAULT_CONFIG['resource_location'],
    "service_location": DEFAULT_CONFIG['service_location'],
    "gateway_field": DEFAULT_CONFIG['gateway_field'],
    "time_interval_minutes": DEFAULT_CONFIG['time_interval_minutes']
}

PREPROCESSING = {
    "window_sizes": DEFAULT_CONFIG['window_sizes'],
    "statistics": DEFAULT_CONFIG['statistics'],
    "time_features": DEFAULT_CONFIG['time_features'],
    "impact_calculation": DEFAULT_CONFIG['impact_calculation'],
    "resample_interval": DEFAULT_CONFIG['resample_interval']
}

MODEL = {
    "app_model_type": DEFAULT_CONFIG['app_model_type'],
    "prediction_model_type": DEFAULT_CONFIG['prediction_model_type'],
    "prediction_horizon": DEFAULT_CONFIG['prediction_horizon'],
    "training_window": DEFAULT_CONFIG['training_window'],
    "validation_split": DEFAULT_CONFIG['validation_split'],
    "hyperparameters": DEFAULT_HYPERPARAMETERS
}

ALERTS = {
    "enabled": True,
    "cpu_threshold": DEFAULT_CONFIG['cpu_threshold'],
    "memory_threshold": DEFAULT_CONFIG['memory_threshold'],
    "disk_threshold": DEFAULT_CONFIG['disk_threshold']
}

LOGGING = {
    "level": DEFAULT_CONFIG['log_level'],
    "file": DEFAULT_CONFIG['log_file'],
    "max_bytes": DEFAULT_CONFIG['log_max_bytes'],
    "backup_count": DEFAULT_CONFIG['log_backup_count']
}
PREDICTION_CONFIG = {
    # 예측 간격 설정 (분 단위)
    'prediction_interval_minutes': int(os.getenv('PREDICTION_INTERVAL_MINUTES', '5')),  # 기본 5분
    
    # 다른 간격 옵션들
    'short_term_interval': 5,    # 단기 예측: 5분
    'medium_term_interval': 15,  # 중기 예측: 15분
    'long_term_interval': 60,    # 장기 예측: 1시간
    
    # 예측 범위별 간격 매핑
    'interval_by_horizon': {
        1: 5,    # 1시간 예측 → 5분 간격
        6: 15,   # 6시간 예측 → 15분 간격
        24: 30,  # 24시간 예측 → 30분 간격
        168: 60  # 7일 예측 → 1시간 간격
    }
}

# 기존 호환성 변수들
RESOURCE_AGGREGATION = DEFAULT_RESOURCE_AGGREGATION
DEFAULT_AGGREGATION = DEFAULT_AGGREGATION_METHOD