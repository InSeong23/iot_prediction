"""
시스템 헬스 모니터링 모듈 - ConfigManager 연동
- 시스템 상태 실시간 모니터링
- 작업 성공/실패 추적
- 알림 및 복구 액션
"""
import os
import sys
import time
import psutil
import threading
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from collections import deque

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.config_manager import ConfigManager
from core.logger import logger
from core.db import DatabaseManager

class HealthMonitor:
    """시스템 헬스 모니터"""
    
    def __init__(self, config_manager: Optional[ConfigManager] = None):
        """초기화"""
        # ConfigManager 없이도 동작하도록 수정
        if config_manager:
            self.config = config_manager
        else:
            self.config = None
        self.db_manager = DatabaseManager()
        
        # 모니터링 상태
        self.is_running = False
        self.monitor_thread = None
        
        # 작업 성과 추적
        self.job_history = {
            'data_collection': deque(maxlen=100),
            'model_training': deque(maxlen=50),
            'prediction': deque(maxlen=100),
            'accuracy_update': deque(maxlen=50),
            'cleanup': deque(maxlen=20)
        }
        
        # 시스템 메트릭 히스토리
        self.system_metrics = deque(maxlen=1440)  # 24시간 (1분마다)
        
        # 경고 임계값 (설정이 없으면 기본값 사용)
        if self.config:
            self.thresholds = {
                'cpu_usage': self.config.get('cpu_threshold', 80.0),
                'memory_usage': self.config.get('memory_threshold', 80.0),
                'disk_usage': self.config.get('disk_threshold', 85.0),
                'job_failure_rate': self.config.get('job_failure_threshold', 0.3),
                'response_time': self.config.get('response_time_threshold', 300)
            }
        else:
            # 기본 임계값
            self.thresholds = {
                'cpu_usage': 80.0,
                'memory_usage': 80.0,
                'disk_usage': 85.0,
                'job_failure_rate': 0.3,
                'response_time': 300
            }
        
        logger.info(f"헬스 모니터 초기화 완료, 임계값: {self.thresholds}")
    
    def start(self):
        """헬스 모니터링 시작"""
        if self.is_running:
            logger.warning("헬스 모니터가 이미 실행 중입니다.")
            return
        
        logger.info("헬스 모니터링 시작")
        self.is_running = True
        
        # 모니터링 스레드 시작
        self.monitor_thread = threading.Thread(target=self._monitoring_loop, daemon=True)
        self.monitor_thread.start()
    
    def stop(self):
        """헬스 모니터링 중지"""
        logger.info("헬스 모니터링 중지")
        self.is_running = False
        
        if self.monitor_thread and self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=5)
        
        if hasattr(self, 'db_manager') and self.db_manager:
            self.db_manager.close()
    
    def _monitoring_loop(self):
        """모니터링 메인 루프"""
        check_interval = 60  # 기본 1분
        if self.config:
            check_interval = self.config.get('health_check_interval_seconds', 60)
        
        while self.is_running:
            try:
                # 시스템 메트릭 수집
                metrics = self._collect_system_metrics()
                self.system_metrics.append(metrics)
                
                # 상태 분석
                self._analyze_system_health()
                
                # 대기
                time.sleep(check_interval)
                
            except Exception as e:
                logger.error(f"헬스 모니터링 루프 오류: {e}")
                time.sleep(check_interval)
    
    def _collect_system_metrics(self) -> Dict[str, Any]:
        """시스템 메트릭 수집"""
        try:
            metrics = {
                'timestamp': datetime.now(),
                'cpu_percent': psutil.cpu_percent(interval=1),
                'memory_percent': psutil.virtual_memory().percent,
                'disk_percent': psutil.disk_usage('/').percent,
                'load_average': os.getloadavg()[0] if hasattr(os, 'getloadavg') else 0,
                'process_count': len(psutil.pids()),
                'network_connections': len(psutil.net_connections())
            }
            
            return metrics
            
        except Exception as e:
            logger.error(f"시스템 메트릭 수집 오류: {e}")
            return {
                'timestamp': datetime.now(),
                'cpu_percent': 0,
                'memory_percent': 0,
                'disk_percent': 0,
                'load_average': 0,
                'process_count': 0,
                'network_connections': 0
            }
    
    def _analyze_system_health(self):
        """시스템 상태 분석"""
        if not self.system_metrics:
            return
        
        latest = self.system_metrics[-1]
        
        # CPU 사용률 확인
        if latest['cpu_percent'] > self.thresholds['cpu_usage']:
            logger.warning(f"CPU 사용률 높음: {latest['cpu_percent']:.1f}%")
        
        # 메모리 사용률 확인
        if latest['memory_percent'] > self.thresholds['memory_usage']:
            logger.warning(f"메모리 사용률 높음: {latest['memory_percent']:.1f}%")
        
        # 디스크 사용률 확인
        if latest['disk_percent'] > self.thresholds['disk_usage']:
            logger.warning(f"디스크 사용률 높음: {latest['disk_percent']:.1f}%")
    
    def perform_health_check(self) -> Dict[str, Any]:
        """전체 시스템 헬스 체크 수행"""
        health_status = {
            'timestamp': datetime.now(),
            'overall_status': 'healthy',
            'components': {},
            'alerts': []
        }
        
        try:
            # 1. 데이터베이스 연결 확인
            db_status = self._check_database_health()
            health_status['components']['database'] = db_status
            
            # 2. 시스템 리소스 확인
            system_status = self._check_system_resources()
            health_status['components']['system'] = system_status
            
            # 3. 데이터 수집 상태 확인
            data_status = self._check_data_collection_health()
            health_status['components']['data_collection'] = data_status
            
            # 4. 모델 상태 확인
            model_status = self._check_model_health()
            health_status['components']['model'] = model_status
            
            # 5. 예측 상태 확인
            prediction_status = self._check_prediction_health()
            health_status['components']['prediction'] = prediction_status
            
            # 6. 작업 성능 확인
            job_status = self._check_job_performance()
            health_status['components']['jobs'] = job_status
            
            # 전체 상태 결정
            component_statuses = [comp['status'] for comp in health_status['components'].values()]
            
            if 'critical' in component_statuses:
                health_status['overall_status'] = 'critical'
            elif 'warning' in component_statuses:
                health_status['overall_status'] = 'warning'
            else:
                health_status['overall_status'] = 'healthy'
            
            return health_status
            
        except Exception as e:
            logger.error(f"헬스 체크 수행 오류: {e}")
            return {
                'timestamp': datetime.now(),
                'overall_status': 'critical',
                'error': str(e)
            }
    
    def _check_database_health(self) -> Dict[str, Any]:
        """데이터베이스 연결 상태 확인"""
        try:
            # 연결 확인
            if not self.db_manager.connection or not self.db_manager.connection.is_connected():
                return {
                    'status': 'critical',
                    'message': '데이터베이스 연결 끊김'
                }
            
            # 간단한 쿼리 테스트
            result = self.db_manager.fetch_one("SELECT 1")
            
            if result:
                return {
                    'status': 'healthy',
                    'message': '데이터베이스 연결 정상'
                }
            else:
                return {
                    'status': 'warning',
                    'message': '데이터베이스 쿼리 응답 없음'
                }
                
        except Exception as e:
            return {
                'status': 'critical',
                'message': f'데이터베이스 오류: {str(e)}'
            }
    
    def _check_system_resources(self) -> Dict[str, Any]:
        """시스템 리소스 상태 확인"""
        try:
            if not self.system_metrics:
                return {
                    'status': 'warning',
                    'message': '시스템 메트릭 데이터 없음'
                }
            
            latest = self.system_metrics[-1]
            warnings = []
            
            # CPU 확인
            if latest['cpu_percent'] > self.thresholds['cpu_usage']:
                warnings.append(f"CPU 사용률 높음: {latest['cpu_percent']:.1f}%")
            
            # 메모리 확인
            if latest['memory_percent'] > self.thresholds['memory_usage']:
                warnings.append(f"메모리 사용률 높음: {latest['memory_percent']:.1f}%")
            
            # 디스크 확인
            if latest['disk_percent'] > self.thresholds['disk_usage']:
                warnings.append(f"디스크 사용률 높음: {latest['disk_percent']:.1f}%")
            
            if warnings:
                return {
                    'status': 'warning',
                    'message': '; '.join(warnings),
                    'metrics': latest
                }
            else:
                return {
                    'status': 'healthy',
                    'message': '시스템 리소스 정상',
                    'metrics': latest
                }
                
        except Exception as e:
            return {
                'status': 'critical',
                'message': f'시스템 리소스 확인 오류: {str(e)}'
            }
    
    def _check_data_collection_health(self) -> Dict[str, Any]:
        """데이터 수집 상태 확인"""
        try:
            # ConfigManager가 없으면 글로벌 모니터링 모드
            if not self.config:
                return {
                    'status': 'healthy',
                    'message': '글로벌 모니터링 모드'
                }
            
            company_domain = self.config.get('company_domain')
            server_id = self.config.get_server_id()
            device_id = self.config.get('device_id')
            
            if not company_domain or not server_id:
                return {
                    'status': 'warning',
                    'message': '회사 도메인 또는 서버 ID 미설정'
                }
            
            device_filter = ""
            params = [company_domain, server_id]
            
            if device_id:
                device_filter = " AND device_id = %s"
                params.append(device_id)
            
            # 최근 1시간 내 데이터 확인
            recent_time = datetime.now() - timedelta(hours=1)
            
            # 시스템 리소스 데이터 확인
            sys_query = f"""
            SELECT COUNT(*) FROM system_resources 
            WHERE company_domain = %s AND {self.db_manager.server_id_field} = %s 
            AND time > %s{device_filter}
            """
            
            params.append(recent_time)
            sys_result = self.db_manager.fetch_one(sys_query, tuple(params))
            sys_count = sys_result[0] if sys_result else 0
            
            # JVM 메트릭 데이터 확인
            jvm_query = f"""
            SELECT COUNT(*) FROM jvm_metrics 
            WHERE company_domain = %s AND {self.db_manager.server_id_field} = %s 
            AND time > %s{device_filter}
            """
            
            jvm_result = self.db_manager.fetch_one(jvm_query, tuple(params))
            jvm_count = jvm_result[0] if jvm_result else 0
            
            # 최소 데이터 요구사항
            min_expected = self.config.get('min_hourly_records', 10)
            
            if sys_count < min_expected and jvm_count < min_expected:
                return {
                    'status': 'critical',
                    'message': f'최근 데이터 수집 중단 (시스템: {sys_count}, JVM: {jvm_count})'
                }
            elif sys_count < min_expected or jvm_count < min_expected:
                return {
                    'status': 'warning',
                    'message': f'일부 데이터 수집 부족 (시스템: {sys_count}, JVM: {jvm_count})'
                }
            else:
                return {
                    'status': 'healthy',
                    'message': f'데이터 수집 정상 (시스템: {sys_count}, JVM: {jvm_count})'
                }
                
        except Exception as e:
            return {
                'status': 'critical',
                'message': f'데이터 수집 상태 확인 오류: {str(e)}'
            }
    
    def _check_model_health(self) -> Dict[str, Any]:
        """모델 상태 확인"""
        try:
            # ConfigManager가 없으면 글로벌 모니터링 모드
            if not self.config:
                return {
                    'status': 'healthy',
                    'message': '글로벌 모니터링 모드'
                }
            
            company_domain = self.config.get('company_domain')
            server_id = self.config.get_server_id()
            device_id = self.config.get('device_id')
            
            if not company_domain or not server_id:
                return {
                    'status': 'warning',
                    'message': '회사 도메인 또는 서버 ID 미설정'
                }
            
            device_filter = ""
            params = [company_domain, server_id]
            
            if device_id:
                device_filter = " AND device_id = %s"
                params.append(device_id)
            
            # 최근 모델 성능 확인
            model_query = f"""
            SELECT 
                application,
                resource_type,
                r2_score,
                mae,
                trained_at
            FROM model_performance 
            WHERE company_domain = %s AND {self.db_manager.server_id_field} = %s{device_filter}
            ORDER BY trained_at DESC
            LIMIT 10
            """
            
            results = self.db_manager.fetch_all(model_query, tuple(params))
            
            if not results:
                return {
                    'status': 'warning',
                    'message': '학습된 모델이 없음'
                }
            
            # 모델 성능 분석
            warnings = []
            latest_training = results[0][4]  # trained_at
            
            # 마지막 학습 시간 확인
            max_age_hours = self.config.get('model_max_age_hours', 168)  # 7일
            age_hours = (datetime.now() - latest_training).total_seconds() / 3600
            
            if age_hours > max_age_hours:
                warnings.append(f"모델이 오래됨 ({age_hours:.1f}시간)")
            
            # 성능 임계값 확인
            min_r2_score = self.config.get('min_r2_score', 0.5)
            max_mae = self.config.get('max_mae', 20.0)
            
            poor_models = []
            for app, resource, r2, mae, trained_at in results:
                if r2 < min_r2_score or mae > max_mae:
                    poor_models.append(f"{app}-{resource}")
            
            if poor_models:
                warnings.append(f"성능 저하 모델: {', '.join(poor_models)}")
            
            if warnings:
                return {
                    'status': 'warning',
                    'message': '; '.join(warnings),
                    'model_count': len(results)
                }
            else:
                return {
                    'status': 'healthy',
                    'message': f'모델 상태 정상 ({len(results)}개)',
                    'latest_training': latest_training
                }
                
        except Exception as e:
            return {
                'status': 'critical',
                'message': f'모델 상태 확인 오류: {str(e)}'
            }
    
    def _check_prediction_health(self) -> Dict[str, Any]:
        """예측 상태 확인"""
        try:
            # ConfigManager가 없으면 글로벌 모니터링 모드
            if not self.config:
                return {
                    'status': 'healthy',
                    'message': '글로벌 모니터링 모드'
                }
            
            company_domain = self.config.get('company_domain')
            server_id = self.config.get_server_id()
            device_id = self.config.get('device_id')
            
            if not company_domain or not server_id:
                return {
                    'status': 'warning',
                    'message': '회사 도메인 또는 서버 ID 미설정'
                }
            device_filter = ""
            params = [company_domain, server_id]
            
            if device_id:
                device_filter = " AND device_id = %s"
                params.append(device_id)
            
            # 최근 예측 확인
            recent_time = datetime.now() - timedelta(hours=2)
            
            pred_query = f"""
            SELECT 
                MAX(prediction_time) as latest_prediction,
                COUNT(*) as prediction_count,
                AVG(error) as avg_error
            FROM predictions 
            WHERE company_domain = %s AND {self.db_manager.server_id_field} = %s 
            AND prediction_time > %s{device_filter}
            """
            
            params.append(recent_time)
            result = self.db_manager.fetch_one(pred_query, tuple(params))
            
            if not result or not result[0]:
                return {
                    'status': 'warning',
                    'message': '최근 예측 결과 없음'
                }
            
            latest_prediction, pred_count, avg_error = result
            
            # 예측 빈도 확인
            prediction_interval = self.config.get('prediction_interval', 60)
            expected_predictions = 120 / prediction_interval  # 2시간 동안 예상 예측 수
            
            warnings = []
            
            if pred_count < expected_predictions * 0.5:  # 50% 미만이면 경고
                warnings.append(f"예측 빈도 부족 ({pred_count}개)")
            
            # 예측 정확도 확인 (실제값이 있는 경우)
            if avg_error and avg_error > self.config.get('max_prediction_error', 15.0):
                warnings.append(f"예측 오차 높음 ({avg_error:.2f})")
            
            if warnings:
                return {
                    'status': 'warning',
                    'message': '; '.join(warnings),
                    'latest_prediction': latest_prediction,
                    'prediction_count': pred_count
                }
            else:
                return {
                    'status': 'healthy',
                    'message': f'예측 상태 정상 ({pred_count}개)',
                    'latest_prediction': latest_prediction
                }
                
        except Exception as e:
            return {
                'status': 'critical',
                'message': f'예측 상태 확인 오류: {str(e)}'
            }
    
    def _check_job_performance(self) -> Dict[str, Any]:
        """작업 성능 확인"""
        try:
            warnings = []
            job_stats = {}
            
            for job_name in self.job_history.keys():
                stats = self.get_job_statistics(job_name, hours=24)
                job_stats[job_name] = stats
                
                if stats.get('total_jobs', 0) == 0:
                    continue  # 작업이 없으면 건너뛰기
                
                # 실패율 확인
                success_rate = stats.get('success_rate', 0)
                if success_rate < (100 - self.thresholds['job_failure_rate'] * 100):
                    warnings.append(f"{job_name} 실패율 높음 ({100-success_rate:.1f}%)")
                
                # 응답 시간 확인
                avg_duration = stats.get('avg_duration', 0)
                if avg_duration > self.thresholds['response_time']:
                    warnings.append(f"{job_name} 응답 시간 지연 ({avg_duration:.1f}초)")
            
            if warnings:
                return {
                    'status': 'warning',
                    'message': '; '.join(warnings),
                    'job_statistics': job_stats
                }
            else:
                return {
                    'status': 'healthy',
                    'message': '작업 성능 정상',
                    'job_statistics': job_stats
                }
                
        except Exception as e:
            return {
                'status': 'critical',
                'message': f'작업 성능 확인 오류: {str(e)}'
            }
    