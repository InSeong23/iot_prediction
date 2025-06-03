"""
모델 학습 파이프라인 - ConfigManager 연동
- 애플리케이션 영향도 모델과 시스템 예측 모델 학습
- 모델 성능 추적 및 자동 재학습 판단
"""
import os
import sys
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.data_pipeline import BasePipeline
from core.config_manager import ConfigManager
from core.logger import logger

class ModelPipeline(BasePipeline):
    """모델 학습 파이프라인"""
    
    def execute(self, mode: str = 'all', **kwargs) -> bool:
        """
        모델 파이프라인 실행
        
        Args:
            mode: 'app' | 'system' | 'all' | 'retrain'
            **kwargs: 추가 파라미터 (visualization 등)
        """
        try:
            logger.info(f"모델 파이프라인 실행 시작: 모드={mode}")
            
            if mode == 'app':
                return self._train_app_models(**kwargs)
            elif mode == 'system':
                return self._train_system_models(**kwargs)
            elif mode == 'all':
                app_success = self._train_app_models(**kwargs)
                system_success = self._train_system_models(**kwargs)
                return app_success and system_success
            elif mode == 'retrain':
                return self._retrain_if_needed(**kwargs)
            else:
                logger.error(f"지원하지 않는 모드: {mode}")
                return False
                
        except Exception as e:
            logger.error(f"모델 파이프라인 실행 오류: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
    
    def _train_app_models(self, **kwargs) -> bool:
        """애플리케이션 영향도 모델 학습"""
        logger.info("애플리케이션 영향도 모델 학습 시작")
        
        try:
            from models.app_models import AppModelManager
            
            # ConfigManager에서 설정 가져오기
            company_domain = self.config.get('company_domain')
            device_id = self.config.get('device_id')
            server_id = self.config.get_server_id()
            
            if not company_domain:
                logger.error("회사 도메인이 설정되지 않았습니다.")
                return False
            
            # AppModelManager 초기화 (ConfigManager 전달)
            manager = AppModelManager(
                config_manager=self.config,
                company_domain=company_domain,
                server_id=server_id,
                device_id=device_id
            )
            
            # 모든 애플리케이션 모델 학습
            success = manager.train_all_models()
            
            if success:
                logger.info("애플리케이션 영향도 모델 학습 완료")
                return True
            else:
                logger.error("애플리케이션 영향도 모델 학습 실패")
                return False
                
        except Exception as e:
            logger.error(f"애플리케이션 모델 학습 오류: {e}")
            return False
    
    def _train_system_models(self, **kwargs) -> bool:
        """시스템 예측 모델 학습"""
        logger.info("시스템 예측 모델 학습 시작")
        
        try:
            from models.prediction import SystemResourcePredictor
            
            # ConfigManager에서 설정 가져오기
            company_domain = self.config.get('company_domain')
            device_id = self.config.get('device_id')
            server_id = self.config.get_server_id()
            
            if not company_domain:
                logger.error("회사 도메인이 설정되지 않았습니다.")
                return False
            
            # 시각화 설정
            visualization = kwargs.get('visualization', self.config.get('visualization_enabled', False))
            
            # SystemResourcePredictor 초기화 (ConfigManager 전달)
            predictor = SystemResourcePredictor(
                config_manager=self.config,
                company_domain=company_domain,
                server_id=server_id,
                device_id=device_id
            )
            
            # 시스템 예측 모델 학습
            success = predictor.train_models(visualization=visualization)
            
            if success:
                logger.info("시스템 예측 모델 학습 완료")
                return True
            else:
                logger.error("시스템 예측 모델 학습 실패")
                return False
                
        except Exception as e:
            logger.error(f"시스템 모델 학습 오류: {e}")
            return False
    
    def _retrain_if_needed(self, **kwargs) -> bool:
        """필요 시 모델 재학습"""
        logger.info("모델 재학습 필요성 검토")
        
        try:
            # 모델 신선도 확인
            needs_retraining = self.check_model_freshness()
            
            if needs_retraining:
                logger.info("모델 재학습이 필요합니다.")
                
                # 성능 저하 확인
                performance_degraded = self.check_performance_degradation()
                
                if performance_degraded:
                    logger.info("모델 성능 저하 감지, 즉시 재학습 시작")
                    return self.execute(mode='all', **kwargs)
                else:
                    # 스케줄 기반 재학습
                    last_training = self.get_last_training_time()
                    training_interval = timedelta(
                        hours=self.config.get('model_training_interval', 360) / 60
                    )
                    
                    if datetime.now() - last_training > training_interval:
                        logger.info("스케줄 기반 모델 재학습 시작")
                        return self.execute(mode='all', **kwargs)
                    else:
                        logger.info("재학습 스케줄에 도달하지 않음")
                        return True
            else:
                logger.info("모델 재학습이 필요하지 않습니다.")
                return True
                
        except Exception as e:
            logger.error(f"모델 재학습 검토 오류: {e}")
            return False
    
    def check_model_freshness(self, max_age_hours: int = None) -> bool:
        """모델 신선도 확인 (재학습 필요 여부)"""
        try:
            if max_age_hours is None:
                # ConfigManager에서 모델 최대 사용 시간 가져오기
                max_age_hours = self.config.get('model_max_age_hours', 168)  # 기본 7일
            
            db = self.get_db_manager()
            company_domain = self.config.get('company_domain')
            server_id = self.config.get_server_id()
            device_id = self.config.get('device_id')
            
            if not company_domain or not server_id:
                logger.warning("회사 도메인 또는 서버 ID가 없어 모델 신선도 확인을 건너뜁니다.")
                return False
            
            device_filter = ""
            params = [company_domain, server_id]
            
            if device_id:
                device_filter = " AND device_id = %s"
                params.append(device_id)
            
            # 최근 학습 시간 확인
            query = f"""
            SELECT MAX(trained_at) FROM model_performance 
            WHERE company_domain = %s AND {db.server_id_field} = %s{device_filter}
            """
            
            result = db.fetch_one(query, tuple(params))
            
            if not result or not result[0]:
                logger.info("학습된 모델이 없음, 재학습 필요")
                return True
            
            last_training = result[0]
            age_hours = (datetime.now() - last_training).total_seconds() / 3600
            
            logger.info(f"마지막 학습: {last_training}, 경과 시간: {age_hours:.1f}시간")
            
            if age_hours > max_age_hours:
                logger.info(f"모델이 오래됨 ({age_hours:.1f}h > {max_age_hours}h), 재학습 필요")
                return True
            else:
                logger.info("모델이 충분히 최신임")
                return False
            
        except Exception as e:
            logger.warning(f"모델 신선도 확인 오류: {e}")
            return False
    
    def check_performance_degradation(self, threshold: float = 0.1) -> bool:
        """모델 성능 저하 확인"""
        try:
            db = self.get_db_manager()
            company_domain = self.config.get('company_domain')
            server_id = self.config.get_server_id()
            device_id = self.config.get('device_id')
            
            if not company_domain or not server_id:
                logger.warning("회사 도메인 또는 서버 ID가 없어 성능 저하 확인을 건너뜁니다.")
                return False
            
            device_filter = ""
            params = [company_domain, server_id]
            
            if device_id:
                device_filter = " AND device_id = %s"
                params.append(device_id)
            
            # 최근 24시간 예측 정확도 확인
            query = f"""
            SELECT resource_type, AVG(error) as avg_error, COUNT(*) as count
            FROM predictions
            WHERE company_domain = %s AND {db.server_id_field} = %s
            AND actual_value IS NOT NULL
            AND prediction_time >= DATE_SUB(NOW(), INTERVAL 24 HOUR){device_filter}
            GROUP BY resource_type
            """
            
            results = db.fetch_all(query, tuple(params))
            
            if not results:
                logger.info("최근 예측 데이터가 없어 성능 저하 확인 불가")
                return False
            
            # 성능 저하 확인
            degraded_resources = []
            
            for resource_type, avg_error, count in results:
                if count < 5:  # 최소 데이터 포인트 확인
                    continue
                
                # ConfigManager에서 성능 임계값 가져오기
                error_threshold = self.config.get(f'{resource_type}_error_threshold', 10.0)
                
                if avg_error > error_threshold:
                    degraded_resources.append(resource_type)
                    logger.warning(f"'{resource_type}' 성능 저하 감지: 평균 오차 {avg_error:.2f}")
                else:
                    logger.info(f"'{resource_type}' 성능 양호: 평균 오차 {avg_error:.2f}")
            
            if degraded_resources:
                logger.warning(f"성능 저하된 리소스: {degraded_resources}")
                return True
            else:
                logger.info("모든 리소스 성능 양호")
                return False
            
        except Exception as e:
            logger.warning(f"성능 저하 확인 오류: {e}")
            return False
    
    def get_last_training_time(self) -> Optional[datetime]:
        """마지막 모델 학습 시간 조회"""
        try:
            db = self.get_db_manager()
            company_domain = self.config.get('company_domain')
            server_id = self.config.get_server_id()
            device_id = self.config.get('device_id')
            
            if not company_domain or not server_id:
                return None
            
            device_filter = ""
            params = [company_domain, server_id]
            
            if device_id:
                device_filter = " AND device_id = %s"
                params.append(device_id)
            
            query = f"""
            SELECT MAX(trained_at) FROM model_performance
            WHERE company_domain = %s AND {db.server_id_field} = %s{device_filter}
            """
            
            result = db.fetch_one(query, tuple(params))
            return result[0] if result and result[0] else datetime.now() - timedelta(days=30)
            
        except Exception as e:
            logger.warning(f"마지막 학습 시간 조회 오류: {e}")
            return datetime.now() - timedelta(days=30)
    