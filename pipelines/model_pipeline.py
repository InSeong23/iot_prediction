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
    
    def get_model_status(self) -> Dict[str, Any]:
        """모델 상태 정보 반환"""
        try:
            db = self.get_db_manager()
            company_domain = self.config.get('company_domain')
            server_id = self.config.get_server_id()
            device_id = self.config.get('device_id')
            
            if not company_domain or not server_id:
                return {
                    'status': 'error',
                    'message': '회사 도메인 또는 서버 ID가 설정되지 않음'
                }
            
            device_filter = ""
            params = [company_domain, server_id]
            
            if device_id:
                device_filter = " AND device_id = %s"
                params.append(device_id)
            
            # 애플리케이션 모델 현황
            app_query = f"""
            SELECT 
                application, 
                resource_type,
                mae, 
                rmse, 
                r2_score,
                trained_at,
                version
            FROM model_performance
            WHERE company_domain = %s AND {db.server_id_field} = %s
            AND application != 'system'{device_filter}
            ORDER BY trained_at DESC
            """
            
            app_results = db.fetch_all(app_query, tuple(params))
            
            # 시스템 모델 현황
            sys_query = f"""
            SELECT 
                resource_type,
                mae, 
                rmse, 
                r2_score,
                trained_at,
                version
            FROM model_performance
            WHERE company_domain = %s AND {db.server_id_field} = %s
            AND application = 'system'{device_filter}
            ORDER BY trained_at DESC
            """
            
            sys_results = db.fetch_all(sys_query, tuple(params))
            
            # 최근 예측 성능
            pred_query = f"""
            SELECT 
                resource_type,
                AVG(error) as avg_error,
                COUNT(*) as prediction_count,
                MAX(prediction_time) as latest_prediction
            FROM predictions
            WHERE company_domain = %s AND {db.server_id_field} = %s
            AND actual_value IS NOT NULL
            AND prediction_time >= DATE_SUB(NOW(), INTERVAL 24 HOUR){device_filter}
            GROUP BY resource_type
            """
            
            pred_results = db.fetch_all(pred_query, tuple(params))
            
            return {
                'status': 'success',
                'company_domain': company_domain,
                'device_id': device_id,
                'application_models': [
                    {
                        'application': row[0],
                        'resource_type': row[1],
                        'mae': row[2],
                        'rmse': row[3],
                        'r2_score': row[4],
                        'trained_at': row[5],
                        'version': row[6]
                    } for row in app_results
                ],
                'system_models': [
                    {
                        'resource_type': row[0],
                        'mae': row[1],
                        'rmse': row[2],
                        'r2_score': row[3],
                        'trained_at': row[4],
                        'version': row[5]
                    } for row in sys_results
                ],
                'recent_predictions': [
                    {
                        'resource_type': row[0],
                        'avg_error': row[1],
                        'prediction_count': row[2],
                        'latest_prediction': row[3]
                    } for row in pred_results
                ]
            }
            
        except Exception as e:
            logger.error(f"모델 상태 조회 오류: {e}")
            return {
                'status': 'error',
                'message': str(e)
            }
    
    def backup_models(self, backup_dir: str = None) -> bool:
        """모델 백업"""
        try:
            if backup_dir is None:
                backup_dir = os.path.join("models", "backup", datetime.now().strftime("%Y%m%d_%H%M%S"))
            
            os.makedirs(backup_dir, exist_ok=True)
            
            company_domain = self.config.get('company_domain')
            server_id = self.config.get_server_id()
            device_id = self.config.get('device_id')
            
            if not company_domain or not server_id:
                logger.error("회사 도메인 또는 서버 ID가 없어 모델 백업을 건너뜁니다.")
                return False
            
            # 모델 디렉토리 경로
            model_base_dir = os.path.join("models", "trained", company_domain, str(server_id))
            
            if not os.path.exists(model_base_dir):
                logger.warning(f"모델 디렉토리가 존재하지 않습니다: {model_base_dir}")
                return False
            
            # 모델 파일 복사
            import shutil
            
            copied_files = 0
            for root, dirs, files in os.walk(model_base_dir):
                for file in files:
                    if file.endswith('.pkl'):
                        src_path = os.path.join(root, file)
                        
                        # 상대 경로 계산
                        rel_path = os.path.relpath(src_path, model_base_dir)
                        dst_path = os.path.join(backup_dir, rel_path)
                        
                        # 디렉토리 생성
                        os.makedirs(os.path.dirname(dst_path), exist_ok=True)
                        
                        # 파일 복사
                        shutil.copy2(src_path, dst_path)
                        copied_files += 1
            
            logger.info(f"모델 백업 완료: {copied_files}개 파일, 위치: {backup_dir}")
            return True
            
        except Exception as e:
            logger.error(f"모델 백업 오류: {e}")
            return False
    
    def validate_training_data(self) -> bool:
        """학습 데이터 유효성 검사"""
        try:
            db = self.get_db_manager()
            company_domain = self.config.get('company_domain')
            server_id = self.config.get_server_id()
            device_id = self.config.get('device_id')
            
            if not company_domain or not server_id:
                logger.error("회사 도메인 또는 서버 ID가 설정되지 않았습니다.")
                return False
            
            device_filter = ""
            params = [company_domain, server_id]
            
            if device_id:
                device_filter = " AND device_id = %s"
                params.append(device_id)
            
            # 최소 데이터 요구사항 확인
            min_records = self.config.get('min_training_records', 100)
            
            # JVM 메트릭 데이터 확인
            jvm_query = f"""
            SELECT COUNT(*) FROM jvm_metrics
            WHERE company_domain = %s AND {db.server_id_field} = %s{device_filter}
            AND time >= DATE_SUB(NOW(), INTERVAL 3 DAY)
            """
            
            jvm_result = db.fetch_one(jvm_query, tuple(params))
            jvm_count = jvm_result[0] if jvm_result else 0
            
            # 시스템 리소스 데이터 확인
            sys_query = f"""
            SELECT COUNT(*) FROM system_resources
            WHERE company_domain = %s AND {db.server_id_field} = %s{device_filter}
            AND time >= DATE_SUB(NOW(), INTERVAL 3 DAY)
            """
            
            sys_result = db.fetch_one(sys_query, tuple(params))
            sys_count = sys_result[0] if sys_result else 0
            
            # 영향도 데이터 확인
            impact_query = f"""
            SELECT COUNT(*) FROM application_impact
            WHERE company_domain = %s AND {db.server_id_field} = %s{device_filter}
            AND time >= DATE_SUB(NOW(), INTERVAL 3 DAY)
            """
            
            impact_result = db.fetch_one(impact_query, tuple(params))
            impact_count = impact_result[0] if impact_result else 0
            
            logger.info(f"학습 데이터 현황: JVM={jvm_count}, 시스템={sys_count}, 영향도={impact_count}")
            
            # 유효성 검사
            if jvm_count < min_records:
                logger.error(f"JVM 메트릭 데이터 부족: {jvm_count} < {min_records}")
                return False
            
            if sys_count < min_records:
                logger.error(f"시스템 리소스 데이터 부족: {sys_count} < {min_records}")
                return False
            
            if impact_count < min_records:
                logger.warning(f"영향도 데이터 부족: {impact_count} < {min_records}, 전처리 필요할 수 있음")
            
            logger.info("학습 데이터 유효성 검사 통과")
            return True
            
        except Exception as e:
            logger.error(f"학습 데이터 유효성 검사 오류: {e}")
            return False
    
    def cleanup_old_models(self, keep_versions: int = 3) -> bool:
        """오래된 모델 버전 정리"""
        try:
            db = self.get_db_manager()
            company_domain = self.config.get('company_domain')
            server_id = self.config.get_server_id()
            device_id = self.config.get('device_id')
            
            if not company_domain or not server_id:
                logger.warning("회사 도메인 또는 서버 ID가 없어 모델 정리를 건너뜁니다.")
                return False
            
            # 모델 성능 테이블에서 오래된 기록 삭제
            device_filter = ""
            params = [company_domain, server_id, keep_versions]
            
            if device_id:
                device_filter = " AND device_id = %s"
                params.append(device_id)
            
            # 각 애플리케이션, 리소스별로 최신 N개 버전만 유지
            cleanup_query = f"""
            DELETE mp1 FROM model_performance mp1
            WHERE mp1.company_domain = %s AND mp1.{db.server_id_field} = %s{device_filter}
            AND (
                SELECT COUNT(*) FROM model_performance mp2
                WHERE mp2.company_domain = mp1.company_domain
                AND mp2.{db.server_id_field} = mp1.{db.server_id_field}
                AND mp2.application = mp1.application
                AND mp2.resource_type = mp1.resource_type
                AND mp2.trained_at > mp1.trained_at
            ) >= %s
            """
            
            result = db.execute_query(cleanup_query, tuple(params))
            
            if result:
                logger.info(f"오래된 모델 성능 기록 정리 완료 (최신 {keep_versions}개 버전 유지)")
                return True
            else:
                logger.warning("모델 성능 기록 정리 실패")
                return False
            
        except Exception as e:
            logger.error(f"모델 정리 오류: {e}")
            return False