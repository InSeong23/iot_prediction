"""
예측 실행 파이프라인 - ConfigManager 연동
- 실시간 예측 수행
- 알림 생성 및 전송
- 예측 정확도 추적 및 업데이트
"""
import os
import sys
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.data_pipeline import BasePipeline
from core.config_manager import ConfigManager
from core.logger import logger

class PredictionPipeline(BasePipeline):
    """예측 실행 파이프라인"""
    
    def execute(self, mode: str = 'predict', **kwargs) -> bool:
        """
        예측 파이프라인 실행
        
        Args:
            mode: 'predict' | 'update-accuracy' | 'validate' | 'alert'
            **kwargs: 추가 파라미터 (hours, threshold 등)
        """
        try:
            logger.info(f"예측 파이프라인 실행 시작: 모드={mode}")
            
            if mode == 'predict':
                return self._run_prediction(**kwargs)
            elif mode == 'update-accuracy':
                return self._update_accuracy(**kwargs)
            elif mode == 'validate':
                return self._validate_predictions(**kwargs)
            elif mode == 'alert':
                return self._process_alerts(**kwargs)
            else:
                logger.error(f"지원하지 않는 모드: {mode}")
                return False
                
        except Exception as e:
            logger.error(f"예측 파이프라인 실행 오류: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
    
    def _run_prediction(self, **kwargs) -> bool:
        """예측 실행"""
        logger.info("시스템 리소스 예측 실행 시작")
        
        try:
            from models.prediction import SystemResourcePredictor
            
            # ConfigManager에서 설정 가져오기
            company_domain = self.config.get('company_domain')
            device_id = self.config.get('device_id')
            server_id = self.config.get_server_id()
            
            if not company_domain:
                logger.error("회사 도메인이 설정되지 않았습니다.")
                return False
            
            # 예측 범위 설정
            hours = kwargs.get('hours', self.config.get('prediction_horizon', 24))
            
            # SystemResourcePredictor 초기화 (ConfigManager 전달)
            predictor = SystemResourcePredictor(
                config_manager=self.config,
                company_domain=company_domain,
                server_id=server_id,
                device_id=device_id
            )
            
            # 모델 로드
            if not predictor.load_models():
                logger.warning("모델 로드 실패, 훈련 시도")
                if not predictor.train_models():
                    logger.error("모델 훈련도 실패")
                    return False
            
            # 예측 실행
            predictions = predictor.predict_future_usage(hours)
            
            if predictions:
                # 예측 결과 저장
                predictor.save_predictions(predictions)
                
                # 알림 처리
                self._process_prediction_alerts(predictions)
                
                logger.info("예측 실행 및 저장 완료")
                return True
            else:
                logger.error("예측 실행 실패")
                return False
                
        except Exception as e:
            logger.error(f"예측 실행 오류: {e}")
            return False
    
    def _update_accuracy(self, **kwargs) -> bool:
        """예측 정확도 업데이트"""
        logger.info("예측 정확도 업데이트 시작")
        
        try:
            from models.prediction import SystemResourcePredictor
            
            # ConfigManager에서 설정 가져오기
            company_domain = self.config.get('company_domain')
            device_id = self.config.get('device_id')
            server_id = self.config.get_server_id()
            
            if not company_domain:
                logger.error("회사 도메인이 설정되지 않았습니다.")
                return False
            
            # SystemResourcePredictor 초기화 (ConfigManager 전달)
            predictor = SystemResourcePredictor(
                config_manager=self.config,
                company_domain=company_domain,
                server_id=server_id,
                device_id=device_id
            )
            
            # 예측 정확도 업데이트
            success = predictor.update_prediction_accuracy()
            
            if success:
                logger.info("예측 정확도 업데이트 완료")
                return True
            else:
                logger.error("예측 정확도 업데이트 실패")
                return False
                
        except Exception as e:
            logger.error(f"예측 정확도 업데이트 오류: {e}")
            return False
    
    def _validate_predictions(self, **kwargs) -> bool:
        """예측 결과 검증"""
        logger.info("예측 결과 검증 시작")
        
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
            
            # 최근 예측 결과 검증
            validation_hours = kwargs.get('hours', 24)
            
            query = f"""
            SELECT 
                resource_type,
                predicted_value,
                actual_value,
                error,
                prediction_time,
                target_time
            FROM predictions
            WHERE company_domain = %s AND {db.server_id_field} = %s
            AND actual_value IS NOT NULL
            AND prediction_time >= DATE_SUB(NOW(), INTERVAL %s HOUR){device_filter}
            ORDER BY prediction_time DESC
            """
            
            params.append(validation_hours)
            results = db.fetch_all(query, tuple(params))
            
            if not results:
                logger.warning("검증할 예측 결과가 없습니다.")
                return True
            
            # 리소스별 정확도 분석
            resource_stats = {}
            
            for row in results:
                resource_type, predicted, actual, error, pred_time, target_time = row
                
                if resource_type not in resource_stats:
                    resource_stats[resource_type] = {
                        'count': 0,
                        'total_error': 0,
                        'errors': [],
                        'predictions': [],
                        'actuals': []
                    }
                
                stats = resource_stats[resource_type]
                stats['count'] += 1
                stats['total_error'] += error
                stats['errors'].append(error)
                stats['predictions'].append(predicted)
                stats['actuals'].append(actual)
            
            # 정확도 통계 계산 및 로깅
            validation_results = {}
            
            for resource_type, stats in resource_stats.items():
                avg_error = stats['total_error'] / stats['count']
                max_error = max(stats['errors'])
                min_error = min(stats['errors'])
                
                # MAPE (Mean Absolute Percentage Error) 계산
                mape = sum(abs((a - p) / a) for a, p in zip(stats['actuals'], stats['predictions']) if a != 0) / len(stats['actuals']) * 100
                
                validation_results[resource_type] = {
                    'count': stats['count'],
                    'avg_error': avg_error,
                    'max_error': max_error,
                    'min_error': min_error,
                    'mape': mape
                }
                
                logger.info(f"'{resource_type}' 예측 검증 결과: "
                          f"평균오차={avg_error:.2f}, MAPE={mape:.2f}%, 검증건수={stats['count']}")
                
                # 성능 임계값 확인
                error_threshold = self.config.get(f'{resource_type}_error_threshold', 10.0)
                mape_threshold = self.config.get(f'{resource_type}_mape_threshold', 15.0)
                
                if avg_error > error_threshold or mape > mape_threshold:
                    logger.warning(f"'{resource_type}' 예측 성능 저하 감지")
            
            logger.info("예측 결과 검증 완료")
            return True
            
        except Exception as e:
            logger.error(f"예측 결과 검증 오류: {e}")
            return False
    
    def _process_alerts(self, **kwargs) -> bool:
        """알림 처리"""
        logger.info("알림 처리 시작")
        
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
            
            # 미처리 알림 조회
            alert_query = f"""
            SELECT 
                id,
                resource_type,
                threshold,
                crossing_time,
                time_to_threshold,
                current_value,
                predicted_value,
                created_at
            FROM alerts
            WHERE company_domain = %s AND {db.server_id_field} = %s
            AND is_notified = FALSE{device_filter}
            ORDER BY created_at DESC
            """
            
            alerts = db.fetch_all(alert_query, tuple(params))
            
            if not alerts:
                logger.info("처리할 알림이 없습니다.")
                return True
            
            logger.info(f"처리할 알림 {len(alerts)}개 발견")
            
            # 알림 처리
            processed_count = 0
            
            for alert in alerts:
                alert_id, resource_type, threshold, crossing_time, time_to_threshold, current_value, predicted_value, created_at = alert
                
                # 알림 메시지 생성
                message = self._create_alert_message(
                    resource_type, threshold, crossing_time, 
                    time_to_threshold, current_value, predicted_value
                )
                
                # 알림 전송 (구현에 따라 다양한 채널 사용 가능)
                if self._send_alert(message, resource_type):
                    # 알림 처리 완료 표시
                    update_query = """
                    UPDATE alerts SET is_notified = TRUE WHERE id = %s
                    """
                    
                    if db.execute_query(update_query, (alert_id,)):
                        processed_count += 1
                        logger.info(f"알림 처리 완료: {resource_type} 임계값 도달")
                    else:
                        logger.error(f"알림 상태 업데이트 실패: ID {alert_id}")
                else:
                    logger.error(f"알림 전송 실패: {resource_type}")
            
            logger.info(f"알림 처리 완료: {processed_count}/{len(alerts)}개")
            return processed_count > 0
            
        except Exception as e:
            logger.error(f"알림 처리 오류: {e}")
            return False
    
    def _process_prediction_alerts(self, predictions: Dict[str, Any]) -> bool:
        """예측 결과에서 알림 추출 및 처리"""
        try:
            if 'alerts' not in predictions or not predictions['alerts']:
                logger.info("생성된 알림이 없습니다.")
                return True
            
            alert_count = 0
            
            for resource_type, alert_info in predictions['alerts'].items():
                # 알림 메시지 생성
                message = self._create_alert_message(
                    resource_type,
                    alert_info['threshold'],
                    alert_info['crossing_time'],
                    alert_info['time_to_threshold'],
                    alert_info['current_value'],
                    alert_info['predicted_value']
                )
                
                # 알림 전송
                if self._send_alert(message, resource_type):
                    alert_count += 1
                    logger.info(f"즉시 알림 전송 완료: {resource_type}")
            
            logger.info(f"예측 기반 즉시 알림 처리: {alert_count}개")
            return True
            
        except Exception as e:
            logger.error(f"예측 알림 처리 오류: {e}")
            return False
    
    def _create_alert_message(self, resource_type: str, threshold: float, 
                            crossing_time: str, time_to_threshold: float,
                            current_value: float, predicted_value: float) -> str:
        """알림 메시지 생성"""
        
        device_info = f" (디바이스: {self.config.get('device_id')})" if self.config.get('device_id') else ""
        
        message = f"""
 시스템 리소스 임계값 도달 예상{device_info}

 리소스: {resource_type.upper()}
 임계값: {threshold}%
 도달 예상 시간: {crossing_time}
 도달까지: {time_to_threshold:.1f}시간
 현재 사용률: {current_value:.1f}%
 예상 사용률: {predicted_value:.1f}%

 즉시 확인 및 조치가 필요합니다.
        """.strip()
        
        return message
    
    def _send_alert(self, message: str, resource_type: str) -> bool:
        """알림 전송 (다양한 채널 지원)"""
        try:
            # 알림 채널 설정 확인
            alert_channels = self.config.get('alert_channels', ['log'])
            
            sent_success = False
            
            # 로그 알림 (기본)
            if 'log' in alert_channels:
                logger.warning(f"[ALERT] {message}")
                sent_success = True
            
            # 이메일 알림 (설정된 경우)
            if 'email' in alert_channels:
                email_success = self._send_email_alert(message, resource_type)
                sent_success = sent_success or email_success
            
            # Slack 알림 (설정된 경우)
            if 'slack' in alert_channels:
                slack_success = self._send_slack_alert(message, resource_type)
                sent_success = sent_success or slack_success
            
            # SMS 알림 (설정된 경우)
            if 'sms' in alert_channels:
                sms_success = self._send_sms_alert(message, resource_type)
                sent_success = sent_success or sms_success
            
            return sent_success
            
        except Exception as e:
            logger.error(f"알림 전송 오류: {e}")
            return False
    
    def _send_email_alert(self, message: str, resource_type: str) -> bool:
        """이메일 알림 전송"""
        try:
            # 이메일 설정 확인
            email_config = self.config.get('email_config', {})
            
            if not email_config.get('enabled', False):
                logger.debug("이메일 알림이 비활성화되어 있습니다.")
                return False
            
            # 실제 이메일 전송 로직은 구현에 따라 다름
            # 여기서는 로그로 대체
            logger.info(f"[EMAIL ALERT] {resource_type} 임계값 도달 이메일 전송됨")
            return True
            
        except Exception as e:
            logger.error(f"이메일 알림 전송 오류: {e}")
            return False
    
    def _send_slack_alert(self, message: str, resource_type: str) -> bool:
        """Slack 알림 전송"""
        try:
            # Slack 설정 확인
            slack_config = self.config.get('slack_config', {})
            
            if not slack_config.get('enabled', False):
                logger.debug("Slack 알림이 비활성화되어 있습니다.")
                return False
            
            # 실제 Slack 전송 로직은 구현에 따라 다름
            # 여기서는 로그로 대체
            logger.info(f"[SLACK ALERT] {resource_type} 임계값 도달 Slack 메시지 전송됨")
            return True
            
        except Exception as e:
            logger.error(f"Slack 알림 전송 오류: {e}")
            return False
    
    def _send_sms_alert(self, message: str, resource_type: str) -> bool:
        """SMS 알림 전송"""
        try:
            # SMS 설정 확인
            sms_config = self.config.get('sms_config', {})
            
            if not sms_config.get('enabled', False):
                logger.debug("SMS 알림이 비활성화되어 있습니다.")
                return False
            
            # 실제 SMS 전송 로직은 구현에 따라 다름
            # 여기서는 로그로 대체
            logger.info(f"[SMS ALERT] {resource_type} 임계값 도달 SMS 전송됨")
            return True
            
        except Exception as e:
            logger.error(f"SMS 알림 전송 오류: {e}")
            return False
    
    def get_prediction_history(self, hours: int = 24) -> Dict[str, Any]:
        """예측 히스토리 조회"""
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
            params = [company_domain, server_id, hours]
            
            if device_id:
                device_filter = " AND device_id = %s"
                params.append(device_id)
            
            # 예측 히스토리 조회
            history_query = f"""
            SELECT 
                prediction_time,
                target_time,
                resource_type,
                predicted_value,
                actual_value,
                error,
                model_version
            FROM predictions
            WHERE company_domain = %s AND {db.server_id_field} = %s
            AND prediction_time >= DATE_SUB(NOW(), INTERVAL %s HOUR){device_filter}
            ORDER BY prediction_time DESC, target_time ASC
            """
            
            results = db.fetch_all(history_query, tuple(params))
            
            # 알림 히스토리 조회
            alert_query = f"""
            SELECT 
                resource_type,
                threshold,
                crossing_time,
                time_to_threshold,
                current_value,
                predicted_value,
                created_at,
                is_notified
            FROM alerts
            WHERE company_domain = %s AND {db.server_id_field} = %s
            AND created_at >= DATE_SUB(NOW(), INTERVAL %s HOUR){device_filter}
            ORDER BY created_at DESC
            """
            
            alert_results = db.fetch_all(alert_query, tuple(params))
            
            return {
                'status': 'success',
                'company_domain': company_domain,
                'device_id': device_id,
                'predictions': [
                    {
                        'prediction_time': row[0],
                        'target_time': row[1],
                        'resource_type': row[2],
                        'predicted_value': row[3],
                        'actual_value': row[4],
                        'error': row[5],
                        'model_version': row[6]
                    } for row in results
                ],
                'alerts': [
                    {
                        'resource_type': row[0],
                        'threshold': row[1],
                        'crossing_time': row[2],
                        'time_to_threshold': row[3],
                        'current_value': row[4],
                        'predicted_value': row[5],
                        'created_at': row[6],
                        'is_notified': row[7]
                    } for row in alert_results
                ]
            }
            
        except Exception as e:
            logger.error(f"예측 히스토리 조회 오류: {e}")
            return {
                'status': 'error',
                'message': str(e)
            }
    
    def get_prediction_summary(self) -> Dict[str, Any]:
        """예측 요약 정보 조회"""
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
            
            # 최근 예측 성능 요약
            performance_query = f"""
            SELECT 
                resource_type,
                COUNT(*) as total_predictions,
                AVG(error) as avg_error,
                MAX(error) as max_error,
                MIN(error) as min_error,
                MAX(prediction_time) as latest_prediction
            FROM predictions
            WHERE company_domain = %s AND {db.server_id_field} = %s
            AND actual_value IS NOT NULL
            AND prediction_time >= DATE_SUB(NOW(), INTERVAL 24 HOUR){device_filter}
            GROUP BY resource_type
            """
            
            perf_results = db.fetch_all(performance_query, tuple(params))
            
            # 활성 알림 수
            active_alerts_query = f"""
            SELECT COUNT(*) FROM alerts
            WHERE company_domain = %s AND {db.server_id_field} = %s
            AND is_notified = FALSE{device_filter}
            """
            
            alert_result = db.fetch_one(active_alerts_query, tuple(params))
            active_alerts = alert_result[0] if alert_result else 0
            
            # 최근 예측 수행 시간
            last_prediction_query = f"""
            SELECT MAX(prediction_time) FROM predictions
            WHERE company_domain = %s AND {db.server_id_field} = %s{device_filter}
            """
            
            last_pred_result = db.fetch_one(last_prediction_query, tuple(params))
            last_prediction = last_pred_result[0] if last_pred_result else None
            
            return {
                'status': 'success',
                'company_domain': company_domain,
                'device_id': device_id,
                'last_prediction_time': last_prediction,
                'active_alerts': active_alerts,
                'performance_summary': [
                    {
                        'resource_type': row[0],
                        'total_predictions': row[1],
                        'avg_error': row[2],
                        'max_error': row[3],
                        'min_error': row[4],
                        'latest_prediction': row[5]
                    } for row in perf_results
                ]
            }
            
        except Exception as e:
            logger.error(f"예측 요약 조회 오류: {e}")
            return {
                'status': 'error',
                'message': str(e)
            }
    
    def schedule_predictions(self, interval_minutes: int = None) -> bool:
        """정기 예측 스케줄링 (실제 스케줄러는 별도 모듈에서 처리)"""
        try:
            if interval_minutes is None:
                interval_minutes = self.config.get('prediction_interval', 60)
            
            logger.info(f"예측 스케줄 설정: {interval_minutes}분 간격")
            
            # 스케줄 정보를 설정에 저장 (실제 스케줄링은 BatchScheduler에서 처리)
            self.config.set('prediction_interval', interval_minutes)
            
            # DB에도 저장 (optional)
            db = self.get_db_manager()
            company_domain = self.config.get('company_domain')
            server_id = self.config.get_server_id()
            
            if company_domain and server_id:
                config_query = f"""
                INSERT INTO configurations
                (company_domain, {db.server_id_field}, config_type, config_key, config_value, is_active)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE config_value = VALUES(config_value), updated_at = NOW()
                """
                
                db.execute_query(config_query, (
                    company_domain, server_id, 'prediction', 'interval_minutes', 
                    str(interval_minutes), True
                ))
            
            return True
            
        except Exception as e:
            logger.error(f"예측 스케줄 설정 오류: {e}")
            return False
    
    def cleanup_old_predictions(self, days: int = 30) -> bool:
        """오래된 예측 데이터 정리"""
        try:
            db = self.get_db_manager()
            company_domain = self.config.get('company_domain')
            server_id = self.config.get_server_id()
            device_id = self.config.get('device_id')
            
            if not company_domain or not server_id:
                logger.warning("회사 도메인 또는 서버 ID가 없어 예측 데이터 정리를 건너뜁니다.")
                return False
            
            cutoff_date = datetime.now() - timedelta(days=days)
            
            device_filter = ""
            params = [cutoff_date, company_domain, server_id]
            
            if device_id:
                device_filter = " AND device_id = %s"
                params.append(device_id)
            
            # 예측 데이터 정리
            pred_query = f"""
            DELETE FROM predictions 
            WHERE prediction_time < %s 
            AND company_domain = %s 
            AND {db.server_id_field} = %s{device_filter}
            """
            
            pred_count_query = f"""
            SELECT COUNT(*) FROM predictions 
            WHERE prediction_time < %s 
            AND company_domain = %s 
            AND {db.server_id_field} = %s{device_filter}
            """
            
            # 삭제 전 개수 확인
            count_result = db.fetch_one(pred_count_query, tuple(params))
            before_count = count_result[0] if count_result else 0
            
            if before_count > 0:
                if db.execute_query(pred_query, tuple(params)):
                    logger.info(f"오래된 예측 데이터 {before_count}개 삭제 완료")
                else:
                    logger.error("예측 데이터 삭제 실패")
                    return False
            
            # 알림 데이터 정리
            alert_query = f"""
            DELETE FROM alerts 
            WHERE created_at < %s 
            AND company_domain = %s 
            AND {db.server_id_field} = %s{device_filter}
            """
            
            alert_count_query = f"""
            SELECT COUNT(*) FROM alerts 
            WHERE created_at < %s 
            AND company_domain = %s 
            AND {db.server_id_field} = %s{device_filter}
            """
            
            alert_count_result = db.fetch_one(alert_count_query, tuple(params))
            alert_before_count = alert_count_result[0] if alert_count_result else 0
            
            if alert_before_count > 0:
                if db.execute_query(alert_query, tuple(params)):
                    logger.info(f"오래된 알림 데이터 {alert_before_count}개 삭제 완료")
                else:
                    logger.error("알림 데이터 삭제 실패")
            
            logger.info(f"예측 데이터 정리 완료: {days}일 이전 데이터 삭제")
            return True
            
        except Exception as e:
            logger.error(f"예측 데이터 정리 오류: {e}")
            return False