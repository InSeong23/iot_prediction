
"""
JVM 메트릭 기반 시스템 리소스 예측 - 통합 예측 모델  
- 분 단위 정각으로 예측 시간 생성  
- actual_value 정확한 시간 매칭
- 기존 코드 호환성 유지
"""
import os
import pickle
import numpy as np
import pandas as pd
import json
from datetime import datetime, timedelta
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import matplotlib.pyplot as plt
import seaborn as sns

from core.config_manager import ConfigManager
from core.logger import logger
from core.db import DatabaseManager
from models.app_models import AppModelManager

class SystemResourcePredictor:
    """시스템 자원 예측기 클래스 - 시간 동기화 개선"""
    
    def __init__(self, config_manager=None, company_domain=None, server_id=None, device_id=None):
        """초기화 함수 - ConfigManager 우선 사용"""
        # ConfigManager 초기화
        if config_manager:
            self.config = config_manager
        else:
            self.config = ConfigManager()
            # 전달받은 파라미터로 설정 업데이트
            if company_domain:
                self.config.set('company_domain', company_domain)
            if device_id:
                self.config.set('device_id', device_id)
        
        self.db_manager = DatabaseManager()
        
        # 설정값들 가져오기
        self.company_domain = self.config.get('company_domain')
        self.device_id = self.config.get('device_id')
        
        # 서버 ID 설정
        if isinstance(server_id, int):
            self.server_id = server_id
        else:
            self.server_id = self.config.get_server_id()
        
        if not self.server_id:
            logger.warning(f"서버 ID를 찾을 수 없습니다. 디바이스: {self.device_id}")
        
        # 모델 설정 가져오기
        model_config = self.config.get_model_config()
        self.model_type = model_config.get('prediction_model_type', 'gradient_boosting')
        self.horizon = model_config.get('prediction_horizon', 24)  # 예측 범위(시간)
        self.training_window = model_config.get('training_window', '3d')
        
        # 하이퍼파라미터 가져오기
        from config.settings import get_hyperparameters
        self.hyperparameters = get_hyperparameters(self.model_type)
        
        # 애플리케이션 모델 관리자
        self.app_model_manager = AppModelManager(self.config, self.company_domain, self.server_id, self.device_id)
        
        # 모델 저장 경로
        self.model_dir = os.path.join("models", "trained", self.company_domain, str(self.server_id), "system")
        os.makedirs(self.model_dir, exist_ok=True)
        
        # 자원 유형별 모델
        self.models = {
            "cpu": None,
            "mem": None,
            "disk": None
        }
        
        # 특성 스케일러
        self.scalers = {
            "cpu": None,
            "mem": None,
            "disk": None
        }
        
        logger.info(f"시스템 자원 예측기 초기화: 회사={self.company_domain}, 서버ID={self.server_id}, 디바이스ID={self.device_id}")
        logger.info(f"모델 타입: {self.model_type}, 예측 범위: {self.horizon}시간, 훈련 기간: {self.training_window}")
    
    def __del__(self):
        """소멸자: 연결 종료"""
        if hasattr(self, 'db_manager') and self.db_manager:
            self.db_manager.close()
    
    def align_prediction_time(self, time_val, interval_minutes=None):
        """예측 시간을 지정된 간격으로 정렬"""
        if isinstance(time_val, str):
            time_val = datetime.fromisoformat(time_val.replace('Z', '+00:00'))
        
        if time_val.tzinfo is not None:
            time_val = time_val.replace(tzinfo=None)
        
        time_val = time_val.replace(second=0, microsecond=0)
        
        if interval_minutes and interval_minutes > 1:
            aligned_minute = (time_val.minute // interval_minutes) * interval_minutes
            time_val = time_val.replace(minute=aligned_minute)
        
        return time_val

    # predict_future_usage 메서드 수정 부분
    def predict_future_usage(self, hours=None):
        """미래 자원 사용량 예측 - 설정 가능한 간격으로 예측"""
        if hours is None:
            hours = self.horizon
        
        # 모델 로드 확인
        if not any(model is not None for model in self.models.values()):
            if not self.load_models():
                logger.error("예측할 모델이 없습니다.")
                return None
        
        # 애플리케이션 모델 로드
        if not self.app_model_manager.load_all_models():
            logger.error("애플리케이션 모델을 로드할 수 없습니다.")
            return None
        
        # 1단계: 최근 JVM 메트릭 조회
        app_features = self.get_latest_jvm_metrics()
        
        if not app_features:
            logger.error("최근 JVM 메트릭 데이터가 없습니다.")
            return None
        
        # 2단계: JVM 메트릭 → 영향도 예측
        app_impacts = self.app_model_manager.predict_impacts(app_features)
        
        if not app_impacts:
            logger.error("영향도 예측 결과가 없습니다.")
            return None
        
        logger.info("영향도 예측 완료, 시스템 리소스 예측 시작")
        
        # 디바이스 ID 정보 추가
        if self.device_id:
            logger.info(f"디바이스 '{self.device_id}'에 대한 예측 수행")
        
        # 예측 간격 설정 (분 단위) - ConfigManager에서 가져오기
        prediction_interval_minutes = self.config.get_prediction_interval(hours)
        logger.info(f"예측 간격: {prediction_interval_minutes}분 (예측 범위: {hours}시간)")
        
        # 예측 결과
        predictions = {}
        prediction_times = []
        
        # 현재 시간을 설정된 간격으로 정렬
        now = datetime.now()
        aligned_now = self.align_prediction_time(now, prediction_interval_minutes)
        next_prediction = aligned_now + timedelta(minutes=prediction_interval_minutes)
        
        # 설정된 간격으로 예측
        total_predictions = hours * 60 // prediction_interval_minutes  # 총 예측 포인트 수
        
        for i in range(total_predictions):
            pred_time = next_prediction + timedelta(minutes=i * prediction_interval_minutes)
            prediction_times.append(pred_time)
            
            # 3단계: 영향도 → 영향도 통계 계산
            impact_stats = self._calculate_impact_statistics(app_impacts, pred_time)
            
            # 4단계: 시스템 모델로 최종 예측 (영향도 통계 → 시스템 리소스)
            input_features = pd.DataFrame([impact_stats])
            
            # 각 자원 유형별로 예측
            for resource_type in ["cpu", "mem", "disk"]:
                if self.models[resource_type] is None or self.scalers[resource_type] is None:
                    continue
                
                try:
                    # 특성 스케일링
                    input_scaled = self.scalers[resource_type].transform(input_features)
                    
                    # 예측 수행
                    pred = self.models[resource_type].predict(input_scaled)[0]
                    
                    # 예측 결과 저장
                    if resource_type not in predictions:
                        predictions[resource_type] = []
                    
                    predictions[resource_type].append(pred)
                    
                except Exception as e:
                    logger.error(f"'{resource_type}' 자원 예측 중 오류: {e}")
                    if resource_type not in predictions:
                        predictions[resource_type] = []
                    predictions[resource_type].append(50.0)  # 기본값
        
        # 임계값 도달 시간 계산
        alerts = self.check_threshold_crossings(prediction_times, predictions)
        
        # 결과 구조화 (시간을 문자열로 변환)
        time_format = '%Y-%m-%d %H:%M:00' if prediction_interval_minutes < 60 else '%Y-%m-%d %H:00:00'
        
        result = {
            'times': [t.strftime(time_format) for t in prediction_times],
            'predictions': predictions,
            'alerts': alerts,
            'device_id': self.device_id,
            'prediction_interval_minutes': prediction_interval_minutes
        }
        
        logger.info(f"예측 완료: {len(prediction_times)}개 포인트, {prediction_interval_minutes}분 간격")
        
        return result

    # update_prediction_accuracy 메서드 수정 부분
    def update_prediction_accuracy(self):
        """예측 정확도 업데이트 - 분 단위 정각 매칭"""
        # 디바이스 필터 설정
        device_filter = ""
        params = [self.company_domain, self.server_id]
        
        if self.device_id:
            device_filter = " AND device_id = %s"
            params.append(self.device_id)
        
        # 과거 예측 조회 
        query = f"""
        SELECT id, resource_type, target_time, predicted_value, device_id
        FROM predictions
        WHERE company_domain = %s AND {self.db_manager.server_id_field} = %s
        AND actual_value IS NULL
        AND target_time <= NOW()
        AND SECOND(target_time) = 0
        AND prediction_time >= DATE_SUB(NOW(), INTERVAL 24 HOUR){device_filter}
        """
        
        predictions = self.db_manager.fetch_all(query, tuple(params))
        
        if not predictions:
            logger.info("업데이트할 예측 결과가 없습니다.")
            return True
        
        logger.info(f"예측 정확도 업데이트: {len(predictions)}개 항목")
        
        # 각 예측에 대한 실제 값 조회 및 업데이트
        updated_count = 0
        for pred in predictions:
            pred_id, resource_type, target_time, predicted_value, device_id = pred
            
            # 정확히 같은 분의 실제 데이터 조회 (정확한 시간 매칭)
            device_clause = ""
            sys_params = [self.company_domain, self.server_id, resource_type, target_time]
            
            if device_id:
                device_clause = " AND device_id = %s"
                sys_params.append(device_id)
            
            # CPU: usage_user, 메모리/디스크: used_percent
            measurement = "usage_user" if resource_type == "cpu" else "used_percent"
            sys_params.insert(3, measurement)
            
            # 실제 자원 사용량 조회 
            resource_query = f"""
            SELECT value
            FROM system_resources
            WHERE company_domain = %s AND {self.db_manager.server_id_field} = %s
            AND resource_type = %s
            AND measurement = %s
            AND time = %s{device_clause}
            LIMIT 1
            """
            
            actual_data = self.db_manager.fetch_one(resource_query, tuple(sys_params))
            
            if actual_data:
                actual_value = actual_data[0]
                error = abs(predicted_value - actual_value)
                
                # 예측 업데이트
                update_query = """
                UPDATE predictions
                SET actual_value = %s, error = %s
                WHERE id = %s
                """
                
                if self.db_manager.execute_query(update_query, (actual_value, error, pred_id)):
                    updated_count += 1
                    logger.debug(f"예측 ID {pred_id} 업데이트: 예측={predicted_value:.2f}, 실제={actual_value:.2f}, 오차={error:.2f}")
                else:
                    logger.warning(f"예측 ID {pred_id} 업데이트 실패")
            else:
                logger.debug(f"예측 ID {pred_id}의 실제 데이터를 찾을 수 없습니다 (시간: {target_time})")
        
        logger.info(f"예측 정확도 업데이트 완료: {updated_count}/{len(predictions)}개 성공")
        
        # 모델 성능 업데이트
        if updated_count > 0:
            self.update_model_performance()
        
        return True
    
    def save_predictions(self, predictions):
        """예측 결과 저장 - 간격 설정 지원"""
        try:
            # 예측 간격 확인
            interval_minutes = predictions.get('prediction_interval_minutes', 5)
            
            # 시간 파싱 (간격에 따른 포맷 결정)
            time_format = '%Y-%m-%d %H:%M:00' if interval_minutes < 60 else '%Y-%m-%d %H:00:00'
            times = [datetime.strptime(t, time_format) for t in predictions['times']]
            
            prediction_time = datetime.now()  # 현재 시간을 예측 시점으로 사용
            
            batch_id = datetime.now().strftime("%Y%m%d%H%M%S")
            device_id = predictions.get('device_id', '')
            
            logger.info(f"예측 결과 저장: {len(times)}개 포인트, {interval_minutes}분 간격")
            
            # 자원별 예측 결과
            for resource_type, values in predictions['predictions'].items():
                # 배치 데이터
                batch_data = []
                
                for i, (target_time, value) in enumerate(zip(times, values)):
                    batch_data.append((
                        self.company_domain,
                        self.server_id,
                        prediction_time,
                        target_time,
                        resource_type,
                        float(value),  # 명시적 형변환
                        None,  # actual_value (나중에 업데이트)
                        None,  # error (나중에 계산)
                        self.model_type,
                        batch_id,
                        device_id
                    ))
                
                # DB에 저장
                query = f"""
                INSERT INTO predictions
                (company_domain, {self.db_manager.server_id_field}, prediction_time, target_time, resource_type, 
                predicted_value, actual_value, error, model_version, batch_id, device_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                result = self.db_manager.execute_query(query, batch_data, many=True)
                
                if result:
                    logger.info(f"'{resource_type}' 예측 결과 {len(batch_data)}개 저장 완료 ({interval_minutes}분 간격)")
                else:
                    logger.error(f"'{resource_type}' 예측 결과 저장 실패")
            
            # 알림 정보 저장
            if predictions.get('alerts'):
                for resource_type, alert in predictions['alerts'].items():
                    query = f"""
                    INSERT INTO alerts
                    (company_domain, {self.db_manager.server_id_field}, resource_type, threshold, crossing_time, 
                    time_to_threshold, current_value, predicted_value, created_at, batch_id, device_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    
                    crossing_time = datetime.strptime(alert['crossing_time'], time_format)
                    device_id = alert.get('device_id', '')
                    
                    params = (
                        self.company_domain,
                        self.server_id,
                        resource_type,
                        float(alert['threshold']),
                        crossing_time,
                        float(alert['time_to_threshold']),
                        float(alert['current_value']),
                        float(alert['predicted_value']),
                        datetime.now(),
                        batch_id,
                        device_id
                    )
                    
                    result = self.db_manager.execute_query(query, params)
                    
                    if result:
                        logger.info(f"'{resource_type}' 알림 정보 저장 완료")
                    else:
                        logger.error(f"'{resource_type}' 알림 정보 저장 실패")
            
            return True
            
        except Exception as e:
            logger.error(f"예측 결과 저장 중 오류: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
    
    def load_training_data(self, start_time=None, end_time=None):
        """학습 데이터 로드 - 스트리밍 아키텍처 우선"""
        if end_time is None:
            end_time = datetime.now()
        
        if start_time is None:
            # 훈련 기간 설정
            value = int(self.training_window[:-1])
            unit = self.training_window[-1].lower()
            
            if unit == 'd':
                start_time = end_time - timedelta(days=value)
            elif unit == 'w':
                start_time = end_time - timedelta(weeks=value)
            else:
                logger.error(f"지원하지 않는 기간 단위: {unit}")
                return None, None
        
        logger.info(f"학습 데이터 로드: {start_time} ~ {end_time}")
        
        # 스트리밍 아키텍처 확인
        use_streaming = os.getenv('ARCHITECTURE', 'streaming').lower() == 'streaming'
        
        if use_streaming:
            # 스트리밍 방식
            return self._load_streaming_training_data(start_time, end_time)
        else:
            # 기존 MySQL 방식
            return self._load_mysql_training_data(start_time, end_time)
    
    def _load_streaming_training_data(self, start_time, end_time):
        """스트리밍 아키텍처용 학습 데이터 로드"""
        logger.info("스트리밍 아키텍처에서 시스템 학습 데이터 로드")
        
        try:
            from data.streaming_collector import StreamingDataCollector
            from data.streaming_preprocessor import StreamingPreprocessor
            
            # 스트리밍 수집기 초기화
            collector = StreamingDataCollector(
                self.config, self.company_domain, self.device_id
            )
            
            # JVM 및 시스템 데이터 수집
            jvm_df, sys_df = collector.get_training_data(start_time, end_time)
            
            if jvm_df.empty or sys_df.empty:
                logger.warning("스트리밍 데이터가 부족합니다.")
                return self._create_dummy_training_data(start_time, end_time)
            
            # 전처리기 초기화
            preprocessor = StreamingPreprocessor(
                self.config, self.company_domain, self.device_id
            )
            
            # 캐시 키 생성
            cache_key = f"{start_time.strftime('%Y%m%d')}_{end_time.strftime('%Y%m%d')}"
            
            # 영향도 계산 (캐시 확인)
            impact_df = preprocessor.load_cached_impacts(cache_key)
            
            if impact_df is None:
                # 영향도 계산 필요
                impact_df = preprocessor.calculate_and_cache_impacts(
                    jvm_df, sys_df, cache_key
                )
            
            if impact_df is None or impact_df.empty:
                logger.warning("영향도 계산 실패, 더미 데이터 사용")
                return self._create_dummy_training_data(start_time, end_time)
            
            # 영향도 통계 계산
            impact_stats_list = []
            
            for time_point, time_group in impact_df.groupby('time'):
                stats_row = {'time': time_point}
                
                for resource_type in ['cpu', 'mem', 'disk']:
                    # 해당 시간, 해당 리소스의 모든 애플리케이션 영향도
                    resource_impacts = time_group[time_group['resource_type'] == resource_type]['impact_score']
                    
                    if len(resource_impacts) > 0:
                        stats_row[f'{resource_type}_impact_sum'] = resource_impacts.sum()
                        stats_row[f'{resource_type}_impact_mean'] = resource_impacts.mean()
                        stats_row[f'{resource_type}_impact_max'] = resource_impacts.max()
                        stats_row[f'{resource_type}_impact_std'] = resource_impacts.std() if len(resource_impacts) > 1 else 0
                    else:
                        stats_row[f'{resource_type}_impact_sum'] = 0
                        stats_row[f'{resource_type}_impact_mean'] = 0
                        stats_row[f'{resource_type}_impact_max'] = 0
                        stats_row[f'{resource_type}_impact_std'] = 0
                
                impact_stats_list.append(stats_row)
            
            # 영향도 통계 데이터프레임 생성
            impact_stats_df = pd.DataFrame(impact_stats_list)
            impact_stats_df['time'] = pd.to_datetime(impact_stats_df['time'])
            impact_stats_df = impact_stats_df.set_index('time')
            
            # 시스템 리소스 피봇
            cpu_data = sys_df[(sys_df['resource_type'] == 'cpu') & (sys_df['measurement'] == 'usage_user')]
            mem_data = sys_df[(sys_df['resource_type'] == 'mem') & (sys_df['measurement'] == 'used_percent')]
            disk_data = sys_df[(sys_df['resource_type'] == 'disk') & (sys_df['measurement'] == 'used_percent')]
            
            sys_combined = pd.concat([cpu_data, mem_data, disk_data])
            sys_pivot = sys_combined.pivot_table(
                index='time', 
                columns='resource_type', 
                values='value',
                aggfunc='mean'
            )
            
            # 공통 시간 인덱스
            common_times = impact_stats_df.index.intersection(sys_pivot.index)
            
            if len(common_times) < 10:
                logger.warning("공통 시간대가 부족합니다")
                return self._create_dummy_training_data(start_time, end_time)
            
            X = impact_stats_df.loc[common_times].copy()
            y = sys_pivot.loc[common_times].copy()
            
            # 시간 특성 추가
            X['hour'] = X.index.hour
            X['day_of_week'] = X.index.dayofweek
            X['is_weekend'] = X['day_of_week'].apply(lambda x: 1 if x >= 5 else 0)
            
            # 결측치 처리
            X = X.fillna(0)
            y = y.fillna(0)
            
            logger.info(f"스트리밍 학습 데이터: X={X.shape}, y={y.shape}")
            
            return X, y
            
        except Exception as e:
            logger.error(f"스트리밍 학습 데이터 로드 오류: {e}")
            return self._create_dummy_training_data(start_time, end_time)
    
    def _load_mysql_training_data(self, start_time, end_time):
        """기존 MySQL 기반 학습 데이터 로드"""
        # 디바이스 ID 필터 설정
        device_filter = ""
        if self.device_id:
            device_filter = f" AND device_id = '{self.device_id}'"
            logger.info(f"디바이스 ID 필터 적용: {self.device_id}")
        
        # 1. 영향도 데이터 로드 (입력 특성)
        impact_query = f"""
        SELECT time, application, resource_type, impact_score
        FROM application_impact
        WHERE company_domain = %s AND {self.db_manager.server_id_field} = %s
        AND time BETWEEN %s AND %s{device_filter}
        ORDER BY time
        """
        
        impact_data = self.db_manager.fetch_all(impact_query, (
            self.company_domain, self.server_id, start_time, end_time
        ))
    
    def train_models(self, X=None, y=None, visualization=None):
        """자원별 예측 모델 학습"""
        if visualization is None:
            visualization = self.config.get('visualization_enabled', False)
                
        if X is None or y is None:
            X, y = self.load_training_data()
            
        if X is None or y is None:
            logger.warning("학습 데이터 로드 실패, 더미 데이터로 대체합니다")
            X, y = self._create_dummy_training_data()
            
            if X is None or y is None:
                logger.error("더미 데이터 생성 실패")
                return False
        
        # 결과 저장용 메트릭
        metrics = {}
        
        # 시간 순서로 데이터 정렬
        X = X.sort_index()
        y = y.sort_index()
        
        # 각 자원 유형별로 모델 학습
        for resource_type in ["cpu", "mem", "disk"]:
            if resource_type not in y.columns:
                logger.warning(f"'{resource_type}' 자원 사용량 데이터가 없어 더미 데이터로 대체합니다")
                # 대상 컬럼이 없으면 더미 데이터 생성
                y[resource_type] = np.random.rand(len(y)) * 50 + 25  # 25~75 범위
            
            logger.info(f"'{resource_type}' 자원 예측 모델 학습 시작")
            
            # 대상 변수
            y_resource = y[resource_type]
            
            # 특성 스케일링
            scaler = StandardScaler()
            X_scaled = scaler.fit_transform(X)
            
            # 시계열 분할 (80% 훈련, 20% 테스트)
            validation_split = self.config.get('validation_split', 0.8)
            split_idx = int(len(X) * validation_split)
            X_train, X_test = X_scaled[:split_idx], X_scaled[split_idx:]
            y_train, y_test = y_resource.iloc[:split_idx], y_resource.iloc[split_idx:]
            
            # 모델 초기화
            if self.model_type == "gradient_boosting":
                model = GradientBoostingRegressor(
                    n_estimators=self.hyperparameters.get("n_estimators", 100),
                    learning_rate=self.hyperparameters.get("learning_rate", 0.1),
                    random_state=42
                )
            else:
                logger.error(f"지원하지 않는 모델 유형: {self.model_type}")
                continue
            
            # 모델 학습
            model.fit(X_train, y_train)
            
            # 테스트 데이터 예측
            if len(X_test) > 0 and len(y_test) > 0:
                y_pred = model.predict(X_test)
                
                # 성능 평가
                mae = mean_absolute_error(y_test, y_pred)
                rmse = np.sqrt(mean_squared_error(y_test, y_pred))
                r2 = r2_score(y_test, y_pred)
            else:
                # 테스트 데이터가 없는 경우 더미 성능 지표 생성
                logger.warning(f"'{resource_type}' 테스트 데이터가 없어 성능 평가를 건너뜁니다")
                mae = 0.1
                rmse = 0.2
                r2 = 0.8
            
            logger.info(f"'{resource_type}' 모델 테스트 성능: MAE={mae:.4f}, RMSE={rmse:.4f}, R²={r2:.4f}")
            
            # 특성 중요도 계산
            feature_importance = model.feature_importances_
            feature_names = X.columns
            
            # 중요도 순으로 정렬
            importance_indices = np.argsort(feature_importance)[::-1]
            top_features = [(feature_names[i], feature_importance[i]) for i in importance_indices[:10]]
            
            logger.info(f"'{resource_type}' 주요 특성(상위 10개): {top_features}")
            
            # 시각화 (설정에 따라)
            if visualization:
                try:
                    self._create_visualizations(resource_type, y_test, y_pred, feature_names, feature_importance)
                except Exception as e:
                    logger.error(f"시각화 생성 중 오류 발생: {e}")
            
            # 모델 및 스케일러 저장
            self.models[resource_type] = model
            self.scalers[resource_type] = scaler
            
            # 메트릭 저장
            metrics[resource_type] = {
                'mae': mae,
                'rmse': rmse,
                'r2': r2,
                'feature_importance': dict(zip(feature_names, feature_importance.tolist())),
                'device_id': self.device_id
            }
            
            # 모델 저장 (디바이스 ID 포함)
            model_filename = f"{resource_type}_model.pkl"
            scaler_filename = f"{resource_type}_scaler.pkl"
            
            if self.device_id:
                model_filename = f"{resource_type}_{self.device_id}_model.pkl"
                scaler_filename = f"{resource_type}_{self.device_id}_scaler.pkl"
            
            model_path = os.path.join(self.model_dir, model_filename)
            scaler_path = os.path.join(self.model_dir, scaler_filename)
            
            # 디렉토리 생성
            os.makedirs(os.path.dirname(model_path), exist_ok=True)
            
            with open(model_path, 'wb') as f:
                pickle.dump(model, f)
            
            with open(scaler_path, 'wb') as f:
                pickle.dump(scaler, f)
            
            logger.info(f"'{resource_type}' 모델 저장 완료: {model_path}")
        
        # 성능 메트릭 DB에 저장
        self.save_model_metrics(metrics)
        
        return True
    
    def _create_visualizations(self, resource_type, y_test, y_pred, feature_names, feature_importance):
        """시각화 생성"""
        # 결과 저장 디렉토리
        viz_dir = os.path.join(self.model_dir, "visualizations")
        os.makedirs(viz_dir, exist_ok=True)
        
        # 디바이스 ID 포함 파일명
        device_suffix = f"_{self.device_id}" if self.device_id else ""
        
        # 1. 실제 vs 예측 시계열 그래프
        plt.figure(figsize=(12, 6))
        
        # 테스트 기간 인덱스 (시간 기반)
        test_times = range(len(y_test))
        
        plt.plot(test_times, y_test, label='실제값', color='blue')
        plt.plot(test_times, y_pred, label='예측값', color='red')
        
        plt.title(f'{resource_type} 자원 사용량 - 실제 vs 예측')
        plt.xlabel('시간')
        plt.ylabel('사용량')
        plt.legend()
        plt.grid(True)
        
        plt.savefig(os.path.join(viz_dir, f'{resource_type}{device_suffix}_prediction.png'))
        plt.close()
        
        # 2. 특성 중요도 그래프
        plt.figure(figsize=(10, 8))
        
        importance_df = pd.DataFrame({
            'Feature': feature_names,
            'Importance': feature_importance
        }).sort_values('Importance', ascending=False)
        
        sns.barplot(x='Importance', y='Feature', data=importance_df.head(15))
        
        plt.title(f'{resource_type} 자원 예측 모델 - 특성 중요도')
        plt.xlabel('중요도')
        plt.ylabel('특성')
        plt.tight_layout()
        
        plt.savefig(os.path.join(viz_dir, f'{resource_type}{device_suffix}_importance.png'))
        plt.close()
        
        logger.info(f"'{resource_type}' 모델 시각화 완료")
    
    def save_model_metrics(self, metrics):
        """모델 성능 메트릭 저장"""
        for resource_type, resource_metrics in metrics.items():
            query = f"""
            INSERT INTO model_performance
            (company_domain, {self.db_manager.server_id_field}, application, resource_type, model_type, 
             mae, rmse, r2_score, feature_importance, trained_at, version, device_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            feature_importance_json = json.dumps(resource_metrics['feature_importance'])
            version = datetime.now().strftime("%Y%m%d%H%M%S")
            device_id = resource_metrics.get('device_id', '')
            
            params = (
                self.company_domain,
                self.server_id,
                "system",  # 통합 모델은 'system'으로 표시
                resource_type,
                self.model_type,
                resource_metrics['mae'],
                resource_metrics['rmse'],
                resource_metrics['r2'],
                feature_importance_json,
                datetime.now(),
                version,
                device_id
            )
            
            self.db_manager.execute_query(query, params)
            logger.info(f"'{resource_type}' 모델 성능 메트릭 저장 완료")
    
    def load_models(self):
        """저장된 모델 로드"""
        for resource_type in ["cpu", "mem", "disk"]:
            # 디바이스 ID 기반 파일명
            model_filename = f"{resource_type}_model.pkl"
            scaler_filename = f"{resource_type}_scaler.pkl"
            
            if self.device_id:
                device_model = f"{resource_type}_{self.device_id}_model.pkl"
                device_scaler = f"{resource_type}_{self.device_id}_scaler.pkl"
                
                # 디바이스별 모델이 있으면 사용
                device_model_path = os.path.join(self.model_dir, device_model)
                device_scaler_path = os.path.join(self.model_dir, device_scaler)
                
                if os.path.exists(device_model_path) and os.path.exists(device_scaler_path):
                    model_filename = device_model
                    scaler_filename = device_scaler
            
            model_path = os.path.join(self.model_dir, model_filename)
            scaler_path = os.path.join(self.model_dir, scaler_filename)
            
            if os.path.exists(model_path) and os.path.exists(scaler_path):
                try:
                    with open(model_path, 'rb') as f:
                        self.models[resource_type] = pickle.load(f)
                    
                    with open(scaler_path, 'rb') as f:
                        self.scalers[resource_type] = pickle.load(f)
                    
                    logger.info(f"'{resource_type}' 모델 로드 완료: {model_path}")
                except Exception as e:
                    logger.error(f"'{resource_type}' 모델 로드 오류: {e}")
            else:
                logger.warning(f"'{resource_type}' 모델 파일이 없습니다: {model_path}")
        
        return any(model is not None for model in self.models.values())
    
    def get_latest_jvm_metrics(self, minutes=30):
        """최근 JVM 메트릭 데이터 조회"""
        use_streaming = os.getenv('ARCHITECTURE', 'streaming').lower() == 'streaming'
        
        try:
            if use_streaming:
                from data.streaming_collector import StreamingDataCollector
                
                collector = StreamingDataCollector(
                    self.config, self.company_domain, self.device_id
                )
                
                jvm_df, _ = collector.get_latest_data(minutes)
                
                if jvm_df.empty:
                    logger.warning("최근 JVM 메트릭 데이터가 없습니다. 더미 데이터를 사용합니다.")
                    return self._create_dummy_metrics()
                
                app_features = {}
                
                for app, app_data in jvm_df.groupby('application'):
                    feature_data = {}
                    
                    for metric_type, metric_data in app_data.groupby('metric_type'):
                        feature_data[metric_type] = metric_data['value'].mean()
                    
                    latest_time = app_data['time'].max()
                    feature_data['hour'] = latest_time.hour
                    feature_data['day_of_week'] = latest_time.weekday()
                    feature_data['is_weekend'] = 1 if latest_time.weekday() >= 5 else 0
                    
                    app_features[app] = pd.DataFrame([feature_data])
                
                if not app_features:
                    logger.warning("애플리케이션 메트릭 데이터가 없습니다. 더미 데이터를 사용합니다.")
                    return self._create_dummy_metrics()
                
                return app_features
                
            else:
                # MySQL 방식 (레거시 호환성)
                return self._get_latest_jvm_metrics_mysql(minutes)
                
        except Exception as e:
            logger.error(f"JVM 메트릭 조회 오류: {e}")
            return self._create_dummy_metrics()
        
    def _create_dummy_metrics(self):
        """더미 JVM 메트릭 데이터 생성"""
        logger.info("더미 JVM 메트릭 데이터 생성")
        
        # ConfigManager에서 애플리케이션 목록 가져오기
        apps = self.config.get_applications()
        
        if not apps:
            # 백업으로 기본 목록 사용
            apps = ["test-gateway", "test-frontend", "test-backend"]
        
        # 더미 데이터 생성
        app_features = {}
        
        for app in apps:
            app_features[app] = self._create_dummy_metric_for_app(app)
        
        return app_features

    def _create_dummy_metric_for_app(self, app):
        """애플리케이션별 더미 메트릭 생성 - 윈도우 특성 포함"""
        from config.settings import JVM_METRICS
        
        metrics = {}
        
        # 기본 JVM 메트릭 생성
        for metric in JVM_METRICS:
            if 'cpu' in metric:
                base_value = 30.0 + np.random.rand() * 10
            elif 'memory' in metric:
                base_value = 1000000000 + np.random.rand() * 500000000
            elif 'gc' in metric:
                base_value = 1000 + np.random.rand() * 500
            elif 'thread' in metric:
                base_value = 50 + np.random.rand() * 20
            elif 'process' in metric:
                base_value = 400 + np.random.rand() * 200
            else:
                base_value = np.random.rand() * 100
            
            metrics[metric] = base_value
            
            # 윈도우 특성 생성 (mean, std, max, min)
            windows = ['5min', '15min', '30min', '60min']
            stats = ['mean', 'std', 'max', 'min']
            
            for window in windows:
                for stat in stats:
                    feature_name = f"{metric}_{stat}_{window}"
                    
                    if stat == 'mean':
                        metrics[feature_name] = base_value
                    elif stat == 'std':
                        metrics[feature_name] = base_value * 0.1  # 표준편차는 평균의 10%
                    elif stat == 'max':
                        metrics[feature_name] = base_value * 1.2  # 최대값은 평균의 120%
                    elif stat == 'min':
                        metrics[feature_name] = base_value * 0.8  # 최소값은 평균의 80%
        
        # 시간 특성
        now = datetime.now()
        metrics['hour'] = now.hour
        metrics['day_of_week'] = now.weekday()
        metrics['is_weekend'] = 1 if now.weekday() >= 5 else 0
        
        logger.info(f"더미 메트릭 생성 완료: {len(metrics)}개 특성")
        
        return pd.DataFrame([metrics])
    
    def _calculate_impact_statistics(self, app_impacts, pred_time):
        """영향도 통계 계산"""
        impact_stats = {}
        
        for resource_type in ["cpu", "mem", "disk"]:
            # 해당 자원에 대한 모든 애플리케이션의 영향도
            impacts = []
            
            for app, app_impact in app_impacts.items():
                if resource_type in app_impact:
                    impacts.append(app_impact[resource_type][0])  # 첫 번째(현재) 영향도 사용
            
            if not impacts:
                logger.warning(f"'{resource_type}' 자원 영향도 데이터가 없습니다.")
                impacts = [0.5]  # 기본값
            
            # 영향도 통계 (학습 데이터와 동일한 특성명)
            impact_stats[f"{resource_type}_impact_sum"] = np.sum(impacts)
            impact_stats[f"{resource_type}_impact_mean"] = np.mean(impacts)
            impact_stats[f"{resource_type}_impact_max"] = np.max(impacts)
            impact_stats[f"{resource_type}_impact_std"] = np.std(impacts)
        
        # 시간 정보 추가
        impact_stats['hour'] = pred_time.hour
        impact_stats['day_of_week'] = pred_time.weekday()
        impact_stats['is_weekend'] = 1 if pred_time.weekday() >= 5 else 0
        
        return impact_stats    
    
    def check_threshold_crossings(self, times, predictions):
        """임계값 도달 시간 확인"""
        alerts = {}
        
        # ConfigManager에서 임계값 가져오기
        thresholds = self.config.get_alert_thresholds()
        
        for resource_type, values in predictions.items():
            threshold_key = f'{resource_type}_threshold' if resource_type != 'mem' else 'memory_threshold'
            threshold = thresholds.get(resource_type, 80.0)
            
            if not threshold:
                continue
            
            # 임계값 초과 인덱스 찾기
            crossing_indices = [i for i, val in enumerate(values) if val >= threshold]
            
            if crossing_indices:
                # 첫 번째 임계값 초과 시점
                first_crossing = crossing_indices[0]
                crossing_time = times[first_crossing]
                time_to_threshold = (crossing_time - times[0]).total_seconds() / 3600  # 시간 단위
                
                alerts[resource_type] = {
                    'threshold': threshold,
                    'crossing_time': crossing_time.strftime('%Y-%m-%d %H:%M:00'),
                    'time_to_threshold': time_to_threshold,
                    'current_value': values[0],
                    'predicted_value': values[first_crossing],
                    'device_id': self.device_id
                }
        
        return alerts
    
    def update_model_performance(self):
        """모델 성능 통계 업데이트"""
        # 디바이스 필터 설정
        device_filter = ""
        params = [self.company_domain, self.server_id]
        
        if self.device_id:
            device_filter = " AND device_id = %s"
            params.append(self.device_id)
        
        # 자원별 최신 예측 성능 계산
        for resource_type in ["cpu", "mem", "disk"]:
            params_resource = params.copy()
            params_resource.insert(2, resource_type)
            params_resource.extend([self.company_domain, self.server_id, resource_type])
            
            # 평균 제외 파라미터
            avg_params = params.copy()
            avg_params.insert(2, resource_type)
            
            query = f"""
            SELECT AVG(error), SQRT(AVG(POWER(error, 2))), 
                   1 - (SUM(POWER(error, 2)) / NULLIF(SUM(POWER(actual_value - (
                       SELECT AVG(actual_value) FROM predictions 
                       WHERE company_domain = %s AND {self.db_manager.server_id_field} = %s AND resource_type = %s 
                       AND actual_value IS NOT NULL{device_filter}
                   ), 2)), 0))
            FROM predictions
            WHERE company_domain = %s AND {self.db_manager.server_id_field} = %s AND resource_type = %s
            AND actual_value IS NOT NULL{device_filter}
            AND prediction_time >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
            """
            
            result = self.db_manager.fetch_one(query, tuple(params_resource))
            
            if result and result[0] is not None:
                mae, rmse, r2 = result
                
                # 성능 저장
                logger.info(f"'{resource_type}' 최근 예측 성능: MAE={mae:.4f}, RMSE={rmse:.4f}, R²={r2:.4f}")
                
                # 모델 성능 메트릭 업데이트 (별도 테이블에 저장)
                query = f"""
                INSERT INTO model_performance
                (company_domain, {self.db_manager.server_id_field}, application, resource_type, model_type, 
                 mae, rmse, r2_score, feature_importance, trained_at, version, device_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                # 메트릭만 저장 (특성 중요도 없음)
                feature_importance = "{}"
                version = datetime.now().strftime("%Y%m%d%H%M%S_perf")
                
                perf_params = [
                    self.company_domain,
                    self.server_id,
                    "system_performance",  # 성능 평가용 특수 값
                    resource_type,
                    self.model_type,
                    mae,
                    rmse,
                    r2,
                    feature_importance,
                    datetime.now(),
                    version,
                    self.device_id if self.device_id else ""
                ]
                
                self.db_manager.execute_query(query, tuple(perf_params))
        
        return True
    
    def _create_dummy_training_data(self, start_time=None, end_time=None):
        """테스트용 더미 학습 데이터 생성 - 올바른 특성 구조"""
        if start_time is None:
            start_time = datetime.now() - timedelta(days=3)
        if end_time is None:
            end_time = datetime.now()
            
        logger.info(f"테스트용 더미 학습 데이터 생성: {start_time} ~ {end_time}")
        
        # 시간 인덱스 생성
        times = pd.date_range(start=start_time, end=end_time, periods=30)
        
        # 영향도 통계 특성 컬럼 정의
        feature_columns = []
        for resource in ['cpu', 'mem', 'disk']:
            feature_columns.extend([
                f'{resource}_impact_sum',
                f'{resource}_impact_mean', 
                f'{resource}_impact_max', 
                f'{resource}_impact_std'
            ])
        
        # 더미 특성 데이터 생성
        X_data = np.random.rand(len(times), len(feature_columns)) * 0.5 + 0.25  # 0.25~0.75 범위
        X = pd.DataFrame(X_data, index=times, columns=feature_columns)
        
        # 시간 특성 추가
        X['hour'] = X.index.hour
        X['day_of_week'] = X.index.dayofweek
        X['is_weekend'] = X['day_of_week'].apply(lambda x: 1 if x >= 5 else 0)
        
        # 더미 목표 변수 생성 (cpu, mem, disk 사용률)
        y_data = {
            'cpu': np.random.rand(len(times)) * 50 + 20,  # 20~70% 범위
            'mem': np.random.rand(len(times)) * 40 + 30,  # 30~70% 범위
            'disk': np.random.rand(len(times)) * 30 + 50   # 50~80% 범위
        }
        y = pd.DataFrame(y_data, index=times)
        
        logger.info(f"더미 학습 데이터 생성 완료: {len(X)}개 샘플, 특성 {X.shape[1]}개")
        
        return X, y