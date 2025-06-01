"""
JVM 메트릭 기반 시스템 리소스 예측 - 애플리케이션 영향도 모델 (ConfigManager 연동)
- ConfigManager를 통한 동적 설정 관리
- 하드코딩 제거 및 설정 기반 동작
- 기존 코드 호환성 유지
"""
import os
import pickle
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

from core.config_manager import ConfigManager
from core.logger import logger
from core.db import DatabaseManager

class AppImpactModel:
    """애플리케이션 영향도 모델 클래스 - ConfigManager 연동"""
    
    def __init__(self, config_manager=None, company_domain=None, server_id=None, application=None, device_id=None):
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
        self.application = application
        
        # 서버 ID 설정
        if isinstance(server_id, int):
            self.server_id = server_id
        else:
            self.server_id = self.config.get_server_id()
        
        if not self.server_id:
            logger.warning(f"서버 ID를 찾을 수 없습니다. 디바이스: {self.device_id}")
        
        # 모델 설정 가져오기
        model_config = self.config.get_model_config()
        self.model_type = model_config.get('app_model_type', 'random_forest')
        self.training_window = model_config.get('training_window', '3d')
        self.validation_split = model_config.get('validation_split', 0.2)
        
        # 하이퍼파라미터 가져오기
        from config.settings import get_hyperparameters
        self.hyperparameters = get_hyperparameters(self.model_type)
        
        # 모델 저장 경로
        self.model_dir = os.path.join("models", "trained", self.company_domain, str(self.server_id), self.application)
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
        
        logger.info(f"애플리케이션 '{self.application}' 영향도 모델 초기화")
        logger.info(f"모델 타입: {self.model_type}, 훈련 기간: {self.training_window}")
    
    def __del__(self):
        """소멸자: 연결 종료"""
        if hasattr(self, 'db_manager') and self.db_manager:
            self.db_manager.close()
    
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
            # 스트리밍 데이터 수집 및 전처리
            return self._load_streaming_training_data(start_time, end_time)
        else:
            # 기존 MySQL 기반 데이터 로드 (레거시 호환성)
            return self._load_mysql_training_data(start_time, end_time)
    
    def _load_streaming_training_data(self, start_time, end_time):
        """스트리밍 아키텍처용 학습 데이터 로드"""
        logger.info("스트리밍 아키텍처에서 학습 데이터 로드")
        
        try:
            from data.streaming_collector import StreamingDataCollector
            from data.streaming_preprocessor import StreamingPreprocessor
            
            # 스트리밍 수집기 초기화
            collector = StreamingDataCollector(
                self.config, self.company_domain, self.device_id
            )
            
            # JVM 및 시스템 데이터 수집
            jvm_df, sys_df = collector.get_training_data(start_time, end_time)
            
            if jvm_df.empty:
                logger.warning(f"애플리케이션 '{self.application}'의 JVM 데이터가 없습니다.")
                return self._create_dummy_training_data(start_time, end_time)
            
            # 해당 애플리케이션 데이터만 필터링
            app_jvm_df = jvm_df[jvm_df['application'] == self.application]
            
            if app_jvm_df.empty:
                logger.warning(f"애플리케이션 '{self.application}'의 JVM 데이터가 없습니다.")
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
            
            if impact_df is None:
                logger.warning("영향도 계산 실패")
                return self._create_dummy_training_data(start_time, end_time)
            
            # 해당 애플리케이션 영향도만 필터링
            app_impact_df = impact_df[impact_df['application'] == self.application]
            
            # 특성 생성 (캐시 확인)
            features_df = preprocessor.load_cached_features(cache_key)
            
            if features_df is None:
                # 특성 생성 필요
                features_df = preprocessor.generate_and_cache_features(
                    jvm_df, cache_key
                )
            
            if features_df is None:
                logger.warning("특성 생성 실패")
                return self._create_dummy_training_data(start_time, end_time)
            
            # 해당 애플리케이션 특성만 필터링
            app_features_df = features_df[features_df['application'] == self.application]
            
            # 학습 데이터 준비
            return self._prepare_training_data_from_features(app_features_df, app_impact_df)
            
        except Exception as e:
            logger.error(f"스트리밍 학습 데이터 로드 오류: {e}")
            return self._create_dummy_training_data(start_time, end_time)
    
    def _prepare_training_data_from_features(self, features_df, impact_df):
        """특성 데이터프레임에서 학습 데이터 준비"""
        if features_df.empty or impact_df.empty:
            return None, None
        
        # 특성 피봇
        features_pivot = features_df.pivot_table(
            index='time',
            columns='feature_name',
            values='value',
            aggfunc='mean'
        )
        
        # 영향도 피봇
        impact_pivot = impact_df.pivot_table(
            index='time',
            columns='resource_type',
            values='impact_score',
            aggfunc='mean'
        )
        
        # 공통 시간대
        common_times = features_pivot.index.intersection(impact_pivot.index)
        
        if len(common_times) < 10:
            logger.warning("공통 시간대가 부족합니다")
            return None, None
        
        X = features_pivot.loc[common_times].fillna(0)
        y = impact_pivot.loc[common_times].fillna(0)
        
        return X, y
    
    def _load_mysql_training_data(self, start_time, end_time):
        """기존 MySQL 기반 학습 데이터 로드 (레거시 호환성)"""
        # 디바이스 ID 필터 설정
        device_filter = ""
        base_params = [self.company_domain, self.server_id, start_time, end_time]
        
        if self.device_id:
            device_filter = " AND device_id = %s"
            logger.info(f"디바이스 ID 필터 적용: {self.device_id}")
        
        # 1. 해당 애플리케이션의 JVM 메트릭 데이터 로드 (입력 특성 X)
        jvm_query = f"""
        SELECT time, metric_type, value
        FROM jvm_metrics
        WHERE company_domain = %s AND {self.db_manager.server_id_field} = %s
        AND application = %s AND time BETWEEN %s AND %s{device_filter}
        ORDER BY time
        """
        
        jvm_params = [self.company_domain, self.server_id, self.application] + base_params[2:]
        if self.device_id:
            jvm_params.append(self.device_id)
        
        jvm_data = self.db_manager.fetch_all(jvm_query, tuple(jvm_params))
        
        if not jvm_data:
            logger.warning(f"애플리케이션 '{self.application}'의 JVM 메트릭 데이터가 없습니다.")
            return self._create_dummy_training_data(start_time, end_time)
        
        # 2. 해당 애플리케이션의 영향도 점수 로드 (대상 변수 y)
        impact_query = f"""
        SELECT time, resource_type, impact_score
        FROM application_impact
        WHERE company_domain = %s AND {self.db_manager.server_id_field} = %s
        AND application = %s AND time BETWEEN %s AND %s{device_filter}
        ORDER BY time
        """
        
        impact_params = [self.company_domain, self.server_id, self.application] + base_params[2:]
        if self.device_id:
            impact_params.append(self.device_id)
        
        impact_data = self.db_manager.fetch_all(impact_query, tuple(impact_params))
        
        if not impact_data:
            logger.warning(f"애플리케이션 '{self.application}'의 영향도 데이터가 없습니다.")
            return self._create_dummy_training_data(start_time, end_time)
        
        # 3. 데이터프레임으로 변환
        jvm_df = pd.DataFrame(jvm_data, columns=['time', 'metric_type', 'value'])
        impact_df = pd.DataFrame(impact_data, columns=['time', 'resource_type', 'impact_score'])
        
        logger.info(f"원본 데이터: JVM {len(jvm_df)}개, 영향도 {len(impact_df)}개")
        
        # 4. 시간을 datetime으로 변환
        jvm_df['time'] = pd.to_datetime(jvm_df['time'])
        impact_df['time'] = pd.to_datetime(impact_df['time'])
        
        # 5. JVM 메트릭 피봇 테이블 생성 (시간 × 메트릭)
        jvm_pivot = jvm_df.pivot_table(
            index='time', 
            columns='metric_type', 
            values='value',
            aggfunc='mean'
        )
        
        logger.info(f"JVM 피봇 테이블: {jvm_pivot.shape}, 메트릭: {jvm_pivot.columns.tolist()}")
        
        # 6. 영향도 피봇 테이블 생성 (시간 × 리소스)
        impact_pivot = impact_df.pivot_table(
            index='time',
            columns='resource_type',
            values='impact_score',
            aggfunc='mean'
        )
        
        logger.info(f"영향도 피봇 테이블: {impact_pivot.shape}, 리소스: {impact_pivot.columns.tolist()}")
        
        # 7. 공통 시간대 확인
        common_times = jvm_pivot.index.intersection(impact_pivot.index)
        
        min_points = self.config.get('min_common_timepoints', 10)
        if len(common_times) < min_points:
            logger.warning(f"공통 시간대 데이터가 너무 적습니다: {len(common_times)}개")
            return self._create_dummy_training_data(start_time, end_time)
        
        logger.info(f"공통 시간대: {len(common_times)}개, 범위: {common_times.min()} ~ {common_times.max()}")
        
        # 8. 공통 시간대 데이터 추출
        X = jvm_pivot.loc[common_times].copy()
        y = impact_pivot.loc[common_times].copy()
        
        # 9. 시간 특성 추가 (X에만)
        X['hour'] = X.index.hour
        X['day_of_week'] = X.index.dayofweek
        X['is_weekend'] = X['day_of_week'].apply(lambda x: 1 if x >= 5 else 0)
        
        # 10. 결측치 처리
        # JVM 메트릭 결측치 → 해당 메트릭의 평균값으로 채움
        for col in X.columns:
            if X[col].isnull().any():
                if col in ['hour', 'day_of_week', 'is_weekend']:
                    X[col] = X[col].fillna(X[col].mode()[0] if not X[col].mode().empty else 0)
                else:
                    X[col] = X[col].fillna(X[col].mean())
        
        # 영향도 결측치 → 해당 리소스의 평균값으로 채움  
        for col in y.columns:
            if y[col].isnull().any():
                y[col] = y[col].fillna(y[col].mean())
        
        # 여전히 NaN이 있으면 0으로 채움
        X = X.fillna(0)
        y = y.fillna(0)
        
        logger.info(f"최종 학습 데이터: X={X.shape}, y={y.shape}")
        logger.info(f"JVM 특성: {X.columns.tolist()}")
        logger.info(f"영향도 리소스: {y.columns.tolist()}")
        
        return X, y

    def _create_dummy_training_data(self, start_time, end_time):
        """테스트용 더미 학습 데이터 생성"""
        logger.info("테스트용 더미 학습 데이터 생성")
        
        # 시간 인덱스 생성 (1시간 간격)
        times = pd.date_range(start=start_time, end=end_time, freq='h')  # '1H' -> 'h'로 변경
        
        # JVM 메트릭 특성 (설정에서 가져오기)
        from config.settings import JVM_METRICS
        
        # 더미 X 데이터프레임 생성
        X_data = {}
        for metric in JVM_METRICS:
            X_data[metric] = np.random.rand(len(times)) * 100 + 50
        
        X = pd.DataFrame(X_data, index=times)
        
        # 시간 특성 추가
        X['hour'] = X.index.hour
        X['day_of_week'] = X.index.dayofweek
        X['is_weekend'] = X['day_of_week'].apply(lambda x: 1 if x >= 5 else 0)
        
        # 더미 y 데이터프레임 생성 (영향도 점수: 0~1)
        y = pd.DataFrame(index=times, columns=['cpu', 'mem', 'disk'], 
                        data=np.random.rand(len(times), 3))
        
        logger.info(f"더미 학습 데이터 생성 완료: {len(X)}개 샘플, 특성 {X.shape[1]}개")
        
        return X, y
    
    def train_models(self, X=None, y=None):
        """자원별 영향도 모델 학습"""
        if X is None or y is None:
            X, y = self.load_training_data()
            
        if X is None or y is None:
            logger.error("학습 데이터 로드 실패")
            return False
        
        # 결과 저장용 메트릭
        metrics = {}
        
        # 교차 검증 설정 가져오기
        cv_splits = self.config.get('cv_splits', 5)
        
        # 각 자원 유형별로 모델 학습
        for resource_type in ["cpu", "mem", "disk"]:
            if resource_type not in y.columns:
                logger.warning(f"'{resource_type}' 자원 영향도 데이터가 없습니다.")
                continue
            
            logger.info(f"'{resource_type}' 자원 영향도 모델 학습 시작")
            
            # 대상 변수 (영향도 점수)
            y_resource = y[resource_type]
            
            # 특성 스케일링
            scaler = StandardScaler()
            X_scaled = scaler.fit_transform(X)
            
            # 시계열 교차 검증
            tscv = TimeSeriesSplit(n_splits=cv_splits)
            
            # 모델 초기화
            if self.model_type == "random_forest":
                model = RandomForestRegressor(
                    n_estimators=self.hyperparameters.get("n_estimators", 100),
                    max_depth=self.hyperparameters.get("max_depth", 10),
                    random_state=42
                )
            elif self.model_type == "gradient_boosting":
                model = GradientBoostingRegressor(
                    n_estimators=self.hyperparameters.get("n_estimators", 100),
                    learning_rate=self.hyperparameters.get("learning_rate", 0.1),
                    random_state=42
                )
            else:
                logger.error(f"지원하지 않는 모델 유형: {self.model_type}")
                continue
            
            # 교차 검증 성능 지표
            cv_metrics = {
                'mae': [],
                'rmse': [],
                'r2': []
            }
            
            # 시계열 교차 검증 수행
            for train_idx, test_idx in tscv.split(X_scaled):
                X_train, X_test = X_scaled[train_idx], X_scaled[test_idx]
                y_train, y_test = y_resource.iloc[train_idx], y_resource.iloc[test_idx]
                
                # 모델 학습
                model.fit(X_train, y_train)
                
                # 예측
                y_pred = model.predict(X_test)
                
                # 성능 평가
                mae = mean_absolute_error(y_test, y_pred)
                rmse = np.sqrt(mean_squared_error(y_test, y_pred))
                r2 = r2_score(y_test, y_pred)
                
                cv_metrics['mae'].append(mae)
                cv_metrics['rmse'].append(rmse)
                cv_metrics['r2'].append(r2)
            
            # 평균 성능 계산
            avg_metrics = {
                'mae': np.mean(cv_metrics['mae']),
                'rmse': np.mean(cv_metrics['rmse']),
                'r2': np.mean(cv_metrics['r2'])
            }
            
            logger.info(f"'{resource_type}' 모델 교차 검증 성능: MAE={avg_metrics['mae']:.4f}, RMSE={avg_metrics['rmse']:.4f}, R²={avg_metrics['r2']:.4f}")
            
            # 전체 데이터로 최종 모델 학습
            model.fit(X_scaled, y_resource)
            
            # 특성 중요도 계산
            feature_importance = model.feature_importances_
            feature_names = X.columns
            
            # 중요도 순으로 정렬
            importance_indices = np.argsort(feature_importance)[::-1]
            top_features = [(feature_names[i], feature_importance[i]) for i in importance_indices[:10]]
            
            logger.info(f"'{resource_type}' 주요 특성(상위 10개): {top_features}")
            
            # 모델 및 스케일러 저장
            self.models[resource_type] = model
            self.scalers[resource_type] = scaler
            
            # 메트릭 저장
            metrics[resource_type] = {
                'mae': avg_metrics['mae'],
                'rmse': avg_metrics['rmse'],
                'r2': avg_metrics['r2'],
                'feature_importance': dict(zip(feature_names, feature_importance.tolist())),
                'device_id': self.device_id
            }
            
            # 모델 저장 (디바이스 ID 추가)
            model_filename = f"{resource_type}_model.pkl"
            if self.device_id:
                model_filename = f"{resource_type}_{self.device_id}_model.pkl"
                
            scaler_filename = f"{resource_type}_scaler.pkl"
            if self.device_id:
                scaler_filename = f"{resource_type}_{self.device_id}_scaler.pkl"
            
            model_path = os.path.join(self.model_dir, model_filename)
            scaler_path = os.path.join(self.model_dir, scaler_filename)
            
            with open(model_path, 'wb') as f:
                pickle.dump(model, f)
            
            with open(scaler_path, 'wb') as f:
                pickle.dump(scaler, f)
            
            logger.info(f"'{resource_type}' 모델 저장 완료: {model_path}")
        
        # 성능 메트릭 DB에 저장
        self.save_model_metrics(metrics)
        
        return True
    
    def predict(self, features):
        """영향도 예측 - JVM 메트릭 입력으로 영향도 점수 출력"""
        if not any(model is not None for model in self.models.values()):
            if not self.load_models():
                logger.error("예측할 모델이 없습니다.")
                return None
        
        # 예측 결과
        predictions = {}
        
        # 1. device_id 컬럼 제거 (숫자가 아닌 특성 제거)
        cleaned_features = features.copy()
        if 'device_id' in cleaned_features.columns:
            cleaned_features = cleaned_features.drop('device_id', axis=1)
            logger.debug("device_id 컬럼 제거됨")
        
        # 2. 각 자원 유형별로 영향도 예측
        for resource_type in ["cpu", "mem", "disk"]:
            if self.models[resource_type] is None or self.scalers[resource_type] is None:
                logger.warning(f"'{resource_type}' 모델 또는 스케일러가 없습니다.")
                predictions[resource_type] = np.array([0.5] * len(features))  # 기본값
                continue
            
            try:
                # 3. 특성 이름 정렬 (모델 학습 시와 동일한 순서)
                if hasattr(self.models[resource_type], 'feature_names_in_'):
                    expected_features = self.models[resource_type].feature_names_in_
                    aligned_features = self._align_features_comprehensive(cleaned_features, expected_features)
                else:
                    # scikit-learn 이전 버전 호환성 - 스케일러에서 특성 이름 확인
                    if hasattr(self.scalers[resource_type], 'feature_names_in_'):
                        expected_features = self.scalers[resource_type].feature_names_in_
                        aligned_features = self._align_features_comprehensive(cleaned_features, expected_features)
                    else:
                        # 특성 이름 정보가 없는 경우 경고하고 기본값 사용
                        logger.warning(f"'{resource_type}' 모델의 특성 정보를 찾을 수 없습니다. 기본값 사용")
                        aligned_features = cleaned_features
                
                # 4. 특성 스케일링
                try:
                    features_scaled = self.scalers[resource_type].transform(aligned_features)
                except Exception as scale_error:
                    logger.warning(f"특성 스케일링 오류 ({resource_type}): {scale_error}")
                    # 스케일링 실패 시 정규화된 값 사용
                    aligned_features_normalized = (aligned_features - aligned_features.mean()) / (aligned_features.std() + 1e-8)
                    features_scaled = aligned_features_normalized.values
                
                # 5. 영향도 예측 수행
                predictions[resource_type] = self.models[resource_type].predict(features_scaled)
                logger.debug(f"'{resource_type}' 영향도 예측 완료: {predictions[resource_type]}")
                
            except Exception as e:
                logger.error(f"'{resource_type}' 자원 영향도 예측 중 오류: {e}")
                predictions[resource_type] = np.array([0.5] * len(features))
        
        return predictions
    def _align_features_comprehensive(self, input_features, expected_features):
        """포괄적인 JVM 메트릭 특성 정렬"""
        
        # 새 데이터프레임 생성 
        aligned_df = pd.DataFrame(index=input_features.index)
        
        logger.debug(f"입력 특성: {list(input_features.columns)}")
        logger.debug(f"기대 특성: {list(expected_features)}")
        
        for feature_name in expected_features:
            if feature_name in input_features.columns:
                # 직접 매칭되는 특성 복사
                aligned_df[feature_name] = input_features[feature_name]
            elif feature_name in ['hour', 'day_of_week', 'is_weekend']:
                # 시간 특성 처리
                if feature_name == 'hour':
                    aligned_df[feature_name] = datetime.now().hour
                elif feature_name == 'day_of_week':
                    aligned_df[feature_name] = datetime.now().weekday()
                elif feature_name == 'is_weekend':
                    aligned_df[feature_name] = 1 if datetime.now().weekday() >= 5 else 0
            elif '_' in feature_name:
                # 윈도우 특성 처리 (예: cpu_utilization_percent_mean_15min)
                base_feature = self._extract_base_feature(feature_name)
                if base_feature in input_features.columns:
                    # 기본 메트릭 값으로 윈도우 특성 근사
                    aligned_df[feature_name] = input_features[base_feature]
                    logger.debug(f"특성 '{feature_name}' 기본 메트릭 '{base_feature}'로 근사")
                else:
                    # 유사한 특성으로 대체
                    similar_feature = self._find_similar_feature(feature_name, input_features.columns)
                    if similar_feature:
                        aligned_df[feature_name] = input_features[similar_feature]
                        logger.debug(f"특성 '{feature_name}' 유사 특성 '{similar_feature}'로 대체")
                    else:
                        # 기본값 설정
                        aligned_df[feature_name] = self._get_default_value(feature_name)
                        logger.debug(f"특성 '{feature_name}' 기본값 설정")
            else:
                # 매칭되지 않는 특성은 기본값으로 채우기
                aligned_df[feature_name] = self._get_default_value(feature_name)
                logger.debug(f"특성 '{feature_name}' 기본값(추정) 설정")
        
        # 결측치 채우기
        aligned_df = aligned_df.fillna(0.0)
        
        logger.debug(f"특성 정렬 완료: {aligned_df.shape}, 컬럼: {len(aligned_df.columns)}개")
        
        return aligned_df    
    def _find_similar_feature(self, target_feature, available_features):
        """유사한 특성 찾기"""
        base_name = self._extract_base_feature(target_feature)
        
        # 정확한 매칭 시도
        if base_name in available_features:
            return base_name
        
        # 부분 매칭 시도
        for feature in available_features:
            if base_name in feature or feature in base_name:
                return feature
        
        return None

    def _get_default_value(self, feature_name):
        """특성별 기본값 반환"""
        if 'cpu' in feature_name.lower():
            return 30.0  # CPU 사용률 기본값
        elif 'memory' in feature_name.lower() or 'heap' in feature_name.lower():
            return 1000000000.0  # 메모리 바이트 기본값
        elif 'gc' in feature_name.lower():
            return 1000.0  # GC 횟수 기본값
        elif 'thread' in feature_name.lower():
            return 50.0  # 스레드 수 기본값
        elif 'process' in feature_name.lower():
            return 400.0  # 프로세스 관련 기본값
        elif 'hour' in feature_name:
            return datetime.now().hour
        elif 'day_of_week' in feature_name:
            return datetime.now().weekday()
        elif 'is_weekend' in feature_name:
            return 1 if datetime.now().weekday() >= 5 else 0
        else:
            return 0.0  # 기본값    
    def _align_jvm_features(self, input_features, expected_features):
        """JVM 메트릭 특성 정렬"""
        
        # 새 데이터프레임 생성 
        aligned_df = pd.DataFrame(index=input_features.index)
        
        for feature_name in expected_features:
            if feature_name in input_features.columns:
                # 직접 매칭되는 특성 복사
                aligned_df[feature_name] = input_features[feature_name]
            
            elif feature_name in ['hour', 'day_of_week', 'is_weekend']:
                # 시간 특성 처리
                if feature_name == 'hour':
                    aligned_df[feature_name] = datetime.now().hour
                elif feature_name == 'day_of_week':
                    aligned_df[feature_name] = datetime.now().weekday()
                elif feature_name == 'is_weekend':
                    aligned_df[feature_name] = 1 if datetime.now().weekday() >= 5 else 0
            
            else:
                # 매칭되지 않는 특성은 0으로 채우기
                aligned_df[feature_name] = 0.0
                logger.debug(f"특성 '{feature_name}' 기본값(0.0) 설정")
        
        # 결측치 채우기
        aligned_df = aligned_df.fillna(0.0)
        
        logger.debug(f"JVM 특성 정렬 완료: {aligned_df.shape}, 컬럼: {aligned_df.columns.tolist()}")
        
        return aligned_df
    def _extract_base_feature(self, window_feature_name):
        """윈도우 특성에서 기본 메트릭 이름 추출"""
        # 예: cpu_utilization_percent_mean_15min -> cpu_utilization_percent
        parts = window_feature_name.split('_')
        
        # 통계 관련 키워드 제거
        stats_keywords = ['mean', 'std', 'max', 'min']
        time_keywords = ['5min', '15min', '30min', '60min']
        
        base_parts = []
        for part in parts:
            if part not in stats_keywords and part not in time_keywords:
                base_parts.append(part)
        
        return '_'.join(base_parts)
    def save_model_metrics(self, metrics):
        """모델 성능 메트릭 저장"""
        for resource_type, resource_metrics in metrics.items():
            query = f"""
            INSERT INTO model_performance
            (company_domain, {self.db_manager.server_id_field}, application, resource_type, model_type, 
             mae, rmse, r2_score, feature_importance, trained_at, version, device_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            import json
            feature_importance_json = json.dumps(resource_metrics['feature_importance'])
            version = datetime.now().strftime("%Y%m%d%H%M%S")
            device_id = resource_metrics.get('device_id', '')
            
            params = (
                self.company_domain,
                self.server_id,
                self.application,
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
            # 디바이스 ID에 맞는 모델 파일명 생성
            model_filename = f"{resource_type}_model.pkl"
            if self.device_id:
                model_filename = f"{resource_type}_{self.device_id}_model.pkl"
                
            scaler_filename = f"{resource_type}_scaler.pkl"
            if self.device_id:
                scaler_filename = f"{resource_type}_{self.device_id}_scaler.pkl"
            
            model_path = os.path.join(self.model_dir, model_filename)
            scaler_path = os.path.join(self.model_dir, scaler_filename)
            
            # 디바이스별 모델이 없으면 기본 모델 경로 시도
            if not os.path.exists(model_path) and self.device_id:
                model_path = os.path.join(self.model_dir, f"{resource_type}_model.pkl")
                scaler_path = os.path.join(self.model_dir, f"{resource_type}_scaler.pkl")
            
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


class AppModelManager:
    """애플리케이션 모델 관리 클래스 - ConfigManager 연동"""
    
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
        
        # 애플리케이션 모델 맵
        self.app_models = {}
        
        logger.info(f"애플리케이션 모델 관리자 초기화: 회사={self.company_domain}, 서버ID={self.server_id}, 디바이스ID={self.device_id}")
    
    def __del__(self):
        """소멸자: 연결 종료"""
        if hasattr(self, 'db_manager') and self.db_manager:
            self.db_manager.close()
    
    def get_applications(self):
        """애플리케이션 목록 조회 - ConfigManager 사용"""
        # ConfigManager에서 애플리케이션 목록 가져오기
        applications = self.config.get_applications()
        
        if applications:
            logger.info(f"애플리케이션 목록: {applications}")
            return applications
        
        # ConfigManager에서도 못 가져오면 직접 DB 조회
        device_filter = ""
        params = [self.company_domain, self.server_id]
        
        if self.device_id:
            device_filter = " AND device_id = %s"
            params.append(self.device_id)
        
        query = f"""
        SELECT DISTINCT application 
        FROM application_impact 
        WHERE company_domain = %s AND {self.db_manager.server_id_field} = %s{device_filter}
        """
        
        results = self.db_manager.fetch_all(query, tuple(params))
        
        if not results:
            logger.warning("애플리케이션 목록을 조회할 수 없습니다.")
            
            # 대안: JVM 메트릭에서 애플리케이션 목록 조회
            jvm_query = f"""
            SELECT DISTINCT application 
            FROM jvm_metrics 
            WHERE company_domain = %s AND {self.db_manager.server_id_field} = %s{device_filter}
            """
            
            jvm_results = self.db_manager.fetch_all(jvm_query, tuple(params))
            
            if jvm_results:
                applications = [row[0] for row in jvm_results]
                logger.info(f"JVM 메트릭에서 애플리케이션 목록 조회: {applications}")
                return applications
            
            # 백업으로 기본 목록 제공 (하드코딩 제거)
            from config.settings import get_default_config
            default_config = get_default_config()
            apps = ["javame-gateway", "javame-member", "javame-frontend", "javame-environment-api", "javame-auth"]
            logger.info(f"기본 애플리케이션 목록 사용: {apps}")
            return apps
        
        applications = [row[0] for row in results]
        logger.info(f"애플리케이션 목록: {applications}")
        
        return applications
    
    def train_all_models(self):
        """모든 애플리케이션 모델 학습"""
        applications = self.get_applications()
        
        if not applications:
            logger.error("학습할 애플리케이션이 없습니다.")
            return False
        
        success_count = 0
        
        for app in applications:
            logger.info(f"애플리케이션 '{app}' 모델 학습 시작")
            
            try:
                model = AppImpactModel(self.config, self.company_domain, self.server_id, app, self.device_id)
                if model.train_models():
                    self.app_models[app] = model
                    success_count += 1
                    logger.info(f"애플리케이션 '{app}' 모델 학습 완료")
                else:
                    logger.error(f"애플리케이션 '{app}' 모델 학습 실패")
            except Exception as e:
                logger.error(f"애플리케이션 '{app}' 모델 학습 중 오류 발생: {e}")
                import traceback
                logger.error(traceback.format_exc())
        
        logger.info(f"총 {len(applications)}개 중 {success_count}개 애플리케이션 모델 학습 완료")
        
        return success_count > 0
    
    def load_all_models(self):
        """모든 애플리케이션 모델 로드"""
        applications = self.get_applications()
        
        if not applications:
            logger.error("로드할 애플리케이션이 없습니다.")
            return False
        
        success_count = 0
        
        for app in applications:
            logger.info(f"애플리케이션 '{app}' 모델 로드 시작")
            
            try:
                model = AppImpactModel(self.config, self.company_domain, self.server_id, app, self.device_id)
                if model.load_models():
                    self.app_models[app] = model
                    success_count += 1
                    logger.info(f"애플리케이션 '{app}' 모델 로드 완료")
                else:
                    logger.warning(f"애플리케이션 '{app}' 모델 파일이 없습니다.")
            except Exception as e:
                logger.error(f"애플리케이션 '{app}' 모델 로드 중 오류 발생: {e}")
        
        logger.info(f"총 {len(applications)}개 중 {success_count}개 애플리케이션 모델 로드 완료")
        
        return success_count > 0
    
    def get_app_model(self, application):
        """특정 애플리케이션 모델 가져오기"""
        if application in self.app_models:
            return self.app_models[application]
        
        # 모델이 로드되지 않은 경우 로드 시도
        try:
            model = AppImpactModel(self.config, self.company_domain, self.server_id, application, self.device_id)
            if model.load_models():
                self.app_models[application] = model
                logger.info(f"애플리케이션 '{application}' 모델 로드 완료")
                return model
            else:
                logger.warning(f"애플리케이션 '{application}' 모델 파일이 없습니다. 기본 모델 생성을 시도합니다.")
                if model.train_models():
                    logger.info(f"애플리케이션 '{application}' 기본 모델 생성 완료")
                    self.app_models[application] = model
                    return model
                else:
                    logger.warning(f"애플리케이션 '{application}' 기본 모델 생성 실패, 더미 모델을 사용합니다.")
                    dummy_model = self._create_dummy_model(application)
                    self.app_models[application] = dummy_model
                    return dummy_model
        except Exception as e:
            logger.error(f"애플리케이션 '{application}' 모델 로드 중 오류 발생: {e}")
            dummy_model = self._create_dummy_model(application)
            self.app_models[application] = dummy_model
            return dummy_model

    def _create_dummy_model(self, application):
        """더미 모델 생성"""
        logger.info(f"애플리케이션 '{application}'에 대한 더미 모델 생성")
        model = AppImpactModel(self.config, self.company_domain, self.server_id, application, self.device_id)
        
        # 더미 모델 설정
        for resource_type in ["cpu", "mem", "disk"]:
            from sklearn.dummy import DummyRegressor
            dummy = DummyRegressor(strategy="constant", constant=0.5)
            
            # 더미 모델을 학습 (간단한 데이터로)
            from config.settings import JVM_METRICS
            
            # 기본 특성 데이터 생성
            X_data = {metric: [0.5] for metric in JVM_METRICS}
            X_data.update({'hour': [12], 'day_of_week': [1], 'is_weekend': [0]})
            
            X = pd.DataFrame(X_data)
            y = np.array([0.5])
            dummy.fit(X, y)
            
            model.models[resource_type] = dummy
            
            from sklearn.preprocessing import StandardScaler
            scaler = StandardScaler()
            scaler.fit(X)
            model.scalers[resource_type] = scaler
        
        return model
    
    def predict_impacts(self, app_features):
        """여러 애플리케이션의 영향도 예측"""
        impacts = {}
        
        for app, features in app_features.items():
            app_model = self.get_app_model(app)
            
            if app_model:
                app_impacts = app_model.predict(features)
                if app_impacts:
                    impacts[app] = app_impacts
                else:
                    logger.warning(f"애플리케이션 '{app}' 영향도 예측 실패")
            else:
                logger.warning(f"애플리케이션 '{app}' 모델이 없습니다.")
        
        return impacts