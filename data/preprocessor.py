"""
JVM 메트릭 기반 시스템 리소스 예측 - 데이터 전처리 모듈 (ConfigManager 연동)
- 수집된 JVM 메트릭 데이터 전처리
- 애플리케이션 영향도 계산
- 특성 생성
"""
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import uuid

from core.config_manager import ConfigManager
from core.logger import logger
from core.db import DatabaseManager

class DataPreprocessor:
    """데이터 전처리 및 특성 생성 클래스"""
    
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
        
        # 전처리 설정 가져오기
        self.window_sizes = self.config.get('window_sizes', [5, 15, 30, 60])
        self.statistics = self.config.get('statistics', ['mean', 'std', 'max', 'min'])
        self.include_time_features = self.config.get('time_features', True)
        self.impact_calculation = self.config.get('impact_calculation', 'correlation')
        self.resample_interval = self.config.get('resample_interval', '5min')
        
        # batch_id 생성
        self.batch_id = str(uuid.uuid4())
        
        logger.info(f"데이터 전처리기 초기화 완료, 배치 ID: {self.batch_id}")
        logger.info(f"회사: {self.company_domain}, 서버ID: {self.server_id}, 디바이스: {self.device_id}")
        logger.info(f"설정 - 윈도우: {self.window_sizes}, 통계: {self.statistics}, 리샘플링: {self.resample_interval}")
    
    def __del__(self):
        """소멸자: 연결 종료"""
        if hasattr(self, 'db_manager') and self.db_manager:
            self.db_manager.close()
    
    def load_jvm_metrics(self, start_time=None, end_time=None, limit=None):
        """MySQL에서 JVM 메트릭 데이터 로드"""
        if end_time is None:
            end_time = datetime.utcnow()
        
        if start_time is None:
            # 기본 로드 기간
            default_period = self.config.get('training_window', '3d')
            value = int(default_period[:-1])
            unit = default_period[-1].lower()
            
            if unit == 'd':
                start_time = end_time - timedelta(days=value)
            elif unit == 'h':
                start_time = end_time - timedelta(hours=value)
            else:
                start_time = end_time - timedelta(days=3)
        
        # 쿼리 구성
        query = f"""
        SELECT time, application, metric_type, value 
        FROM jvm_metrics 
        WHERE company_domain = %s AND {self.db_manager.server_id_field} = %s
        AND time BETWEEN %s AND %s
        """
        
        params = [self.company_domain, self.server_id, start_time, end_time]
        
        if self.device_id:
            query += " AND device_id = %s"
            params.append(self.device_id)
            logger.debug(f"디바이스 ID 필터 적용: {self.device_id}")
        
        query += " ORDER BY time"
        
        if limit:
            query += f" LIMIT {limit}"
        
        try:
            conn = self.db_manager.connection
            df = pd.read_sql(query, conn, params=tuple(params))
            logger.info(f"JVM 메트릭 데이터 {len(df)}개 로드 완료 ({start_time} ~ {end_time})")
            return df
        except Exception as e:
            logger.error(f"JVM 메트릭 데이터 로드 오류: {e}")
            return pd.DataFrame()
    
    def load_system_resources(self, start_time=None, end_time=None, limit=None):
        """MySQL에서 시스템 리소스 데이터 로드"""
        if end_time is None:
            end_time = datetime.now()
        
        if start_time is None:
            # 기본 로드 기간
            default_period = self.config.get('training_window', '3d')
            value = int(default_period[:-1])
            unit = default_period[-1].lower()
            
            if unit == 'd':
                start_time = end_time - timedelta(days=value)
            elif unit == 'h':
                start_time = end_time - timedelta(hours=value)
            else:
                start_time = end_time - timedelta(days=3)
        
        # 쿼리 구성
        query = f"""
        SELECT time, resource_type, measurement, value 
        FROM system_resources 
        WHERE company_domain = %s AND {self.db_manager.server_id_field} = %s
        AND time BETWEEN %s AND %s
        """
        
        params = [self.company_domain, self.server_id, start_time, end_time]
        
        if self.device_id:
            query += " AND device_id = %s"
            params.append(self.device_id)
            logger.debug(f"디바이스 ID 필터 적용: {self.device_id}")
        
        query += " ORDER BY time"
        
        if limit:
            query += f" LIMIT {limit}"
        
        try:
            conn = self.db_manager.connection
            df = pd.read_sql(query, conn, params=tuple(params))
            logger.info(f"시스템 리소스 데이터 {len(df)}개 로드 완료 ({start_time} ~ {end_time})")
            return df
        except Exception as e:
            logger.error(f"시스템 리소스 데이터 로드 오류: {e}")
            return pd.DataFrame()
    
    def preprocess_jvm_metrics(self, jvm_df):
        """JVM 메트릭 데이터 전처리"""
        if jvm_df.empty:
            logger.warning("전처리할 JVM 메트릭 데이터가 없음")
            return pd.DataFrame()
        
        logger.info("JVM 메트릭 데이터 전처리 시작")
        
        # 시간을 DateTime 형식으로 변환
        jvm_df['time'] = pd.to_datetime(jvm_df['time'])
        
        # 결측치 확인
        missing_values = jvm_df.isnull().sum()
        if missing_values.sum() > 0:
            logger.info(f"JVM 메트릭 결측치 현황: {missing_values.to_dict()}")
        
        # 애플리케이션-메트릭 조합별로 처리
        results = []
        
        for (app, metric), group in jvm_df.groupby(['application', 'metric_type']):
            logger.debug(f"전처리 중: {app}.{metric} ({len(group)}개 데이터)")
            
            # 시간 인덱스로 변환하여 시계열 생성
            ts = group.set_index('time')['value'].sort_index()
            
            # 결측치 처리
            ts_filled = self._handle_missing_values(ts)
            
            # 이상치 처리
            ts_cleaned = self._handle_outliers(ts_filled)
            
            # 처리된 데이터 저장
            for time, value in ts_cleaned.items():
                results.append({
                    'time': time,
                    'application': app,
                    'metric_type': metric,
                    'value': value
                })
        
        # 결과를 데이터프레임으로 변환
        if results:
            processed_df = pd.DataFrame(results)
            logger.info(f"JVM 메트릭 데이터 전처리 완료: {len(processed_df)}개")
            return processed_df
        else:
            logger.warning("처리된 JVM 데이터가 없음")
            return pd.DataFrame()
    
    def _handle_missing_values(self, ts):
        """결측치 처리"""
        if ts.empty:
            return ts
        
        # 1. 짧은 결측치는 선형 보간법으로 처리
        ts_filled = ts.interpolate(method='linear', limit=5)
        
        # 2. 긴 결측치는 전후 값으로 채우기
        ts_filled = ts_filled.fillna(method='ffill').fillna(method='bfill')
        
        # 3. 여전히 남아있는 결측치는 평균으로 처리
        if ts_filled.isna().any() and not ts_filled.dropna().empty:
            ts_filled = ts_filled.fillna(ts_filled.dropna().mean())
        
        # 4. 최후의 수단으로 0으로 채우기
        ts_filled = ts_filled.fillna(0)
        
        return ts_filled
    
    def _handle_outliers(self, ts):
        """이상치 처리 (IQR 방법)"""
        if len(ts) <= 10:
            return ts
        
        Q1 = ts.quantile(0.25)
        Q3 = ts.quantile(0.75)
        IQR = Q3 - Q1
        
        if IQR > 0:  # IQR이 0이 아닌 경우만 처리
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            ts_cleaned = ts.clip(lower=lower_bound, upper=upper_bound)
        else:
            ts_cleaned = ts
        
        return ts_cleaned
    
    def calculate_impact_scores(self, jvm_df_processed, sys_df_processed):
        """영향도 점수 계산 (일관된 리샘플링 간격 적용)"""
        logger.info("영향도 점수 계산 시작")
        
        try:
            if jvm_df_processed.empty or sys_df_processed.empty:
                logger.warning("영향도 계산을 위한 데이터가 충분하지 않음")
                return pd.DataFrame()

            # 데이터 형식 검증 및 변환
            logger.debug("시간 형식 검증 및 변환")
            if not pd.api.types.is_datetime64_any_dtype(jvm_df_processed['time']):
                jvm_df_processed['time'] = pd.to_datetime(jvm_df_processed['time'])
            
            if not pd.api.types.is_datetime64_any_dtype(sys_df_processed['time']):
                sys_df_processed['time'] = pd.to_datetime(sys_df_processed['time'])
            
            # 데이터 기본 정보 로깅
            logger.info(f"데이터 현황 - JVM: {len(jvm_df_processed)}행, 시스템: {len(sys_df_processed)}행")
            logger.info(f"JVM 애플리케이션: {jvm_df_processed['application'].unique().tolist()}")
            logger.info(f"시스템 리소스: {sys_df_processed['resource_type'].unique().tolist()}")
            
            # 시스템 리소스 데이터 필터링
            sys_filtered = self._filter_system_resources(sys_df_processed)
            
            if sys_filtered.empty:
                logger.error("필터링된 시스템 리소스 데이터가 없음")
                return pd.DataFrame()
            
            # 피봇 테이블 생성
            logger.debug("피봇 테이블 생성")
            jvm_pivot, sys_pivot = self._create_pivot_tables(jvm_df_processed, sys_filtered)
            
            if jvm_pivot.empty or sys_pivot.empty:
                logger.error("피봇 테이블 생성 실패")
                return pd.DataFrame()
            
            # 시계열 리샘플링
            logger.debug(f"시계열 리샘플링 (간격: {self.resample_interval})")
            jvm_resampled, sys_resampled = self._resample_timeseries(jvm_pivot, sys_pivot)
            
            # 공통 시간대 확인
            common_times = jvm_resampled.index.intersection(sys_resampled.index)
            logger.info(f"공통 시간대 수: {len(common_times)}")
            
            min_points = self.config.get('min_common_timepoints', 5)
            if len(common_times) < min_points:
                logger.warning(f"공통 시간대가 너무 적음: {len(common_times)}개 (최소 {min_points}개 필요)")
                return pd.DataFrame()
            
            # 영향도 계산 수행
            impact_scores = self._compute_correlations(jvm_resampled, sys_resampled, common_times)
            
            if impact_scores:
                impact_df = pd.DataFrame(impact_scores)
                logger.info(f"영향도 계산 완료: {len(impact_df)}개 생성")
                return impact_df
            else:
                logger.warning("영향도 계산 결과가 없음")
                return pd.DataFrame()
        
        except Exception as e:
            logger.error(f"영향도 계산 중 오류 발생: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return pd.DataFrame()
    
    def _filter_system_resources(self, sys_df):
        """시스템 리소스 데이터 필터링"""
        # CPU: usage_user, 메모리/디스크: used_percent만 사용
        sys_filtered = sys_df[
            ((sys_df['resource_type'] == 'cpu') & (sys_df['measurement'] == 'usage_user')) |
            ((sys_df['resource_type'] == 'mem') & (sys_df['measurement'] == 'used_percent')) |
            ((sys_df['resource_type'] == 'disk') & (sys_df['measurement'] == 'used_percent'))
        ]
        
        logger.debug(f"시스템 리소스 필터링: {len(sys_df)} → {len(sys_filtered)}")
        return sys_filtered
    
    def _create_pivot_tables(self, jvm_df, sys_df):
        """피봇 테이블 생성"""
        try:
            jvm_pivot = jvm_df.pivot_table(
                index='time', 
                columns=['application', 'metric_type'], 
                values='value',
                aggfunc='mean'  # 동일 시간 중복 데이터 평균 처리
            )
            
            sys_pivot = sys_df.pivot_table(
                index='time', 
                columns=['resource_type', 'measurement'], 
                values='value',
                aggfunc='mean'
            )
            
            logger.debug(f"피봇 테이블 생성 완료 - JVM: {jvm_pivot.shape}, 시스템: {sys_pivot.shape}")
            return jvm_pivot, sys_pivot
            
        except Exception as e:
            logger.error(f"피봇 테이블 생성 오류: {e}")
            return pd.DataFrame(), pd.DataFrame()
    
    def _resample_timeseries(self, jvm_pivot, sys_pivot):
        """시계열 리샘플링"""
        try:
            jvm_resampled = jvm_pivot.resample(self.resample_interval).mean().interpolate(method='linear', limit=2)
            sys_resampled = sys_pivot.resample(self.resample_interval).mean().interpolate(method='linear', limit=2)
            
            logger.debug(f"리샘플링 완료 - JVM: {jvm_resampled.shape}, 시스템: {sys_resampled.shape}")
            return jvm_resampled, sys_resampled
            
        except Exception as e:
            logger.error(f"리샘플링 오류: {e}")
            return pd.DataFrame(), pd.DataFrame()
    
    def _compute_correlations(self, jvm_resampled, sys_resampled, common_times):
        """상관관계 기반 영향도 계산"""
        jvm_aligned = jvm_resampled.loc[common_times]
        sys_aligned = sys_resampled.loc[common_times]
        
        impact_scores = []
        correlation_data = []
        
        # 각 리소스별로 처리
        for resource_col in sys_aligned.columns:
            resource_type, measurement = resource_col
            sys_values = sys_aligned[resource_col]
            
            logger.debug(f"리소스 '{resource_type}-{measurement}' 영향도 계산 중")
            
            # 각 애플리케이션별로 처리
            for app in jvm_aligned.columns.get_level_values(0).unique():
                app_metrics = [col for col in jvm_aligned.columns if col[0] == app]
                
                if not app_metrics:
                    continue
                
                # 애플리케이션의 모든 메트릭과 상관관계 계산
                corr_sum = 0
                corr_count = 0
                valid_metrics = []
                
                for app_metric in app_metrics:
                    jvm_values = jvm_aligned[app_metric]
                    
                    # 데이터 유효성 검사
                    if jvm_values.isnull().all() or sys_values.isnull().all():
                        continue
                    
                    # 피어슨 상관계수 계산
                    try:
                        corr = sys_values.corr(jvm_values)
                        metric_type = app_metric[1]
                        
                        if not np.isnan(corr):
                            corr_sum += abs(corr)  # 절대값 사용
                            corr_count += 1
                            valid_metrics.append((metric_type, corr))
                            
                            # 상관관계 데이터 저장
                            correlation_data.append({
                                'application': app,
                                'metric_type': metric_type,
                                'resource_type': resource_type,
                                'measurement': measurement,
                                'correlation': corr,
                                'abs_correlation': abs(corr)
                            })
                    except Exception as e:
                        logger.debug(f"상관관계 계산 오류 ({app_metric}): {e}")
                
                # 평균 상관계수를 영향도로 사용
                if corr_count > 0:
                    impact_score = corr_sum / corr_count
                    logger.debug(f"'{app}' → '{resource_type}' 영향도: {impact_score:.4f}")
                    
                    # 시간별 영향도 점수 생성
                    for time_point in pd.date_range(
                        start=common_times[0], 
                        end=common_times[-1], 
                        freq=self.resample_interval
                    ):
                        if time_point in common_times:
                            impact_scores.append({
                                'time': time_point,
                                'application': app,
                                'resource_type': resource_type,
                                'impact_score': impact_score,
                                'calculation_method': self.impact_calculation
                            })
        
        # 상관관계 분석 결과 로깅
        if correlation_data:
            corr_df = pd.DataFrame(correlation_data)
            logger.info(f"상관관계 분석 완료 - 평균: {corr_df['abs_correlation'].mean():.4f}, 최대: {corr_df['abs_correlation'].max():.4f}")
        
        return impact_scores
    
    def save_impact_scores(self, impact_df):
        """영향도 점수 저장"""
        if impact_df.empty:
            logger.warning("저장할 영향도 데이터가 없음")
            return False
        
        logger.info(f"영향도 데이터 저장 시작: {len(impact_df)}개")
        
        batch_data = []
        for _, row in impact_df.iterrows():
            batch_data.append((
                self.company_domain,
                self.server_id,
                row['time'],
                row['application'],
                row['resource_type'],
                row['impact_score'],
                row['calculation_method'],
                self.device_id if self.device_id else "",
                self.batch_id
            ))
        
        if not batch_data:
            return False
        
        # 동적 필드명 사용
        insert_query = f"""
        INSERT INTO application_impact 
        (company_domain, {self.db_manager.server_id_field}, time, application, resource_type, impact_score, calculation_method, device_id, batch_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # 배치 처리
        batch_size = self.config.get('batch_size', 1000)
        success = True
        
        for i in range(0, len(batch_data), batch_size):
            chunk = batch_data[i:i + batch_size]
            logger.debug(f"영향도 데이터 저장: 배치 {i//batch_size + 1}/{(len(batch_data) + batch_size - 1)//batch_size}")
            
            result = self.db_manager.execute_query(insert_query, chunk, many=True)
            if not result:
                logger.error(f"영향도 데이터 배치 {i//batch_size + 1} 저장 실패")
                success = False
        
        if success:
            logger.info(f"영향도 데이터 {len(batch_data)}개 저장 완료")
        else:
            logger.warning("일부 영향도 데이터 저장 실패")
        
        return success
    
    def generate_features(self, jvm_df_processed):
        """시계열 특성 생성 (효율적이고 중복 없는 버전)"""
        if jvm_df_processed.empty:
            logger.warning("특성 생성을 위한 데이터가 없음")
            return pd.DataFrame()
        
        logger.info("시계열 특성 생성 시작")
        
        # 피봇 테이블로 변환
        try:
            pivot_df = jvm_df_processed.pivot_table(
                index='time', 
                columns=['application', 'metric_type'], 
                values='value',
                aggfunc='mean'
            )
            logger.debug(f"특성 생성용 피봇 테이블: {pivot_df.shape}")
        except Exception as e:
            logger.error(f"특성 생성용 피봇 테이블 생성 오류: {e}")
            return pd.DataFrame()
        
        # 결과 저장용 리스트
        all_features = []
        
        # 애플리케이션별 처리
        applications = pivot_df.columns.get_level_values(0).unique()
        
        for app in applications:
            logger.debug(f"애플리케이션 '{app}' 특성 생성 중...")
            
            # 해당 애플리케이션의 메트릭만 추출
            app_cols = [col for col in pivot_df.columns if col[0] == app]
            app_df = pivot_df[app_cols].copy()
            
            # 멀티인덱스 컬럼을 단순 인덱스로 변경
            app_df.columns = [col[1] for col in app_df.columns]
            
            # 결측치 제거
            app_df = app_df.dropna()
            
            if app_df.empty:
                logger.warning(f"애플리케이션 '{app}' 데이터가 없음")
                continue
            
            # 윈도우별 특성 생성
            app_features = self._generate_app_features(app, app_df)
            all_features.extend(app_features)
        
        # 시간 특성 추가
        if self.include_time_features and all_features:
            logger.debug("시간 특성 추가 중...")
            time_features = self._generate_time_features(applications, all_features)
            all_features.extend(time_features)
        
        # 데이터프레임으로 변환
        if all_features:
            feature_df = pd.DataFrame(all_features)
            
            # 중복 제거
            feature_df = feature_df.drop_duplicates(
                subset=['time', 'application', 'feature_name'], 
                keep='last'
            )
            
            logger.info(f"특성 생성 완료: {len(feature_df)}개 특성, {feature_df['feature_name'].nunique()}개 유형")
            return feature_df
        else:
            logger.warning("생성된 특성이 없음")
            return pd.DataFrame()
    
    def _generate_app_features(self, app, app_df):
        """애플리케이션별 특성 생성"""
        features = []
        
        for window_size in self.window_sizes:
            # 롤링 윈도우 적용
            rolling_window = app_df.rolling(window=f"{window_size}min", min_periods=1)
            
            # 통계별 계산
            stats_dict = {}
            for stat in self.statistics:
                if stat == 'mean':
                    stats_dict[stat] = rolling_window.mean()
                elif stat == 'std':
                    stats_dict[stat] = rolling_window.std()
                elif stat == 'max':
                    stats_dict[stat] = rolling_window.max()
                elif stat == 'min':
                    stats_dict[stat] = rolling_window.min()
            
            # 통계별 데이터를 리샘플링하여 저장
            for stat_name, stat_df in stats_dict.items():
                # 리샘플링 (중복 제거)
                resampled = stat_df.resample(self.resample_interval).last().dropna()
                
                # 각 메트릭별로 특성 생성
                for metric in resampled.columns:
                    for timestamp, value in resampled[metric].items():
                        if not pd.isna(value):
                            features.append({
                                'time': timestamp,
                                'application': app,
                                'feature_name': f"{metric}_{stat_name}_{window_size}min",
                                'value': float(value),
                                'window_size': window_size
                            })
        
        return features
    
    def _generate_time_features(self, applications, all_features):
        """시간 특성 생성"""
        time_features = []
        
        # 유니크한 시간 목록 추출
        unique_times = set(feature['time'] for feature in all_features)
        
        # 각 애플리케이션별로 시간 특성 추가
        for app in applications:
            for time_point in unique_times:
                # 시간 특성들
                time_feature_list = [
                    ('hour', time_point.hour),
                    ('day_of_week', time_point.dayofweek),
                    ('is_weekend', 1 if time_point.dayofweek >= 5 else 0)
                ]
                
                for feature_name, value in time_feature_list:
                    time_features.append({
                        'time': time_point,
                        'application': app,
                        'feature_name': feature_name,
                        'value': float(value),
                        'window_size': 0  # 시간 특성은 윈도우 크기 0
                    })
        
        return time_features
    
    def save_features(self, features_df):
        """생성된 특성을 MySQL에 저장"""
        if features_df.empty:
            logger.warning("저장할 특성 데이터가 없음")
            return False
        
        logger.info(f"특성 데이터 저장 시작: {len(features_df)}개")
        
        # 기존 데이터 삭제 (선택적)
        delete_query = f"""
        DELETE FROM feature_data 
        WHERE company_domain = %s AND {self.db_manager.server_id_field} = %s AND device_id = %s
        """
        
        self.db_manager.execute_query(delete_query, (
            self.company_domain, self.server_id, self.device_id or ""
        ))
        
        logger.debug("기존 특성 데이터 삭제 완료")
        
        # 배치 데이터 준비
        batch_data = []
        for _, row in features_df.iterrows():
            batch_data.append((
                self.company_domain,
                self.server_id,
                row['time'],
                row['application'],
                row['feature_name'],
                float(row['value']),
                int(row['window_size']),
                self.device_id if self.device_id else "",
                self.batch_id
            ))
        
        # 배치 처리로 저장
        insert_query = f"""
        INSERT INTO feature_data 
        (company_domain, {self.db_manager.server_id_field}, time, application, feature_name, value, window_size, device_id, batch_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        batch_size = self.config.get('batch_size', 1000)
        total_batches = (len(batch_data) + batch_size - 1) // batch_size
        success_count = 0
        
        logger.debug(f"총 {total_batches}개 배치로 나누어 저장")
        
        for i in range(0, len(batch_data), batch_size):
            chunk = batch_data[i:i + batch_size]
            batch_num = i // batch_size + 1
            
            try:
                result = self.db_manager.execute_query(insert_query, chunk, many=True)
                if result:
                    success_count += len(chunk)
                else:
                    logger.error(f"배치 {batch_num} 저장 실패")
            except Exception as e:
                logger.error(f"배치 {batch_num} 저장 오류: {e}")
        
        if success_count > 0:
            logger.info(f"특성 데이터 저장 완료: {success_count}/{len(batch_data)}개")
            return True
        else:
            logger.error("특성 데이터 저장 실패")
            return False
    
    def run_preprocessing_pipeline(self, start_time=None, end_time=None):
        """전체 전처리 파이프라인 실행"""
        logger.info("전처리 파이프라인 실행 시작")
        
        try:
            # 1. 데이터 로드
            logger.info("데이터 로드 단계")
            jvm_df = self.load_jvm_metrics(start_time, end_time)
            if jvm_df.empty:
                logger.error("JVM 메트릭 데이터가 없어 전처리 중단")
                return False
            
            sys_df = self.load_system_resources(start_time, end_time)
            if sys_df.empty:
                logger.error("시스템 리소스 데이터가 없어 전처리 중단")
                return False
            
            # 2. 데이터 전처리
            logger.info("데이터 전처리 단계")
            jvm_df_processed = self.preprocess_jvm_metrics(jvm_df)
            if jvm_df_processed.empty:
                logger.error("JVM 메트릭 전처리 실패")
                return False
            
            # 3. 영향도 계산 및 저장
            logger.info("영향도 계산 단계")
            impact_df = self.calculate_impact_scores(jvm_df_processed, sys_df)
            
            impact_success = True
            if impact_df.empty:
                logger.warning("영향도 계산 결과가 없음")
                impact_success = False
            else:
                impact_success = self.save_impact_scores(impact_df)
            
            # 4. 특성 생성 및 저장
            logger.info("특성 생성 단계")
            features_df = self.generate_features(jvm_df_processed)
            
            features_success = True
            if features_df.empty:
                logger.warning("특성 생성 결과가 없음")
                features_success = False
            else:
                features_success = self.save_features(features_df)
            
            # 결과 평가
            pipeline_success = impact_success or features_success  # 하나라도 성공하면 OK
            
            if pipeline_success:
                logger.info("전처리 파이프라인 완료")
            else:
                logger.error("전처리 파이프라인 실패")
            
            return pipeline_success
            
        except Exception as e:
            logger.error(f"전처리 파이프라인 오류: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False