#!/usr/bin/env python
"""
JVM 메트릭 기반 시스템 리소스 예측 - 스트리밍 아키텍처 메인 엔트리포인트
InfluxDB 직접 처리 및 파일 캐싱 기반의 효율적인 멀티 테넌트 시스템
"""
import os
import sys
import argparse
import logging

# 프로젝트 루트 디렉토리를 패스에 추가
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# .env 파일 로드
try:
    from dotenv import load_dotenv
    load_dotenv()
    print("환경 설정 파일 로드 완료")
except ImportError:
    print("python-dotenv가 설치되지 않았습니다. 환경변수 직접 설정 필요")
except Exception as e:
    print(f"환경 설정 파일 로드 오류: {e}")

from core.logger import logger, set_context
from scheduler.global_scheduler import StreamingGlobalScheduler

def validate_streaming_environment():
    """스트리밍 아키텍처 환경변수 유효성 검사"""
    required_env = [
        'MYSQL_HOST', 'MYSQL_USER', 'MYSQL_PASSWORD', 'MYSQL_DATABASE',
        'INFLUXDB_URL', 'INFLUXDB_TOKEN'
    ]
    
    missing_env = [env for env in required_env if not os.getenv(env)]
    
    if missing_env:
        logger.error(f"필수 환경변수가 설정되지 않았습니다: {missing_env}")
        logger.error("다음 환경변수들이 필요합니다:")
        for env in required_env:
            logger.error(f"  - {env}")
        return False
    
    return True

def setup_logging():
    """로깅 설정"""
    log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
    logging.getLogger().setLevel(getattr(logging, log_level))
    
    logger.info(f"로깅 레벨 설정: {log_level}")

def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(description='JVM 메트릭 예측 스트리밍 글로벌 시스템')
    parser.add_argument('--mode', choices=['scheduler', 'health-check', 'config-test', 'status', 'migrate'], 
                       default='scheduler', help='실행 모드')
    parser.add_argument('--dry-run', action='store_true', help='실제 실행 없이 설정만 확인')
    parser.add_argument('--cache-cleanup', action='store_true', help='캐시 정리 후 종료')
    
    args = parser.parse_args()
    
    try:
        # 로깅 설정
        setup_logging()
        
        # 글로벌 로그 컨텍스트 설정
        set_context(company_domain="streaming_global")
        
        logger.info("JVM 메트릭 예측 스트리밍 시스템 시작")
        logger.info(f"실행 모드: {args.mode}")
        
        # 모드별 실행
        if args.mode == 'config-test':
            run_config_test()
        elif args.mode == 'health-check':
            if not validate_streaming_environment():
                sys.exit(1)
            run_health_check()
        elif args.mode == 'status':
            if not validate_streaming_environment():
                sys.exit(1)
            run_status_check()
        elif args.mode == 'migrate':
            run_migration_setup()
        elif args.mode == 'scheduler':
            if args.cache_cleanup:
                run_cache_cleanup_only()
            elif args.dry_run:
                logger.info("드라이런 모드: 스트리밍 스케줄러 설정만 확인")
                scheduler = StreamingGlobalScheduler()
                logger.info("스트리밍 글로벌 스케줄러 초기화 성공")
                status = scheduler.get_status()
                logger.info(f"시스템 상태: {status}")
                return
            else:
                if not validate_streaming_environment():
                    sys.exit(1)
                run_streaming_scheduler()
        else:
            logger.error(f"알 수 없는 모드: {args.mode}")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("사용자에 의한 종료")
    except Exception as e:
        logger.error(f"애플리케이션 실행 중 오류: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)

def run_config_test():
    """스트리밍 설정 테스트"""
    logger.info("스트리밍 글로벌 설정 테스트 시작")
    
    # 환경변수 확인
    logger.info("환경변수 확인:")
    logger.info(f"MySQL 호스트: {os.getenv('MYSQL_HOST', 'localhost')}")
    logger.info(f"MySQL 데이터베이스: {os.getenv('MYSQL_DATABASE', 'jvm_metrics')}")
    logger.info(f"InfluxDB URL: {os.getenv('INFLUXDB_URL', 'http://localhost:8086')}")
    logger.info(f"InfluxDB 조직: {os.getenv('INFLUXDB_ORG', 'javame')}")
    
    # 스트리밍 스케줄 설정 확인
    logger.info("스트리밍 스케줄 설정:")
    logger.info(f"회사 감지 간격: {os.getenv('DISCOVERY_INTERVAL', '60')}분")
    logger.info(f"스트리밍 처리 간격: {os.getenv('STREAMING_INTERVAL', '30')}분")
    logger.info(f"모델 학습 간격: {os.getenv('MODEL_TRAINING_INTERVAL', '360')}분")
    logger.info(f"예측 간격: {os.getenv('PREDICTION_INTERVAL', '60')}분")
    logger.info(f"캐시 정리 간격: {os.getenv('CACHE_CLEANUP_INTERVAL', '720')}분")
    logger.info(f"헬스 체크 간격: {os.getenv('HEALTH_CHECK_INTERVAL', '15')}분")
    
    # 캐시 설정 확인
    logger.info("캐시 설정:")
    logger.info(f"캐시 TTL: {os.getenv('CACHE_TTL_HOURS', '6')}시간")
    logger.info(f"최대 캐시 보관: {os.getenv('MAX_CACHE_AGE_HOURS', '48')}시간")
    logger.info(f"최대 동시 처리: {os.getenv('MAX_WORKERS', '3')}개")
    logger.info(f"즉시 실행 여부: {os.getenv('RUN_IMMEDIATE', 'true')}")
    
    # 캐시 디렉토리 확인
    cache_base = os.path.join(os.getcwd(), "cache")
    logger.info(f"캐시 기본 디렉토리: {cache_base}")
    
    if os.path.exists(cache_base):
        # 기존 캐시 현황 확인
        total_files = 0
        total_size = 0
        
        for root, dirs, files in os.walk(cache_base):
            cache_files = [f for f in files if f.endswith('.pkl')]
            total_files += len(cache_files)
            
            for file in cache_files:
                file_path = os.path.join(root, file)
                total_size += os.path.getsize(file_path)
        
        logger.info(f"기존 캐시 현황: {total_files}개 파일, {total_size / (1024*1024):.2f}MB")
    else:
        logger.info("캐시 디렉토리가 없습니다. 첫 실행 시 자동 생성됩니다.")
    
    logger.info("스트리밍 글로벌 설정 테스트 완료")

def run_health_check():
    """스트리밍 헬스 체크 실행"""
    logger.info("스트리밍 글로벌 헬스 체크 시작")
    
    try:
        # 먼저 기본 DB 연결 확인
        logger.info("데이터베이스 연결 상태 확인")
        try:
            from core.db import DatabaseManager
            test_db = DatabaseManager()
            if test_db.connection and test_db.connection.is_connected():
                logger.info("데이터베이스 연결 정상")
            else:
                logger.error("데이터베이스 연결 실패")
            test_db.close()
        except Exception as db_error:
            logger.error(f"데이터베이스 연결 확인 중 오류: {db_error}")
        
        # 스트리밍 글로벌 스케줄러 초기화 및 실행
        scheduler = StreamingGlobalScheduler()
        
        # 회사 감지
        scheduler.discover_and_register_companies()
        
        # 헬스 체크 실행
        scheduler.run_health_check()
        
        # 헬스 모니터에서 상세 헬스 체크 수행
        if hasattr(scheduler, 'health_monitor') and scheduler.health_monitor:
            logger.info("상세 헬스 체크 수행")
            health_status = scheduler.health_monitor.perform_health_check()
            
            logger.info(f"전체 헬스 상태: {health_status['overall_status']}")
            
            # 컴포넌트별 상태 출력
            if 'components' in health_status:
                for component, status in health_status['components'].items():
                    status_text = status.get('status', 'unknown')
                    message = status.get('message', '정보 없음')
                    logger.info(f"  {component}: {status_text} - {message}")
        
        # 전체 상태 확인
        status = scheduler.get_status()
        logger.info(f"스케줄러 상태: {status.get('status', 'unknown')}")
        logger.info(f"아키텍처: {status.get('architecture', 'unknown')}")
        
        if 'total_companies' in status:
            logger.info(f"등록된 회사 수: {status['total_companies']}")
            
            for company, info in status.get('companies', {}).items():
                logger.info(f"회사 '{company}': 디바이스 {info.get('devices', 0)}개")
                if info.get('device_list'):
                    logger.info(f"  디바이스 목록: {', '.join(info['device_list'])}")
        
        # 캐시 상태 확인
        if 'cache_summary' in status:
            cache_info = status['cache_summary']
            logger.info(f"전체 캐시: {cache_info.get('total_files', 0)}개 파일, {cache_info.get('total_size_mb', 0)}MB")
        
        if 'error' in status:
            logger.error(f"스케줄러 상태 오류: {status['error']}")
        
        scheduler.stop()
        
    except Exception as e:
        logger.error(f"스트리밍 헬스 체크 오류: {e}")
        import traceback
        logger.error(traceback.format_exc())
    
    logger.info("스트리밍 글로벌 헬스 체크 완료")

def run_status_check():
    """스트리밍 상태 확인"""
    logger.info("스트리밍 시스템 상태 확인 시작")
    
    try:
        scheduler = StreamingGlobalScheduler()
        
        # 초기 설정 실행 (회사 감지)
        scheduler.discover_and_register_companies()
        
        # 상태 정보 출력
        status = scheduler.get_status()
        
        logger.info(f"시스템 상태: {status['status']}")
        logger.info(f"아키텍처: {status.get('architecture', 'streaming')}")
        logger.info(f"총 회사 수: {status['total_companies']}")
        
        if status['total_companies'] > 0:
            logger.info("회사별 상세 정보:")
            for company, info in status['companies'].items():
                logger.info(f"  {company}:")
                logger.info(f"    디바이스 수: {info['devices']}")
                if info['device_list']:
                    logger.info(f"    디바이스 목록: {', '.join(info['device_list'])}")
                else:
                    logger.info(f"    디바이스 목록: 없음")
        else:
            logger.warning("등록된 회사가 없습니다.")
            logger.info("InfluxDB에서 데이터를 확인해주세요.")
        
        # 캐시 상태 정보
        if 'cache_summary' in status:
            cache_info = status['cache_summary']
            logger.info("캐시 현황:")
            logger.info(f"  총 파일 수: {cache_info.get('total_files', 0)}개")
            logger.info(f"  총 크기: {cache_info.get('total_size_mb', 0)}MB")
        
        scheduler.stop()
        
    except Exception as e:
        logger.error(f"상태 확인 오류: {e}")
        import traceback
        logger.error(traceback.format_exc())
    
    logger.info("스트리밍 시스템 상태 확인 완료")

def run_migration_setup():
    """기존 시스템에서 스트리밍 아키텍처로 마이그레이션"""
    logger.info("스트리밍 아키텍처 마이그레이션 시작")
    
    try:
        # 스트리밍 데이터베이스 설정 실행
        from scripts.streamlined_database_setup import create_streamlined_tables, insert_default_streaming_configs, validate_streaming_setup
        
        logger.info("1. 스트리밍 테이블 생성")
        if not create_streamlined_tables():
            logger.error("스트리밍 테이블 생성 실패")
            return False
        
        logger.info("2. 스트리밍 기본 설정 삽입")
        if not insert_default_streaming_configs():
            logger.error("스트리밍 기본 설정 삽입 실패")
            return False
        
        logger.info("3. 스트리밍 설정 검증")
        if not validate_streaming_setup():
            logger.error("스트리밍 설정 검증 실패")
            return False
        
        # 기본 캐시 디렉토리 생성
        logger.info("4. 캐시 디렉토리 생성")
        cache_base = os.path.join(os.getcwd(), "cache")
        os.makedirs(cache_base, exist_ok=True)
        logger.info(f"캐시 디렉토리 생성: {cache_base}")
        
        # 환경변수 확인
        logger.info("5. 환경변수 확인")
        if not validate_streaming_environment():
            logger.error("환경변수 설정 오류")
            return False
        
        logger.info("✅ 스트리밍 아키텍처 마이그레이션 완료!")
        logger.info("")
        logger.info("🚀 다음 단계:")
        logger.info("  1. python main_streaming.py --mode scheduler")
        logger.info("  2. 또는 docker run으로 컨테이너 실행")
        logger.info("")
        logger.info("📊 주요 변경사항:")
        logger.info("  - InfluxDB 직접 조회로 MySQL 부하 감소")
        logger.info("  - 파일 캐싱으로 중간 데이터 효율 관리")
        logger.info("  - 불필요한 테이블 제거로 DB 최적화")
        
        return True
        
    except Exception as e:
        logger.error(f"마이그레이션 오류: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

def run_cache_cleanup_only():
    """캐시 정리만 실행하고 종료"""
    logger.info("캐시 정리 전용 모드 시작")
    
    try:
        scheduler = StreamingGlobalScheduler()
        
        # 회사 감지 (캐시 정리 대상 확인용)
        scheduler.discover_and_register_companies()
        
        # 캐시 정리 실행
        scheduler.run_cache_cleanup()
        
        logger.info("캐시 정리 완료, 프로그램 종료")
        
    except Exception as e:
        logger.error(f"캐시 정리 오류: {e}")
        import traceback
        logger.error(traceback.format_exc())

def run_streaming_scheduler():
    """스트리밍 글로벌 스케줄러 실행"""
    logger.info("스트리밍 글로벌 스케줄러 시작")
    
    scheduler = StreamingGlobalScheduler()
    
    try:
        # 스트리밍 글로벌 스케줄러 시작
        success = scheduler.start()
        
        if success:
            logger.info("스트리밍 글로벌 스케줄러 정상 종료")
        else:
            logger.error("스트리밍 글로벌 스케줄러 비정상 종료")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("사용자에 의한 스트리밍 글로벌 스케줄러 종료")
        scheduler.stop()
    except Exception as e:
        logger.error(f"스트리밍 글로벌 스케줄러 실행 중 오류: {e}")
        scheduler.stop()
        raise

if __name__ == "__main__":
    main()