#!/usr/bin/env python
"""
JVM 메트릭 기반 시스템 리소스 예측 - 메인 애플리케이션 엔트리포인트
GlobalScheduler를 사용한 멀티 테넌트 배치 애플리케이션
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
from scheduler.global_scheduler import GlobalScheduler

def validate_environment():
    """환경변수 유효성 검사"""
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
    parser = argparse.ArgumentParser(description='JVM 메트릭 예측 글로벌 배치 애플리케이션')
    parser.add_argument('--mode', choices=['scheduler', 'health-check', 'config-test', 'status'], 
                       default='scheduler', help='실행 모드')
    parser.add_argument('--dry-run', action='store_true', help='실제 실행 없이 설정만 확인')
    
    args = parser.parse_args()
    
    try:
        # 로깅 설정
        setup_logging()
        
        # 글로벌 로그 컨텍스트 설정
        set_context(company_domain="global")
        
        logger.info("JVM 메트릭 예측 글로벌 시스템 시작")
        logger.info(f"실행 모드: {args.mode}")
        
        # 모드별 실행
        if args.mode == 'config-test':
            run_config_test()
        elif args.mode == 'health-check':
            if not validate_environment():
                sys.exit(1)
            run_health_check()
        elif args.mode == 'status':
            if not validate_environment():
                sys.exit(1)
            run_status_check()
        elif args.mode == 'scheduler':
            if args.dry_run:
                logger.info("드라이런 모드: 글로벌 스케줄러 설정만 확인")
                scheduler = GlobalScheduler()
                logger.info("글로벌 스케줄러 초기화 성공")
                status = scheduler.get_status()
                logger.info(f"시스템 상태: {status}")
                return
            else:
                if not validate_environment():
                    sys.exit(1)
                run_global_scheduler()
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
    """설정 테스트"""
    logger.info("글로벌 설정 테스트 시작")
    
    # 환경변수 확인
    logger.info("환경변수 확인:")
    logger.info(f"MySQL 호스트: {os.getenv('MYSQL_HOST', 'localhost')}")
    logger.info(f"MySQL 데이터베이스: {os.getenv('MYSQL_DATABASE', 'jvm_metrics')}")
    logger.info(f"InfluxDB URL: {os.getenv('INFLUXDB_URL', 'http://localhost:8086')}")
    logger.info(f"InfluxDB 조직: {os.getenv('INFLUXDB_ORG', 'javame')}")
    
    # 스케줄 설정 확인
    logger.info("스케줄 설정:")
    logger.info(f"회사 감지 간격: {os.getenv('DISCOVERY_INTERVAL', '60')}분")
    logger.info(f"데이터 수집 간격: {os.getenv('DATA_COLLECTION_INTERVAL', '30')}분")
    logger.info(f"모델 학습 간격: {os.getenv('MODEL_TRAINING_INTERVAL', '360')}분")
    logger.info(f"예측 간격: {os.getenv('PREDICTION_INTERVAL', '60')}분")
    logger.info(f"헬스 체크 간격: {os.getenv('HEALTH_CHECK_INTERVAL', '15')}분")
    
    # 처리 설정 확인
    logger.info("처리 설정:")
    logger.info(f"최대 동시 처리 회사 수: {os.getenv('MAX_WORKERS', '3')}")
    logger.info(f"배치 크기: {os.getenv('BATCH_SIZE', '1000')}")
    logger.info(f"즉시 실행 여부: {os.getenv('RUN_IMMEDIATE', 'true')}")
    
    logger.info("글로벌 설정 테스트 완료")

def run_health_check():
    """헬스 체크 실행"""
    logger.info("글로벌 헬스 체크 시작")
    
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
        
        # 글로벌 스케줄러 초기화 및 실행
        scheduler = GlobalScheduler()
        
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
        
        if 'total_companies' in status:
            logger.info(f"등록된 회사 수: {status['total_companies']}")
            
            for company, info in status.get('companies', {}).items():
                logger.info(f"회사 '{company}': 디바이스 {info.get('devices', 0)}개")
                if info.get('device_list'):
                    logger.info(f"  디바이스 목록: {', '.join(info['device_list'])}")
        else:
            logger.warning("회사 정보를 가져올 수 없습니다")
            
        if 'error' in status:
            logger.error(f"스케줄러 상태 오류: {status['error']}")
        
        scheduler.stop()
        
    except Exception as e:
        logger.error(f"헬스 체크 오류: {e}")
        import traceback
        logger.error(traceback.format_exc())
    
    logger.info("글로벌 헬스 체크 완료")

def run_status_check():
    """상태 확인"""
    logger.info("시스템 상태 확인 시작")
    
    try:
        scheduler = GlobalScheduler()
        
        # 초기 설정 실행 (회사 감지)
        scheduler.discover_and_register_companies()
        
        # 상태 정보 출력
        status = scheduler.get_status()
        
        logger.info(f"시스템 상태: {status['status']}")
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
        
        scheduler.stop()
        
    except Exception as e:
        logger.error(f"상태 확인 오류: {e}")
        import traceback
        logger.error(traceback.format_exc())
    
    logger.info("시스템 상태 확인 완료")

def run_global_scheduler():
    """글로벌 스케줄러 실행"""
    logger.info("글로벌 스케줄러 시작")
    
    scheduler = GlobalScheduler()
    
    try:
        # 글로벌 스케줄러 시작
        success = scheduler.start()
        
        if success:
            logger.info("글로벌 스케줄러 정상 종료")
        else:
            logger.error("글로벌 스케줄러 비정상 종료")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("사용자에 의한 글로벌 스케줄러 종료")
        scheduler.stop()
    except Exception as e:
        logger.error(f"글로벌 스케줄러 실행 중 오류: {e}")
        scheduler.stop()
        raise

if __name__ == "__main__":
    main()