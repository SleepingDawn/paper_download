# Experiment Reports

이 폴더에는 벤치마크용 실험 입력과 결과 보고문을 둡니다.

현재 정리된 문서:

- `benchmark_random100_seed20260311.csv`
  - `ready_to_download.csv`에서 seed `20260311`로 샘플링한 100건 입력
- `benchmark_random100_seed20260311.md`
  - 실행 조건, 실측 결과, 실패 사례 후속 분석, 시사점 정리
- `publisher_triage_benchmark_20260313.csv`
  - 주요 publisher를 균형 있게 뽑아 landing / viewer / direct control 문제를 분리해서 보는 입력
- `publisher_triage_benchmark_20260313.md`
  - 생성 규칙, 권장 실행 명령, publisher별 분석 포인트 정리
- `elsevier_smoke_download_20260313.csv`
  - Linux seeded profile이 실제로 Elsevier landing / PDF discovery를 통과하는지 빠르게 확인하는 2건 smoke 입력
- `publisher_download_benchmark_20260314.csv`
  - publisher별 end-to-end download를 비교하기 위한 서버용 benchmark 입력
- `publisher_download_benchmark_20260314.md`
  - 권장 detached 실행 명령과 분석 포인트 정리
  - `scripts/run_publisher_benchmark_detached.sh`로 전체 또는 특정 publisher만 바로 실행 가능
