# Experiment Reports

이 폴더에는 벤치마크용 실험 입력과 결과 보고문을 둡니다.

현재 정리된 문서:

- `benchmark_random100_seed20260311.csv`
  - `ready_to_download.csv`에서 seed `20260311`로 샘플링한 100건 입력
- `benchmark_random100_seed20260311.md`
  - 실행 조건, 실측 결과, 실패 사례 후속 분석, 시사점 정리
- `linux_headless_experiment_plan.md`
  - Linux 서버 headless 실험 설계, 성공/실패 버킷, local_mac 대비 검증 포인트
- `linux_headless_suite/`
  - `pilot_sample.csv`, `full_sample.csv`, `suite_manifest.json`
  - `ready_to_download.csv` 기반 publisher-stratified Linux 실험 입력
- `build_linux_headless_suite.py`
  - 위 suite CSV/manifest를 재생성하는 샘플 빌더
- `run_linux_headless_suite.py`
  - landing probe, 다운로드 실행, 요약 리포트 생성을 연결하는 Linux headless runner
- `summarize_linux_headless_suite.py`
  - landing 결과와 다운로드 결과를 합쳐 publisher별 진단 표를 만드는 요약기
- `../scripts/run_linux_suite_bg.sh`
  - SSH가 끊겨도 유지되는 `nohup` 기반 background launcher
- `../scripts/prepare_linux_server_env.sh`
  - 서버별 `SEED_PROFILE`, `CHROME_PATH`, `PYTHON_BIN`, output root를 `config/linux_server.env`로 기록하는 env 준비 스크립트
- `../scripts/check_linux_suite_status.sh`
  - PID, stage 상태, 최근 로그를 확인하는 상태 점검 스크립트
- `../scripts/tail_linux_suite_logs.sh`
  - root/stage 로그를 `tail -F`로 보는 보조 스크립트
- `../scripts/collect_linux_suite_artifacts.sh`
  - 분석에 필요한 run 산출물을 tar.gz로 묶는 수집 스크립트

권장 순서:

1. `bash scripts/prepare_linux_server_env.sh`
2. `bash scripts/run_linux_suite_bg.sh --suite pilot`
3. `bash scripts/check_linux_suite_status.sh <run-name>`
4. `bash scripts/collect_linux_suite_artifacts.sh <run-name>`
