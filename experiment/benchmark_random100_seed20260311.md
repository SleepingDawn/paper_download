# Random 100 Benchmark

## 목적

메인 다운로드 로직을 실제 입력 분포에서 측정하기 위해 `ready_to_download.csv` 1109건 중 100건을 무작위 샘플링해 실행했다.

이 문서는 아래를 기록한다.

- 실험 입력
- 실행 조건
- 실측 결과
- 실패 사례 후속 분석
- 현재 단계의 시사점

## 실험 입력

- 원본 입력: `ready_to_download.csv`
- 원본 크기: `1109`건
- 샘플링 seed: `20260311`
- 샘플 CSV: `experiment/benchmark_random100_seed20260311.csv`

샘플링 후 실행에 사용한 주요 산출물:

- 실행 요약: `outputs/benchmark_random100_seed20260311/summary.json`
- 실패 목록: `outputs/benchmark_random100_seed20260311/failed_papers.jsonl`
- 시도별 기록: `outputs/benchmark_random100_seed20260311/download_attempts.jsonl`

## 실행 조건

실행 시점:

- 날짜: `2026-03-11`
- 환경: local desktop

실행 명령:

```bash
python3 -u parallel_download.py \
  --doi_path experiment/benchmark_random100_seed20260311.csv \
  --max_workers 2 \
  --headless 1 \
  --deep-retry-headless 0 \
  --precheck-landing 0 \
  --abort-on-landing-block 1 \
  --after-first-pass deep \
  --output_dir benchmark_random100_seed20260311 \
  --pdf_output_dir pdfs/benchmark_random100_seed20260311 \
  --non-interactive
```

설정 해석:

- 1차 패스: `headless`
- deep retry: `headful`
- 별도 landing precheck 없음
- landing에서 challenge/block이 보이면 즉시 중단
- publisher pacing 유지
  - cooldown `7.0s`
  - global start spacing `1.5s`
  - jitter `0.7s ~ 1.8s`

## 실측 결과

요약:

- 총 논문 수: `100`
- 총 소요 시간: `1447.63초` (`24분 7.63초`)
- 논문 1건당 평균 시간: `14.48초`
- 1차 패스 성공: `98`
- deep retry 추가 성공: `0`
- 최종 다운로드 성공: `98 / 100 = 98.0%`

랜딩 관련:

- `precheck-landing=0` 실행이라 integrated landing이 실제로 시도된 건만 집계된다.
- integrated landing 시도: `30`
- integrated landing 성공: `30`
- integrated landing 성공률: `100%`

실패 이유 분포:

- 1차 패스
  - `FAIL_ACCESS_RIGHTS: 1`
  - `FAIL_TIMEOUT/NETWORK: 1`
- deep retry
  - `FAIL_ACCESS_RIGHTS: 1`
  - `FAIL_TIMEOUT/NETWORK: 1`

관찰:

- 이 샘플에서는 deep retry가 성공률을 더 올리지는 못했다.
- 즉, 당시 남은 실패는 일반적인 retry 부족보다 publisher-specific failure였다.

## 실패 사례 후속 분석

벤치마크 직후 남은 실패는 2건이었다.

### 1. Elsevier `10.1016/j.jpcs.2023.111747`

초기 실패 원인:

- 첫 실패는 코드 버그였다.
  - `urlunparse` 미-import로 `FAIL_TIMEOUT/NETWORK`
- deep retry에서는 ScienceDirect PDF 시도 직전 full-page screenshot 이후 Chrome/Drission 연결이 끊겼다.
  - macOS crash report가 실제로 생성됐다.
  - 로그에는 `与页面的连接已断开`이 반복됐다.

적용한 수정:

- `urlunparse` import 복구
- browser disconnect 예외를 별도로 감지하고 즉시 세션 재생성
- Elsevier pre-PDF screenshot을 기본 비활성화
- Elsevier full-page screenshot 대신 필요할 때만 viewport capture 사용

후속 검증:

- 재검증 입력: `outputs/benchmark_failure_reprobe.csv`
- 재검증 결과: `headless 1차`에서 성공
- 결과 로그: `outputs/benchmark_failure_reprobe/Closed_Access/logs/download_log_10.1016_j.jpcs.2023.111747.pdf.pdf.txt`

현재 판단:

- 이 케이스는 해결됨
- 실패 원인은 publisher challenge보다 로직/안정성 문제였다

### 2. SPIE `10.1117/12.2551492`

확인된 사실:

- article landing 자체는 성공한다
- 실패는 landing 이후 PDF asset 단계에서 발생한다
- direct `.pdf` endpoint는 browser cookie를 실어도 HTML gate를 반환할 수 있다
- 로그상 direct PDF 단계는 `FAIL_VIEWER_HTML`로 귀결됐다

스크린샷/로그 해석:

- benchmark screenshot에서는 article page가 열려 있고 PDF 관련 UI도 보인다
- 그러나 direct `.pdf` 또는 `.short` 경로는 HTML 응답으로 돌아온다
- 후속 probe에서도 `button-click-candidate`, `navigation-preauth-cffi`까지는 실행됐지만 최종 asset은 여전히 HTML gate였다

적용한 수정:

- PDF 버튼 클릭 뒤 열린 새 탭/새 viewer를 이후 로직이 이어받도록 변경
- direct PDF navigation 전에 cookie-aware CFFI 회수를 먼저 시도하도록 순서 조정
- SPIE 같은 케이스에서도 button-click 이후 candidate recovery가 실제로 돌도록 연결

후속 검증:

- 재검증 입력: `outputs/benchmark_failure_reprobe.csv`
- 추가 단일 probe: `outputs/spie_single_probe_after_click_patch/summary.json`
- 현재 상태: 여전히 실패

현재 판단:

- 이 케이스는 아직 미해결
- 문제의 핵심은 랜딩이 아니라 SPIE PDF asset 단계의 HTML gate
- 범용 retry만으로는 해결되지 않는다

## 시사점

이번 벤치마크에서 얻은 결론은 다음과 같다.

1. 메인 파이프라인은 현재 샘플 기준으로 `98%`까지는 안정적으로 도달한다.
2. 남은 실패는 랜딩 일반론보다 publisher-specific asset gate에 더 가깝다.
3. Elsevier 쪽 불안정성은 로직 수정으로 실제 해소됐다.
4. SPIE는 `article landing 성공`과 `PDF asset 획득 성공`을 분리해서 봐야 한다.
5. 향후 우선순위는 범용 retry 확대가 아니라 SPIE 전용 asset handoff 또는 gate 우회 전략이다.

## 현재 액션 아이템

- SPIE article page에서 실제 PDF asset을 여는 intermediate viewer 또는 signed endpoint가 더 있는지 확인
- direct `.pdf` 외 다른 viewer handoff가 있는 경우 SPIE domain branch 추가
- 현재 벤치마크 수치 업데이트가 필요하면 이 문서를 기준으로 갱신
