# Linux Seeded Profile Setup

`linux_cli_seeded`는 macOS Chrome profile이 아니라 Linux에서 생성한 Chrome `user-data-dir` 루트를 기대합니다.

즉 서버에 가져갈 경로는 아래처럼 보여야 합니다.

```text
linux_chrome_user_data/
  Local State
  First Run                # 있으면 포함
  Last Version             # 있으면 포함
  Default/
    Preferences
    Network/Cookies
    Local Storage/
    IndexedDB/
```

## 권장 방식

맥북에서 Linux VM을 띄운 뒤, 그 안에서 전용 Chrome profile을 warm 상태로 만들고 tar로 묶어 서버로 복사합니다.

## 1. Ubuntu VM 준비

UTM, Parallels, VMware 중 하나로 Ubuntu Desktop VM을 만듭니다.

- 권장: Ubuntu 24.04 LTS 또는 22.04 LTS
- 최소 자원: 2 vCPU, 4 GB RAM, 20 GB disk
- 가능하면 서버와 비슷한 Chrome major version을 사용
- repo를 VM 안에 clone하거나, host 폴더를 VM에 공유해 아래 스크립트를 실행할 수 있게 준비

## 2. VM 안에 Chrome 설치

Ubuntu 예시:

```bash
sudo apt-get update
sudo apt-get install -y wget gnupg
wget -qO- https://dl.google.com/linux/linux_signing_key.pub | sudo gpg --dearmor -o /usr/share/keyrings/google-chrome.gpg
echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-chrome.gpg] http://dl.google.com/linux/chrome/deb/ stable main" | sudo tee /etc/apt/sources.list.d/google-chrome.list
sudo apt-get update
sudo apt-get install -y google-chrome-stable
```

`google-chrome-stable`가 없다면 `chromium-browser`로 대체할 수 있지만, 서버 실행 환경과 최대한 맞추는 편이 낫습니다.

## 3. 전용 Linux profile warm up

아래처럼 별도 `user-data-dir`로 실행합니다.

```bash
export SEED_ROOT="$HOME/chrome-seeds/snu_sciencedirect_seed"
mkdir -p "$SEED_ROOT"

google-chrome \
  --user-data-dir="$SEED_ROOT" \
  --profile-directory=Default \
  --no-first-run \
  --no-default-browser-check \
  --password-store=basic
```

브라우저에서 다음을 수행합니다.

1. 기관 인증 또는 SSO 로그인
2. ScienceDirect article landing 몇 건 열기
3. View PDF 또는 PDF viewer까지 몇 건 열기
4. 필요하면 IEEE, Springer 등 자주 쓸 publisher도 같이 열어 상태를 저장

중요:

- warm up이 끝나면 Chrome을 정상 종료해야 합니다.
- 살아 있는 Chrome 프로세스가 없는지 확인한 뒤 복사해야 합니다.

예시:

```bash
pgrep -a chrome
```

## 4. profile 구조 점검

이 repo의 검사 스크립트로 구조를 먼저 확인할 수 있습니다.

```bash
python3 scripts/check_linux_seed_profile.py \
  --profile-root "$SEED_ROOT" \
  --profile-name Default
```

성공 기준은 적어도 다음입니다.

- 루트에 `Default/`가 있음
- `Default/Preferences`가 있음
- 가능하면 `Local State`가 있음
- 가능하면 `Network/Cookies` 또는 `Cookies`, `Local Storage`, `IndexedDB`가 있음

## 5. tar bundle 만들기

스크립트는 락/캐시류를 제외하고, 현재 코드가 기대하는 형태의 bundle을 만듭니다.

가장 간단한 방법:

```bash
bash scripts/build_linux_seed_bundle.sh \
  "$SEED_ROOT" \
  "$HOME/linux_chrome_user_data_seed.tar.gz" \
  Default
```

수동으로 나눠서 실행하려면:

```bash
python3 scripts/package_linux_seed_profile.py \
  --source-root "$SEED_ROOT" \
  --output "$HOME/linux_chrome_user_data_seed.tar.gz" \
  --profile-name Default
```

이 archive는 내부에 최상위 폴더 하나를 포함합니다. 그 폴더 안에 `Default/`, `Local State`, `.codex_profile_seed_ready` 등이 들어갑니다.

## 6. 서버로 복사하고 풀기

```bash
scp "$HOME/linux_chrome_user_data_seed.tar.gz" user@server:/path/to/

ssh user@server
cd /path/to
tar -xzf linux_chrome_user_data_seed.tar.gz
```

예를 들어 archive 이름이 `linux_chrome_user_data_seed.tar.gz`였다면, 압축 해제 후 사용할 경로는 보통:

```text
/path/to/linux_chrome_user_data_seed
```

서버에서 다시 한 번 점검:

```bash
python3 scripts/check_linux_seed_profile.py \
  --profile-root /path/to/linux_chrome_user_data_seed \
  --profile-name Default
```

## 7. 서버 실행

```bash
python3 -u parallel_download.py \
  --runtime-preset linux_cli_seeded \
  --persistent-profile-dir /path/to/linux_chrome_user_data_seed \
  --profile-name Default \
  --doi_path ready_to_download.csv \
  --max_workers 1 \
  --non-interactive
```

필요하면 랜딩만 먼저 확인:

```bash
python3 -u landing_access_repro.py \
  --input ready_to_download.csv \
  --runtime-preset linux_cli_seeded \
  --persistent-profile-dir /path/to/linux_chrome_user_data_seed \
  --profile-name Default
```

## 운영 메모

- macOS 기본 Chrome profile을 그대로 복사해서 쓰는 것은 현재 코드와 맞지 않습니다.
- full profile을 뜯어오는 방식이라도, Elsevier는 최종적으로 서버 IP 평판 때문에 challenge가 날 수 있습니다.
- profile은 다운로드 런마다 런타임 clone으로 복사되므로, seed root 자체는 비교적 안전한 원본으로 유지하는 편이 좋습니다.
- profile을 다시 갱신하고 싶으면 VM에서 추가 warm up 후 새 tar를 다시 만들어 교체하면 됩니다.
