# Xvfb Local Build Guide (for this server environment)

이 문서는 **sudo 없이 Linux CLI 서버에서 Xvfb를 홈 디렉토리(`$HOME/.local`)에 직접 빌드/설치**하는 과정을, 실제로 성공한 방법 기준으로 정리한 가이드다.  
환경은 대략 다음과 같았다.

- 서버 계정: `yongyong0206`
- 쉘: bash 계열
- OS 계열: RHEL/Fedora 계열로 보임
- 컴파일러: `gcc 12.1.1`
- Meson: `0.62.2`
- Ninja: `1.10.2`
- 사용자 권한만 사용 (`sudo` 없음)

핵심 결론은 다음과 같다.

- `Xvfb`는 시스템에 없었음
- `xauth`는 이미 있었음
- `xkbcomp`는 `/usr/bin/xkbcomp`에 이미 있었음
- `xorg-server`는 **Meson**으로 빌드
- `libXfont2`, `font-util`은 **autotools (`./configure && make && make install`)**로 빌드
- 최종적으로 `~/.local/bin/Xvfb` 실행 성공

---

## 1. 먼저 확인할 것

시작 전에 아래를 확인한다.

```bash
command -v Xvfb
command -v xvfb-run
command -v xauth
command -v xkbcomp
meson --version
ninja --version
gcc --version
pkg-config --version
```

이 환경에서는 결과가 대략 다음과 같았다.

- `Xvfb`: 없음
- `xvfb-run`: 없음
- `xauth`: 있음 (`/usr/bin/xauth`)
- `xkbcomp`: 있음 (`/usr/bin/xkbcomp`)

따라서 시스템 패키지 설치 대신 **로컬 빌드**로 진행했다.

---

## 2. 중요한 개념 정리

### 2.1 venv가 문제였는가?

**주된 문제는 아니었다.**

- `venv`가 문제였던 부분: `pip install --user ...` 가 막힌 것
- 실제 빌드 실패 원인: 의존성(`xfont2`, `fontutil`) 부족, 그리고 프로젝트별 빌드 시스템 차이

즉,

- `xorg-server` → Meson 사용
- `libXfont2`, `font-util` → Meson이 아니라 autotools 사용

### 2.2 홈 디렉토리에 설치한 Xvfb를 venv 안 Python에서 쓸 수 있는가?

**가능하다.**

`Xvfb`는 파이썬 패키지가 아니라 일반 실행파일이므로,

- `PATH`
- `LD_LIBRARY_PATH`
- `DISPLAY`

만 제대로 잡히면 `venv` 안의 Python에서도 그대로 사용할 수 있다.

---

## 3. 실제로 성공한 전체 절차

아래 순서대로 진행했다.

1. `libXfont2` 로컬 빌드/설치
2. `font-util` 로컬 빌드/설치
3. `xorg-server`에서 `Xvfb`만 켜는 최소 옵션으로 Meson 빌드
4. `~/.local/bin/Xvfb` 실행 확인

---

## 4. 공통 환경변수 설정

빌드 전 아래 환경을 잡아두는 것이 좋다.

```bash
mkdir -p "$HOME/tmp" "$HOME/.local" "$HOME/.local/var/lib/xkb"

export TMPDIR="$HOME/tmp"
export TEMP="$HOME/tmp"
export TMP="$HOME/tmp"

export PREFIX="$HOME/.local"
export PATH="$PREFIX/bin:$PATH"
export PKG_CONFIG_PATH="$PREFIX/lib/pkgconfig:$PREFIX/lib64/pkgconfig:$PREFIX/share/pkgconfig:${PKG_CONFIG_PATH:-}"
export LD_LIBRARY_PATH="$PREFIX/lib:$PREFIX/lib64:${LD_LIBRARY_PATH:-}"
```

설명:

- `TMPDIR`: configure/build 중 임시파일 생성 문제 방지
- `PREFIX`: 로컬 설치 위치를 `~/.local`로 통일
- `PKG_CONFIG_PATH`: 직접 설치한 `.pc` 파일을 찾게 함
- `LD_LIBRARY_PATH`: 직접 설치한 `.so` 라이브러리를 실행 시 찾게 함

---

## 5. libXfont2 설치 (필수)

`xorg-server` 빌드가 처음 막힌 핵심 이유는 `xfont2` 부재였다.

### 5.1 다운로드 및 빌드

```bash
cd "$HOME/src"

curl -LO "https://www.x.org/releases/individual/lib/libXfont2-2.0.7.tar.xz"
tar -xf "libXfont2-2.0.7.tar.xz"
cd "libXfont2-2.0.7"

rm -f config.cache
make distclean >/dev/null 2>&1 || true

BUILD_TRIPLET="$(gcc -dumpmachine)"
./configure --prefix="$PREFIX" --build="$BUILD_TRIPLET"
make -j"$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 4)"
make install
```

### 5.2 설치 확인

```bash
pkg-config --modversion xfont2
ls "$PREFIX/lib/pkgconfig" "$PREFIX/lib64/pkgconfig" 2>/dev/null | grep -i xfont2 || true
```

성공 기준:

- `pkg-config --modversion xfont2` → `2.0.7`
- `xfont2.pc` 존재

---

## 6. font-util 설치 (필수)

`xfont2`를 해결한 뒤, 다음 blocker는 `fontutil`이었다.

### 6.1 다운로드 및 빌드

```bash
cd "$HOME/src"

curl -LO "https://www.x.org/releases/individual/font/font-util-1.3.2.tar.gz"
tar -xf "font-util-1.3.2.tar.gz"
cd "font-util-1.3.2"

rm -f config.cache
make distclean >/dev/null 2>&1 || true

BUILD_TRIPLET="$(gcc -dumpmachine)"
./configure --prefix="$PREFIX" --build="$BUILD_TRIPLET" --with-fontrootdir="$PREFIX/share/fonts/X11"
make -j"$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 4)"
make install
```

### 6.2 설치 확인

```bash
pkg-config --modversion fontutil
pkg-config --variable=fontrootdir fontutil
ls "$PREFIX/lib/pkgconfig" "$PREFIX/lib64/pkgconfig" 2>/dev/null | grep -i fontutil
```

성공 기준:

- `pkg-config --modversion fontutil` → `1.3.2`
- `pkg-config --variable=fontrootdir fontutil` → `~/.local/share/fonts/X11`
- `fontutil.pc` 존재

---

## 7. xorg-server에서 Xvfb 빌드

이제 필요한 의존성이 맞춰졌으므로 `xorg-server`를 Meson으로 빌드한다.

### 7.1 다운로드

```bash
cd "$HOME/src"

curl -LO "https://www.x.org/releases/individual/xserver/xorg-server-21.1.21.tar.xz"
tar -xf "xorg-server-21.1.21.tar.xz"
cd "xorg-server-21.1.21"
```

이미 받아둔 경우 다운로드는 생략 가능하다.

### 7.2 Meson setup

```bash
cd "$HOME/src/xorg-server-21.1.21"
rm -rf build

meson setup build \
  --prefix="$PREFIX" \
  -Dxvfb=true \
  -Dxorg=false \
  -Dxephyr=false \
  -Dxnest=false \
  -Ddocs=false \
  -Ddevel-docs=false \
  -Ddocs-pdf=false \
  -Dxkb_bin_dir=/usr/bin \
  -Dxkb_dir=/usr/share/X11/xkb \
  -Dxkb_output_dir="$HOME/.local/var/lib/xkb"
```

### 7.3 compile / install

```bash
meson compile -C build
meson install -C build
```

### 7.4 왜 이런 옵션을 썼는가?

- `-Dxvfb=true`: Xvfb 빌드 활성화
- `-Dxorg=false`: 일반 Xorg 서버는 빌드하지 않음
- `-Dxephyr=false`, `-Dxnest=false`: 불필요한 다른 X 서버 끔
- `-Ddocs=false`, `-Ddevel-docs=false`, `-Ddocs-pdf=false`: 문서 빌드 비활성화
- `-Dxkb_bin_dir=/usr/bin`: 시스템에 이미 있는 `/usr/bin/xkbcomp` 사용
- `-Dxkb_dir=/usr/share/X11/xkb`: 시스템 XKB 데이터 사용
- `-Dxkb_output_dir=...`: 사용자 writable 디렉토리 사용

### 7.5 성공 로그의 핵심 포인트

성공 시 setup 단계에서 아래 같은 줄이 보여야 한다.

- `Run-time dependency xfont2 found: YES 2.0.7`
- `Run-time dependency fontutil found: YES 1.3.2`
- `Build targets in project: ...`

그리고 compile/install 단계 마지막에 아래 같은 줄이 보여야 한다.

- `Linking target hw/vfb/Xvfb`
- `Installing hw/vfb/Xvfb to /home/.../.local/bin`

---

## 8. 설치 확인

```bash
export PATH="$HOME/.local/bin:$PATH"
export LD_LIBRARY_PATH="$HOME/.local/lib:$HOME/.local/lib64:${LD_LIBRARY_PATH:-}"

command -v Xvfb
```

성공 기준:

```bash
/home/yongyong0206/.local/bin/Xvfb
```

주의: 이 빌드의 `Xvfb`는 `-version` 옵션을 받지 않았다.  
따라서 `Xvfb -version`은 실패할 수 있으며, 이것은 이상이 아니다.

---

## 9. 실제 실행 테스트

```bash
export PATH="$HOME/.local/bin:$PATH"
export LD_LIBRARY_PATH="$HOME/.local/lib:$HOME/.local/lib64:${LD_LIBRARY_PATH:-}"

Xvfb :99 -screen 0 1280x1024x24 &
XVFB_PID=$!
export DISPLAY=:99

xdpyinfo >/dev/null 2>&1 && echo "Xvfb OK" || echo "Xvfb failed"

kill "$XVFB_PID"
```

이 환경에서 실제 결과는:

```bash
Xvfb OK
```

즉, 정상 동작했다.

---

## 10. 실행 시 보였던 경고들

실행 시 이런 메시지가 나왔다.

```text
_XSERVTransmkdir: ERROR: euid != 0,directory /tmp/.X11-unix will not be created.
The XKEYBOARD keymap compiler (xkbcomp) reports:
> Warning:          Could not resolve keysym XF86EmojiPicker
Errors from xkbcomp are not fatal to the X server
```

해석:

### 10.1 `/tmp/.X11-unix` 관련 경고

- root가 아니라서 `/tmp/.X11-unix`를 직접 만들지 못했다는 뜻
- 하지만 실제로 `Xvfb OK`가 나왔으므로 **치명적이지 않았음**

### 10.2 `XF86EmojiPicker` 관련 경고

- 특정 keysym을 못 찾았다는 경고
- 로그에도 나오듯 **fatal 아님**
- 일반적인 브라우저 자동화/GUI 실행에서는 보통 큰 문제 없음

---

## 11. 실사용 방법

### 11.1 가장 기본적인 사용 예

```bash
export PATH="$HOME/.local/bin:$PATH"
export LD_LIBRARY_PATH="$HOME/.local/lib:$HOME/.local/lib64:${LD_LIBRARY_PATH:-}"

Xvfb :99 -screen 0 1280x1024x24 &
XVFB_PID=$!
export DISPLAY=:99

python your_script.py

kill "$XVFB_PID"
```

### 11.2 Selenium 실행 전 사용 예

```bash
export PATH="$HOME/.local/bin:$PATH"
export LD_LIBRARY_PATH="$HOME/.local/lib:$HOME/.local/lib64:${LD_LIBRARY_PATH:-}"

Xvfb :99 -screen 0 1365x1024x24 &
XVFB_PID=$!
export DISPLAY=:99

python run_selenium.py

kill "$XVFB_PID"
```

---

## 12. 편의용 shell 함수

매번 길게 입력하기 귀찮다면 `~/.bashrc` 등에 아래를 넣어둘 수 있다.

```bash
start_xvfb () {
  export PATH="$HOME/.local/bin:$PATH"
  export LD_LIBRARY_PATH="$HOME/.local/lib:$HOME/.local/lib64:${LD_LIBRARY_PATH:-}"
  Xvfb :99 -screen 0 1280x1024x24 &
  export XVFB_PID=$!
  export DISPLAY=:99
  echo "DISPLAY=$DISPLAY PID=$XVFB_PID"
}

stop_xvfb () {
  kill "$XVFB_PID"
  unset XVFB_PID
  unset DISPLAY
}
```

사용:

```bash
start_xvfb
python your_script.py
stop_xvfb
```

---

## 13. Python 코드 안에서 직접 Xvfb 띄우기

```python
import os
import subprocess
import time

home = os.path.expanduser("~")
env = os.environ.copy()
env["PATH"] = f"{home}/.local/bin:" + env.get("PATH", "")
env["LD_LIBRARY_PATH"] = (
    f"{home}/.local/lib:{home}/.local/lib64:" + env.get("LD_LIBRARY_PATH", "")
)

proc = subprocess.Popen(
    ["Xvfb", ":99", "-screen", "0", "1280x1024x24"],
    env=env,
)

time.sleep(1)
os.environ["DISPLAY"] = ":99"

# 여기서 selenium / playwright / GUI 프로그램 실행

proc.terminate()
proc.wait()
```

---

## 14. 실패했을 때 체크 포인트

### 14.1 `xorg-server` Meson setup이 실패할 때

```bash
sed -n '1,260p' build/meson-logs/meson-log.txt
grep -Ei 'Dependency|not found|found: NO|error:' build/meson-logs/meson-log.txt
```

### 14.2 `pkg-config`가 로컬 설치한 라이브러리를 못 찾을 때

```bash
echo "$PKG_CONFIG_PATH"
pkg-config --modversion xfont2
pkg-config --modversion fontutil
ls "$HOME/.local/lib/pkgconfig"
```

### 14.3 autotools configure가 임시 디렉토리 문제로 실패할 때

```bash
mkdir -p "$HOME/tmp"
export TMPDIR="$HOME/tmp"
export TEMP="$HOME/tmp"
export TMP="$HOME/tmp"
```

### 14.4 `Xvfb: command not found`가 뜰 때

```bash
export PATH="$HOME/.local/bin:$PATH"
command -v Xvfb
```

### 14.5 실행은 되는데 라이브러리 문제 의심 시

```bash
export LD_LIBRARY_PATH="$HOME/.local/lib:$HOME/.local/lib64:${LD_LIBRARY_PATH:-}"
```

---

## 15. 시스템 패키지 자동 설치 프롬프트 관련 주의

이 서버에서는 `Xvfb` 명령이 없을 때 자동으로 아래 같은 프롬프트가 떴다.

```text
Install package 'xorg-x11-server-Xvfb' to provide command 'Xvfb'? [N/y]
```

이 경로는 결국 인증 단계에서 막혔으므로, **사용자 권한만 있는 경우 의미가 없다.**  
따라서 로컬 빌드가 완료되기 전까지는 이 프롬프트에 기대지 말고, `N` 또는 Enter로 넘기는 편이 낫다.

---

## 16. 한 번에 다시 따라하는 요약 절차

### 16.1 공통 환경

```bash
mkdir -p "$HOME/src" "$HOME/tmp" "$HOME/.local" "$HOME/.local/var/lib/xkb"

export TMPDIR="$HOME/tmp"
export TEMP="$HOME/tmp"
export TMP="$HOME/tmp"

export PREFIX="$HOME/.local"
export PATH="$PREFIX/bin:$PATH"
export PKG_CONFIG_PATH="$PREFIX/lib/pkgconfig:$PREFIX/lib64/pkgconfig:$PREFIX/share/pkgconfig:${PKG_CONFIG_PATH:-}"
export LD_LIBRARY_PATH="$PREFIX/lib:$PREFIX/lib64:${LD_LIBRARY_PATH:-}"
```

### 16.2 libXfont2

```bash
cd "$HOME/src"
curl -LO "https://www.x.org/releases/individual/lib/libXfont2-2.0.7.tar.xz"
tar -xf "libXfont2-2.0.7.tar.xz"
cd "libXfont2-2.0.7"
rm -f config.cache
make distclean >/dev/null 2>&1 || true
BUILD_TRIPLET="$(gcc -dumpmachine)"
./configure --prefix="$PREFIX" --build="$BUILD_TRIPLET"
make -j"$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 4)"
make install
pkg-config --modversion xfont2
```

### 16.3 font-util

```bash
cd "$HOME/src"
curl -LO "https://www.x.org/releases/individual/font/font-util-1.3.2.tar.gz"
tar -xf "font-util-1.3.2.tar.gz"
cd "font-util-1.3.2"
rm -f config.cache
make distclean >/dev/null 2>&1 || true
BUILD_TRIPLET="$(gcc -dumpmachine)"
./configure --prefix="$PREFIX" --build="$BUILD_TRIPLET" --with-fontrootdir="$PREFIX/share/fonts/X11"
make -j"$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 4)"
make install
pkg-config --modversion fontutil
```

### 16.4 xorg-server / Xvfb

```bash
cd "$HOME/src"
curl -LO "https://www.x.org/releases/individual/xserver/xorg-server-21.1.21.tar.xz"
tar -xf "xorg-server-21.1.21.tar.xz"
cd "xorg-server-21.1.21"
rm -rf build

meson setup build \
  --prefix="$PREFIX" \
  -Dxvfb=true \
  -Dxorg=false \
  -Dxephyr=false \
  -Dxnest=false \
  -Ddocs=false \
  -Ddevel-docs=false \
  -Ddocs-pdf=false \
  -Dxkb_bin_dir=/usr/bin \
  -Dxkb_dir=/usr/share/X11/xkb \
  -Dxkb_output_dir="$HOME/.local/var/lib/xkb"

meson compile -C build
meson install -C build
```

### 16.5 실행 테스트

```bash
export PATH="$HOME/.local/bin:$PATH"
export LD_LIBRARY_PATH="$HOME/.local/lib:$HOME/.local/lib64:${LD_LIBRARY_PATH:-}"

command -v Xvfb

Xvfb :99 -screen 0 1280x1024x24 &
XVFB_PID=$!
export DISPLAY=:99

xdpyinfo >/dev/null 2>&1 && echo "Xvfb OK" || echo "Xvfb failed"

kill "$XVFB_PID"
```

---

## 17. 최종 결론

이 서버 환경에서는 **sudo 없이도 Xvfb를 홈 디렉토리에 성공적으로 로컬 빌드/설치**할 수 있었다.  
성공의 핵심은 다음 세 가지였다.

1. `xorg-server`만 보지 말고, 먼저 `libXfont2`와 `font-util`을 로컬 설치할 것
2. `xorg-server`는 Meson, 나머지는 autotools라는 점을 구분할 것
3. `PATH`, `PKG_CONFIG_PATH`, `LD_LIBRARY_PATH`, `DISPLAY`를 정확히 잡을 것

최종 설치 위치:

- `Xvfb`: `~/.local/bin/Xvfb`
- `libXfont2`: `~/.local/lib`
- `xfont2.pc`, `fontutil.pc`: `~/.local/lib/pkgconfig`

이후에는 `venv` 안의 Python에서도 그대로 사용할 수 있다.
