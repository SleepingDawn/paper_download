import time
import random
from DrissionPage import ChromiumPage
from DrissionPage.common import Settings
from tools_exp import solve_captcha_drission

# 1. 간단한 로거 클래스 (사용하신 코드와의 호환성을 위해)
class SimpleLogger:
    def info(self, msg): print(f"[INFO] {msg}")
    def warning(self, msg): print(f"[WARN] {msg}")
    def error(self, msg): print(f"[ERR] {msg}")

# 3. 테스트 실행 코드
def run_test():
    # 브라우저 설정
    logger = SimpleLogger()
    page = ChromiumPage()
    
    target_url = "https://2captcha.com/demo/cloudflare-turnstile"
    logger.info(f"테스트 페이지 접속: {target_url}")
    
    page.get(target_url)
    time.sleep(3) # 페이지 로딩 대기

    # 캡차 해결 시도
    solve_captcha_drission(page, logger)

    # 4. 최종 결과 검증 (데모 페이지 특화)
    # Turnstile이 해결되면 'cf-turnstile-response' 라는 hidden input에 값이 채워짐
    token_input = page.ele('@name=cf-turnstile-response')
    
    result_text = page.ele('.solver-message') # 2captcha 데모 페이지의 결과 텍스트
    
    print("-" * 30)
    if token_input and token_input.value:
        print(f"✅ 테스트 성공! 생성된 토큰: {token_input.value[:30]}...")
    elif result_text and "success" in result_text.text.lower():
        print("✅ 테스트 성공! (Success 메시지 감지)")
    else:
        print("❌ 테스트 실패: 토큰이 생성되지 않았습니다.")
    print("-" * 30)
    
    # 확인을 위해 잠시 대기
    input("엔터 키를 누르면 브라우저를 닫습니다...")
    page.quit()

if __name__ == "__main__":
    run_test()