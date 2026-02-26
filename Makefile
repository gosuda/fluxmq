.PHONY: build run stop clean bench bench-full bench-quick warmup test help status

# FluxMQ 빌드 & 벤치마크 Makefile

BINARY     := ./target/release/fluxmq
JAVA_TESTS := ./fluxmq-java-tests/pom.xml
BENCH_CLASS := com.fluxmq.tests.ComprehensiveBenchmark
PID_FILE   := .fluxmq.pid
RESULT_TMP := /tmp/fluxmq-bench-results.txt

help: ## 도움말
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

build: ## Release 빌드
	cargo build --release

run: build stop ## FluxMQ 서버 시작 (기존 프로세스 종료 후)
	@rm -rf ./data
	@$(BINARY) &
	@sleep 3
	@pgrep fluxmq > $(PID_FILE) && echo "FluxMQ started (PID: $$(cat $(PID_FILE)))" || echo "Failed to start"

stop: ## FluxMQ 서버 종료
	@if pgrep fluxmq > /dev/null 2>&1; then \
		kill $$(pgrep fluxmq) 2>/dev/null; \
		sleep 1; \
		echo "FluxMQ stopped"; \
	else \
		echo "FluxMQ not running"; \
	fi
	@rm -f $(PID_FILE)

clean: stop ## 빌드 + 데이터 정리
	cargo clean
	rm -rf ./data

# ── 벤치마크 ──────────────────────────────────────────

warmup: ## JVM 웜업 2회
	@echo "── Warmup 1/2 ──"
	@mvn exec:java -Dexec.mainClass="$(BENCH_CLASS)" -q -f $(JAVA_TESTS) 2>&1 | grep "처리량:"
	@echo "── Warmup 2/2 ──"
	@mvn exec:java -Dexec.mainClass="$(BENCH_CLASS)" -q -f $(JAVA_TESTS) 2>&1 | grep "처리량:"
	@echo "── Warmup done ──"

bench: ## 벤치마크 1회 (웜업 없이)
	@mvn exec:java -Dexec.mainClass="$(BENCH_CLASS)" -q -f $(JAVA_TESTS) 2>&1 | grep -E "시나리오|처리량:|메시지 수:"

bench-full: run warmup ## 전체 벤치마크 (서버 재시작 + 웜업 2회 + 측정 3회 + 결과 요약)
	@rm -f $(RESULT_TMP)
	@echo ""
	@echo "══════════════════════════════════════"
	@echo "  측정 시작 (3회)"
	@echo "══════════════════════════════════════"
	@for i in 1 2 3; do \
		echo ""; \
		echo "── Run $$i/3 ──"; \
		mvn exec:java -Dexec.mainClass="$(BENCH_CLASS)" -q -f $(JAVA_TESTS) 2>&1 \
			| grep "처리량:" \
			| awk -v run=$$i '{print run, $$0}' \
			| tee -a $(RESULT_TMP); \
	done
	@echo ""
	@echo "══════════════════════════════════════════════════════════════════"
	@echo "  FluxMQ Benchmark Results"
	@echo "══════════════════════════════════════════════════════════════════"
	@echo ""
	@awk ' \
		BEGIN { \
			name[1]="단일스레드 (100K)"; name[2]="멀티스레드4 (100K)"; name[3]="대용량 (500K)"; \
			lo[1]=350000; lo[2]=450000; lo[3]=900000; \
		} \
		{ \
			run=$$1; \
			match($$0, /[0-9,]+[ ]*msg\/sec/); \
			raw=substr($$0, RSTART, RLENGTH); \
			gsub(/,/, "", raw); \
			gsub(/[ ]*msg\/sec/, "", raw); \
			v=int(raw); \
			idx++; s=idx; if(s>3){s=s-3}; if(s>3){s=s-3}; \
			data[s,run]=v; \
		} \
		END { \
			printf "  %-22s %10s %10s %10s │ %10s  %s\n", \
				"시나리오", "Run 1", "Run 2", "Run 3", "중간값", "판정"; \
			printf "  ─────────────────────────────────────────────────────────────\n"; \
			for(i=1;i<=3;i++){ \
				a=data[i,1]; b=data[i,2]; c=data[i,3]; \
				if((a<=b && b<=c)||(c<=b && b<=a)) med=b; \
				else if((b<=a && a<=c)||(c<=a && a<=b)) med=a; \
				else med=c; \
				if(med>=lo[i]) mark="✅"; else mark="❌"; \
				printf "  %-22s %8dK %8dK %8dK │ %8dK  %s\n", \
					name[i], a/1000, b/1000, c/1000, med/1000, mark; \
			} \
			printf "  ─────────────────────────────────────────────────────────────\n"; \
			printf "  기준: 단일 350-400K │ 멀티 450-500K │ 대용량 900K-1.2M\n"; \
		} \
	' $(RESULT_TMP)
	@echo ""
	@rm -f $(RESULT_TMP)

bench-quick: ## 빠른 벤치마크 (웜업 1회 + 측정 1회, 서버 실행 중이어야 함)
	@echo "── Warmup ──"
	@mvn exec:java -Dexec.mainClass="$(BENCH_CLASS)" -q -f $(JAVA_TESTS) 2>&1 | grep "처리량:"
	@echo "── Measure ──"
	@mvn exec:java -Dexec.mainClass="$(BENCH_CLASS)" -q -f $(JAVA_TESTS) 2>&1 | grep -E "시나리오|처리량:"

# ── 테스트 ────────────────────────────────────────────

test: ## Rust 단위 테스트
	cargo test --release

status: ## 서버 상태 확인
	@if pgrep fluxmq > /dev/null 2>&1; then \
		echo "FluxMQ running (PID: $$(pgrep fluxmq))"; \
		curl -s http://localhost:8080/metrics 2>/dev/null | head -5 || true; \
	else \
		echo "FluxMQ not running"; \
	fi
