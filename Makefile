tests:
	clojure -A:dev:test:clj-tests -J-Dguardrails.config=guardrails-test.edn -J-Dguardrails.enabled

dev:
	clojure -A:dev:test:clj-tests -J-Dguardrails.config=guardrails-test.edn -J-Dguardrails.enabled --watch --fail-fast --no-capture-output
