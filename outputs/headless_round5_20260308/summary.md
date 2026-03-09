# Headless Round 5 Summary

## Structural Panel
- before counts: {'success_landing': 6, 'publisher_error': 1, 'unknown_non_success': 1, 'challenge_detected': 1}
- after2 counts: {'success_landing': 6, 'publisher_error': 1, 'unknown_non_success': 1, 'challenge_detected': 1}
- p50: 46322.0 ms -> 22178.0 ms
- p90: 54481.0 ms -> 48000.2 ms

## IEEE / Elsevier Focus Panel
- counts: {'success_landing': 8, 'publisher_error': 1}
- elsevier: {'success_landing': 5}
- ieee: {'publisher_error': 1, 'success_landing': 3}

## Diagnostics
- powdermat_manual_headless_direct_open: direct view.php?doi open sampled repeatedly outside the probe and classified as success_landing every time
- ieee_headless_search_diagnostic: searchresult.jsp rendered title/body, but showed "No results found" and "Something went wrong in getting results" with no /document/ anchors exposed
- optica_policy: treated as practical fail-fast target; still challenge_detected in headless
