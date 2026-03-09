# Landing Experiment Summary

- Sample size: 100
- Publishers covered: Advanced Materials, American Association for the Advancement of Science, American Chemical Society, American Institute of Physics, Annual Reviews, Elsevier BV, Georg Thieme Verlag, IOP Publishing, Institute of Electrical and Electronics Engineers, Institute of Physics, Journal of Micro/Nanopatterning Materials and Metrology, Journal of Vacuum Science & Technology B Nanotechnology and Microelectronics Materials Processing Measurement and Phenomena, Multidisciplinary Digital Publishing Institute, Nature Portfolio, Royal Society of Chemistry, Springer Science+Business Media, The Surface Finishing Society of Japan, Wiley
- Legacy success-like count: 97
- Legacy reclassified as non-success: 0

## Counts by Classifier State
- blank_or_incomplete: 1
- challenge_detected: 1
- direct_pdf_handoff: 1
- success_landing: 97

## Representative Failure Reasons
- content_populated: 97
- doi_match: 97
- expected_domain_match: 97
- publisher_marker_present: 93
- strong_meta_present: 85
- fail_block_bot_signal: 1
- url_marker=challenge_or_bot: 1
- direct_pdf_response_observed: 1

## Remaining Weak Spots
- A few publisher shells still finish with too little visible text; the classifier now rejects them, but the browser-side recovery options stay intentionally conservative.
- Challenge pages remain a hard stop. [blocked] No CAPTCHA, Turnstile, or Cloudflare bypass is implemented.
