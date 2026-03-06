# First-pass Failure Analysis (top100, workers=4)

- success: 84 / 100
- failed: 16 / 100
- success_ratio: 84.00%

## Failure reason counts
- FAIL_NO_CANDIDATE: 10
- FAIL_BLOCK: 6

## Failure stage counts
- drission: 10
- landing: 6

## Failure by publisher
- Elsevier BV: 6
- American Institute of Physics: 4
- American Chemical Society: 4
- nan: 1
- Springer Science+Business Media: 1

## Failure list
| doi | publisher | reason | stage | primary_evidence |
|---|---|---|---|---|
| 10.1007/s12598-024-02864-w | Springer Science+Business Media | FAIL_NO_CANDIDATE | drission | pdf_link_not_found_or_download_failed |
| 10.1016/j.ccr.2024.215870 | Elsevier BV | FAIL_NO_CANDIDATE | drission | budget_exceeded_before_download |
| 10.1016/j.ccr.2024.215942 | Elsevier BV | FAIL_NO_CANDIDATE | drission | budget_exceeded_before_download |
| 10.1016/j.dche.2024.100140 | Elsevier BV | FAIL_NO_CANDIDATE | drission | budget_exceeded_before_download |
| 10.1016/j.ijmachtools.2024.104173 | Elsevier BV | FAIL_NO_CANDIDATE | drission | pdf_link_not_found_or_download_failed |
| 10.1016/j.jmst.2024.04.016 | Elsevier BV | FAIL_NO_CANDIDATE | drission | pdf_link_not_found_or_download_failed |
| 10.1016/j.susmat.2024.e00935 | Elsevier BV | FAIL_NO_CANDIDATE | drission | budget_exceeded_before_download |
| 10.1021/acsami.3c14821 | American Chemical Society | FAIL_BLOCK | landing | keyword=forbidden |
| 10.1021/acsnano.3c10495 | American Chemical Society | FAIL_BLOCK | landing | keyword=forbidden |
| 10.1021/acsomega.3c08717 | American Chemical Society | FAIL_BLOCK | landing | keyword=forbidden |
| 10.1021/jacs.3c12630 | American Chemical Society | FAIL_BLOCK | landing | keyword=forbidden |
| 10.1063/5.0188699 | American Institute of Physics | FAIL_BLOCK | landing | keyword=forbidden |
| 10.1063/5.0207496 | American Institute of Physics | FAIL_NO_CANDIDATE | drission | budget_exceeded_before_requests |
| 10.1116/6.0003838 | American Institute of Physics | FAIL_NO_CANDIDATE | drission | budget_exceeded_before_requests |
| 10.1116/6.0003941 | American Institute of Physics | FAIL_BLOCK | landing | keyword=forbidden |
| 10.7567/ssdm.2024.c-4-02 | nan | FAIL_NO_CANDIDATE | drission | pdf_link_not_found_or_download_failed |