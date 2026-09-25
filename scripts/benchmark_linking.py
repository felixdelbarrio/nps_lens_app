"""Compare linker CPU/wall time and process peak RSS on the checked-in Excel fixtures.

Run once with --baseline-ref <git ref> and once without it, in separate processes.
Only aggregate counts are printed; no customer text or identifiers leave the process.
"""

from __future__ import annotations

import argparse
import json
import resource
import subprocess
import sys
import time
import types

from nps_lens.analytics.nps_helix_link import link_incidents_to_nps_topics
from nps_lens.core.nps_math import focus_mask
from nps_lens.ingest.helix_incidents import read_helix_incidents_excel
from nps_lens.ingest.nps_thermal import read_nps_thermal_excel
from nps_lens.testing.fixtures import fixture_excel


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-ref")
    args = parser.parse_args()
    linker = link_incidents_to_nps_topics
    if args.baseline_ref:
        module = types.ModuleType("linking_benchmark_baseline")
        sys.modules[module.__name__] = module
        for file in ("evidence_highlights", "nps_helix_link"):
            source = subprocess.check_output(
                ["git", "show", f"{args.baseline_ref}:src/nps_lens/analytics/{file}.py"], text=True
            ).replace("from nps_lens.analytics.evidence_highlights import contributing_terms", "")
            exec(compile(source, f"baseline/{file}.py", "exec"), module.__dict__)
        linker = module.link_incidents_to_nps_topics

    nps = read_nps_thermal_excel(
        str(fixture_excel("NPS Térmico Senda - 03Marzo.xlsx")),
        service_origin="BBVA México",
        service_origin_n1="ENTERPRISE WEB",
    ).df
    nps = nps.loc[focus_mask(nps, focus_group="detractor")]
    helix = read_helix_incidents_excel(
        str(fixture_excel("issues_20260506_173749.xlsx")),
        service_origin="BBVA México",
        service_origin_n1="ENTERPRISE WEB",
        service_origin_n2="",
    ).df
    wall, cpu = time.perf_counter(), time.process_time()
    assignments, links = linker(nps, helix)
    print(
        json.dumps(
            {
                "implementation": args.baseline_ref or "working-tree",
                "nps": len(nps),
                "helix": len(helix),
                "wall_s": round(time.perf_counter() - wall, 3),
                "cpu_s": round(time.process_time() - cpu, 3),
                "process_peak_rss_mb": round(
                    resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
                    / (1024**2 if sys.platform == "darwin" else 1024),
                    3,
                ),
                "pairs": len(links),
                "linked_incidents": int(links.incident_id.nunique()),
                "assignments": len(assignments),
                "linked_comments": int(links.nps_id.nunique()),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
