from __future__ import annotations

import argparse
from pathlib import Path

from regional_tennis_core import RegionConfig, run_region_report

REGION = RegionConfig(
    region_code='central_asia',
    region_name='Central Asia',
    countries=('UZB', 'KAZ', 'KGZ', 'TJK', 'TKM'),
    output_filename='tennis_central_asia.json',
)


def main() -> None:
    parser = argparse.ArgumentParser(description='Generate Central Asia regional tennis report')
    parser.add_argument('--root-dir', type=Path, default=Path('.'))
    parser.add_argument('--output-dir', type=Path, default=Path('docs/generated/regional'))
    args = parser.parse_args()
    out = run_region_report(args.root_dir.resolve(), args.output_dir.resolve(), REGION)
    print(out)


if __name__ == '__main__':
    main()
