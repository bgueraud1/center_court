from __future__ import annotations

import argparse
from pathlib import Path

from regional_tennis_core import RegionConfig, run_region_report

REGION = RegionConfig(
    region_code='africa',
    region_name='Africa',
    countries=('RSA', 'ALG', 'ANG', 'BEN', 'BOT', 'BUR', 'BDI', 'CPV', 'CMR', 'CAF', 'COM', 'CGO', 'COD', 'CIV', 'DJI', 'EGY', 'ERI', 'SWZ', 'ETH', 'GAB', 'GAM', 'GHA', 'GUI', 'GBS', 'GEQ', 'KEN', 'LES', 'LBR', 'LBA', 'MAD', 'MAW', 'MLI', 'MAR', 'MRI', 'MTN', 'MOZ', 'NAM', 'NIG', 'NGR', 'UGA', 'RWA', 'STP', 'SEN', 'SEY', 'SLE', 'SOM', 'SUD', 'SSD', 'TAN', 'CHA', 'TOG', 'TUN', 'ZAM', 'ZIM'),
    output_filename='tennis_africa.json',
)


def main() -> None:
    parser = argparse.ArgumentParser(description='Generate Africa regional tennis report')
    parser.add_argument('--root-dir', type=Path, default=Path('.'))
    parser.add_argument('--output-dir', type=Path, default=Path('docs/generated/regional'))
    args = parser.parse_args()
    out = run_region_report(args.root_dir.resolve(), args.output_dir.resolve(), REGION)
    print(out)


if __name__ == '__main__':
    main()
