"""
Network Data Pipeline - ETL/ELT Pipeline for Network Management Data
Implements ingestion, data quality checks, transformation, and analytics.
"""

import warnings, click
warnings.filterwarnings('ignore')
from rich import print
from pathlib import Path

from src import (
    ingest_device_inventory, 
    ingest_interface_stats, 
    ingest_syslog, 
    perform_quality_control,
    transform,
    generate_analytics
)

@click.command()
@click.option('--output-dir', 
              type=click.Path(), 
              default='outputs')
@click.option('--device-inventory-path', 
              type=click.Path(exists=True), 
              default='data/device_inventory.csv')
@click.option('--interface-stats-path', 
              type=click.Path(exists=True), 
              default='data/interface_stats.csv')
@click.option('--syslog-path', 
              type=click.Path(exists=True), 
              default='data/syslog.jsonl')
def main(output_dir, device_inventory_path, interface_stats_path, syslog_path):
    assert output_dir is not None, "Output directory is required"
    assert device_inventory_path is not None, "Device inventory path is required"
    assert interface_stats_path is not None, "Interface stats path is required"
    assert syslog_path is not None, "Syslog path is required"

    """Main pipeline execution."""
    # Setup paths
    base_dir = Path(__file__).parent
    output_dir = base_dir / 'outputs'
    output_dir.mkdir(exist_ok=True)
    
    # 1. Ingest
    print("="*25)
    print("Step 1: Ingesting data...")
    print("="*25)
    device_inventory = ingest_device_inventory(device_inventory_path)
    interface_stats = ingest_interface_stats(interface_stats_path)
    syslog = ingest_syslog(syslog_path)
    print(f"  - Device inventory: {len(device_inventory)} records")
    print(device_inventory)
    print(f"  - Interface stats: {len(interface_stats)} records")
    print(interface_stats)
    print(f"  - Syslog: {len(syslog)} records")
    print(syslog)
    print("="*50)
    
    # 2. Data Quality Checks
    print("="*25)
    print("Step 2: Performing data quality checks...")
    print("="*25)
    valid_interface_stats, valid_syslog, invalid_records = perform_quality_control(
        interface_stats, syslog, device_inventory
    )
    print(f"  - Valid interface stats: {len(valid_interface_stats)} records")
    print(valid_interface_stats)
    print(f"  - Valid syslog: {len(valid_syslog)} records")
    print(valid_syslog)
    print(f"  - Invalid records: {len(invalid_records)} records")
    print(invalid_records)
    # Save invalid records
    if len(invalid_records) > 0:
        invalid_records.to_csv(output_dir / 'invalid_records.csv', index=False)
        print(f"  - Invalid records saved to: {output_dir / 'invalid_records.csv'}")
    
    print("="*50)
    
    # 3. Transform
    print("="*25)
    print("Step 3: Transforming data...")
    print("="*25)
    transformed_df = transform(valid_interface_stats, valid_syslog, device_inventory)
    print("Transformed data:")
    print(transformed_df)
    print(f"  - Transformed records: {len(transformed_df)} records")
    print("="*50)
    # Save transformed data
    transformed_df.to_csv(output_dir / 'transformed_data.csv', index=False)
    print(f"  - Transformed data saved to: {output_dir / 'transformed_data.csv'}")
    
    print("="*50)
    
    # 4. Analytics
    print("="*25)
    print("Step 4: Generating analytics...")
    print("="*25)
    analytics_df = generate_analytics(transformed_df, valid_syslog, output_dir)
    print(f"  - Device summaries: {len(analytics_df)} devices")
    print(analytics_df)
    # Save analytics
    analytics_df.to_csv(output_dir / 'device_summary.csv', index=False)
    print(f"  - Analytics saved to: {output_dir / 'device_summary.csv'}")
    print(f"  - Plots saved to: {output_dir}")
    
    print("\nPipeline completed successfully!")
    print(f"\nOutput files:")
    print(f"  - {output_dir / 'transformed_data.csv'}")
    print(f"  - {output_dir / 'device_summary.csv'}")
    print(f"  - {output_dir / 'analytics_dashboard.png'}")
    print(f"  - {output_dir / 'avg_utilization_by_device.png'}")
    print(f"  - {output_dir / 'max_utilization_by_device.png'}")
    print(f"  - {output_dir / 'error_count_by_device.png'}")
    if len(transformed_df) > 0 and 'ts' in transformed_df.columns:
        print(f"  - {output_dir / 'utilization_over_time.png'}")
    if len(invalid_records) > 0:
        print(f"  - {output_dir / 'invalid_records.csv'}")

if __name__ == '__main__':
    main()
