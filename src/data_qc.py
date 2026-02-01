import pandas as pd
from rich import print
from typing import Tuple
from pathlib import Path
from datetime import datetime

def validate_timestamp(ts: str) -> bool:
    """Validate ISO 8601 timestamp (UTC)."""
    try:
        datetime.fromisoformat(ts.replace('Z', '+00:00'))
        return True
    except (ValueError, AttributeError):
        return False


def validate_utilization(value: float) -> bool:
    """Validate utilization is numeric and between 0-100."""
    return pd.notna(value) and isinstance(value, (int, float)) and 0 <= value <= 100


def validate_oper_status(value: int) -> bool:
    """Validate oper_status is in {1, 2}."""
    return pd.notna(value) and value in {1, 2}


def perform_quality_control(
    interface_stats: pd.DataFrame,
    syslog: pd.DataFrame,
    device_inventory: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Perform data quality checks and separate valid/invalid records.
    Returns: (valid_interface_stats, valid_syslog, invalid_records)
    """
    invalid_records = []
    valid_devices = set(device_inventory['device'].unique())
    
    # DQ checks for interface_stats
    for idx, row in interface_stats.iterrows():
        reasons = []
        
        # Check 1: device must exist in device_inventory
        if row['device'] not in valid_devices:
            reasons.append('device_not_in_inventory')
        
        # Check 2: ts must be valid ISO 8601 timestamp
        if not validate_timestamp(row['ts']):
            reasons.append('invalid_timestamp')
        
        # Check 3: util_in and util_out must be numeric and 0-100
        if not validate_utilization(row['util_in']):
            reasons.append('invalid_util_in')
        if not validate_utilization(row['util_out']):
            reasons.append('invalid_util_out')
        
        # Bonus: oper_status in {1, 2}
        if not validate_oper_status(row['oper_status']):
            reasons.append('invalid_oper_status')
        
        if reasons:
            invalid_records.append({
                'source': 'interface_stats',
                'record_index': idx,
                'record': row.to_dict(),
                'reason': '; '.join(reasons)
            })
    
    # DQ checks for syslog
    for idx, row in syslog.iterrows():
        reasons = []
        
        # Check 1: device must exist in device_inventory
        if row['device'] not in valid_devices:
            reasons.append('device_not_in_inventory')
        
        # Check 2: ts must be valid ISO 8601 timestamp
        if not validate_timestamp(row['ts']):
            reasons.append('invalid_timestamp')
        
        if reasons:
            invalid_records.append({
                'source': 'syslog',
                'record_index': idx,
                'record': row.to_dict(),
                'reason': '; '.join(reasons)
            })
    
    # Filter valid records
    invalid_interface_indices = {r['record_index'] for r in invalid_records if r['source'] == 'interface_stats'}
    invalid_syslog_indices = {r['record_index'] for r in invalid_records if r['source'] == 'syslog'}
    
    valid_interface_stats = interface_stats[~interface_stats.index.isin(invalid_interface_indices)].copy()
    valid_syslog = syslog[~syslog.index.isin(invalid_syslog_indices)].copy()
    
    # Create invalid records DataFrame
    invalid_df = pd.DataFrame(invalid_records)
    
    return valid_interface_stats, valid_syslog, invalid_df


if __name__ == '__main__':
    """Test data quality control functions."""
    from pathlib import Path
    from rich import print
    from src.data_ingestion import (
        ingest_device_inventory,
        ingest_interface_stats,
        ingest_syslog
    )
    
    # Setup paths
    base_dir = Path(__file__).parent.parent
    data_dir = base_dir / 'data'
    
    print("="*50)
    print("Testing Data Quality Control Module")
    print("="*50)
    
    # Load data first
    print("\n[bold]Loading data for QC testing...[/bold]")
    device_inventory = ingest_device_inventory(data_dir)
    interface_stats = ingest_interface_stats(data_dir)
    syslog = ingest_syslog(data_dir)
    
    # Test quality control
    print("\n[bold]Testing perform_quality_control:[/bold]")
    valid_interface_stats, valid_syslog, invalid_records = perform_quality_control(
        interface_stats, syslog, device_inventory
    )
    
    print(f"\nValid interface stats: {len(valid_interface_stats)} records")
    print(valid_interface_stats)
    
    print(f"\nValid syslog: {len(valid_syslog)} records")
    print(valid_syslog)
    
    print(f"\nInvalid records: {len(invalid_records)} records")
    if len(invalid_records) > 0:
        print(invalid_records)
    else:
        print("[green]No invalid records found![/green]")
    
    print("\n[bold green]Quality control tests completed![/bold green]")