import pandas as pd
from rich import print
from datetime import timedelta
from schemas.schema import (
    TransformedRecord
)

def transform(
    valid_interface_stats: pd.DataFrame,
    valid_syslog: pd.DataFrame,
    device_inventory: pd.DataFrame
) -> pd.DataFrame:
    """
    Transform: Join interface stats with device metadata and matching syslog events.
    Syslog events match if same device and within ±5 minutes of interface stats timestamp.
    """
    # Convert timestamps to datetime
    valid_interface_stats['ts_dt'] = pd.to_datetime(valid_interface_stats['ts'], utc=True)
    valid_syslog['ts_dt'] = pd.to_datetime(valid_syslog['ts'], utc=True)
    
    # Join interface stats with device inventory
    joined = valid_interface_stats.merge(
        device_inventory,
        on='device',
        how='left'
    )
    
    # Join with syslog events within ±5 minutes
    result_rows = []
    for _, intf_row in joined.iterrows():
        intf_ts = intf_row['ts_dt']
        device = intf_row['device']
        
        # Find matching syslog events (same device, within ±5 minutes)
        time_window = timedelta(minutes=5)
        matching_syslog = valid_syslog[
            (valid_syslog['device'] == device) &
            (valid_syslog['ts_dt'] >= intf_ts - time_window) &
            (valid_syslog['ts_dt'] <= intf_ts + time_window)
        ]
        
        # If multiple syslog events match, take the first one (or could aggregate)
        if len(matching_syslog) > 0:
            syslog_row = matching_syslog.iloc[0]
            syslog_severity = syslog_row['severity']
            syslog_msg = syslog_row['message']
        else:
            syslog_severity = None
            syslog_msg = None
        
        # Build output record using schema
        result_rows.append(TransformedRecord(
            ts=intf_row['ts'],
            device=device,
            site=intf_row['site'],
            vendor=intf_row['vendor'],
            role=intf_row['role'],
            ifName=intf_row['ifName'],
            util_in=float(intf_row['util_in']),
            util_out=float(intf_row['util_out']),
            oper_status=int(intf_row['oper_status']),
            syslog_severity=syslog_severity,
            syslog_msg=syslog_msg
        ))
    
    # Convert to DataFrame
    result_df = pd.DataFrame([vars(r) for r in result_rows])
    return result_df


if __name__ == '__main__':
    """Test data transformation functions."""
    from pathlib import Path
    from rich import print
    from src.data_ingestion import (
        ingest_device_inventory,
        ingest_interface_stats,
        ingest_syslog
    )
    from src.data_qc import perform_quality_control
    
    # Setup paths
    base_dir = Path(__file__).parent.parent
    data_dir = base_dir / 'data'
    
    print("="*50)
    print("Testing Data Transformation Module")
    print("="*50)
    
    # Load and validate data first
    print("\n[bold]Loading and validating data for transformation testing...[/bold]")
    device_inventory = ingest_device_inventory(data_dir)
    interface_stats = ingest_interface_stats(data_dir)
    syslog = ingest_syslog(data_dir)
    
    valid_interface_stats, valid_syslog, invalid_records = perform_quality_control(
        interface_stats, syslog, device_inventory
    )
    
    # Test transformation
    print("\n[bold]Testing transform:[/bold]")
    transformed_df = transform(valid_interface_stats, valid_syslog, device_inventory)
    
    print(f"\nTransformed records: {len(transformed_df)}")
    print(transformed_df)
    
    print("\n[bold green]Transformation tests completed![/bold green]")