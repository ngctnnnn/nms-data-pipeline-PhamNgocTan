from rich import print
from pathlib import Path
import json, pandas as pd
from schemas import (
    DeviceInventory, 
    InterfaceStats, 
    Syslog, 
    TransformedRecord, 
    DeviceSummary
)

def ingest_device_inventory(device_inventory_path: Path) -> pd.DataFrame:
    """Ingest device inventory CSV with schema validation."""
    df = pd.read_csv(device_inventory_path)
    # Validate and convert using schema
    records = []
    for _, row in df.iterrows():
        record = DeviceInventory(
            device=str(row['device']),
            site=str(row['site']),
            vendor=str(row['vendor']),
            role=str(row['role'])
        )
        records.append(record)
    # Convert back to DataFrame
    df_validated = pd.DataFrame([vars(r) for r in records])
    return df_validated


def ingest_interface_stats(interface_stats_path: Path) -> pd.DataFrame:
    """Ingest interface stats CSV with schema validation."""
    df = pd.read_csv(interface_stats_path)
    # Validate and convert using schema
    records = []
    for _, row in df.iterrows():
        record = InterfaceStats(
            ts=str(row['ts']),
            device=str(row['device']),
            ifName=str(row['ifName']),
            util_in=float(row['util_in']),
            util_out=float(row['util_out']),
            admin_status=int(row['admin_status']),
            oper_status=int(row['oper_status'])
        )
        records.append(record)
    # Convert back to DataFrame
    df_validated = pd.DataFrame([vars(r) for r in records])
    return df_validated


def ingest_syslog(syslog_path: Path) -> pd.DataFrame:
    """Ingest syslog JSONL with schema validation."""
    records = []
    with open(syslog_path, 'r') as f:
        for line in f:
            data = json.loads(line.strip())
            record = Syslog(
                ts=str(data['ts']),
                device=str(data['device']),
                severity=str(data['severity']),
                message=str(data['message'])
            )
            records.append(record)
    # Convert to DataFrame
    df = pd.DataFrame([vars(r) for r in records])
    return df


if __name__ == '__main__':
    """Test data ingestion functions."""
    from rich import print
    
    # Setup paths
    base_dir = Path(__file__).parent.parent
    data_dir = base_dir / 'data'
    device_inventory_path = data_dir / 'device_inventory.csv'
    interface_stats_path = data_dir / 'interface_stats.csv'
    syslog_path = data_dir / 'syslog.jsonl'
    
    print("="*50)
    print("Testing Data Ingestion Module")
    print("="*50)
    
    # Test device inventory ingestion
    print("\n[bold]Testing ingest_device_inventory:[/bold]")
    device_inventory = ingest_device_inventory(device_inventory_path)
    print(f"Records: {len(device_inventory)}")
    print(device_inventory)
    
    # Test interface stats ingestion
    print("\n[bold]Testing ingest_interface_stats:[/bold]")
    interface_stats = ingest_interface_stats(interface_stats_path)
    print(f"Records: {len(interface_stats)}")
    print(interface_stats)
    
    # Test syslog ingestion
    print("\n[bold]Testing ingest_syslog:[/bold]")
    syslog = ingest_syslog(syslog_path)
    print(f"Records: {len(syslog)}")
    print(syslog)
    
    print("\n[bold green]All ingestion tests completed![/bold green]")