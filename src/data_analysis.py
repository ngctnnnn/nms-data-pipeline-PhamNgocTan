import pandas as pd
from rich import print
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns
from schemas.schema import (
    DeviceSummary
)


def generate_analytics_plots(
    device_summary: pd.DataFrame,
    transformed_df: pd.DataFrame,
    output_dir: Path
):
    """Generate visualization plots for analytics using matplotlib and seaborn."""
    sns.set_theme(style='whitegrid')
    
    # 1. Average Utilization by Device
    fig, ax = plt.subplots(figsize=(10, 6))
    device_summary_sorted = device_summary.sort_values('avg_utilization', ascending=False)
    sns.barplot(data=device_summary_sorted, x='device', y='avg_utilization',
                color='steelblue', alpha=0.7, ax=ax)
    ax.set_xlabel('Device', fontsize=12)
    ax.set_ylabel('Average Utilization (%)', fontsize=12)
    ax.set_title('Average Utilization by Device', fontsize=14, fontweight='bold')
    ax.set_ylim(0, max(device_summary['avg_utilization']) * 1.1)
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig(output_dir / 'avg_utilization_by_device.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    # 2. Max Utilization by Device
    fig, ax = plt.subplots(figsize=(10, 6))
    device_summary_sorted = device_summary.sort_values('max_utilization', ascending=False)
    sns.barplot(data=device_summary_sorted, x='device', y='max_utilization',
                color='coral', alpha=0.7, ax=ax)
    ax.set_xlabel('Device', fontsize=12)
    ax.set_ylabel('Max Utilization (%)', fontsize=12)
    ax.set_title('Maximum Utilization by Device', fontsize=14, fontweight='bold')
    ax.set_ylim(0, max(device_summary['max_utilization']) * 1.1)
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig(output_dir / 'max_utilization_by_device.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    # 3. Error Count by Device
    fig, ax = plt.subplots(figsize=(10, 6))
    device_summary_sorted = device_summary.sort_values('error_count', ascending=False)
    palette = ['#c0392b' if x > 0 else 'gray' for x in device_summary_sorted['error_count']]
    sns.barplot(data=device_summary_sorted, x='device', y='error_count',
                palette=palette, alpha=0.7, ax=ax)
    ax.set_xlabel('Device', fontsize=12)
    ax.set_ylabel('Error Count', fontsize=12)
    ax.set_title('Error Count by Device (ERROR Severity Syslogs)', fontsize=14, fontweight='bold')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig(output_dir / 'error_count_by_device.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    # 4. Utilization Over Time (time series: one value per device per timestamp)
    if 'ts' in transformed_df.columns and len(transformed_df) > 0:
        ts_df = transformed_df.copy()
        ts_df['ts_dt'] = pd.to_datetime(ts_df['ts'], utc=True)
        ts_agg = ts_df.groupby(['device', 'ts_dt'], as_index=False)['utilization'].mean()
        ts_agg = ts_agg.sort_values('ts_dt')
        
        fig, ax = plt.subplots(figsize=(12, 6))
        sns.lineplot(data=ts_agg, x='ts_dt', y='utilization', hue='device',
                     marker='o', markersize=6, ax=ax)
        ax.set_xlabel('Time', fontsize=12)
        ax.set_ylabel('Utilization (%)', fontsize=12)
        ax.set_title('Utilization Over Time by Device', fontsize=14, fontweight='bold')
        ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.savefig(output_dir / 'utilization_over_time.png', dpi=300, bbox_inches='tight')
        plt.close()
    
    # 5. Combined Summary Dashboard
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle('Network Device Analytics Dashboard', fontsize=16, fontweight='bold')
    
    device_summary_avg = device_summary.sort_values('avg_utilization', ascending=False)
    device_summary_max = device_summary.sort_values('max_utilization', ascending=False)
    device_summary_err = device_summary.sort_values('error_count', ascending=False)
    err_palette = ['#c0392b' if x > 0 else 'gray' for x in device_summary_err['error_count']]

    sns.barplot(data=device_summary_avg, x='device', y='avg_utilization',
                color='steelblue', alpha=0.7, ax=axes[0, 0])
    axes[0, 0].set_title('Average Utilization', fontweight='bold')
    axes[0, 0].set_ylabel('Utilization (%)')
    axes[0, 0].tick_params(axis='x', rotation=45)

    sns.barplot(data=device_summary_max, x='device', y='max_utilization',
                color='coral', alpha=0.7, ax=axes[0, 1])
    axes[0, 1].set_title('Maximum Utilization', fontweight='bold')
    axes[0, 1].set_ylabel('Utilization (%)')
    axes[0, 1].tick_params(axis='x', rotation=45)

    sns.barplot(data=device_summary_err, x='device', y='error_count',
                palette=err_palette, alpha=0.7, ax=axes[1, 0])
    axes[1, 0].set_title('Error Count', fontweight='bold')
    axes[1, 0].set_ylabel('Count')
    axes[1, 0].tick_params(axis='x', rotation=45)

    # Avg vs Max: melt for seaborn
    summary_long = device_summary.melt(
        id_vars=['device'], value_vars=['avg_utilization', 'max_utilization'],
        var_name='metric', value_name='utilization'
    )
    summary_long['metric'] = summary_long['metric'].map(
        {'avg_utilization': 'Avg', 'max_utilization': 'Max'}
    )
    sns.barplot(data=summary_long, x='device', y='utilization', hue='metric',
                palette={'Avg': 'steelblue', 'Max': 'coral'}, alpha=0.7, ax=axes[1, 1])
    axes[1, 1].set_title('Avg vs Max Utilization', fontweight='bold')
    axes[1, 1].set_ylabel('Utilization (%)')
    axes[1, 1].tick_params(axis='x', rotation=45)
    axes[1, 1].legend(title='')

    plt.tight_layout()
    plt.savefig(output_dir / 'analytics_dashboard.png', dpi=300, bbox_inches='tight')
    plt.close()


def generate_analytics(
    transformed_df: pd.DataFrame, 
    valid_syslog: pd.DataFrame,
    output_dir: Path
) -> pd.DataFrame:
    """
    Generate device-level summary analytics with visualizations:
    - avg_utilization: average of (util_in + util_out) / 2
    - max_utilization: maximum of (util_in + util_out) / 2
    - error_count: count of ERROR severity syslogs per device
    
    Returns: DataFrame with device summaries
    Also saves plots to output_dir
    """
    # Calculate per-record utilization (average of util_in and util_out)
    transformed_df = transformed_df.copy()
    transformed_df['utilization'] = (transformed_df['util_in'] + transformed_df['util_out']) / 2
    
    # Device-level aggregation
    device_summary = transformed_df.groupby('device').agg({
        'utilization': ['mean', 'max']
    }).reset_index()
    device_summary.columns = ['device', 'avg_utilization', 'max_utilization']
    
    # Count ERROR syslogs per device
    error_syslogs = valid_syslog[valid_syslog['severity'] == 'ERROR']
    error_counts = error_syslogs.groupby('device').size().reset_index(name='error_count')
    
    # Merge error counts
    device_summary = device_summary.merge(error_counts, on='device', how='left')
    device_summary['error_count'] = device_summary['error_count'].fillna(0).astype(int)
    
    # Round utilization values
    device_summary['avg_utilization'] = device_summary['avg_utilization'].round(2)
    device_summary['max_utilization'] = device_summary['max_utilization'].round(2)
    
    # Validate against schema
    summary_records = [
        DeviceSummary(**row) for _, row in device_summary.iterrows()
    ]
    summary_df = pd.DataFrame([vars(r) for r in summary_records])
    
    # Generate plots
    generate_analytics_plots(summary_df, transformed_df, output_dir)
    
    return summary_df


if __name__ == '__main__':
    """Test data analysis functions."""
    from pathlib import Path
    from rich import print
    from src.data_ingestion import (
        ingest_device_inventory,
        ingest_interface_stats,
        ingest_syslog
    )
    from src.data_qc import perform_quality_control
    from src.data_transformation import transform
    
    # Setup paths
    base_dir = Path(__file__).parent.parent
    data_dir = base_dir / 'data'
    output_dir = base_dir / 'outputs'
    output_dir.mkdir(exist_ok=True)
    
    print("="*50)
    print("Testing Data Analysis Module")
    print("="*50)
    
    # Load, validate, and transform data first
    print("\n[bold]Loading, validating, and transforming data for analysis testing...[/bold]")
    device_inventory = ingest_device_inventory(data_dir)
    interface_stats = ingest_interface_stats(data_dir)
    syslog = ingest_syslog(data_dir)
    
    valid_interface_stats, valid_syslog, invalid_records = perform_quality_control(
        interface_stats, syslog, device_inventory
    )
    
    transformed_df = transform(valid_interface_stats, valid_syslog, device_inventory)
    
    # Test analytics generation
    print("\n[bold]Testing generate_analytics:[/bold]")
    analytics_df = generate_analytics(transformed_df, valid_syslog, output_dir)
    
    print(f"\nDevice summaries: {len(analytics_df)} devices")
    print(analytics_df)
    
    print(f"\n[bold]Plots saved to:[/bold] {output_dir}")
    print(f"  - analytics_dashboard.png")
    print(f"  - avg_utilization_by_device.png")
    print(f"  - max_utilization_by_device.png")
    print(f"  - error_count_by_device.png")
    if 'ts' in transformed_df.columns and len(transformed_df) > 0:
        print(f"  - utilization_over_time.png")
    
    print("\n[bold green]Analysis tests completed![/bold green]")
