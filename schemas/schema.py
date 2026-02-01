from typing import Optional
from dataclasses import dataclass

@dataclass
class DeviceInventory:
    device: str
    site: str
    vendor: str
    role: str

@dataclass
class InterfaceStats:
    ts: str
    device: str
    ifName: str
    util_in: float
    util_out: float
    admin_status: int
    oper_status: int

@dataclass
class Syslog:
    ts: str
    device: str
    severity: str
    message: str

@dataclass
class InterfaceStats:
    ts: str
    device: str
    ifName: str
    util_in: float
    util_out: float
    admin_status: int
    oper_status: int

@dataclass
class TransformedRecord:
    ts: str
    device: str
    site: str
    vendor: str
    role: str
    ifName: str
    util_in: float
    util_out: float
    oper_status: int
    syslog_severity: Optional[str] = None
    syslog_msg: Optional[str] = None

@dataclass
class DeviceSummary:
    device: str
    avg_utilization: float
    max_utilization: float
    error_count: int