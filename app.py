import csv
import json
import os
import importlib.util
import threading
import uuid
import zipfile
from datetime import datetime, date
from io import BytesIO
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional

import numpy as np
import pandas as pd
from flask import Flask, jsonify, render_template, request, send_file

app = Flask(__name__, static_folder="static", template_folder="templates")

# ======================================================================================
# CONSTANTS
# ======================================================================================

AR_COLUMNS: List[str] = [
    "ID",
    "Transaction Batch Source Name",
    "Transaction Type Name",
    "Payment Terms",
    "Transaction Date",
    "Accounting Date",
    "Transaction Number",
    "Original System Bill-to Customer Reference",
    "Original System Bill-to Customer Address Reference",
    "Original System Bill-to Customer Contact Reference",
    "Original System Ship-to Customer Reference",
    "Original System Ship-to Customer Address Reference",
    "Original System Ship-to Customer Contact Reference",
    "Original System Ship-to Customer Account Reference",
    "Original System Ship-to Customer Account Address Reference",
    "Original System Ship-to Customer Account Contact Reference",
    "Original System Sold-to Customer Reference",
    "Original System Sold-to Customer Account Reference",
    "Bill-to Customer Account Number",
    "Bill-to Customer Site Number",
    "Bill-to Contact Party Number",
    "Ship-to Customer Account Number",
    "Ship-to Customer Site Number",
    "Ship-to Contact Party Number",
    "Sold-to Customer Account Number",
    "Transaction Line Type",
    "Transaction Line Description",
    "Currency Code",
    "Currency Conversion Type",
    "Currency Conversion Date",
    "Currency Conversion Rate",
    "Transaction Line Amount",
    "Transaction Line Quantity",
    "Customer Ordered Quantity",
    "Unit Selling Price",
    "Unit Standard Price",
    "Line Transactions Flexfield Context",
    "Line Transactions Flexfield Segment 1",
    "Line Transactions Flexfield Segment 2",
    "Line Transactions Flexfield Segment 3",
    "Line Transactions Flexfield Segment 4",
    "Line Transactions Flexfield Segment 5",
    "Line Transactions Flexfield Segment 6",
    "Line Transactions Flexfield Segment 7",
    "Line Transactions Flexfield Segment 8",
    "Line Transactions Flexfield Segment 9",
    "Line Transactions Flexfield Segment 10",
    "Line Transactions Flexfield Segment 11",
    "Line Transactions Flexfield Segment 12",
    "Line Transactions Flexfield Segment 13",
    "Line Transactions Flexfield Segment 14",
    "Line Transactions Flexfield Segment 15",
    "Primary Salesperson Number",
    "Tax Classification Code",
    "Legal Entity Identifier",
    "Accounted Amount in Ledger Currency",
    "Sales Order Number",
    "Sales Order Date",
    "Actual Ship Date",
    "Warehouse Code",
    "Unit of Measure Code",
    "Unit of Measure Name",
    "Invoicing Rule Name",
    "Revenue Scheduling Rule Name",
    "Number of Revenue Periods",
    "Revenue Scheduling Rule Start Date",
    "Revenue Scheduling Rule End Date",
    "Reason Code Meaning",
    "Last Period to Credit",
    "Transaction Business Category Code",
    "Product Fiscal Classification Code",
    "Product Category Code",
    "Product Type",
    "Line Intended Use Code",
    "Assessable Value",
    "Document Sub Type",
    "Default Taxation Country",
    "User Defined Fiscal Classification",
    "Tax Invoice Number",
    "Tax Invoice Date",
    "Tax Regime Code",
    "Tax",
    "Tax Status Code",
    "Tax Rate Code",
    "Tax Jurisdiction Code",
    "First Party Registration Number",
    "Third Party Registration Number",
    "Final Discharge Location",
    "Taxable Amount",
    "Taxable Flag",
    "Tax Exemption Flag",
    "Tax Exemption Reason Code",
    "Tax Exemption Reason Code Meaning",
    "Tax Exemption Certificate Number",
    "Line Amount Includes Tax Flag",
    "Tax Precedence",
    "Credit Method To Be Used For Lines With Revenue Scheduling Rules",
    "Credit Method To Be Used For Transactions With Split Payment Terms",
    "Reason Code",
    "Tax Rate",
    "FOB Point",
    "Carrier",
    "Shipping Reference",
    "Sales Order Line Number",
    "Sales Order Source",
    "Sales Order Revision Number",
    "Purchase Order Number",
    "Purchase Order Revision Number",
    "Purchase Order Date",
    "Agreement Name",
    "Memo Line Name",
    "Document Number",
    "Original System Batch Name",
    "Link-to Transactions Flexfield Context",
    "Link-to Transactions Flexfield Segment 1",
    "Link-to Transactions Flexfield Segment 2",
    "Link-to Transactions Flexfield Segment 3",
    "Link-to Transactions Flexfield Segment 4",
    "Link-to Transactions Flexfield Segment 5",
    "Link-to Transactions Flexfield Segment 6",
    "Link-to Transactions Flexfield Segment 7",
    "Link-to Transactions Flexfield Segment 8",
    "Link-to Transactions Flexfield Segment 9",
    "Link-to Transactions Flexfield Segment 10",
    "Link-to Transactions Flexfield Segment 11",
    "Link-to Transactions Flexfield Segment 12",
    "Link-to Transactions Flexfield Segment 13",
    "Link-to Transactions Flexfield Segment 14",
    "Link-to Transactions Flexfield Segment 15",
    "Reference Transactions Flexfield Context",
    "Reference Transactions Flexfield Segment 1",
    "Reference Transactions Flexfield Segment 2",
    "Reference Transactions Flexfield Segment 3",
    "Reference Transactions Flexfield Segment 4",
    "Reference Transactions Flexfield Segment 5",
    "Reference Transactions Flexfield Segment 6",
    "Reference Transactions Flexfield Segment 7",
    "Reference Transactions Flexfield Segment 8",
    "Reference Transactions Flexfield Segment 9",
    "Reference Transactions Flexfield Segment 10",
    "Reference Transactions Flexfield Segment 11",
    "Reference Transactions Flexfield Segment 12",
    "Reference Transactions Flexfield Segment 13",
    "Reference Transactions Flexfield Segment 14",
    "Reference Transactions Flexfield Segment 15",
    "Link To Parent Line Context",
    "Link To Parent Line Segment 1",
    "Link To Parent Line Segment 2",
    "Link To Parent Line Segment 3",
    "Link To Parent Line Segment 4",
    "Link To Parent Line Segment 5",
    "Link To Parent Line Segment 6",
    "Link To Parent Line Segment 7",
    "Link To Parent Line Segment 8",
    "Link To Parent Line Segment 9",
    "Link To Parent Line Segment 10",
    "Link To Parent Line Segment 11",
    "Link To Parent Line Segment 12",
    "Link To Parent Line Segment 13",
    "Link To Parent Line Segment 14",
    "Link To Parent Line Segment 15",
    "Receipt Method Name",
    "Printing Option",
    "Related Batch Source Name",
    "Related Transaction Number",
    "Inventory Item Number",
    "Inventory Item Segment 2",
    "Inventory Item Segment 3",
    "Inventory Item Segment 4",
    "Inventory Item Segment 5",
    "Inventory Item Segment 6",
    "Inventory Item Segment 7",
    "Inventory Item Segment 8",
    "Inventory Item Segment 9",
    "Inventory Item Segment 10",
    "Inventory Item Segment 11",
    "Inventory Item Segment 12",
    "Inventory Item Segment 13",
    "Inventory Item Segment 14",
    "Inventory Item Segment 15",
    "Inventory Item Segment 16",
    "Inventory Item Segment 17",
    "Inventory Item Segment 18",
    "Inventory Item Segment 19",
    "Inventory Item Segment 20",
    "Bill To Customer Bank Account Name",
    "Reset Transaction Date Flag",
    "Payment Server Order Number",
    "Last Transaction on Debit Authorization",
    "Approval Code",
    "Address Verification Code",
    "Transaction Line Translated Description",
    "Consolidated Billing Number",
    "Promised Commitment Amount",
    "Payment Set Identifier",
    "Original Accounting Date",
    "Invoiced Line Accounting Level",
    "Override AutoAccounting Flag",
    "Historical Flag",
    "Deferral Exclusion Flag",
    "Payment Attributes",
    "Invoice Billing Date",
    "Invoice Lines Flexfield Context",
    "Invoice Lines Flexfield Segment 1",
    "Invoice Lines Flexfield Segment 2",
    "Invoice Lines Flexfield Segment 3",
    "Invoice Lines Flexfield Segment 4",
    "Invoice Lines Flexfield Segment 5",
    "Invoice Lines Flexfield Segment 6",
    "Invoice Lines Flexfield Segment 7",
    "Invoice Lines Flexfield Segment 8",
    "Invoice Lines Flexfield Segment 9",
    "Invoice Lines Flexfield Segment 10",
    "Invoice Lines Flexfield Segment 11",
    "Invoice Lines Flexfield Segment 12",
    "Invoice Lines Flexfield Segment 13",
    "Invoice Lines Flexfield Segment 14",
    "Invoice Lines Flexfield Segment 15",
    "Invoice Transactions Flexfield Context",
    "Invoice Transactions Flexfield Segment 1",
    "Invoice Transactions Flexfield Segment 2",
    "Invoice Transactions Flexfield Segment 3",
    "Invoice Transactions Flexfield Segment 4",
    "Invoice Transactions Flexfield Segment 5",
    "Invoice Transactions Flexfield Segment 6",
    "Invoice Transactions Flexfield Segment 7",
    "Invoice Transactions Flexfield Segment 8",
    "Invoice Transactions Flexfield Segment 9",
    "Invoice Transactions Flexfield Segment 10",
    "Invoice Transactions Flexfield Segment 11",
    "Invoice Transactions Flexfield Segment 12",
    "Invoice Transactions Flexfield Segment 13",
    "Invoice Transactions Flexfield Segment 14",
    "Invoice Transactions Flexfield Segment 15",
    "Receivables Transaction Region Information Flexfield Context",
    "Receivables Transaction Region Information Flexfield Segment 1",
    "Receivables Transaction Region Information Flexfield Segment 2",
    "Receivables Transaction Region Information Flexfield Segment 3",
    "Receivables Transaction Region Information Flexfield Segment 4",
    "Receivables Transaction Region Information Flexfield Segment 5",
    "Receivables Transaction Region Information Flexfield Segment 6",
    "Receivables Transaction Region Information Flexfield Segment 7",
    "Receivables Transaction Region Information Flexfield Segment 8",
    "Receivables Transaction Region Information Flexfield Segment 9",
    "Receivables Transaction Region Information Flexfield Segment 10",
    "Receivables Transaction Region Information Flexfield Segment 11",
    "Receivables Transaction Region Information Flexfield Segment 12",
    "Receivables Transaction Region Information Flexfield Segment 13",
    "Receivables Transaction Region Information Flexfield Segment 14",
    "Receivables Transaction Region Information Flexfield Segment 15",
    "Receivables Transaction Region Information Flexfield Segment 16",
    "Receivables Transaction Region Information Flexfield Segment 17",
    "Receivables Transaction Region Information Flexfield Segment 18",
    "Receivables Transaction Region Information Flexfield Segment 19",
    "Receivables Transaction Region Information Flexfield Segment 20",
    "Receivables Transaction Region Information Flexfield Segment 21",
    "Receivables Transaction Region Information Flexfield Segment 22",
    "Receivables Transaction Region Information Flexfield Segment 23",
    "Receivables Transaction Region Information Flexfield Segment 24",
    "Receivables Transaction Region Information Flexfield Segment 25",
    "Receivables Transaction Region Information Flexfield Segment 26",
    "Receivables Transaction Region Information Flexfield Segment 27",
    "Receivables Transaction Region Information Flexfield Segment 28",
    "Receivables Transaction Region Information Flexfield Segment 29",
    "Receivables Transaction Region Information Flexfield Segment 30",
    "Line Global Descriptive Flexfield Attribute Category",
    "Line Global Descriptive Flexfield Segment 1",
    "Line Global Descriptive Flexfield Segment 2",
    "Line Global Descriptive Flexfield Segment 3",
    "Line Global Descriptive Flexfield Segment 4",
    "Line Global Descriptive Flexfield Segment 5",
    "Line Global Descriptive Flexfield Segment 6",
    "Line Global Descriptive Flexfield Segment 7",
    "Line Global Descriptive Flexfield Segment 8",
    "Line Global Descriptive Flexfield Segment 9",
    "Line Global Descriptive Flexfield Segment 10",
    "Line Global Descriptive Flexfield Segment 11",
    "Line Global Descriptive Flexfield Segment 12",
    "Line Global Descriptive Flexfield Segment 13",
    "Line Global Descriptive Flexfield Segment 14",
    "Line Global Descriptive Flexfield Segment 15",
    "Line Global Descriptive Flexfield Segment 16",
    "Line Global Descriptive Flexfield Segment 17",
    "Line Global Descriptive Flexfield Segment 18",
    "Line Global Descriptive Flexfield Segment 19",
    "Line Global Descriptive Flexfield Segment 20",
    "Comments",
    "Notes from Source",
    "Credit Card Token Number",
    "Credit Card Expiration Date",
    "First Name of the Credit Card Holder",
    "Last Name of the Credit Card Holder",
    "Credit Card Issuer Code",
    "Masked Credit Card Number",
    "Credit Card Authorization Request Identifier",
    "Credit Card Voice Authorization Code",
    "Receivables Transaction Region Information Flexfield Number Segment 1",
    "Receivables Transaction Region Information Flexfield Number Segment 2",
    "Receivables Transaction Region Information Flexfield Number Segment 3",
    "Receivables Transaction Region Information Flexfield Number Segment 4",
    "Receivables Transaction Region Information Flexfield Number Segment 5",
    "Receivables Transaction Region Information Flexfield Number Segment 6",
    "Receivables Transaction Region Information Flexfield Number Segment 7",
    "Receivables Transaction Region Information Flexfield Number Segment 8",
    "Receivables Transaction Region Information Flexfield Number Segment 9",
    "Receivables Transaction Region Information Flexfield Number Segment 10",
    "Receivables Transaction Region Information Flexfield Number Segment 11",
    "Receivables Transaction Region Information Flexfield Number Segment 12",
    "Receivables Transaction Region Information Flexfield Date Segment 1",
    "Receivables Transaction Region Information Flexfield Date Segment 2",
    "Receivables Transaction Region Information Flexfield Date Segment 3",
    "Receivables Transaction Region Information Flexfield Date Segment 4",
    "Receivables Transaction Region Information Flexfield Date Segment 5",
    "Receivables Transaction Line Region Information Flexfield Number Segment 1",
    "Receivables Transaction Line Region Information Flexfield Number Segment 2",
    "Receivables Transaction Line Region Information Flexfield Number Segment 3",
    "Receivables Transaction Line Region Information Flexfield Number Segment 4",
    "Receivables Transaction Line Region Information Flexfield Number Segment 5",
    "Receivables Transaction Line Region Information Flexfield Date Segment 1",
    "Receivables Transaction Line Region Information Flexfield Date Segment 2",
    "Receivables Transaction Line Region Information Flexfield Date Segment 3",
    "Receivables Transaction Line Region Information Flexfield Date Segment 4",
    "Receivables Transaction Line Region Information Flexfield Date Segment 5",
    "Freight Charge",
    "Insurance Charge",
    "Packing Charge",
    "Miscellaneous Charge",
    "Commercial Discount",
    "Enforce Chronological Document Sequencing",
    "Payments transaction identifier",
    "Interface Status",
    "Invoice Lines Flexfield Number Segment 1",
    "Invoice Lines Flexfield Number Segment 2",
    "Invoice Lines Flexfield Number Segment 3",
    "Invoice Lines Flexfield Number Segment 4",
    "Invoice Lines Flexfield Number Segment 5",
    "Invoice Lines Flexfield Date Segment 1",
    "Invoice Lines Flexfield Date Segment 2",
    "Invoice Lines Flexfield Date Segment 3",
    "Invoice Lines Flexfield Date Segment 4",
    "Invoice Lines Flexfield Date Segment 5",
    "Invoice Transactions Flexfield Number Segment 1",
    "Invoice Transactions Flexfield Number Segment 2",
    "Invoice Transactions Flexfield Number Segment 3",
    "Invoice Transactions Flexfield Number Segment 4",
    "Invoice Transactions Flexfield Number Segment 5",
    "Invoice Transactions Flexfield Date Segment 1",
    "Invoice Transactions Flexfield Date Segment 2",
    "Invoice Transactions Flexfield Date Segment 3",
    "Invoice Transactions Flexfield Date Segment 4",
    "Invoice Transactions Flexfield Date Segment 5",
    "ADDITIONAL_LINE_CONTEXT",
    "ADDITIONAL_LINE_ATTRIBUTE1",
    "ADDITIONAL_LINE_ATTRIBUTE2",
    "ADDITIONAL_LINE_ATTRIBUTE3",
    "ADDITIONAL_LINE_ATTRIBUTE4",
    "ADDITIONAL_LINE_ATTRIBUTE5",
    "ADDITIONAL_LINE_ATTRIBUTE6",
    "ADDITIONAL_LINE_ATTRIBUTE7",
    "ADDITIONAL_LINE_ATTRIBUTE8",
    "ADDITIONAL_LINE_ATTRIBUTE9",
    "ADDITIONAL_LINE_ATTRIBUTE10",
    "ADDITIONAL_LINE_ATTRIBUTE11",
    "ADDITIONAL_LINE_ATTRIBUTE12",
    "ADDITIONAL_LINE_ATTRIBUTE13",
    "ADDITIONAL_LINE_ATTRIBUTE14",
    "ADDITIONAL_LINE_ATTRIBUTE15",
    "END",
]

AR_STATIC = {
    "Transaction Batch Source Name": "Manual_Imported",
    "Transaction Type Name": "Vend Invoice",
    "Payment Terms": "IMMEDIATE",
    "Transaction Line Type": "LINE",
    "Currency Code": "SAR",
    "Currency Conversion Type": "Corporate",
    "Currency Conversion Rate": "1",
    "Line Transactions Flexfield Context": "Legacy",
    "Unit of Measure Code": "Ea",
    "Default Taxation Country": "SA",
    "END": "END",
    "Comments": "AlQurashi-KSA",
}

RECEIPT_COLUMNS = [
    "Business Unit",
    "Batch Source",
    "Batch Name",
    "Receipt Method",
    "Remittance Bank",
    "Remittance Bank Account",
    "Batch Date",
    "Accounting Date",
    "Deposit Date",
    "Currency",
    "Sequence Number",
    "Receipt Number",
    "Receipt Amount",
    "Receipt Date",
    "Accounting Date (Receipt)",
    "Currency (Receipt)",
    "Document Number",
    "Customer Name",
    "Customer Account Number",
    "Customer Site Number",
    "Misc Charge Type",
    "Original Payment Method",
    "Calculation Details",
]

DEFAULT_PAYMENT_METHODS = [
    {"method": "Cash", "bank_charge_pct": 0.0, "cap": 0.0, "apply_cap": False, "generate_miss": False},
    {"method": "Mada", "bank_charge_pct": 1.5, "cap": 10.0, "apply_cap": True, "generate_miss": True},
    {"method": "Visa", "bank_charge_pct": 2.0, "cap": 10.0, "apply_cap": True, "generate_miss": True},
    {"method": "MasterCard", "bank_charge_pct": 2.0, "cap": 10.0, "apply_cap": True, "generate_miss": True},
    {"method": "Amex", "bank_charge_pct": 2.5, "cap": 15.0, "apply_cap": True, "generate_miss": True},
    {"method": "Apple Pay", "bank_charge_pct": 1.5, "cap": 10.0, "apply_cap": True, "generate_miss": True},
    {"method": "STC Pay", "bank_charge_pct": 1.0, "cap": 5.0, "apply_cap": True, "generate_miss": True},
    {"method": "Tabby", "bank_charge_pct": 0.0, "cap": 0.0, "apply_cap": False, "generate_miss": False},
    {"method": "Tamara", "bank_charge_pct": 0.0, "cap": 0.0, "apply_cap": False, "generate_miss": False},
]

STANDARD_RECEIPT_METHODS = {"CASH", "MADA", "VISA", "MASTERCARD"}
BNPL_METHODS = {"TAMARA", "TABBY"}
ROUNDING_KEYWORDS = {"ROUNDING", "CASH ROUNDING"}

jobs: Dict[str, Dict[str, Any]] = {}
jobs_lock = threading.Lock()


def ensure_dir(path: str) -> None:
    Path(path).mkdir(parents=True, exist_ok=True)


def today_iso() -> str:
    return date.today().isoformat()


def normalize_method(method: str) -> str:
    return str(method).strip().upper()


def default_charge_config() -> Dict[str, Any]:
    return {"tax_rate": 5.0, "methods": DEFAULT_PAYMENT_METHODS}


def update_job(job_id: str, **kwargs) -> None:
    with jobs_lock:
        if job_id not in jobs:
            jobs[job_id] = {}
        jobs[job_id].update(kwargs)


def read_table(uploaded, kind: str) -> pd.DataFrame:
    name = getattr(uploaded, "filename", "") or ""
    if name.lower().endswith(".xlsx") or name.lower().endswith(".xls"):
        return pd.read_excel(uploaded, engine="openpyxl")
    return pd.read_csv(uploaded)


def validate_required_columns(df: pd.DataFrame, required: List[str]) -> Tuple[bool, List[str]]:
    missing = [c for c in required if c not in df.columns]
    return len(missing) == 0, missing


def validate_upload(file_storage, file_type: str) -> Tuple[bool, List[str]]:
    if not file_storage:
        return False, ["File missing"]
    df = read_table(file_storage, file_type)
    if file_type == "line_items":
        required = [
            "Order Lines/Order Ref",
            "Order Lines/Subtotal w/o Tax",
            "Order Lines/Product/Barcode",
            "Order Lines/Product/Name",
            "Order Lines/Quantity",
            "Order Lines/Order Ref/Date",
            "Order Lines/Register Name",
        ]
    elif file_type == "payments":
        required = ["Order Ref", "Branch", "Payments/Payment Method", "Payments/Amount"]
    elif file_type == "metadata":
        required = [
            "SUBINVENTORY",
            "CUSTOMER_TYPE",
            "BILL_TO_ACCOUNT",
            "SITE_NUMBER",
            "BILL_TO_NAME",
            "BUSINESS_UNIT",
            "REGION",
            "COST_CENTER_CODE",
        ]
    elif file_type == "registers":
        required = []
        has_register = any("REGISTER_NAME" in str(col).upper() for col in df.columns)
        if not has_register:
            return False, ['Column containing "REGISTER_NAME"']
        return True, []
    else:
        return False, [f"Unknown file type: {file_type}"]

    valid, missing = validate_required_columns(df, required)
    return valid, missing


fusion_module: Optional[Any] = None


def load_template_module():
    global fusion_module
    if fusion_module is not None:
        return fusion_module
    template_path = Path(__file__).with_name("Odoo-export-FBDA-template.py")
    spec = importlib.util.spec_from_file_location("fusion_template", template_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Unable to load template module")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    fusion_module = module
    return module


def build_bank_charges_csv(config: List[Dict[str, Any]], tax_rate_pct: float, target_dir: Path) -> Optional[Path]:
    rows: List[Dict[str, Any]] = []
    for row in config or []:
        if not to_bool(row.get("generate_miss", True)):
            continue
        method = str(row.get("method", "")).strip()
        if not method:
            continue
        charge_rate = float(row.get("bank_charge_pct", 0) or 0) / 100.0
        cap_amount = float(row.get("cap", 0) or 0)
        if not to_bool(row.get("apply_cap", False)):
            cap_amount = 0.0
        rows.append(
            {
                "PAYMENT_METHOD": method,
                "CHARGE_RATE": charge_rate,
                "TAX_RATE": float(tax_rate_pct) / 100.0,
                "CAP_AMOUNT": cap_amount,
                "RECEIPT_METHOD_ID": "",
                "BANK_ACCOUNT_NUM": "",
                "ORG_ID": "",
                "ACTIVITY_NAME": "Bank Charges",
                "CASH_ROUNDING": "Y" if "ROUNDING" in method.upper() else "N",
            }
        )
    if not rows:
        return None
    ensure_dir(target_dir)
    path = Path(target_dir) / "BANK_CHARGES.csv"
    fieldnames = [
        "PAYMENT_METHOD",
        "CHARGE_RATE",
        "TAX_RATE",
        "CAP_AMOUNT",
        "RECEIPT_METHOD_ID",
        "BANK_ACCOUNT_NUM",
        "ORG_ID",
        "ACTIVITY_NAME",
        "CASH_ROUNDING",
    ]
    with open(path, "w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return path


def create_zip(zip_path: Path, files: List[Path]) -> None:
    ensure_dir(zip_path.parent)
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for file in files:
            zf.write(file, arcname=Path(file).name)


def method_from_receipt_filename(fname: str) -> str:
    parts = Path(fname).stem.split("_")
    return parts[1] if len(parts) > 1 else ""


def compact_to_iso(date_compact: str) -> str:
    if len(date_compact) == 8 and date_compact.isdigit():
        return f"{date_compact[:4]}-{date_compact[4:6]}-{date_compact[6:]}"
    return date_compact


def format_misc_example(detail: Dict[str, Any]) -> str:
    receipt_amount = float(detail.get("receipt_amount", 0.0))
    rate = float(detail.get("charge_rate", 0.0))
    tax = float(detail.get("tax_rate", 0.0))
    cap = float(detail.get("cap_amount", 0.0))
    misc_amount = float(detail.get("misc_amount", 0.0))
    if detail.get("cash_rounding"):
        return f"Rounding {receipt_amount:.3f} → {misc_amount:.3f}"
    base = f"{receipt_amount:.2f} × {rate:.4f} × (1+{tax:.4f}) = {abs(misc_amount):.4f}"
    if cap:
        base += f" cap {cap:.2f}"
    return base


def build_summary(integration: Any) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any], List[Dict[str, Any]]]:
    ar_df = getattr(integration, "last_ar_df", None) or pd.DataFrame()
    total_ar_amount = float(ar_df["Transaction Line Amount"].sum()) if not ar_df.empty else 0.0
    total_sales_amount = float(sum(getattr(integration, "invoice_ar_total", {}).values()))
    total_standard = sum(float(df["Receipt Amount"].sum()) for df in getattr(integration, "last_receipt_files", {}).values())
    total_miss_raw = sum(float(df["Amount"].sum()) for df in getattr(integration, "last_misc_files", {}).values())

    net_settlement = total_standard + total_miss_raw
    verification_passed = abs(total_ar_amount - (total_standard + abs(total_miss_raw))) <= 0.01

    standard_breakdown: Dict[str, Dict[str, Any]] = {}
    for detail in getattr(integration, "last_receipt_details", []):
        method = detail.get("method", "")
        entry = standard_breakdown.setdefault(method, {"amount": 0.0, "files": 0, "transactions": 0})
        entry["amount"] += float(detail.get("net_amount", 0.0))
        entry["files"] += 1
        entry["transactions"] += int(detail.get("row_count", 0))
    if not standard_breakdown:
        for fname, df in getattr(integration, "last_receipt_files", {}).items():
            method = method_from_receipt_filename(fname)
            entry = standard_breakdown.setdefault(method, {"amount": 0.0, "files": 0, "transactions": 0})
            entry["amount"] += float(df["Receipt Amount"].sum())
            entry["files"] += 1
            entry["transactions"] += len(df)

    miss_breakdown: Dict[str, Dict[str, Any]] = {}
    rounding_breakdown: List[Dict[str, Any]] = []
    for detail in getattr(integration, "last_misc_details", []):
        method = detail.get("method", "")
        entry = miss_breakdown.setdefault(method, {"amount": 0.0, "cap_count": 0, "example": ""})
        entry["amount"] += float(detail.get("misc_amount", 0.0))
        if detail.get("cap_applied"):
            entry["cap_count"] += 1
        if not entry["example"]:
            entry["example"] = format_misc_example(detail)
        if detail.get("cash_rounding"):
            rounding_breakdown.append(
                {
                    "store": detail.get("store", ""),
                    "date": compact_to_iso(str(detail.get("date", ""))),
                    "rounding_amount": float(detail.get("receipt_amount", 0.0)),
                    "miss_amount": float(detail.get("misc_amount", 0.0)),
                }
            )

    summary = {
        "total_sales_amount": round(total_sales_amount, 3),
        "total_ar_amount": round(total_ar_amount, 3),
        "total_standard_receipts": round(total_standard, 3),
        "total_miss_receipts": round(abs(total_miss_raw), 3),
        "net_settlement": round(net_settlement, 3),
        "verification_passed": verification_passed,
        "caps": [],
        "rounding": [fmt for fmt in (d.get("store") for d in rounding_breakdown) if fmt],
    }
    return summary, standard_breakdown, miss_breakdown, rounding_breakdown


def build_result_payload(integration: Any, output_dir: Path, job_dir: Path) -> Dict[str, Any]:
    summary, standard_breakdown, miss_breakdown, rounding_breakdown = build_summary(integration)

    ar_path = getattr(integration, "last_ar_path", None)
    if ar_path is None:
        ar_candidates = list((output_dir / "AR_Invoices").glob("*.csv"))
        ar_path = ar_candidates[0] if ar_candidates else None

    verification_path = getattr(integration, "last_log_path", None)
    if verification_path is None:
        ver_candidates = list(output_dir.glob("Verification_Report_*.txt"))
        verification_path = ver_candidates[0] if ver_candidates else None

    standard_paths = getattr(integration, "last_standard_paths", []) or list((output_dir / "Receipts").rglob("*.csv"))
    misc_paths = getattr(integration, "last_misc_paths", []) or list((output_dir / "MiscReceipts").rglob("*.csv"))

    standard_zip = job_dir / "standard_receipts.zip"
    miss_zip = job_dir / "misc_receipts.zip"
    all_zip = job_dir / "all_outputs.zip"

    create_zip(standard_zip, standard_paths)
    create_zip(miss_zip, misc_paths)

    all_files: List[Path] = []
    for candidate in [ar_path, verification_path]:
        if candidate:
            all_files.append(Path(candidate))
    all_files.extend(standard_paths)
    all_files.extend(misc_paths)
    unique_files = list(dict.fromkeys(all_files))
    create_zip(all_zip, unique_files)

    return {
        "ar_path": str(ar_path) if ar_path else "",
        "verification_report": str(verification_path) if verification_path else "",
        "standard_zip": str(standard_zip),
        "miss_zip": str(miss_zip),
        "all_zip": str(all_zip),
        "summary": summary,
        "standard_breakdown": standard_breakdown,
        "miss_breakdown": miss_breakdown,
        "rounding_breakdown": rounding_breakdown,
        "progress_steps": [
            "Preparing files...",
            "Loading input data...",
            "Generating AR invoices...",
            "Aggregating receipts...",
            "Calculating misc receipts...",
            "Running verification...",
            "Saving files...",
            "Complete",
        ],
    }


def parse_date(value: str) -> datetime:
    try:
        return datetime.strptime(value, "%Y-%m-%d")
    except Exception:
        return datetime.now()


def format_transaction_number(num: int, customer_type: str) -> str:
    if customer_type.upper() in BNPL_METHODS:
        return f"BLK-{num:04d}"
    return f"BLK-{num:07d}"


def make_safe_name(name: str) -> str:
    return "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in str(name))


def to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).lower() in {"1", "true", "yes", "y", "on"}


def compute_unit_price(amount: float, qty: float) -> float:
    if qty == 0:
        return amount
    return amount / qty


def is_discount(name: str) -> bool:
    lowered = str(name).lower()
    return "discount" in lowered


def aggregate_method_key(store: str, dt: datetime, method: str) -> Tuple[str, str, str]:
    date_str = dt.strftime("%Y-%m-%d")
    return store, date_str, method


def save_csv(df: pd.DataFrame, path: str) -> None:
    ensure_dir(Path(path).parent)
    df.to_csv(path, index=False, encoding="utf-8-sig", quoting=csv.QUOTE_ALL)


@app.route("/")
def index() -> str:
    return render_template("index.html")


@app.route("/api/validate-file", methods=["POST"])
def api_validate_file():
    file_type = request.form.get("fileType")
    uploaded = request.files.get("file")
    valid, missing = validate_upload(uploaded, file_type)
    return jsonify({"valid": valid, "missing": missing})


@app.route("/api/config/defaults", methods=["GET"])
def api_defaults():
    return jsonify(default_charge_config())


@app.route("/api/config/save", methods=["POST"])
def api_save_config():
    payload = request.get_json(force=True, silent=True) or {}
    ensure_dir("config")
    path = Path("config") / "payment_config.json"
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
    return jsonify({"saved": True, "path": str(path)})


@app.route("/api/process", methods=["POST"])
def api_process():
    txn_date_str = request.form.get("transaction_date") or today_iso()
    acct_date_str = request.form.get("accounting_date") or today_iso()
    start_seq = int(request.form.get("start_sequence") or 1)
    legacy1 = int(request.form.get("legacy_segment1") or 1)
    legacy2 = int(request.form.get("legacy_segment2") or 1)
    tax_rate = float(request.form.get("tax_rate") or 5.0)
    charges_json = request.form.get("charges") or ""
    try:
        charges = json.loads(charges_json) if charges_json else default_charge_config()["methods"]
    except Exception:
        charges = default_charge_config()["methods"]

    required_files = {
        "line_items": request.files.get("line_items"),
        "payments": request.files.get("payments"),
        "metadata": request.files.get("metadata"),
        "registers": request.files.get("registers"),
    }
    for key, value in required_files.items():
        valid, missing = validate_upload(value, key)
        if not valid:
            return (
                jsonify({"error": f"{key} invalid", "missing": missing}),
                400,
            )

    job_id = uuid.uuid4().hex
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    job_dir = Path("outputs") / f"{timestamp}_{job_id}"
    ensure_dir(job_dir)

    saved_paths = {}
    for name, file_storage in required_files.items():
        target = job_dir / f"{name}{Path(file_storage.filename).suffix}"
        file_storage.save(target)
        saved_paths[name] = target

    payload = {
        "transaction_date": txn_date_str,
        "accounting_date": acct_date_str,
        "start_sequence": start_seq,
        "legacy1": legacy1,
        "legacy2": legacy2,
        "tax_rate": tax_rate,
        "charges": charges,
        "paths": saved_paths,
        "job_dir": str(job_dir),
    }

    jobs[job_id] = {
        "status": "queued",
        "progress": 0,
        "message": "Queued",
        "result": None,
        "errors": [],
    }

    thread = threading.Thread(target=process_job, args=(job_id, payload), daemon=True)
    thread.start()

    return jsonify({"job_id": job_id})


@app.route("/api/status/<job_id>", methods=["GET"])
def api_status(job_id: str):
    job = jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404
    return jsonify(job)


@app.route("/api/download/<job_id>/ar", methods=["GET"])
def download_ar(job_id: str):
    job = jobs.get(job_id)
    if not job or not job.get("result"):
        return jsonify({"error": "Job not ready"}), 404
    ar_path = job["result"]["ar_path"]
    return send_file(ar_path, as_attachment=True)


@app.route("/api/download/<job_id>/standard", methods=["GET"])
def download_standard(job_id: str):
    job = jobs.get(job_id)
    if not job or not job.get("result"):
        return jsonify({"error": "Job not ready"}), 404
    path = job["result"]["standard_zip"]
    return send_file(path, as_attachment=True)


@app.route("/api/download/<job_id>/miss", methods=["GET"])
def download_miss(job_id: str):
    job = jobs.get(job_id)
    if not job or not job.get("result"):
        return jsonify({"error": "Job not ready"}), 404
    path = job["result"]["miss_zip"]
    return send_file(path, as_attachment=True)


@app.route("/api/download/<job_id>/all", methods=["GET"])
def download_all(job_id: str):
    job = jobs.get(job_id)
    if not job or not job.get("result"):
        return jsonify({"error": "Job not ready"}), 404
    path = job["result"]["all_zip"]
    return send_file(path, as_attachment=True)


@app.route("/api/download/<job_id>/verification", methods=["GET"])
def download_verification(job_id: str):
    job = jobs.get(job_id)
    if not job or not job.get("result"):
        return jsonify({"error": "Job not ready"}), 404
    path = job["result"]["verification_report"]
    return send_file(path, as_attachment=True)


# ======================================================================================
# PROCESSING PIPELINE
# ======================================================================================


def process_job(job_id: str, payload: Dict[str, Any]) -> None:
    try:
        update_job(job_id, status="running", progress=5, message="Preparing files...")

        module = load_template_module()
        output_dir = Path(payload["job_dir"]) / "ORACLE_FUSION_OUTPUT"
        ensure_dir(output_dir)

        bank_charges_path = build_bank_charges_csv(
            payload.get("charges", []),
            payload.get("tax_rate", 5.0),
            output_dir,
        )

        update_job(job_id, progress=15, message="Loading input data...")

        integration = module.OracleFusionIntegration(
            output_dir=str(output_dir),
            start_seq=payload.get("start_sequence", 1),
            start_legacy_seq_1=payload.get("legacy1", 1),
            start_legacy_seq_2=payload.get("legacy2", 1),
        )

        paths = payload["paths"]
        integration.load_data(
            str(paths["line_items"]),
            str(paths["payments"]),
            str(paths["metadata"]),
            str(paths["registers"]),
            str(bank_charges_path) if bank_charges_path else None,
        )

        update_job(job_id, progress=35, message="Generating AR invoices...")
        ar_df = integration.generate_ar_invoices()
        integration.save_ar(ar_df)

        update_job(job_id, progress=55, message="Aggregating receipts...")
        receipt_files = integration.generate_receipts()
        integration.save_receipts(receipt_files)

        update_job(job_id, progress=70, message="Calculating misc receipts...")
        misc_files = integration.generate_misc_receipts(receipt_files)
        integration.save_misc_receipts(misc_files)

        update_job(job_id, progress=82, message="Running verification...")
        integration._write_final_crosscheck(ar_df, receipt_files, misc_files)
        integration.vlog.close()
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_path = output_dir / f"Verification_Report_{ts}.txt"
        integration.vlog.write(log_path)
        integration.last_log_path = log_path
        integration.vlog.print_summary()

        update_job(job_id, progress=92, message="Saving files...")
        result_payload = build_result_payload(integration, output_dir, Path(payload["job_dir"]))

        update_job(
            job_id,
            progress=100,
            status="completed",
            message="Processing complete",
            result=result_payload,
        )
    except Exception as exc:  # noqa: BLE001
        update_job(job_id, status="failed", progress=100, message=str(exc))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
