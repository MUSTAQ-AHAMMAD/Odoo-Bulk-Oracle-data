import csv
import json
import os
import threading
import uuid
from datetime import datetime, date
from io import BytesIO
from pathlib import Path
from typing import Dict, List, Tuple, Any

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
        update_job(job_id, status="running", progress=5, message="Forward filling payments...")

        txn_date = parse_date(payload["transaction_date"])
        acct_date = parse_date(payload["accounting_date"])
        txn_date_str = txn_date.strftime("%Y-%m-%d 00:00:00")
        acct_date_str = acct_date.strftime("%Y-%m-%d 00:00:00")

        paths = payload["paths"]
        line_items_df = read_table(open(paths["line_items"], "rb"), "line_items")
        payments_df = read_table(open(paths["payments"], "rb"), "payments")
        metadata_df = read_table(open(paths["metadata"], "rb"), "metadata")
        registers_df = read_table(open(paths["registers"], "rb"), "registers")

        # Step 1: forward fill critical columns
        payments_df["Order Ref"] = payments_df["Order Ref"].ffill()
        payments_df["Branch"] = payments_df["Branch"].ffill()

        warnings_log: List[str] = []
        caps_log: List[str] = []
        rounding_log: List[str] = []

        update_job(job_id, progress=12, message="Building invoice index...")

        # Build mapping for orders to store and date
        order_store_map: Dict[str, str] = {}
        order_date_map: Dict[str, datetime] = {}

        for _, row in line_items_df.iterrows():
            order_ref = str(row.get("Order Lines/Order Ref", "")).strip()
            if not order_ref:
                continue
            store_name = str(row.get("Order Lines/Register Name", "")).strip()
            date_val = row.get("Order Lines/Order Ref/Date")
            parsed_date = pd.to_datetime(date_val, errors="coerce")
            if pd.isna(parsed_date):
                parsed_date = txn_date
            order_store_map[order_ref] = store_name
            order_date_map[order_ref] = parsed_date

        register_col = next((c for c in registers_df.columns if "REGISTER_NAME" in str(c).upper()), None)
        register_lookup = {}
        if register_col:
            for _, row in registers_df.iterrows():
                register_lookup[str(row.get(register_col, "")).strip()] = row.to_dict()

        metadata_lookup = {}
        for _, row in metadata_df.iterrows():
            metadata_lookup[str(row.get("SUBINVENTORY", "")).strip()] = row.to_dict()

        update_job(job_id, progress=18, message="Determining customer types...")

        customer_type_map: Dict[str, str] = {}
        payments_df["method_norm"] = payments_df["Payments/Payment Method"].apply(normalize_method)
        for order_ref, group in payments_df.groupby("Order Ref"):
            methods = set(group["method_norm"])
            if any(m == "TAMARA" for m in methods):
                customer_type_map[str(order_ref)] = "TAMARA"
            elif any(m == "TABBY" for m in methods):
                customer_type_map[str(order_ref)] = "TABBY"
            else:
                customer_type_map[str(order_ref)] = "NORMAL"

        update_job(job_id, progress=26, message="Generating AR invoices...")

        transaction_map: Dict[str, str] = {}
        normal_counter = payload["start_sequence"]
        tabby_counter = payload["start_sequence"]
        tamara_counter = payload["start_sequence"]
        segment1 = payload["legacy1"]
        segment2 = payload["legacy2"]

        ar_records: List[Dict[str, Any]] = []
        total_sales_amount = 0.0

        for _, row in line_items_df.iterrows():
            order_ref = str(row.get("Order Lines/Order Ref", "")).strip()
            if not order_ref:
                warnings_log.append("Skipped line item with blank Order Ref.")
                continue

            store_name = order_store_map.get(order_ref)
            order_dt = order_date_map.get(order_ref, txn_date)
            if store_name is None or order_dt is None:
                warnings_log.append(f"Skipped line item {order_ref} due to missing store/date.")
                continue

            customer_type = customer_type_map.get(order_ref, "NORMAL")
            if order_ref not in transaction_map:
                if customer_type == "TABBY":
                    if tabby_counter > 9999:
                        raise ValueError("TABBY sequence overflow (max 9999)")
                    transaction_map[order_ref] = format_transaction_number(tabby_counter, customer_type)
                    tabby_counter += 1
                elif customer_type == "TAMARA":
                    if tamara_counter > 9999:
                        raise ValueError("TAMARA sequence overflow (max 9999)")
                    transaction_map[order_ref] = format_transaction_number(tamara_counter, customer_type)
                    tamara_counter += 1
                else:
                    if normal_counter > 9_999_999:
                        raise ValueError("NORMAL sequence overflow (max 9999999)")
                    transaction_map[order_ref] = format_transaction_number(normal_counter, customer_type)
                    normal_counter += 1

            amount = float(row.get("Order Lines/Subtotal w/o Tax", 0) or 0)
            qty = float(row.get("Order Lines/Quantity", 0) or 0)
            product_name = str(row.get("Order Lines/Product/Name", ""))
            barcode = str(row.get("Order Lines/Product/Barcode", ""))

            discount_flag = is_discount(product_name)
            inventory_item = "" if discount_flag else barcode
            memo_line = "Discount Item" if discount_flag else ""
            line_desc = "Discount Item" if discount_flag else product_name[:240]

            unit_price = compute_unit_price(amount, qty)

            record = {col: "" for col in AR_COLUMNS}
            record["Transaction Batch Source Name"] = AR_STATIC["Transaction Batch Source Name"]
            record["Transaction Type Name"] = AR_STATIC["Transaction Type Name"]
            record["Payment Terms"] = AR_STATIC["Payment Terms"]
            record["Transaction Date"] = txn_date_str
            record["Accounting Date"] = acct_date_str
            record["Transaction Number"] = transaction_map[order_ref]
            record["Transaction Line Type"] = AR_STATIC["Transaction Line Type"]
            record["Transaction Line Description"] = line_desc
            record["Currency Code"] = AR_STATIC["Currency Code"]
            record["Currency Conversion Type"] = AR_STATIC["Currency Conversion Type"]
            record["Currency Conversion Date"] = acct_date.strftime("%Y-%m-%d")
            record["Currency Conversion Rate"] = AR_STATIC["Currency Conversion Rate"]
            record["Transaction Line Amount"] = round(amount, 2)
            record["Transaction Line Quantity"] = qty
            record["Unit Selling Price"] = round(unit_price, 4)
            record["Line Transactions Flexfield Context"] = AR_STATIC["Line Transactions Flexfield Context"]
            record["Line Transactions Flexfield Segment 1"] = f"LEGACY{segment1:08d}"
            record["Line Transactions Flexfield Segment 2"] = f"LEGACY{segment2:08d}"
            record["Tax Classification Code"] = "OUTPUT-GOODS-DOM-15%"
            record["Sales Order Number"] = order_ref
            record["Unit of Measure Code"] = AR_STATIC["Unit of Measure Code"]
            record["Default Taxation Country"] = AR_STATIC["Default Taxation Country"]
            record["Inventory Item Number"] = inventory_item
            record["Comments"] = AR_STATIC["Comments"]
            record["END"] = AR_STATIC["END"]
            if memo_line:
                record["Memo Line Name"] = memo_line

            # Metadata mapping
            meta_row = metadata_lookup.get(store_name, {})
            if store_name not in metadata_lookup:
                warnings_log.append(f"Metadata lookup missing for store {store_name}; leaving customer fields blank.")
            record["Bill-to Customer Account Number"] = meta_row.get("BILL_TO_ACCOUNT", "")
            record["Bill-to Customer Site Number"] = meta_row.get("SITE_NUMBER", "")

            ar_records.append(record)
            total_sales_amount += amount
            segment1 += 1
            segment2 += 1

        ar_df = pd.DataFrame(ar_records, columns=AR_COLUMNS)

        update_job(job_id, progress=40, message="Calculating total sales...")

        total_ar_amount = float(ar_df["Transaction Line Amount"].sum()) if not ar_df.empty else 0.0

        # Step 6: Standard receipts aggregation
        update_job(job_id, progress=50, message="Generating standard receipts...")

        standard_groups: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
        for _, row in payments_df.iterrows():
            order_ref = str(row.get("Order Ref", "")).strip()
            method = normalize_method(row.get("Payments/Payment Method", ""))
            if method not in STANDARD_RECEIPT_METHODS:
                continue
            store_name = order_store_map.get(order_ref)
            sale_dt = order_date_map.get(order_ref, txn_date)
            if not store_name or sale_dt is None:
                warnings_log.append(f"Payment for Order Ref {order_ref} skipped (no matching store/date).")
                continue
            amount = float(row.get("Payments/Amount", 0) or 0)
            key = aggregate_method_key(store_name, sale_dt, method)
            group = standard_groups.setdefault(
                key,
                {
                    "amount": 0.0,
                    "transactions": 0,
                    "customer": metadata_lookup.get(store_name, {}),
                },
            )
            group["amount"] += amount
            group["transactions"] += 1

        standard_receipt_files: Dict[str, pd.DataFrame] = {}
        standard_breakdown: Dict[str, Dict[str, Any]] = {}
        seq_global = 1
        standard_folder = Path(payload["job_dir"]) / "Receipts" / "Standard"
        ensure_dir(standard_folder)

        for (store, date_str, method), data in standard_groups.items():
            records = []
            seq_local = 1
            safe_store = make_safe_name(store or "STORE")
            filename = f"Receipt_{method}_{safe_store}_{date_str}.csv"
            receipt_number_base = f"{method}-{safe_store}-{date_str}"
            meta_row = data.get("customer", {})
            if not meta_row:
                warnings_log.append(f"Metadata missing for standard receipt store {store}; using blanks.")
            business_unit = meta_row.get("BUSINESS_UNIT", "")
            customer_name = meta_row.get("BILL_TO_NAME", store)
            customer_account = meta_row.get("BILL_TO_ACCOUNT", "")
            customer_site = meta_row.get("SITE_NUMBER", "")
            for _ in range(1):
                record = {
                    "Business Unit": business_unit,
                    "Batch Source": "Spreadsheet",
                    "Batch Name": f"Receipt_{method}_{safe_store}_{date_str}",
                    "Receipt Method": method,
                    "Remittance Bank": method,
                    "Remittance Bank Account": f"{method} Account",
                    "Batch Date": f"{date_str} 00:00:00",
                    "Accounting Date": f"{date_str} 00:00:00",
                    "Deposit Date": f"{date_str} 00:00:00",
                    "Currency": "SAR",
                    "Sequence Number": f"{seq_local:04d}",
                    "Receipt Number": f"{receipt_number_base}-{seq_global:04d}",
                    "Receipt Amount": round(data["amount"], 2),
                    "Receipt Date": f"{date_str} 00:00:00",
                    "Accounting Date (Receipt)": f"{date_str} 00:00:00",
                    "Currency (Receipt)": "SAR",
                    "Document Number": "",
                    "Customer Name": customer_name,
                    "Customer Account Number": customer_account,
                    "Customer Site Number": customer_site,
                    "Misc Charge Type": "",
                    "Original Payment Method": method,
                    "Calculation Details": "",
                }
                records.append(record)
                seq_local += 1
                seq_global += 1
            df = pd.DataFrame(records, columns=RECEIPT_COLUMNS)
            path = standard_folder / filename
            save_csv(df, path)
            standard_receipt_files[str(path)] = df
            breakdown = standard_breakdown.setdefault(method, {"amount": 0.0, "files": 0, "transactions": 0})
            breakdown["amount"] += data["amount"]
            breakdown["files"] += 1
            breakdown["transactions"] += data["transactions"]

        # Step 7: Miss receipts for bank charges
        update_job(job_id, progress=62, message="Calculating bank charges...")

        method_config = {m["method"].upper(): m for m in payload["charges"]}
        miss_bank_records: Dict[str, pd.DataFrame] = {}
        miss_folder = Path(payload["job_dir"]) / "Receipts" / "Miss" / "Bank_Charges"
        ensure_dir(miss_folder)

        miss_breakdown: Dict[str, Dict[str, Any]] = {}

        grouped_payments = payments_df.copy()
        grouped_payments["store"] = grouped_payments["Order Ref"].map(order_store_map)
        grouped_payments["sale_date"] = grouped_payments["Order Ref"].map(order_date_map)

        for (store, sale_dt, method), group in grouped_payments.groupby(["store", "sale_date", "method_norm"]):
            if not store or pd.isna(sale_dt):
                warnings_log.append("Payment missing store/date for bank charge; skipped.")
                continue
            config = method_config.get(method)
            if not config or not to_bool(config.get("generate_miss")):
                continue

            amount = float(group["Payments/Amount"].sum())
            bank_pct = float(config.get("bank_charge_pct", 0.0))
            cap_amount = float(config.get("cap", 0.0)) if to_bool(config.get("apply_cap")) else None
            bank_rate = bank_pct / 100.0
            tax_rate = payload["tax_rate"] / 100.0
            temp1 = amount * bank_rate
            temp2 = 1 + tax_rate
            misc_charges = temp1 * temp2
            cap_applied = False
            if cap_amount is not None and misc_charges > cap_amount:
                misc_charges = cap_amount
                cap_applied = True
                caps_log.append(
                    f"Cap applied for {method} {store} {sale_dt.date()}: capped to {cap_amount:.2f}"
                )
            misc_receipt_amount = 0 - misc_charges

            date_str = pd.to_datetime(sale_dt).strftime("%Y-%m-%d")
            safe_store = make_safe_name(store)
            filename = f"MISS_RCPT_{method}_{safe_store}_{date_str}.csv"
            meta_row = metadata_lookup.get(store, {})
            if store not in metadata_lookup:
                warnings_log.append(f"Metadata missing for bank charge store {store}; using blanks.")
            record = {
                "Business Unit": meta_row.get("BUSINESS_UNIT", ""),
                "Batch Source": "Spreadsheet",
                "Batch Name": f"MISS_RCPT_{method}_{safe_store}_{date_str}",
                "Receipt Method": method,
                "Remittance Bank": method,
                "Remittance Bank Account": f"{method} Account",
                "Batch Date": f"{date_str} 00:00:00",
                "Accounting Date": f"{date_str} 00:00:00",
                "Deposit Date": f"{date_str} 00:00:00",
                "Currency": "SAR",
                "Sequence Number": "0001",
                "Receipt Number": f"MISC-{method}-{safe_store}-{date_str}-001",
                "Receipt Amount": round(misc_receipt_amount, 3),
                "Receipt Date": f"{date_str} 00:00:00",
                "Accounting Date (Receipt)": f"{date_str} 00:00:00",
                "Currency (Receipt)": "SAR",
                "Document Number": "",
                "Customer Name": meta_row.get("BILL_TO_NAME", store),
                "Customer Account Number": meta_row.get("BILL_TO_ACCOUNT", ""),
                "Customer Site Number": meta_row.get("SITE_NUMBER", ""),
                "Misc Charge Type": "Bank Charge",
                "Original Payment Method": method,
                "Calculation Details": f"{amount:.2f} × {bank_rate:.3f} = {temp1:.3f} × {temp2:.3f} = {temp1 * temp2:.3f} → {misc_receipt_amount:.3f}",
            }

            df = pd.DataFrame([record], columns=RECEIPT_COLUMNS)
            path = miss_folder / filename
            save_csv(df, path)
            miss_bank_records[str(path)] = df

            br = miss_breakdown.setdefault(method, {"amount": 0.0, "cap_count": 0, "example": record["Calculation Details"]})
            br["amount"] += misc_receipt_amount
            if cap_applied:
                br["cap_count"] += 1

        # Step 8: Cash rounding miss receipts
        update_job(job_id, progress=74, message="Generating cash rounding receipts...")

        miss_rounding_records: Dict[str, pd.DataFrame] = {}
        rounding_folder = Path(payload["job_dir"]) / "Receipts" / "Miss" / "Cash_Rounding"
        ensure_dir(rounding_folder)

        rounding_groups: Dict[Tuple[str, str], float] = {}
        for _, row in payments_df.iterrows():
            method_norm = normalize_method(row.get("Payments/Payment Method", ""))
            if not any(key in method_norm for key in ROUNDING_KEYWORDS):
                continue
            order_ref = str(row.get("Order Ref", "")).strip()
            store_name = order_store_map.get(order_ref)
            sale_dt = order_date_map.get(order_ref, txn_date)
            if not store_name or sale_dt is None:
                warnings_log.append("Rounding payment missing store/date; skipped.")
                continue
            amount = float(row.get("Payments/Amount", 0) or 0)
            key = (store_name, pd.to_datetime(sale_dt).strftime("%Y-%m-%d"))
            rounding_groups[key] = rounding_groups.get(key, 0.0) + amount

        for (store, date_str), amt in rounding_groups.items():
            misc_charges = amt
            misc_receipt_amount = 0 - misc_charges
            safe_store = make_safe_name(store)
            filename = f"MISS_RCPT_CashRounding_{safe_store}_{date_str}.csv"
            meta_row = metadata_lookup.get(store, {})
            if store not in metadata_lookup:
                warnings_log.append(f"Metadata missing for rounding store {store}; using blanks.")
            record = {
                "Business Unit": meta_row.get("BUSINESS_UNIT", ""),
                "Batch Source": "Spreadsheet",
                "Batch Name": f"MISS_RCPT_CASH_{safe_store}_{date_str}",
                "Receipt Method": "Cash",
                "Remittance Bank": "Cash",
                "Remittance Bank Account": "Cash Account",
                "Batch Date": f"{date_str} 00:00:00",
                "Accounting Date": f"{date_str} 00:00:00",
                "Deposit Date": f"{date_str} 00:00:00",
                "Currency": "SAR",
                "Sequence Number": "0001",
                "Receipt Number": f"MISC-CASH-{safe_store}-{date_str}-001",
                "Receipt Amount": round(misc_receipt_amount, 3),
                "Receipt Date": f"{date_str} 00:00:00",
                "Accounting Date (Receipt)": f"{date_str} 00:00:00",
                "Currency (Receipt)": "SAR",
                "Document Number": "",
                "Customer Name": meta_row.get("BILL_TO_NAME", store),
                "Customer Account Number": meta_row.get("BILL_TO_ACCOUNT", ""),
                "Customer Site Number": meta_row.get("SITE_NUMBER", ""),
                "Misc Charge Type": "Cash Rounding",
                "Original Payment Method": "Cash",
                "Calculation Details": f"Rounding amount {amt:.3f} → -{amt:.3f} = {misc_receipt_amount:.3f}",
            }
            rounding_log.append(record["Calculation Details"])
            df = pd.DataFrame([record], columns=RECEIPT_COLUMNS)
            path = rounding_folder / filename
            save_csv(df, path)
            miss_rounding_records[str(path)] = df

        # Step 9: Verification cross-check
        update_job(job_id, progress=84, message="Verifying totals...")

        total_standard = sum(v["Receipt Amount"].sum() for v in standard_receipt_files.values()) if standard_receipt_files else 0.0
        total_miss = 0.0
        for df in miss_bank_records.values():
            total_miss += df["Receipt Amount"].sum()
        for df in miss_rounding_records.values():
            total_miss += df["Receipt Amount"].sum()
        net_settlement = total_standard + total_miss
        verification_ok = abs(total_ar_amount - (total_standard + abs(total_miss))) <= 0.01

        # Step 10: Save outputs and report
        update_job(job_id, progress=92, message="Saving files...")

        ar_path = Path(payload["job_dir"]) / "AR_Invoices.csv"
        save_csv(ar_df, ar_path)

        verification_report = Path(payload["job_dir"]) / "verification_report.txt"
        with open(verification_report, "w", encoding="utf-8") as handle:
            handle.write("FUSION INTEGRATION VERIFICATION REPORT\n")
            handle.write("=" * 60 + "\n")
            handle.write(f"Total AR Amount: {total_ar_amount:.3f}\n")
            handle.write(f"Total Standard Receipts: {total_standard:.3f}\n")
            handle.write(f"Total Miss Receipts: {total_miss:.3f}\n")
            handle.write(f"Net Settlement: {net_settlement:.3f}\n")
            handle.write(
                f"Check (AR == Standard + abs(Miss)): {'PASS' if verification_ok else 'FAIL'}\n\n"
            )
            if caps_log:
                handle.write("Cap Applied Details:\n")
                for line in caps_log:
                    handle.write(f"- {line}\n")
                handle.write("\n")
            if rounding_log:
                handle.write("Cash Rounding Adjustments:\n")
                for line in rounding_log:
                    handle.write(f"- {line}\n")
                handle.write("\n")
            if warnings_log:
                handle.write("Warnings:\n")
                for w in warnings_log:
                    handle.write(f"- {w}\n")

        # Create zip bundles
        all_zip = Path(payload["job_dir"]) / "all_outputs.zip"
        standard_zip = Path(payload["job_dir"]) / "standard_receipts.zip"
        miss_zip = Path(payload["job_dir"]) / "miss_receipts.zip"

        def build_zip(zip_path: Path, files: List[Path]) -> None:
            import zipfile

            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
                for file in files:
                    zf.write(file, arcname=file.name)

        build_zip(standard_zip, [Path(p) for p in standard_receipt_files.keys()])
        miss_files = [Path(p) for p in miss_bank_records.keys()] + [Path(p) for p in miss_rounding_records.keys()]
        build_zip(miss_zip, miss_files)
        all_files = [ar_path, verification_report] + list(Path(payload["job_dir"]).rglob("*.csv"))
        build_zip(all_zip, all_files)

        update_job(
            job_id,
            progress=100,
            status="completed",
            message="Processing complete",
            result={
                "ar_path": str(ar_path),
                "verification_report": str(verification_report),
                "standard_zip": str(standard_zip),
                "miss_zip": str(miss_zip),
                "all_zip": str(all_zip),
                "summary": {
                    "total_sales_amount": round(total_sales_amount, 3),
                    "total_ar_amount": round(total_ar_amount, 3),
                    "total_standard_receipts": round(total_standard, 3),
                    "total_miss_receipts": round(abs(total_miss), 3),
                    "net_settlement": round(net_settlement, 3),
                    "verification_passed": verification_ok,
                    "caps": caps_log,
                    "rounding": rounding_log,
                },
                "standard_breakdown": standard_breakdown,
                "miss_breakdown": miss_breakdown,
                "rounding_breakdown": [
                    {"store": store, "date": date, "rounding_amount": amt, "miss_amount": -amt}
                    for (store, date), amt in rounding_groups.items()
                ],
                "progress_steps": [
                    "Forward filling payments...",
                    "Building invoice index...",
                    "Generating AR invoices...",
                    "Calculating total sales...",
                    "Generating standard receipts...",
                    "Calculating bank charges...",
                    "Generating cash rounding receipts...",
                    "Verifying totals...",
                    "Saving files...",
                    "Complete",
                ],
            },
        )
    except Exception as exc:  # noqa: BLE001
        update_job(job_id, status="failed", progress=100, message=str(exc))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
