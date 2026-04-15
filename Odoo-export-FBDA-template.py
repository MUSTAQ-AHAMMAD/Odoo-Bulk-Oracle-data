"""
================================================================================
ORACLE FUSION FINANCIAL INTEGRATION MODULE
================================================================================

MAPPING RULES:
--------------
1. Bill-to Customer Account Number  => BILL_TO_ACCOUNT from FUSION_SALES_METADATA
   Bill-to Customer Site Number     => BILL_TO_SITE_NUMBER (or SITE_NUMBER)
                                       from FUSION_SALES_METADATA
   Lookup key: SUBINVENTORY + CUSTOMER_TYPE

2. AR Invoice columns match EXACTLY the FBDA template

3. Inventory Item Number (Barcode) stored as text - no scientific notation

4. Standard Receipts generated ONLY for: Cash, Mada, Visa, MasterCard
   NOT for: Tabby, Tamara

5. Receipt Aggregation:
   - Payments file is forward-filled (Odoo merged-cell export fix)
   - Amounts summed row-by-row per (store, date, method)
   - ONE FILE PER PAYMENT METHOD per store per date

6. Transaction Number format:
   - NORMAL : BLK-{SEQ:07d}
   - TABBY  : BLK-{SEQ:04d}
   - TAMARA : BLK-{SEQ:04d}

7. LEGACY Flexfield Segments:
   - Two independent random 9-digit start numbers generated every run
   - Guaranteed different from each other (min 50M gap)
   - No manual input needed — changes automatically each run

8. Miscellaneous Receipts:
   - Generated for each standard receipt (Cash, Mada, Visa, MasterCard)
   - Formula: MiscAmount = 0 - (payment_amount × bank_charge_rate × (1 + tax_rate))
   - Cash Rounding: MiscAmount = 0 - raw_rounding_amount
   - Cap: if miscCharges > cap_amount → cap at cap_amount (per method config)
   - Bank charge rates loaded from BANK_CHARGES CSV file

================================================================================
INPUT FILES:
    - Line Items Excel          : POS Order lines
    - Payments Excel            : POS Payments  (forward-filled Order Ref)
    - FUSION_SALES_METADATA CSV : customer account/site mapping
    - VENDHQ_REGISTERS CSV      : register name to outlet mapping
    - BANK_CHARGES CSV          : bank charge rates per payment method
================================================================================
"""

from __future__ import annotations

import random
import re
import time
import warnings
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

# ============================================================================
# CONSTANTS
# ============================================================================

RECEIPT_PAYMENT_METHODS    = {"Cash", "Mada", "Visa", "MasterCard"}
NO_RECEIPT_PAYMENT_METHODS = {"TABBY", "TAMARA"}

PAYMENT_METHOD_NORM: Dict[str, str] = {
    "CASH":        "Cash",
    "MADA":        "Mada",
    "VISA":        "Visa",
    "MASTERCARD":  "MasterCard",
    "MASTER CARD": "MasterCard",
    "MC":          "MasterCard",
    "TAMARA":      "TAMARA",
    "TABBY":       "TABBY",
    "AMEX":        "Amex",
    "APPLE PAY":   "Apple Pay",
    "APPLEPAY":    "Apple Pay",
    "STC PAY":     "STC Pay",
    "STCPAY":      "STC Pay",
}

PAYMENT_BANK_MAP: Dict[str, Tuple[str, str]] = {
    "Cash":       ("Cash Bank",       "Cash Account"),
    "Mada":       ("Mada Bank",       "Mada Account"),
    "Visa":       ("Visa Bank",       "Visa Account"),
    "MasterCard": ("MasterCard Bank", "MasterCard Account"),
    "Amex":       ("Amex Bank",       "Amex Account"),
    "Apple Pay":  ("Apple Pay Bank",  "Apple Pay Account"),
    "STC Pay":    ("STC Pay Bank",    "STC Pay Account"),
}
DEFAULT_BANK: Tuple[str, str] = ("Cash Bank", "Cash Account")

AR_STATIC: Dict[str, str] = {
    "Transaction Batch Source Name":       "Manual_Imported",
    "Transaction Type Name":               "Vend Invoice",
    "Payment Terms":                       "IMMEDIATE",
    "Transaction Line Type":               "LINE",
    "Currency Code":                       "SAR",
    "Currency Conversion Type":            "Corporate",
    "Currency Conversion Rate":            "1",
    "Line Transactions Flexfield Context": "Legacy",
    "Unit of Measure Code":                "Ea",
    "Default Taxation Country":            "SA",
    "Comments":                            "AlQurashi-KSA",
    "END":                                 "END",
}

DEFAULT_TAX_CODE  = "OUTPUT-GOODS-DOM-15%"
DEFAULT_TAX_RATE  = 0.05    # 5% VAT applied to bank charges
DEFAULT_ORG_ID    = "101"   # Oracle Org ID — update if needed

# ============================================================================
# EXACT COLUMN NAMES AS THEY APPEAR IN THE EXCEL FILES
# ============================================================================
COL_LI_ORDER_REF  = "Order Lines/Order Ref"
COL_LI_AMOUNT     = "Order Lines/Subtotal w/o Tax"
COL_LI_BARCODE    = "Order Lines/Product/Barcode"
COL_LI_PRODUCT    = "Order Lines/Product/Name"
COL_LI_QTY        = "Order Lines/Quantity"
COL_LI_DATE       = "Order Lines/Order Ref/Date"
COL_LI_STORE      = "Order Lines/Register Name"

COL_PAY_ORDER_REF = "Order Ref"
COL_PAY_BRANCH    = "Branch"
COL_PAY_METHOD    = "Payments/Payment Method"
COL_PAY_AMOUNT    = "Payments/Amount"

# Misc receipt template columns (matches misc_receipt_template.csv exactly)
MISC_RECEIPT_COLUMNS = [
    "Amount",
    "CurrencyCode",
    "DepositDate",
    "ReceiptDate",
    "GlDate",
    "OrgId",
    "ReceiptNumber",
    "ReceiptMethodId",
    "ReceiptMethodName",
    "ReceivableActivityName",
    "BankAccountNumber",
]


# ============================================================================
# RANDOM SEQUENCE GENERATOR
# ============================================================================

def _generate_legacy_sequences() -> Tuple[int, int]:
    """
    Generates two independent random 9-digit start numbers.
    Guaranteed different by at least 50,000,000.
    Fresh every run — nanosecond clock + OS entropy.
    """
    rng = random.Random(time.time_ns())
    while True:
        seg1 = rng.randint(100_000_000, 899_999_999)
        seg2 = rng.randint(100_000_000, 899_999_999)
        if abs(seg1 - seg2) >= 50_000_000:
            return seg1, seg2


# ============================================================================
# BANK CHARGES LOADER
# ============================================================================

class BankChargesCache:
    """
    Loads bank charge configuration from a CSV file.

    Expected CSV columns:
        PAYMENT_METHOD   : e.g. Cash, Mada, Visa, MasterCard
        CHARGE_RATE      : e.g. 0.015  (1.5%)
        TAX_RATE         : e.g. 0.05   (5%) — optional, defaults to DEFAULT_TAX_RATE
        CAP_AMOUNT       : e.g. 10.00  — optional, 0 or blank = no cap
        RECEIPT_METHOD_ID: Oracle receipt method ID
        BANK_ACCOUNT_NUM : Bank account number for misc receipt
        ORG_ID           : Oracle Org ID
        ACTIVITY_NAME    : Receivable Activity Name (e.g. "Bank Charges")
        CASH_ROUNDING    : Y/N — if Y, this method uses cash rounding formula

    Example CSV:
        PAYMENT_METHOD,CHARGE_RATE,TAX_RATE,CAP_AMOUNT,RECEIPT_METHOD_ID,BANK_ACCOUNT_NUM,ORG_ID,ACTIVITY_NAME,CASH_ROUNDING
        Cash,0.0,0.05,0.0,11111,123456789,101,Cash Rounding,Y
        Mada,0.015,0.05,0.0,22222,987654321,101,Bank Charges,N
        Visa,0.02,0.05,10.0,33333,111222333,101,Bank Charges,N
        MasterCard,0.02,0.05,10.0,44444,444555666,101,Bank Charges,N
    """

    def __init__(self, charges_path: str):
        self.path:    str                  = charges_path
        self.charges: Dict[str, dict]      = {}
        self._load()

    def _load(self):
        df = pd.read_csv(self.path, encoding="utf-8-sig", dtype=str)
        df.columns = df.columns.str.strip().str.upper()

        required = {"PAYMENT_METHOD", "CHARGE_RATE", "RECEIPT_METHOD_ID",
                    "BANK_ACCOUNT_NUM", "ORG_ID", "ACTIVITY_NAME"}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(
                f"Bank charges CSV missing columns: {missing}\n"
                f"  Available: {list(df.columns)}"
            )

        for _, row in df.iterrows():
            method = normalise_payment(safe_str(row.get("PAYMENT_METHOD", "")))
            if not method:
                continue
            self.charges[method] = {
                "charge_rate":       safe_float(row.get("CHARGE_RATE", 0)),
                "tax_rate":          safe_float(row.get("TAX_RATE", DEFAULT_TAX_RATE)),
                "cap_amount":        safe_float(row.get("CAP_AMOUNT", 0)),
                "receipt_method_id": safe_str(row.get("RECEIPT_METHOD_ID", "")),
                "bank_account_num":  safe_str(row.get("BANK_ACCOUNT_NUM", "")),
                "org_id":            safe_str(row.get("ORG_ID", DEFAULT_ORG_ID)),
                "activity_name":     safe_str(row.get("ACTIVITY_NAME", "Bank Charges")),
                "cash_rounding":     safe_str(row.get("CASH_ROUNDING", "N")).upper() == "Y",
            }

    def get(self, method: str) -> Optional[dict]:
        """Returns charge config for the given payment method, or None."""
        return self.charges.get(method)

    def has_charges(self, method: str) -> bool:
        cfg = self.charges.get(method)
        if cfg is None:
            return False
        # cash rounding always generates misc receipt
        if cfg["cash_rounding"]:
            return True
        # standard: only if charge_rate > 0
        return cfg["charge_rate"] > 0


# ============================================================================
# MISC RECEIPT AMOUNT CALCULATOR
# ============================================================================

def calc_misc_receipt_amount(
    payment_amount: float,
    charge_rate:    float,
    tax_rate:       float,
    cap_amount:     float,
    cash_rounding:  bool,
) -> float:
    """
    Calculates the Miscellaneous Receipt amount.

    Standard formula (bank charges):
        temp1       = payment_amount × charge_rate
        temp2       = 1 + tax_rate
        miscCharges = temp1 × temp2
        if cap_amount > 0 and miscCharges > cap_amount:
            miscCharges = cap_amount
        MiscReceiptAmount = 0 - miscCharges

    Cash Rounding formula:
        miscCharges       = payment_amount   (raw rounding amount)
        MiscReceiptAmount = 0 - miscCharges

    Examples:
        Standard: 300 SAR, rate=0.015, tax=0.05, cap=0
            miscCharges = 300 × 0.015 × 1.05 = 4.725
            MiscAmount  = 0 - 4.725 = -4.725

        Capped:   1000 SAR, rate=0.02, tax=0.05, cap=10
            miscCharges = 1000 × 0.02 × 1.05 = 21.00
            21.00 > 10 → cap at 10.00
            MiscAmount  = 0 - 10.00 = -10.00

        Cash Rounding: -0.35 SAR
            miscCharges = -0.35
            MiscAmount  = 0 - (-0.35) = +0.35
    """
    if cash_rounding:
        misc_charges = payment_amount
    else:
        temp1        = payment_amount * charge_rate
        temp2        = 1.0 + tax_rate
        misc_charges = temp1 * temp2
        # apply cap (fix: correct comparison — cap if strictly greater than cap)
        if cap_amount > 0 and misc_charges > cap_amount:
            misc_charges = cap_amount

    return round(0.0 - misc_charges, 4)


# ============================================================================
# AR INVOICE COLUMN HEADERS  (matches FBDA template exactly)
# ============================================================================

def get_ar_columns() -> List[str]:
    return [
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
]


# ============================================================================
# HELPER UTILITIES
# ============================================================================

def safe_str(val) -> str:
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return ""
    return str(val).strip()


def normalise_store(name: str) -> str:
    return name.upper().strip()


def barcode_to_text(val) -> str:
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return ""
    raw = str(val).strip()
    if "e" in raw.lower():
        try:
            raw = str(int(float(raw)))
        except (ValueError, OverflowError):
            pass
    if raw.endswith(".0"):
        raw = raw[:-2]
    return raw


def safe_float(val, default: float = 0.0) -> float:
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def format_datetime(dt) -> str:
    if isinstance(dt, datetime):
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(dt, pd.Timestamp):
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    return str(dt)


def format_date(dt) -> str:
    if isinstance(dt, (datetime, pd.Timestamp)):
        return dt.strftime("%Y-%m-%d")
    return str(dt)[:10]


def normalise_payment(raw: str) -> str:
    key = raw.upper().strip()
    if key in PAYMENT_METHOD_NORM:
        return PAYMENT_METHOD_NORM[key]
    if "MADA"   in key: return "Mada"
    if "VISA"   in key: return "Visa"
    if "MASTER" in key or key.startswith("MC"): return "MasterCard"
    if "CASH"   in key: return "Cash"
    if "TAMARA" in key: return "TAMARA"
    if "TABBY"  in key: return "TABBY"
    if "APPLE"  in key: return "Apple Pay"
    if "STC"    in key: return "STC Pay"
    return raw.strip()


def is_discount_line(product_name: str) -> bool:
    if not product_name:
        return False
    lower = product_name.lower()
    return any(k in lower for k in ("discount", "100.0% discount", "100% discount"))


def safe_filename(text: str) -> str:
    return re.sub(r"[^A-Z0-9_]", "", text.upper().replace(" ", "_"))


# ============================================================================
# VERIFICATION LOGGER
# ============================================================================

class VerificationLog:

    def __init__(self):
        self.run_ts    = datetime.now()
        self.sections: List[Tuple[str, List[str]]] = []
        self._current: Optional[Tuple[str, List[str]]] = None

    def section(self, title: str):
        self._flush()
        self._current = (title, [])

    def _flush(self):
        if self._current:
            self.sections.append(self._current)
        self._current = None

    def add(self, line: str = ""):
        if self._current is None:
            self._current = ("GENERAL", [])
        self._current[1].append(line)

    def close(self):
        self._flush()

    def kv(self, label: str, value, width: int = 40):
        self.add(f"  {label:<{width}} {value}")

    def table_row(self, *cols, widths=(30, 12, 12, 12, 20)):
        parts = [f"{str(c):<{w}}" for c, w in zip(cols, widths)]
        self.add("  " + "  ".join(parts))

    def divider(self, char: str = "-", width: int = 70):
        self.add("  " + char * width)

    def write(self, path: Path):
        with open(path, "w", encoding="utf-8") as f:
            f.write("=" * 72 + "\n")
            f.write("  ORACLE FUSION INTEGRATION — VERIFICATION REPORT\n")
            f.write(f"  Generated : {self.run_ts.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("=" * 72 + "\n\n")
            for title, lines in self.sections:
                f.write(f"{'─'*72}\n")
                f.write(f"  {title}\n")
                f.write(f"{'─'*72}\n")
                for line in lines:
                    f.write(line + "\n")
                f.write("\n")
        print(f"  ✓ Verification report : {path}")

    def print_summary(self):
        print("\n" + "=" * 72)
        print("  VERIFICATION SUMMARY")
        print("=" * 72)
        for title, lines in self.sections:
            if any(kw in title.upper() for kw in
                   ("INPUT", "INVOICE", "AR RECORD", "RECEIPT", "MISC",
                    "METADATA", "PAYMENT", "FINAL", "SEQUENCE")):
                print(f"\n  ── {title}")
                for line in lines[:40]:
                    print(line)
                if len(lines) > 40:
                    print(f"       ... {len(lines)-40} more lines in report file")
        print("=" * 72 + "\n")


# ============================================================================
# METADATA LOADER
# ============================================================================

class MetadataCache:
    """
    Lookup key: (SUBINVENTORY.upper(), CUSTOMER_TYPE.upper())

    Fallback chain:
      1. exact      — direct match                               ✓
      2. partial    — prefix match, same ctype                   ⚠
      3. type_only  — ONLY when store name is empty/unknown      ⚠⚠
      4. none       — blank account/site returned                ✗✗
    """

    _SITE_COL_ALIASES = ("BILL_TO_SITE_NUMBER", "SITE_NUMBER")

    def __init__(self, metadata_path: str):
        self.path            = metadata_path
        self.primary:        Dict[Tuple[str, str], dict] = {}
        self.by_type:        Dict[str, dict]             = {}
        self._site_col_used: str                         = ""
        self._load()

    def _load(self):
        df = pd.read_csv(self.path, encoding="utf-8-sig", dtype=str)
        df.columns = df.columns.str.strip('"').str.strip()

        site_col_found = None
        for alias in self._SITE_COL_ALIASES:
            if alias in df.columns:
                site_col_found = alias
                break
        if site_col_found is None:
            raise ValueError(
                f"Metadata CSV missing site-number column.\n"
                f"  Expected one of: {self._SITE_COL_ALIASES}\n"
                f"  Available: {list(df.columns)}"
            )
        if site_col_found != "SITE_NUMBER":
            df.rename(columns={site_col_found: "SITE_NUMBER"}, inplace=True)
        self._site_col_used = site_col_found

        required = {"SUBINVENTORY", "CUSTOMER_TYPE", "BILL_TO_ACCOUNT",
                    "SITE_NUMBER", "BILL_TO_NAME", "BUSINESS_UNIT"}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"Metadata CSV missing columns: {missing}")

        for _, row in df.iterrows():
            subinv = safe_str(row.get("SUBINVENTORY")).upper()
            ctype  = safe_str(row.get("CUSTOMER_TYPE")).upper()
            if not subinv or not ctype:
                continue
            entry = {
                "BILL_TO_ACCOUNT":  safe_str(row.get("BILL_TO_ACCOUNT")),
                "SITE_NUMBER":      safe_str(row.get("SITE_NUMBER")),
                "BILL_TO_NAME":     safe_str(row.get("BILL_TO_NAME")),
                "BUSINESS_UNIT":    safe_str(row.get("BUSINESS_UNIT", "AlQurashi-KSA")),
                "CUSTOMER_TYPE":    safe_str(row.get("CUSTOMER_TYPE")),
                "SUBINVENTORY":     safe_str(row.get("SUBINVENTORY")),
                "REGION":           safe_str(row.get("REGION", "SA")),
                "COST_CENTER_CODE": safe_str(row.get("COST_CENTER_CODE", "")),
            }
            self.primary[(subinv, ctype)] = entry
            if ctype not in self.by_type:
                self.by_type[ctype] = entry

    def get(self, store_name: str, customer_type: str) -> Tuple[dict, str]:
        subinv = normalise_store(store_name)
        ctype  = customer_type.upper().strip()

        row = self.primary.get((subinv, ctype))
        if row:
            return row, "exact"

        if subinv:
            for (s, t), v in self.primary.items():
                if t == ctype and (subinv.startswith(s) or s.startswith(subinv)):
                    return v, "partial"

        if not subinv:
            row = self.by_type.get(ctype)
            if row:
                return row, "type_only"

        return {
            "BILL_TO_ACCOUNT": "", "SITE_NUMBER": "",
            "BILL_TO_NAME": store_name, "BUSINESS_UNIT": "AlQurashi-KSA",
            "CUSTOMER_TYPE": customer_type, "SUBINVENTORY": store_name,
            "REGION": "SA", "COST_CENTER_CODE": "",
        }, "none"


# ============================================================================
# REGISTER CACHE
# ============================================================================

class RegisterCache:

    def __init__(self, registers_path: str):
        self.name_map: Dict[str, str] = {}
        self._load(registers_path)

    def _load(self, path: str):
        df = pd.read_csv(path, encoding="utf-8-sig", dtype=str)
        df.columns = df.columns.str.strip('"').str.strip()
        reg_col = next((c for c in df.columns if "REGISTER_NAME" in c.upper()), None)
        if reg_col is None:
            return
        for _, row in df.iterrows():
            reg = safe_str(row.get(reg_col))
            if reg:
                self.name_map[reg.upper()] = reg

    def resolve(self, raw_name: str) -> str:
        return self.name_map.get(normalise_store(raw_name), raw_name)


# ============================================================================
# TRANSACTION NUMBER GENERATOR
# ============================================================================

class TxnNumberGenerator:

    def __init__(self, start_seq: int = 1):
        self._start        = max(1, int(start_seq))
        self._normal_cache: Dict[Tuple[str, str], str]      = {}
        self._normal_seq:   int                             = self._start
        self._bnpl_cache:   Dict[Tuple[str, str, str], str] = {}
        self._bnpl_seq:     int                             = self._start

    def get_normal(self, store_name: str, sale_date) -> str:
        ds  = format_date(sale_date)
        key = (store_name.upper().strip(), ds)
        if key not in self._normal_cache:
            self._normal_cache[key] = f"BLK-{self._normal_seq:07d}"
            self._normal_seq += 1
        return self._normal_cache[key]

    def get_bnpl(self, store_name: str, sale_date, customer_type: str) -> str:
        ds  = format_date(sale_date)
        ct  = customer_type.upper()
        key = (store_name.upper().strip(), ds, ct)
        if key not in self._bnpl_cache:
            self._bnpl_cache[key] = f"BLK-{self._bnpl_seq:04d}"
            self._bnpl_seq += 1
        return self._bnpl_cache[key]

    def get(self, store_name: str, sale_date, customer_type: str) -> str:
        if customer_type.upper() in NO_RECEIPT_PAYMENT_METHODS:
            return self.get_bnpl(store_name, sale_date, customer_type)
        return self.get_normal(store_name, sale_date)


# ============================================================================
# MAIN INTEGRATION CLASS
# ============================================================================

class OracleFusionIntegration:

    AR_COLUMNS   = get_ar_columns()
    RCP_COLUMNS  = RECEIPT_COLUMNS
    MISC_COLUMNS = MISC_RECEIPT_COLUMNS

    def __init__(
        self,
        output_dir:         str = "ORACLE_FUSION_OUTPUT",
        start_seq:          int = 1,
        start_legacy_seq_1: int = 1,
        start_legacy_seq_2: int = 1,
    ):
        self.output_dir         = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.start_seq          = max(1, int(start_seq))
        self.start_legacy_seq_1 = max(1, int(start_legacy_seq_1))
        self.start_legacy_seq_2 = max(1, int(start_legacy_seq_2))

        self.txn_gen       = TxnNumberGenerator(start_seq=self.start_seq)
        self.vlog          = VerificationLog()
        self.segment_seq_1 = self.start_legacy_seq_1
        self.segment_seq_2 = self.start_legacy_seq_2

        self.metadata_cache:    Optional[MetadataCache]     = None
        self.register_cache:    Optional[RegisterCache]     = None
        self.bank_charges:      Optional[BankChargesCache]  = None

        self._li_df:  Optional[pd.DataFrame] = None
        self._pay_df: Optional[pd.DataFrame] = None

        self.invoice_store:     Dict[str, str]              = {}
        self.invoice_date:      Dict[str, datetime]         = {}
        self.invoice_ctype:     Dict[str, str]              = {}
        self.invoice_to_ar_txn: Dict[str, str]              = {}
        self.invoice_ar_total:  Dict[str, float]            = {}

        self._inv_pay_totals:     Dict[str, Dict[str, float]] = defaultdict(
                                                                   lambda: defaultdict(float))
        self._receipt_agg_detail: dict = {}

    # ──────────────────────────────────────────────────────────────────
    # DATA LOADING
    # ──────────────────────────────────────────────────────────────────

    def load_data(
        self,
        line_items_path:  str,
        payments_path:    str,
        metadata_path:    str,
        registers_path:   str,
        bank_charges_path: Optional[str] = None,
    ):
        vl = self.vlog
        vl.section("1. INPUT FILES & SEQUENCE SETTINGS")
        vl.kv("Transaction seq start",    str(self.start_seq))
        vl.kv("  NORMAL  first number",   f"BLK-{self.start_seq:07d}")
        vl.kv("  TABBY   first number",   f"BLK-{self.start_seq:04d}")
        vl.kv("  TAMARA  first number",   f"BLK-{self.start_seq:04d}")
        vl.kv("Segment 1 start (random)", f"LEGACY{self.start_legacy_seq_1:09d}")
        vl.kv("Segment 2 start (random)", f"LEGACY{self.start_legacy_seq_2:09d}")
        vl.kv("Gap Seg1 ↔ Seg2",
               f"{abs(self.start_legacy_seq_2 - self.start_legacy_seq_1):,}")
        vl.add()

        self.metadata_cache = MetadataCache(metadata_path)
        vl.kv("Metadata file",              Path(metadata_path).name)
        vl.kv("(store, type) pairs loaded", len(self.metadata_cache.primary))
        vl.add()
        vl.add("  Metadata rows loaded:")
        for (subinv, ctype), entry in sorted(self.metadata_cache.primary.items()):
            vl.add(f"    ({subinv}, {ctype:<12}) → "
                   f"acct={entry['BILL_TO_ACCOUNT']:<10}  "
                   f"site={entry['SITE_NUMBER']}")

        self.register_cache = RegisterCache(registers_path)
        vl.add()
        vl.kv("Registers file",   Path(registers_path).name)
        vl.kv("Registers loaded", len(self.register_cache.name_map))

        # bank charges (optional)
        if bank_charges_path:
            self.bank_charges = BankChargesCache(bank_charges_path)
            vl.add()
            vl.kv("Bank charges file",    Path(bank_charges_path).name)
            vl.kv("Methods configured",   len(self.bank_charges.charges))
            vl.add("  Bank charge config:")
            for m, cfg in sorted(self.bank_charges.charges.items()):
                cap_str = (f"cap={cfg['cap_amount']:.2f}"
                           if cfg["cap_amount"] > 0 else "no cap")
                rounding = " [CASH ROUNDING]" if cfg["cash_rounding"] else ""
                vl.add(f"    {m:<14}  rate={cfg['charge_rate']:.4f}"
                       f"  tax={cfg['tax_rate']:.4f}  {cap_str}"
                       f"  activity='{cfg['activity_name']}'{rounding}")
        else:
            vl.add()
            vl.add("  ⚠  No bank charges file provided — misc receipts skipped")

        # line items
        self._li_df = self._read_file(
            line_items_path,
            str_cols=[COL_LI_ORDER_REF, COL_LI_BARCODE, COL_LI_PRODUCT],
        )
        vl.add()
        vl.kv("Line items file",     Path(line_items_path).name)
        vl.kv("Line items raw rows", len(self._li_df))
        self._build_invoice_index()
        vl.kv("Unique invoices (Order Ref)", len(self.invoice_store))
        vl.add("  Sample invoice → store mappings (first 5):")
        for inv, store in list(self.invoice_store.items())[:5]:
            vl.add(f"    {inv}  →  '{store}'")

        # payments
        self._pay_df = self._read_file(
            payments_path,
            str_cols=[COL_PAY_ORDER_REF, COL_PAY_METHOD],
        )
        vl.add()
        vl.kv("Payments file",     Path(payments_path).name)
        vl.kv("Payments raw rows", len(self._pay_df))
        self._build_ctype_index()
        vl.kv("Payments rows after ffill",   len(self._pay_df))
        vl.kv("Unique invoices in payments", len(self._inv_pay_totals))

        # Section 2
        vl.section("2. PAYMENT METHOD BREAKDOWN (payments file, after ffill)")
        m_counts:  Dict[str, int]   = defaultdict(int)
        m_amounts: Dict[str, float] = defaultdict(float)
        for methods in self._inv_pay_totals.values():
            for m, amt in methods.items():
                m_counts[m]  += 1
                m_amounts[m] += amt
        vl.table_row("Payment Method", "Invoices", "Total Amount (SAR)",
                     widths=(25, 12, 22))
        vl.divider()
        for m in sorted(m_counts.keys()):
            vl.table_row(m, m_counts[m], f"{m_amounts[m]:,.2f}",
                         widths=(25, 12, 22))
        vl.divider()
        vl.table_row("TOTAL", sum(m_counts.values()),
                     f"{sum(m_amounts.values()):,.2f}", widths=(25, 12, 22))

        # Section 3
        vl.section("3. INVOICE TYPE BREAKDOWN")
        tc: Dict[str, int] = defaultdict(int)
        for ct in self.invoice_ctype.values():
            tc[ct] += 1
        for ct, cnt in sorted(tc.items()):
            vl.kv(ct, f"{cnt:,} invoices")
        vl.kv("Total", f"{len(self.invoice_ctype):,} invoices")

    # ──────────────────────────────────────────────────────────────────

    def _read_file(
        self, path: str, str_cols: Optional[List[str]] = None
    ) -> pd.DataFrame:
        dtype_map = {c: str for c in (str_cols or [])}
        p = path.lower()
        if p.endswith(".xlsx") or p.endswith(".xls"):
            return pd.read_excel(path, dtype=dtype_map)
        return pd.read_csv(path, encoding="utf-8-sig", dtype=dtype_map)

    def _build_invoice_index(self):
        df = self._li_df
        for col in [COL_LI_ORDER_REF, COL_LI_AMOUNT]:
            if col not in df.columns:
                raise ValueError(
                    f"Line items file missing column '{col}'.\n"
                    f"  Available: {list(df.columns)}"
                )
        if COL_LI_DATE in df.columns:
            df[COL_LI_DATE] = pd.to_datetime(df[COL_LI_DATE], errors="coerce")
        if COL_LI_BARCODE in df.columns:
            df[COL_LI_BARCODE] = (
                df[COL_LI_BARCODE].fillna("").astype(str).apply(barcode_to_text)
            )
        for _, row in df.iterrows():
            inv = safe_str(row.get(COL_LI_ORDER_REF, ""))
            if not inv:
                continue
            if inv not in self.invoice_store:
                store = safe_str(row.get(COL_LI_STORE, ""))
                if not store and "/" in inv:
                    store = inv.split("/")[0].strip()
                dt = row.get(COL_LI_DATE, pd.NaT)
                self.invoice_store[inv] = store
                self.invoice_date[inv]  = dt
        self._li_df = df

    def _build_ctype_index(self):
        """Forward-fills Order Ref (Odoo merged-cell fix) then classifies invoices."""
        df = self._pay_df
        for col in [COL_PAY_ORDER_REF, COL_PAY_METHOD, COL_PAY_AMOUNT]:
            if col not in df.columns:
                raise ValueError(
                    f"Payments file missing column '{col}'.\n"
                    f"  Available columns: {list(df.columns)}"
                )
        df[COL_PAY_ORDER_REF] = (
            df[COL_PAY_ORDER_REF].replace("", np.nan).ffill()
        )
        if COL_PAY_BRANCH in df.columns:
            df[COL_PAY_BRANCH] = (
                df[COL_PAY_BRANCH].replace("", np.nan).ffill()
            )
        self._pay_df = df

        for _, row in df.iterrows():
            inv    = safe_str(row.get(COL_PAY_ORDER_REF, ""))
            method = normalise_payment(safe_str(row.get(COL_PAY_METHOD, "")))
            amount = safe_float(row.get(COL_PAY_AMOUNT, 0))
            if not inv or amount == 0:
                continue
            self._inv_pay_totals[inv][method] += amount

        for inv, methods in self._inv_pay_totals.items():
            if "TAMARA" in methods:
                self.invoice_ctype[inv] = "TAMARA"
            elif "TABBY" in methods:
                self.invoice_ctype[inv] = "TABBY"
            else:
                self.invoice_ctype[inv] = "NORMAL"

        for inv in self.invoice_store:
            if inv not in self.invoice_ctype:
                self.invoice_ctype[inv] = "NORMAL"

    # ──────────────────────────────────────────────────────────────────
    # RECEIPT AGGREGATION
    # ──────────────────────────────────────────────────────────────────

    def _build_receipt_aggregation(self) -> Dict[Tuple[str, str, str], float]:
        df = self._pay_df

        agg:      Dict[Tuple[str, str, str], float] = defaultdict(float)
        agg_pos:  Dict[Tuple[str, str, str], float] = defaultdict(float)
        agg_neg:  Dict[Tuple[str, str, str], float] = defaultdict(float)
        agg_rows: Dict[Tuple[str, str, str], int]   = defaultdict(int)
        agg_ar:   Dict[Tuple[str, str], str]        = {}

        skipped_bnpl = skipped_unknown = skipped_zero = skipped_no_ref = 0
        included = 0

        for _, row in df.iterrows():
            inv    = safe_str(row.get(COL_PAY_ORDER_REF, ""))
            method = normalise_payment(safe_str(row.get(COL_PAY_METHOD, "")))
            amount = safe_float(row.get(COL_PAY_AMOUNT, 0))

            if amount == 0:
                skipped_zero += 1
                continue
            ctype = self.invoice_ctype.get(inv, "NORMAL")
            if ctype in ("TABBY", "TAMARA"):
                skipped_bnpl += 1
                continue
            if method.upper() in NO_RECEIPT_PAYMENT_METHODS:
                continue
            if method not in RECEIPT_PAYMENT_METHODS:
                skipped_unknown += 1
                continue

            store    = self.invoice_store.get(inv, "")
            sale_dt  = self.invoice_date.get(inv, None)
            date_str = format_date(sale_dt) if sale_dt is not None else ""
            if not store and COL_PAY_BRANCH in df.columns:
                store = safe_str(row.get(COL_PAY_BRANCH, ""))
            if not store or not date_str:
                skipped_no_ref += 1
                continue

            sd_key = (store, date_str)
            if sd_key not in agg_ar:
                ar_txn = self.invoice_to_ar_txn.get(inv, "")
                if ar_txn:
                    agg_ar[sd_key] = ar_txn

            key = (store, date_str, method)
            agg[key]      += amount
            agg_rows[key] += 1
            if amount >= 0: agg_pos[key] += amount
            else:           agg_neg[key] += amount
            included += 1

        self._receipt_agg_detail = {
            "agg": agg, "agg_pos": agg_pos, "agg_neg": agg_neg,
            "agg_rows": agg_rows, "agg_ar": agg_ar,
            "skipped_bnpl": skipped_bnpl, "skipped_unknown": skipped_unknown,
            "skipped_no_ref": skipped_no_ref, "skipped_zero": skipped_zero,
            "included": included,
        }
        return agg

    # ──────────────────────────────────────────────────────────────────
    # AR INVOICE GENERATION
    # ──────────────────────────────────────────────────────────────────

    def generate_ar_invoices(self) -> pd.DataFrame:
        vl = self.vlog
        self.segment_seq_1    = self.start_legacy_seq_1
        self.segment_seq_2    = self.start_legacy_seq_2
        self.invoice_ar_total = defaultdict(float)

        records = []
        meta_exact = meta_partial = meta_typeonly = meta_none = 0
        meta_issues: List[str] = []
        total_product_lines = total_discount_lines = 0
        total_zero_qty_lines = total_zero_amt_lines = 0

        store_stats: Dict[str, Dict] = defaultdict(lambda: {
            "invoices": set(), "lines": 0, "discount_lines": 0, "amount": 0.0,
        })
        txn_registry: Dict[str, Dict] = {}

        invoices = sorted(
            self.invoice_store.keys(),
            key=lambda i: (
                format_date(self.invoice_date.get(i, datetime.min)),
                self.invoice_store.get(i, ""),
            ),
        )

        for inv in invoices:
            store     = self.invoice_store[inv]
            ctype     = self.invoice_ctype.get(inv, "NORMAL")
            sale_date = self.invoice_date.get(inv, datetime.now())
            txn_num   = self.txn_gen.get(store, sale_date, ctype)
            self.invoice_to_ar_txn[inv] = txn_num

            meta, match_type = self.metadata_cache.get(store, ctype)
            bill_to_account  = meta["BILL_TO_ACCOUNT"]
            bill_to_site     = meta["SITE_NUMBER"]

            if   match_type == "exact":     meta_exact    += 1
            elif match_type == "partial":   meta_partial  += 1
            elif match_type == "type_only": meta_typeonly += 1
            else:                           meta_none     += 1

            if match_type != "exact":
                meta_issues.append(
                    f"    {inv:<30} store='{store}' ctype='{ctype}'"
                    f" match='{match_type}'"
                    f" → acct='{bill_to_account}' site='{bill_to_site}'"
                )

            if txn_num not in txn_registry:
                txn_registry[txn_num] = {
                    "store": store, "date": format_date(sale_date),
                    "ctype": ctype, "invoices": 0, "lines": 0, "amount": 0.0,
                }
            txn_registry[txn_num]["invoices"] += 1

            inv_lines = self._li_df[
                self._li_df[COL_LI_ORDER_REF].astype(str).str.strip() == inv
            ]

            for _, item in inv_lines.iterrows():
                product_name = safe_str(item.get(COL_LI_PRODUCT, ""))
                barcode      = safe_str(item.get(COL_LI_BARCODE, ""))
                quantity     = safe_float(item.get(COL_LI_QTY, 0))
                amount       = safe_float(item.get(COL_LI_AMOUNT, 0))
                unit_price   = abs(amount / quantity) if quantity != 0 else 0.0

                is_disc = is_discount_line(product_name)
                if is_disc: total_discount_lines += 1
                else:       total_product_lines  += 1
                if quantity == 0: total_zero_qty_lines += 1
                if amount   == 0: total_zero_amt_lines += 1

                ss = store_stats[store]
                ss["invoices"].add(inv)
                ss["lines"]   += 1
                ss["amount"]  += amount
                if is_disc: ss["discount_lines"] += 1

                txn_registry[txn_num]["lines"]  += 1
                txn_registry[txn_num]["amount"] += amount
                self.invoice_ar_total[inv]      += amount

                row: Dict = {col: "" for col in self.AR_COLUMNS}
                row["Transaction Batch Source Name"]          = AR_STATIC["Transaction Batch Source Name"]
                row["Transaction Type Name"]                  = AR_STATIC["Transaction Type Name"]
                row["Payment Terms"]                          = AR_STATIC["Payment Terms"]
                row["Transaction Date"]                       = format_datetime(sale_date)
                row["Accounting Date"]                        = format_datetime(sale_date)
                row["Transaction Number"]                     = txn_num
                row["Bill-to Customer Account Number"]        = bill_to_account
                row["Bill-to Customer Site Number"]           = bill_to_site
                row["Transaction Line Type"]                  = AR_STATIC["Transaction Line Type"]
                row["Transaction Line Description"]           = (
                    "Discount Item" if is_disc else product_name[:240]
                )
                row["Currency Code"]                          = AR_STATIC["Currency Code"]
                row["Currency Conversion Type"]               = AR_STATIC["Currency Conversion Type"]
                row["Currency Conversion Date"]               = format_date(sale_date)
                row["Currency Conversion Rate"]               = AR_STATIC["Currency Conversion Rate"]
                row["Transaction Line Amount"]                = round(amount, 2)
                row["Transaction Line Quantity"]              = quantity
                row["Unit Selling Price"]                     = round(unit_price, 2)
                row["Line Transactions Flexfield Context"]    = AR_STATIC["Line Transactions Flexfield Context"]
                row["Line Transactions Flexfield Segment 1"] = f"LEGACY{self.segment_seq_1:09d}"
                row["Line Transactions Flexfield Segment 2"] = f"LEGACY{self.segment_seq_2:09d}"
                self.segment_seq_1 += 1
                self.segment_seq_2 += 1
                row["Tax Classification Code"]                = DEFAULT_TAX_CODE
                row["Sales Order Number"]                     = inv
                row["Unit of Measure Code"]                   = AR_STATIC["Unit of Measure Code"]
                row["Default Taxation Country"]               = AR_STATIC["Default Taxation Country"]
                row["Comments"]                               = AR_STATIC["Comments"]
                row["END"]                                    = AR_STATIC["END"]
                if is_disc:
                    row["Memo Line Name"]        = "Discount Item"
                    row["Inventory Item Number"] = ""
                else:
                    row["Inventory Item Number"] = barcode
                records.append(row)

        df_ar = pd.DataFrame(records, columns=self.AR_COLUMNS)

        # Section 4
        vl.section("4. AR INVOICE — STORE BREAKDOWN")
        vl.table_row("Store", "Invoices", "Lines", "Discount", "Amount (SAR)",
                     widths=(30, 10, 8, 10, 18))
        vl.divider()
        gi = gl = gd = 0; ga = 0.0
        for store in sorted(store_stats.keys()):
            ss  = store_stats[store]
            ni  = len(ss["invoices"]); nl = ss["lines"]
            nd  = ss["discount_lines"]; amt = ss["amount"]
            gi += ni; gl += nl; gd += nd; ga += amt
            vl.table_row(store, ni, nl, nd, f"{amt:,.2f}",
                         widths=(30, 10, 8, 10, 18))
        vl.divider()
        vl.table_row("GRAND TOTAL", gi, gl, gd, f"{ga:,.2f}",
                     widths=(30, 10, 8, 10, 18))

        # Section 5
        vl.section("5. TRANSACTION NUMBER REGISTER")
        vl.table_row("Txn Number", "Store", "Date", "Type",
                     "Invoices", "Lines", "Amount",
                     widths=(18, 25, 12, 8, 10, 7, 16))
        vl.divider(width=100)
        for txn in sorted(txn_registry.keys()):
            tr = txn_registry[txn]
            vl.table_row(txn, tr["store"], tr["date"], tr["ctype"],
                         tr["invoices"], tr["lines"], f"{tr['amount']:,.2f}",
                         widths=(18, 25, 12, 8, 10, 7, 16))

        # Section 6
        vl.section("6. AR RECORD STATISTICS")
        vl.kv("Total AR rows",       f"{len(df_ar):,}")
        vl.kv("  Product lines",     f"{total_product_lines:,}")
        vl.kv("  Discount lines",    f"{total_discount_lines:,}")
        vl.kv("  Zero-qty lines",    f"{total_zero_qty_lines:,}")
        vl.kv("  Zero-amount lines", f"{total_zero_amt_lines:,}")
        vl.add()
        vl.kv("Segment 1 range",
               f"LEGACY{self.start_legacy_seq_1:09d}  →  "
               f"LEGACY{self.segment_seq_1 - 1:09d}")
        vl.kv("Segment 2 range",
               f"LEGACY{self.start_legacy_seq_2:09d}  →  "
               f"LEGACY{self.segment_seq_2 - 1:09d}")
        vl.add()
        vl.kv("Total Transaction Line Amount",
               f"{df_ar['Transaction Line Amount'].sum():,.2f} SAR")
        vl.kv("Unique Transaction Numbers",
               f"{df_ar['Transaction Number'].nunique():,}")
        vl.kv("Unique Invoices",
               f"{df_ar['Sales Order Number'].nunique():,}")
        vl.kv("Rows with EMPTY Bill-to Account",
               f"{(df_ar['Bill-to Customer Account Number'] == '').sum():,}")
        vl.kv("Rows with EMPTY Bill-to Site",
               f"{(df_ar['Bill-to Customer Site Number'] == '').sum():,}")

        # Section 7
        vl.section("7. METADATA LOOKUP QUALITY")
        total_lu = meta_exact + meta_partial + meta_typeonly + meta_none
        vl.kv("Total invoice lookups",      f"{total_lu:,}")
        vl.kv("  Exact matches  ✓",         f"{meta_exact:,}")
        vl.kv("  Partial matches ⚠",        f"{meta_partial:,}")
        vl.kv("  Type-only fallback ⚠⚠",    f"{meta_typeonly:,}")
        vl.kv("  No match ✗✗",              f"{meta_none:,}")
        if meta_issues:
            vl.add()
            vl.add("  Non-exact match details:")
            for line in meta_issues:
                vl.add(line)

        return df_ar

    # ──────────────────────────────────────────────────────────────────
    # RECEIPT GENERATION
    # ──────────────────────────────────────────────────────────────────

    def generate_receipts(self) -> Dict[str, pd.DataFrame]:
        vl  = self.vlog
        agg = self._build_receipt_aggregation()
        d   = self._receipt_agg_detail

        receipt_files:       Dict[str, pd.DataFrame] = {}
        receipt_detail_rows: List[Dict]              = []
        seq = 1

        for (store, date_str, method), total in sorted(agg.items()):
            ar_txn               = d["agg_ar"].get((store, date_str), "")
            meta, _              = self.metadata_cache.get(store, "NORMAL")
            bank_name, bank_acct = PAYMENT_BANK_MAP.get(method, DEFAULT_BANK)
            batch_date           = f"{date_str} 00:00:00"
            safe_store_part      = safe_filename(store)
            safe_method_part     = safe_filename(method)
            date_compact         = date_str.replace("-", "")

            batch_name     = f"RCPT_{safe_method_part}_{safe_store_part}_{date_compact}"
            receipt_number = f"{method}-{ar_txn}" if ar_txn else f"RCPT-{seq:08d}"
            filename       = f"Receipt_{safe_method_part}_{safe_store_part}_{date_compact}.csv"

            rcp_row = {
                "Business Unit":             meta["BUSINESS_UNIT"],
                "Batch Source":              "Spreadsheet",
                "Batch Name":                batch_name,
                "Receipt Method":            method,
                "Remittance Bank":           bank_name,
                "Remittance Bank Account":   bank_acct,
                "Batch Date":                batch_date,
                "Accounting Date":           batch_date,
                "Deposit Date":              batch_date,
                "Currency":                  "SAR",
                "Sequence Number":           "0001",
                "Receipt Number":            receipt_number,
                "Receipt Amount":            round(total, 2),
                "Receipt Date":              batch_date,
                "Accounting Date (Receipt)": batch_date,
                "Currency (Receipt)":        "SAR",
                "Document Number":           "",
                "Customer Name":             meta["BILL_TO_NAME"],
                "Customer Account Number":   meta["BILL_TO_ACCOUNT"],
                "Customer Site Number":      meta["SITE_NUMBER"],
            }

            receipt_files[filename] = pd.DataFrame([rcp_row], columns=self.RCP_COLUMNS)
            key = (store, date_str, method)
            receipt_detail_rows.append({
                "filename":   filename,
                "store":      store,
                "date":       date_str,
                "method":     method,
                "row_count":  d["agg_rows"].get(key, 0),
                "pos_amount": d["agg_pos"].get(key, 0.0),
                "neg_amount": d["agg_neg"].get(key, 0.0),
                "net_amount": total,
                "rcpt_num":   receipt_number,
            })
            seq += 1

        # Section 8
        vl.section("8. RECEIPT RECORDS — DETAIL")
        vl.kv("Odoo merged-cell fix", "Order Ref forward-filled before processing")
        vl.add()
        vl.kv("Payment rows included",       f"{d['included']:,}")
        vl.kv("Skipped — BNPL",             f"{d['skipped_bnpl']:,}")
        vl.kv("Skipped — unknown method",   f"{d['skipped_unknown']:,}")
        vl.kv("Skipped — no line item ref", f"{d['skipped_no_ref']:,}")
        vl.kv("Skipped — zero amount",      f"{d['skipped_zero']:,}")
        vl.kv("Receipt files to write",     f"{len(receipt_files):,}")
        vl.add()

        vl.table_row("File", "Rows", "Sales(+)", "Refunds(-)", "Net Amount",
                     widths=(55, 6, 14, 14, 14))
        vl.divider(width=105)
        receipt_grand = 0.0
        for r in receipt_detail_rows:
            vl.table_row(
                r["filename"], r["row_count"],
                f"{r['pos_amount']:,.2f}", f"{r['neg_amount']:,.2f}",
                f"{r['net_amount']:,.2f}", widths=(55, 6, 14, 14, 14),
            )
            receipt_grand += r["net_amount"]
        vl.divider(width=105)
        vl.table_row("GRAND TOTAL", "", "", "", f"{receipt_grand:,.2f}",
                     widths=(55, 6, 14, 14, 14))
        vl.add()
        vl.add("  Per-method totals:")
        method_totals:      Dict[str, float] = defaultdict(float)
        method_file_counts: Dict[str, int]   = defaultdict(int)
        for r in receipt_detail_rows:
            method_totals[r["method"]]      += r["net_amount"]
            method_file_counts[r["method"]] += 1
        for m in sorted(method_totals.keys()):
            vl.add(f"    {m:<14}  {method_file_counts[m]:>3} file(s)  "
                   f"{method_totals[m]:>14,.2f} SAR")
        vl.add(f"\n    {'Grand Total':<28}  {receipt_grand:>14,.2f} SAR")

        return receipt_files

    # ──────────────────────────────────────────────────────────────────
    # MISCELLANEOUS RECEIPT GENERATION
    # ──────────────────────────────────────────────────────────────────

    def generate_misc_receipts(
        self,
        receipt_files: Dict[str, pd.DataFrame],
    ) -> Dict[str, pd.DataFrame]:
        """
        Generates one Miscellaneous Receipt file per standard receipt file.

        Source amount: the Receipt Amount from the corresponding standard receipt.
        Formula per method:
            Standard bank charge:
                miscCharges = receipt_amount × charge_rate × (1 + tax_rate)
                if cap_amount > 0 and miscCharges > cap_amount:
                    miscCharges = cap_amount        ← correct cap (fixed)
                MiscAmount = 0 - miscCharges

            Cash Rounding:
                miscCharges = receipt_amount        ← raw rounding amount
                MiscAmount  = 0 - miscCharges

        Output filename:
            MiscReceipt_<Method>_<Store>_<YYYYMMDD>.csv

        Output folder:
            ORACLE_FUSION_OUTPUT/MiscReceipts/<Method>/
        """
        vl = self.vlog
        vl.section("9. MISCELLANEOUS RECEIPTS — DETAIL")

        if self.bank_charges is None:
            vl.add("  ⚠  No bank charges file loaded — misc receipts skipped.")
            return {}

        misc_files:       Dict[str, pd.DataFrame] = {}
        misc_detail_rows: List[Dict]              = []
        skipped_no_config = 0
        skipped_zero      = 0
        seq               = 1

        for std_fname, std_df in sorted(receipt_files.items()):
            # parse method from standard receipt filename
            # Receipt_<Method>_<Store>_<Date>.csv
            parts        = std_fname.replace(".csv", "").split("_")
            method       = parts[1] if len(parts) > 1 else ""
            store_part   = parts[2] if len(parts) > 2 else ""
            date_part    = parts[3] if len(parts) > 3 else ""

            cfg = self.bank_charges.get(method)
            if cfg is None:
                skipped_no_config += 1
                continue

            # receipt amount = total from the standard receipt file
            receipt_amount = safe_float(std_df["Receipt Amount"].sum())

            # calculate misc amount
            misc_amount = calc_misc_receipt_amount(
                payment_amount = receipt_amount,
                charge_rate    = cfg["charge_rate"],
                tax_rate       = cfg["tax_rate"],
                cap_amount     = cfg["cap_amount"],
                cash_rounding  = cfg["cash_rounding"],
            )

            if misc_amount == 0:
                skipped_zero += 1
                continue

            # dates from standard receipt
            receipt_date = safe_str(
                std_df["Receipt Date"].iloc[0]
                if "Receipt Date" in std_df.columns else ""
            )
            if not receipt_date:
                date_str     = f"{date_part[:4]}-{date_part[4:6]}-{date_part[6:8]}"
                receipt_date = f"{date_str} 00:00:00"
            date_str_compact = date_part  # YYYYMMDD

            # receipt number: MISC-<method>-<seq>
            misc_rcpt_num = f"MISC-{method}-{seq:08d}"

            filename = (
                f"MiscReceipt_{safe_filename(method)}_"
                f"{store_part}_{date_str_compact}.csv"
            )

            misc_row = {
                "Amount":                 misc_amount,
                "CurrencyCode":           "SAR",
                "DepositDate":            receipt_date,
                "ReceiptDate":            receipt_date,
                "GlDate":                 receipt_date,
                "OrgId":                  cfg["org_id"],
                "ReceiptNumber":          misc_rcpt_num,
                "ReceiptMethodId":        cfg["receipt_method_id"],
                "ReceiptMethodName":      method,
                "ReceivableActivityName": cfg["activity_name"],
                "BankAccountNumber":      cfg["bank_account_num"],
            }

            misc_files[filename] = pd.DataFrame([misc_row], columns=self.MISC_COLUMNS)
            misc_detail_rows.append({
                "filename":      filename,
                "method":        method,
                "std_receipt":   std_fname,
                "receipt_amount":receipt_amount,
                "charge_rate":   cfg["charge_rate"],
                "tax_rate":      cfg["tax_rate"],
                "cap_amount":    cfg["cap_amount"],
                "cash_rounding": cfg["cash_rounding"],
                "misc_amount":   misc_amount,
                "rcpt_num":      misc_rcpt_num,
                "activity":      cfg["activity_name"],
            })
            seq += 1

        # ── log detail ────────────────────────────────────────────────
        vl.kv("Misc receipt files generated", f"{len(misc_files):,}")
        vl.kv("Skipped — no charge config",   f"{skipped_no_config:,}")
        vl.kv("Skipped — zero misc amount",   f"{skipped_zero:,}")
        vl.add()
        vl.add("  Formula: MiscAmount = 0 - (ReceiptAmount × rate × (1 + tax))")
        vl.add("  Cap: if miscCharges > cap_amount → use cap_amount")
        vl.add("  Cash Rounding: MiscAmount = 0 - ReceiptAmount (direct)")
        vl.add()

        vl.table_row("Misc File", "Method", "Std Rcpt Amt",
                     "Rate", "Misc Amount",
                     widths=(55, 14, 16, 8, 14))
        vl.divider(width=110)
        misc_grand = 0.0
        for r in misc_detail_rows:
            cap_note = (f" [cap={r['cap_amount']:.2f}]"
                        if r["cap_amount"] > 0 else "")
            rounding_note = " [ROUNDING]" if r["cash_rounding"] else ""
            vl.table_row(
                r["filename"],
                r["method"],
                f"{r['receipt_amount']:,.2f}",
                f"{r['charge_rate']:.4f}{cap_note}{rounding_note}",
                f"{r['misc_amount']:,.4f}",
                widths=(55, 14, 16, 8, 14),
            )
            misc_grand += r["misc_amount"]
        vl.divider(width=110)
        vl.table_row("GRAND TOTAL", "", "", "", f"{misc_grand:,.4f}",
                     widths=(55, 14, 16, 8, 14))

        # per-method summary
        vl.add()
        vl.add("  Per-method misc totals:")
        mt: Dict[str, float] = defaultdict(float)
        mc: Dict[str, int]   = defaultdict(int)
        for r in misc_detail_rows:
            mt[r["method"]] += r["misc_amount"]
            mc[r["method"]] += 1
        for m in sorted(mt.keys()):
            vl.add(f"    {m:<14}  {mc[m]:>3} file(s)  {mt[m]:>14,.4f} SAR")
        vl.add(f"\n    {'Grand Total':<28}  {misc_grand:>14,.4f} SAR")

        return misc_files

    # ───────────────────────────────���──────────────────────────────────
    # OUTPUT SAVING
    # ──────────────────────────────────────────────────────────────────

    def save_ar(self, df: pd.DataFrame):
        vl = self.vlog
        folder = self.output_dir / "AR_Invoices"
        folder.mkdir(parents=True, exist_ok=True)
        ts    = datetime.now().strftime("%Y%m%d_%H%M%S")
        fpath = folder / f"AR_Invoice_Import_{ts}.csv"
        df.to_csv(fpath, index=False, encoding="utf-8-sig", quoting=1)

        vl.section("10. OUTPUT FILES — AR INVOICES")
        vl.kv("File",         str(fpath))
        vl.kv("Rows written", f"{len(df):,}")
        vl.kv("Columns",      f"{len(df.columns):,}")
        vl.kv("Total amount", f"{df['Transaction Line Amount'].sum():,.2f} SAR")
        vl.kv("Encoding",     "UTF-8-BOM (QUOTE_ALL)")
        vl.kv("Segment 1  start → end",
               f"LEGACY{self.start_legacy_seq_1:09d}  →  "
               f"LEGACY{self.segment_seq_1 - 1:09d}")
        vl.kv("Segment 2  start → end",
               f"LEGACY{self.start_legacy_seq_2:09d}  →  "
               f"LEGACY{self.segment_seq_2 - 1:09d}")
        print(f"\n  ✓ AR Invoice saved : {fpath}")
        print(f"    Rows             : {len(df):,}")
        print(f"    Amount           : {df['Transaction Line Amount'].sum():,.2f} SAR")
        print(f"    Segment 1 range  : "
              f"LEGACY{self.start_legacy_seq_1:09d} → "
              f"LEGACY{self.segment_seq_1 - 1:09d}")
        print(f"    Segment 2 range  : "
              f"LEGACY{self.start_legacy_seq_2:09d} → "
              f"LEGACY{self.segment_seq_2 - 1:09d}")

    def save_receipts(self, receipt_files: Dict[str, pd.DataFrame]):
        vl   = self.vlog
        base = self.output_dir / "Receipts"

        vl.section("11. OUTPUT FILES — STANDARD RECEIPTS")
        vl.add("  Folder: Receipts/<Method>/<filename>.csv")
        vl.add()

        method_totals: Dict[str, float] = defaultdict(float)
        method_counts: Dict[str, int]   = defaultdict(int)

        for fname, df in sorted(receipt_files.items()):
            parts  = fname.replace(".csv", "").split("_")
            method = parts[1] if len(parts) > 1 else "Other"
            folder = base / method
            folder.mkdir(parents=True, exist_ok=True)
            fpath  = folder / fname
            df.to_csv(fpath, index=False, encoding="utf-8-sig", quoting=1)
            amt = df["Receipt Amount"].sum()
            method_totals[method] += amt
            method_counts[method] += 1
            vl.kv(f"  {fname}", f"{amt:,.2f} SAR")
            print(f"  ✓ {fname:<65}  {amt:,.2f} SAR")

        vl.add()
        vl.add("  Summary by method:")
        for m in sorted(method_totals.keys()):
            vl.add(f"    {m:<14}  {method_counts[m]:>3} file(s)  "
                   f"{method_totals[m]:>14,.2f} SAR")
        total_all = sum(method_totals.values())
        vl.add()
        vl.kv("  GRAND TOTAL", f"{total_all:,.2f} SAR")
        print(f"\n  Standard receipt grand total : {total_all:,.2f} SAR")

    def save_misc_receipts(self, misc_files: Dict[str, pd.DataFrame]):
        """
        Saves misc receipt CSVs to:
            ORACLE_FUSION_OUTPUT/MiscReceipts/<Method>/<filename>.csv
        """
        vl   = self.vlog
        base = self.output_dir / "MiscReceipts"

        vl.section("12. OUTPUT FILES — MISC RECEIPTS")
        vl.add("  Folder: MiscReceipts/<Method>/<filename>.csv")
        vl.add()

        if not misc_files:
            vl.add("  (none generated)")
            print("  ℹ  No misc receipt files to save.")
            return

        method_totals: Dict[str, float] = defaultdict(float)
        method_counts: Dict[str, int]   = defaultdict(int)

        for fname, df in sorted(misc_files.items()):
            # MiscReceipt_<Method>_<Store>_<Date>.csv
            parts  = fname.replace(".csv", "").split("_")
            method = parts[1] if len(parts) > 1 else "Other"
            folder = base / method
            folder.mkdir(parents=True, exist_ok=True)
            fpath  = folder / fname
            df.to_csv(fpath, index=False, encoding="utf-8-sig", quoting=1)
            amt = df["Amount"].sum()
            method_totals[method] += amt
            method_counts[method] += 1
            vl.kv(f"  {fname}", f"{amt:,.4f} SAR")
            print(f"  ✓ {fname:<65}  {amt:,.4f} SAR")

        vl.add()
        vl.add("  Summary by method:")
        for m in sorted(method_totals.keys()):
            vl.add(f"    {m:<14}  {method_counts[m]:>3} file(s)  "
                   f"{method_totals[m]:>14,.4f} SAR")
        total_all = sum(method_totals.values())
        vl.add()
        vl.kv("  GRAND TOTAL", f"{total_all:,.4f} SAR")
        print(f"\n  Misc receipt grand total : {total_all:,.4f} SAR")

    # ──────────────────────────────────────────────────────────────────
    # FINAL CROSS-CHECK
    # ──────────────────────────────────────────────────────────────────

    def _write_final_crosscheck(
        self,
        ar_df:         pd.DataFrame,
        receipt_files: Dict[str, pd.DataFrame],
        misc_files:    Dict[str, pd.DataFrame],
    ):
        vl = self.vlog
        vl.section("13. FINAL CROSS-CHECK")

        ar_total   = ar_df["Transaction Line Amount"].sum()
        rcpt_total = sum(df["Receipt Amount"].sum() for df in receipt_files.values())
        expected   = sum(self._receipt_agg_detail["agg"].values())
        misc_total = sum(df["Amount"].sum() for df in misc_files.values())

        vl.kv("AR total (all lines)",      f"{ar_total:,.2f} SAR")
        vl.kv("Expected receipt total",    f"{expected:,.2f} SAR")
        vl.kv("Actual receipt total",      f"{rcpt_total:,.2f} SAR")
        diff = abs(rcpt_total - expected)
        vl.kv("Receipt difference",
               f"{diff:,.2f} SAR  "
               + ("✓ MATCH" if diff < 0.01 else "⚠ CHECK"))
        vl.add()
        vl.kv("Misc receipt total",        f"{misc_total:,.4f} SAR")
        vl.kv("Net (receipts + misc)",     f"{rcpt_total + misc_total:,.4f} SAR")

        vl.add()
        vl.add("  Standard receipt breakdown by method:")
        mt: Dict[str, float] = defaultdict(float)
        for df in receipt_files.values():
            for _, row in df.iterrows():
                mt[row["Receipt Method"]] += row["Receipt Amount"]
        for m in sorted(mt.keys()):
            vl.add(f"    {m:<15} {mt[m]:>14,.2f} SAR")

        vl.add()
        vl.add("  Misc receipt breakdown by method:")
        mmt: Dict[str, float] = defaultdict(float)
        for fname, df in misc_files.items():
            parts  = fname.replace(".csv", "").split("_")
            method = parts[1] if len(parts) > 1 else "Other"
            mmt[method] += df["Amount"].sum()
        for m in sorted(mmt.keys()):
            vl.add(f"    {m:<15} {mmt[m]:>14,.4f} SAR")

        ar_invs         = set(ar_df["Sales Order Number"].unique())
        input_invs      = set(self.invoice_store.keys())
        in_input_not_ar = input_invs - ar_invs
        vl.add()
        vl.kv("Invoices in input but not in AR", f"{len(in_input_not_ar):,}")
        if in_input_not_ar:
            for inv in sorted(in_input_not_ar)[:20]:
                vl.add(f"    • {inv}")

        seg1_u = ar_df["Line Transactions Flexfield Segment 1"].nunique()
        seg2_u = ar_df["Line Transactions Flexfield Segment 2"].nunique()
        vl.add()
        vl.kv("Segment 1 unique",
               f"{seg1_u:,}  "
               + ("✓ all unique" if len(ar_df) == seg1_u
                  else f"⚠ {len(ar_df)-seg1_u} duplicates"))
        vl.kv("Segment 2 unique",
               f"{seg2_u:,}  "
               + ("✓ all unique" if len(ar_df) == seg2_u
                  else f"⚠ {len(ar_df)-seg2_u} duplicates"))
        vl.add()
        vl.add(f"  Finished at : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # ──────────────────────────────────────────────────────────────────
    # PIPELINE ENTRY POINT
    # ──────────────────────────────────────────────────────────────────

    def run(
        self,
        line_items_path:   str,
        payments_path:     str,
        metadata_path:     str,
        registers_path:    str,
        bank_charges_path: Optional[str] = None,
    ):
        self.load_data(line_items_path, payments_path,
                       metadata_path, registers_path,
                       bank_charges_path)
        ar_df = self.generate_ar_invoices()
        self.save_ar(ar_df)
        rcp   = self.generate_receipts()
        self.save_receipts(rcp)
        misc  = self.generate_misc_receipts(rcp)
        self.save_misc_receipts(misc)
        self._write_final_crosscheck(ar_df, rcp, misc)

        self.vlog.close()
        ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_path = self.output_dir / f"Verification_Report_{ts}.txt"
        self.vlog.write(log_path)
        self.vlog.print_summary()

        print("\n" + "=" * 72)
        print("✅  ORACLE FUSION INTEGRATION COMPLETE")
        print("=" * 72)


# ============================================================================
# MAIN
# ============================================================================

def main():
    INPUT = {
        "line_items":   "Point of Sale Orders (pos.order) - 2026-04-12T162030.266.xlsx",
        "payments":     "Point of Sale Orders (pos.order) - 2026-04-12T162041.258.xlsx",
        "metadata":     "FUSION_SALES_METADATA_202604121703.csv",
        "registers":    "VENDHQ_REGISTERS_202604121654.csv",
        # Bank charges CSV — controls misc receipt calculation
        # Set to None to skip misc receipts entirely
        # See BankChargesCache docstring above for required columns
        "bank_charges": "BANK_CHARGES.csv",
    }

    # ════════════════════════════════��═══════════════════════════════
    #  TRANSACTION SEQUENCE — set manually before each run
    # ════════════════════════════════════════════════════════════════
    START_TXN_SEQUENCE = 1

    # ════════════════════════════════════════════════════════════════
    #  LEGACY FLEXFIELD SEQUENCES — auto-generated randomly each run
    #  ✓ Different from each other  ✓ Fresh every run  ✓ Min 50M gap
    # ════════════════════════════════════════════════════════════════
    START_LEGACY_SEQ_1, START_LEGACY_SEQ_2 = _generate_legacy_sequences()

    print("=" * 72)
    print("  SEQUENCE NUMBERS FOR THIS RUN")
    print("=" * 72)
    print(f"  Transaction seq        : {START_TXN_SEQUENCE}"
          f"   →  BLK-{START_TXN_SEQUENCE:07d}")
    print(f"  Segment 1 start        : {START_LEGACY_SEQ_1:,}"
          f"   →  LEGACY{START_LEGACY_SEQ_1:09d}")
    print(f"  Segment 2 start        : {START_LEGACY_SEQ_2:,}"
          f"   →  LEGACY{START_LEGACY_SEQ_2:09d}")
    print(f"  Gap Seg1 ↔ Seg2        : "
          f"{abs(START_LEGACY_SEQ_2 - START_LEGACY_SEQ_1):,}")
    print("=" * 72 + "\n")

    integration = OracleFusionIntegration(
        output_dir         = "ORACLE_FUSION_OUTPUT",
        start_seq          = START_TXN_SEQUENCE,
        start_legacy_seq_1 = START_LEGACY_SEQ_1,
        start_legacy_seq_2 = START_LEGACY_SEQ_2,
    )
    try:
        integration.run(
            INPUT["line_items"],
            INPUT["payments"],
            INPUT["metadata"],
            INPUT["registers"],
            INPUT.get("bank_charges"),
        )
    except FileNotFoundError as e:
        print(f"\n❌  File not found: {e}")
    except Exception as e:
        import traceback
        print(f"\n❌  Error: {e}")
        traceback.print_exc()


if __name__ == "__main__":
    main()