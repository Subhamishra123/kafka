import pandas as pd
import random
import uuid
import logging
import argparse
from datetime import datetime
import boto3
from io import StringIO
from botocore.exceptions import BotoCoreError, ClientError
from faker import Faker
import os

fake = Faker()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger(__name__)


def luhn_checksum(num_str):
    """Calculate Luhn checksum for a number string."""
    digits = [int(d) for d in num_str]
    for i in range(len(digits) - 2, -1, -2):
        digits[i] *= 2
        if digits[i] > 9:
            digits[i] -= 9
    return sum(digits) % 10


def generate_card_number(prefix, length=16):
    """Generate a valid-looking card number using Luhn algorithm."""
    number = prefix + ''.join(str(random.randint(0, 9)) for _ in range(length - len(prefix) - 1))
    checksum = luhn_checksum(number + "0")
    check_digit = (10 - checksum) % 10
    return number + str(check_digit)


def generate_debit_card():
    """Generate random debit card details with customer name."""
    card_types = {
        "VISA": "4",
        "MasterCard": str(random.choice(range(51, 56))),
        "RuPay": "60"
    }
    banks = [
        "State Bank of India",
        "HDFC Bank",
        "ICICI Bank",
        "Axis Bank",
        "Kotak Mahindra Bank",
        "Punjab National Bank",
        "Bank of Baroda",
        "Union Bank of India",
        "IDFC First Bank",
        "Yes Bank"
    ]

    card_type = random.choice(list(card_types.keys()))
    prefix = card_types[card_type]
    card_number = generate_card_number(prefix)
    bank_name = random.choice(banks)

    return {
        "customer_name": fake.name(),
        "debit_card_number": card_number,
        "debit_card_type": card_type,
        "bank_name": bank_name
    }


def generate_mock_data(num_records: int, date_str: str) -> pd.DataFrame:
    debit_card_details = generate_debit_card()

    data = []
    for i in range(num_records):
        debit_card_details = generate_debit_card()
        records = {
            "customer_id": i + 1,
            "name": debit_card_details["customer_name"],
            "debit_card_number": debit_card_details["debit_card_number"],
            "debit_card_type": debit_card_details["debit_card_type"],
            "bank_name": debit_card_details["bank_name"],
            "transaction_date": date_str,
            "amount_spend": random.randint(0, 10000)
        }
        data.append(records)

    return pd.DataFrame(data)


# ---------------------------
# Save locally
# ---------------------------
def save_local(df: pd.DataFrame, base_path: str, date_str: str) -> str:
    """Save DataFrame locally with Hive-style partitioning structure."""
    timestamp = datetime.now().strftime("%H%M%S")
    unique_id = uuid.uuid4().hex[:8]
    filename = f"mock_data_{timestamp}_{unique_id}.csv"

    # Local path (mimicking Hive partitioning)
    local_dir = os.path.join("C:\\Temp\\daily_data", base_path, f"dt={date_str}")
    os.makedirs(local_dir, exist_ok=True)

    local_path = os.path.join(local_dir, filename)
    df.to_csv(local_path, index=False)

    logger.info(f"💾 Saved local file: {local_path}")
    return local_path, filename


# ---------------------------
# Upload to S3
# ---------------------------

def upload_to_s3(df: pd.DataFrame, bucket: str, base_path: str, date_str: str, filename: str) -> str:
    """Upload DataFrame as CSV to S3 with Hive-style partitioning."""
    s3 = boto3.client("s3")

    # Convert DataFrame to CSV in-memory
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    # Hive partition path
    s3_key = f"{base_path}/dt={date_str}/{filename}"

    try:
        s3.put_object(Bucket=bucket, Key=s3_key, Body=csv_buffer.getvalue())
        logger.info(f"✅ Uploaded file to s3://{bucket}/{s3_key}")
        return s3_key
    except (BotoCoreError, ClientError) as e:
        logger.error(f"❌ Failed to upload to S3: {e}")
        raise

# ---------------------------
# Main
# ---------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Generate mock CSV data, save locally, and upload to S3 with Hive partitioning"
    )

    #parser.add_argument("--bucket", required=True, help="S3 bucket name")
    parser.add_argument("--bucket", default="glue-gds-assignment-1", help="S3 bucket name")
    parser.add_argument("--base-path", default="mock_data", help="S3 base path")
    parser.add_argument("--records", type=int, default=100, help="Number of records to generate")
    parser.add_argument("--date", default=datetime.today().strftime("%Y-%m-%d"),
                        help="Partition date (default: today)")

    args = parser.parse_args()

    logger.info(f"Generating {args.records} records for partition {args.date}...")

    # Generate data
    df = generate_mock_data(args.records, args.date)

    # Save locally
    local_path, filename = save_local(df, args.base_path, args.date)
    print(local_path)

    # Upload to S3
    upload_to_s3(df, args.bucket, args.base_path, args.date, filename)


if __name__ == "__main__":
    main()

