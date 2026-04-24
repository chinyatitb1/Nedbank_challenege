DIM_ACCOUNTS_RENAMES = {
    "customer_ref": "customer_id",
}


DIM_CUSTOMERS_COLUMNS = [
    "customer_sk",
    "customer_id",
    "gender",
    "province",
    "income_band",
    "segment",
    "risk_score",
    "kyc_status",
    "age_band",
]


DIM_ACCOUNTS_COLUMNS = [
    "account_sk",
    "account_id",
    "customer_id",
    "account_type",
    "account_status",
    "open_date",
    "product_tier",
    "digital_channel",
    "credit_limit",
    "current_balance",
    "last_activity_date",
]


FACT_TRANSACTIONS_COLUMNS = [
    "transaction_sk",
    "transaction_id",
    "account_sk",
    "customer_sk",
    "transaction_date",
    "transaction_timestamp",
    "transaction_type",
    "merchant_category",
    "merchant_subcategory",
    "amount",
    "currency",
    "channel",
    "province",
    "dq_flag",
    "ingestion_timestamp",
]
