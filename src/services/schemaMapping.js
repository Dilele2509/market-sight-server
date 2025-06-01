const SCHEMA_MAPPINGS = {
    customers: {
        required_fields: [
            "customer_id",
            "first_name",
            "last_name",
            "email",
            "phone",
            "gender",
            "birth_date",
            "registration_date",
            "address",
            "city",
            "business_id"
        ]
    },
    product_lines: {
        required_fields: [
            "product_line_id",
            "name",
            "category",
            "subcategory",
            "brand",
            "unit_cost"
        ]
    },
    stores: {
        required_fields: [
            "store_id",
            "store_name",
            "address",
            "city",
            "store_type",
            "opening_date",
            "region",
            "business_id"
        ]
    },
    transactions: {
        required_fields: [
            "transaction_id",
            "customer_id",
            "store_id",
            "transaction_date",
            "total_amount",
            "payment_method",
            "product_line_id",
            "quantity",
            "unit_price",
            "business_id"
        ]
    }
};

export{
    SCHEMA_MAPPINGS
}