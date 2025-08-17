# Data Cleaning in SQL

- Synthetic Data = Artificial Data
- Techniques
    1. Standardize the column data such as status
        1. Using case when lower() like then
    2. Remove the strings from numeric column such as quantity
        1. For string word, using case when lower() like then
        2. For other numbers, cast() int64
    3. Keep the string column such as name in proper format
        1. Using initcap()
    4. Remove Duplicates
        1. Using row_number() over (partition by  lower(email) order by order_id)