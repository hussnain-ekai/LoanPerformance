{{ config(
    materialized='table',
    schema='data_mart',
    description='Monitor loan repayment behavior, delinquency trends, and portfolio risk to optimize recovery strategies and improve financial performance.'
) }}

WITH loan_accounts AS (
    SELECT
        LOANID,
        CUSTOMERID,
        OUTSTANDINGAMOUNT,
        PRINCIPALAMOUNT,
        LOANSTATUS
    FROM {{ source('core', 'FISERV_LOAN_ACCOUNTS') }}
),
payments AS (
    SELECT
        LOANID,
        PAYMENTMETHOD,
        AMOUNT
    FROM {{ source('core', 'FISERV_PAYMENTS') }}
),
customers AS (
    SELECT
        CUSTOMERID
    FROM {{ source('core', 'FISERV_CUSTOMERS') }}
)

SELECT
    loan_accounts.LOANID,
    SUM(loan_accounts.OUTSTANDINGAMOUNT) AS total_outstanding_loan_amount,
    COUNT(CASE WHEN loan_accounts.LOANSTATUS = 'Delinquent' THEN loan_accounts.LOANID END) AS monthly_delinquent_loans,
    SUM(CASE WHEN payments.PAYMENTMETHOD = 'Principal' THEN payments.AMOUNT ELSE 0 END) AS total_principal_paid,
    SUM(CASE WHEN payments.PAYMENTMETHOD = 'Interest' THEN payments.AMOUNT ELSE 0 END) AS total_interest_paid,
    AVG(loan_accounts.OUTSTANDINGAMOUNT / NULLIF(loan_accounts.PRINCIPALAMOUNT, 0)) AS average_recovery_rate
FROM loan_accounts
LEFT JOIN payments ON loan_accounts.LOANID = payments.LOANID
LEFT JOIN customers ON loan_accounts.CUSTOMERID = customers.CUSTOMERID
GROUP BY loan_accounts.LOANID