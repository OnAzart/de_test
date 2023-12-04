SELECT orders.*, transactions.*, verification.*
FROM orders
LEFT JOIN transactions on orders.id=transactions.order_id
LEFT JOIN verification on transactions.id=verification.transaction_id 
WHERE (uploaded_at is Null
OR updated_at >= uploaded_at)
AND orders.id is not null
AND transactions.id is not null
AND verification.id is not null
;
