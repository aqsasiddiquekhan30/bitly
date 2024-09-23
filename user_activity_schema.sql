CREATE TABLE user_activity (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT,
    activity_type VARCHAR(255), 
    timestamp datetime, 
    total_activity_count int,
    load_date DATETIME DEFAULT CURRENT_TIMESTAMP
    
);

